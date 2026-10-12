// Creates or updates the ETL events through the Cronicle API at every container
// start. Idempotent: fixed event ids, and every field we own is sent again.
'use strict';
const fs = require('fs');

const PORT = process.env.CRONICLE_WebServer__http_port || '3012';
const BASE = `http://127.0.0.1:${PORT}/api/app`;
const MARKER = '/tmp/etl-provisioned'; // read by the compose healthcheck
const GRACE_MINUTES = 1; // the publisher's fixed grace after a window end (window.PublishGrace)

// parseGoDuration accepts Go's time.ParseDuration syntax (5m, 1h, 90s, 1m30s)
// and returns milliseconds.
function parseGoDuration(raw) {
  const unit = { ns: 1e-6, us: 1e-3, 'µs': 1e-3, ms: 1, s: 1e3, m: 6e4, h: 3.6e6 };
  if (typeof raw !== 'string') throw new Error(`invalid duration ${JSON.stringify(raw)}`);
  // Go accepts one leading '+'. A '-' is rejected here, which is the same
  // outcome as in Go for ETL_WINDOW: no window is negative.
  const s = raw.startsWith('+') ? raw.slice(1) : raw;
  if (s === '') throw new Error(`invalid duration ${JSON.stringify(raw)}`);
  if (s === '0') return 0;
  const re = /(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|ms|s|m|h)/gy;
  let ms = 0, pos = 0, m;
  while ((m = re.exec(s)) !== null) { ms += parseFloat(m[1]) * unit[m[2]]; pos = re.lastIndex; }
  if (pos !== s.length) throw new Error(`invalid duration ${JSON.stringify(raw)}`);
  return ms;
}

// windowMinutes applies the ETL_WINDOW rule of internal/window: whole minutes,
// 1m to 60m, and a divisor of 60m.
function windowMinutes(raw) {
  const min = parseGoDuration(raw) / 6e4;
  if (!Number.isInteger(min) || min < 1 || min > 60 || 60 % min !== 0) {
    throw new Error(`ETL_WINDOW=${raw}: must be whole minutes, between 1m and 60m, and divide 60m`);
  }
  return min;
}

// publishMinutes lists the minutes of the hour at which the publisher runs:
// every window end (aligned to the hour) plus the grace.
function publishMinutes(n) {
  const out = [];
  for (let m = 0; m < 60; m++) if (m % n === GRACE_MINUTES % n) out.push(m);
  return out;
}

function shell(cmd) {
  return { script: `#!/bin/sh\nexec ${cmd}\n`, annotate: 0, json: 0 };
}

// timeZone returns tz if it is an IANA time zone name the runtime knows.
function timeZone(tz) {
  try { new Intl.DateTimeFormat('en-US', { timeZone: tz }); } catch (_) {
    throw new Error(`ETL_SCHEDULE_TZ=${tz}: not an IANA time zone name`);
  }
  return tz;
}

// events returns the desired events for the given environment.
function events(env) {
  const n = windowMinutes(env.ETL_WINDOW || '5m');
  const tz = timeZone(env.ETL_SCHEDULE_TZ || 'UTC');
  const common = {
    enabled: 1, category: 'general', target: 'maingrp', plugin: 'shellplug',
    max_children: 1, catch_up: 1, multiplex: 0, detached: 0, queue: 0,
    retries: 3, retry_delay: 60,
    notify_success: '', notify_fail: '', web_hook: env.CRONICLE_WEB_HOOK || '',
  };
  return [
    { ...common, id: 'etlpublish', title: 'etl-publish',
      notes: `Publishes the ${n}-minute window that ended ${GRACE_MINUTES} minute before the job time ($JOB_NOW).`,
      params: shell('etl publish --now "$JOB_NOW"'),
      timing: { minutes: publishMinutes(n) }, timezone: 'UTC', timeout: 300 },
    { ...common, id: 'etlretention', title: 'etl-retention', category: 'maintenance',
      notes: 'Drops pipeline.dedup_claims partitions and written consumer batches older than ETL_CLAIMS_RETENTION_DAYS.',
      params: shell('etl maintenance retention'),
      timing: { hours: [11], minutes: [0] }, timezone: tz, timeout: 1800 },
    { ...common, id: 'etlvacuum', title: 'etl-vacuum', category: 'maintenance',
      notes: 'VACUUM (ANALYZE) of the tables the pipeline updates.',
      params: shell('etl maintenance vacuum'),
      timing: { hours: [11], minutes: [30] }, timezone: tz, timeout: 1800 },
  ];
}

async function api(name, body) {
  const res = await fetch(`${BASE}/${name}/v1`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': process.env.CRONICLE_API_KEY || '' },
    body: JSON.stringify(body),
  });
  return res.json();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// schedule waits until this server is primary and returns the existing events.
async function schedule(timeoutMs) {
  const until = Date.now() + timeoutMs;
  let last = '';
  while (Date.now() < until) {
    try {
      const r = await api('get_schedule', { offset: 0, limit: 1000 });
      if (r.code === 0) return r.rows || [];
      last = JSON.stringify(r);
    } catch (e) { last = String(e); }
    await sleep(1000);
  }
  throw new Error('Cronicle API not ready: ' + last);
}

async function main() {
  try { fs.unlinkSync(MARKER); } catch (_) { /* first start */ }
  const wanted = events(process.env); // validates ETL_WINDOW before any call
  const existing = new Set((await schedule(120000)).map((e) => e.id));
  for (const ev of wanted) {
    const verb = existing.has(ev.id) ? 'update' : 'create';
    const r = await api(`${verb}_event`, ev);
    if (r.code !== 0) throw new Error(`${verb}_event ${ev.id}: ${JSON.stringify(r)}`);
    console.log(`provision: ${verb}d ${ev.title} timing=${JSON.stringify(ev.timing)} timezone=${ev.timezone}`);
  }
  fs.writeFileSync(MARKER, new Date().toISOString() + '\n');
}

if (require.main === module) {
  main().catch((e) => {
    console.error('provision: FAILED: ' + e.message);
    const cronicle = Number(process.argv[2]);
    if (cronicle > 0) { try { process.kill(cronicle, 'SIGTERM'); } catch (_) { /* gone */ } }
    process.exit(1);
  });
}

module.exports = { parseGoDuration, windowMinutes, publishMinutes, timeZone, events };
