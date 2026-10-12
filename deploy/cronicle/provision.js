// Creates or updates the ETL events through the Cronicle API at every container
// start. Idempotent: fixed event ids, and every field we own is sent again.
'use strict';
const fs = require('fs');

const PORT = process.env.CRONICLE_WebServer__http_port || '3012';
const BASE = `http://127.0.0.1:${PORT}/api/app`;
const MARKER = '/tmp/etl-provisioned'; // read by the compose healthcheck
const GRACE_MINUTES = 1; // the publisher's fixed grace after a window end (window.PublishGrace)

// The exporter and this script must agree on ETL_WINDOW, so it is read the
// way internal/config reads it: strings.TrimSpace, a blank value is the
// default, then time.ParseDuration and window.Validate.

// goTrimSpace is strings.TrimSpace: it strips the runes unicode.IsSpace
// reports (String.prototype.trim differs: it strips U+FEFF but not U+0085).
const GO_SPACE = '\\t\\n\\v\\f\\r \\u0085\\u00a0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000';
const GO_TRIM = new RegExp(`^[${GO_SPACE}]+|[${GO_SPACE}]+$`, 'g');
function goTrimSpace(s) {
  return String(s ?? '').replace(GO_TRIM, '');
}

// parseGoDuration is a port of Go's time.ParseDuration (same syntax, units,
// overflow checks and float step for fractions) and returns nanoseconds as a
// BigInt.
const MAX_DURATION = 1n << 63n;
const UNITS = new Map([
  ['ns', 1n], ['us', 1000n], ['µs', 1000n], ['μs', 1000n], // micro sign, Greek mu
  ['ms', 1000000n], ['s', 1000000000n], ['m', 60000000000n], ['h', 3600000000000n],
]);
function parseGoDuration(orig) {
  const invalid = (why = 'invalid duration') => new Error(`${why} ${JSON.stringify(orig)}`);
  if (typeof orig !== 'string') throw invalid();
  const isDigit = (c) => c >= '0' && c <= '9';
  let s = orig, neg = false, d = 0n;
  if (s !== '' && (s[0] === '-' || s[0] === '+')) { neg = s[0] === '-'; s = s.slice(1); }
  if (s === '0') return 0n;
  if (s === '') throw invalid();
  while (s !== '') {
    if (!(s[0] === '.' || isDigit(s[0]))) throw invalid();
    let i = 0, v = 0n; // leadingInt
    for (; i < s.length && isDigit(s[i]); i++) {
      if (v > MAX_DURATION / 10n) throw invalid();
      v = v * 10n + BigInt(s[i]);
      if (v > MAX_DURATION) throw invalid();
    }
    const pre = i > 0;
    s = s.slice(i);
    let f = 0n, scale = 1, post = false;
    if (s !== '' && s[0] === '.') { // leadingFraction: stops gaining precision on overflow
      s = s.slice(1);
      let j = 0, overflow = false;
      for (; j < s.length && isDigit(s[j]); j++) {
        if (overflow) continue;
        if (f > (MAX_DURATION - 1n) / 10n) { overflow = true; continue; }
        const y = f * 10n + BigInt(s[j]);
        if (y > MAX_DURATION) { overflow = true; continue; }
        f = y;
        scale *= 10;
      }
      post = j > 0;
      s = s.slice(j);
    }
    if (!pre && !post) throw invalid();
    let k = 0;
    while (k < s.length && !(s[k] === '.' || isDigit(s[k]))) k++;
    if (k === 0) throw invalid('missing unit in duration');
    const unit = UNITS.get(s.slice(0, k));
    if (unit === undefined) throw invalid(`unknown unit ${JSON.stringify(s.slice(0, k))} in duration`);
    s = s.slice(k);
    if (v > MAX_DURATION / unit) throw invalid();
    v *= unit;
    if (f > 0n) {
      v += BigInt(Math.trunc(Number(f) * (Number(unit) / scale)));
      if (v > MAX_DURATION) throw invalid();
    }
    d += v;
    if (d > MAX_DURATION) throw invalid();
  }
  if (neg) return -d;
  if (d > MAX_DURATION - 1n) throw invalid();
  return d;
}

// windowMinutes applies window.Validate to a trimmed, non-blank ETL_WINDOW:
// whole minutes, 1m to 60m, and a divisor of 60m.
function windowMinutes(raw) {
  const min = 60000000000n, hour = 60n * min;
  let size;
  try { size = parseGoDuration(raw); } catch (e) {
    throw new Error(`ETL_WINDOW=${raw}: not a Go duration (examples: 1m, 5m, 15m): ${e.message}`);
  }
  if (size < min || size > hour || size % min !== 0n || hour % size !== 0n) {
    throw new Error(`ETL_WINDOW=${raw}: must be whole minutes, between 1m and 60m, and divide 60m`);
  }
  return Number(size / min);
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

// zoneValidator returns a check for time zone names. Cronicle schedules with
// its bundled moment-timezone (tz data 2021e in v0.9.135), which has no zone
// for newer names such as Europe/Kyiv and then schedules in UTC with only a
// console warning, so inside the Cronicle image that library decides. The plain Node image of
// make cronicle-test has no Cronicle: Intl decides there.
const CRONICLE_MOMENT = '/opt/cronicle/node_modules/moment-timezone';
function zoneValidator(load = () => require(CRONICLE_MOMENT)) {
  let moment;
  try { moment = load(); } catch (e) {
    if (e.code !== 'MODULE_NOT_FOUND') throw e;
    return (name) => {
      try { new Intl.DateTimeFormat('en-US', { timeZone: name }); return true; } catch (_) { return false; }
    };
  }
  return (name) => moment.tz.zone(name) !== null;
}

// timeZone returns the trimmed ETL_SCHEDULE_TZ (blank: UTC) if it is a time
// zone name the scheduler knows. Offsets such as +03:00 are refused: Intl
// accepts them, moment-timezone does not.
function timeZone(raw, isZone = zoneValidator()) {
  const tz = goTrimSpace(raw) || 'UTC';
  if (/^[+-]/.test(tz) || !isZone(tz)) {
    throw new Error(`ETL_SCHEDULE_TZ=${tz}: not an IANA time zone name the scheduler knows (Cronicle bundles tz data 2021e)`);
  }
  return tz;
}

// events returns the desired events for the given environment. isZone
// replaces the time zone check in tests.
function events(env, { isZone } = {}) {
  const n = windowMinutes(goTrimSpace(env.ETL_WINDOW) || '5m');
  const tz = timeZone(env.ETL_SCHEDULE_TZ, isZone);
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

// api calls Cronicle with the provisioning API key. Cronicle stores that key
// in every event it creates (event.api_key), and its job_launch_failure web
// hook posts the whole event, so a CRONICLE_WEB_HOOK receiver sees the key. An
// admin login would avoid that, but a password changed in the UI (as the
// README advises) would then fail at every restart, and the sixth failed login
// in an hour locks the admin account (max_failed_logins_per_hour = 5).
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

module.exports = { goTrimSpace, parseGoDuration, windowMinutes, publishMinutes, zoneValidator, timeZone, events };
