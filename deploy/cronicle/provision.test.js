// node --test deploy/cronicle/  (make cronicle-test runs it in the Node image)
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseGoDuration, windowMinutes, publishMinutes, timeZone, events } = require('./provision.js');

test('parseGoDuration follows Go syntax', () => {
  assert.equal(parseGoDuration('5m'), 300000);
  assert.equal(parseGoDuration('1h'), 3600000);
  assert.equal(parseGoDuration('1m30s'), 90000);
  assert.equal(parseGoDuration('300s'), 300000);
  assert.equal(parseGoDuration('+5m'), 300000); // time.ParseDuration accepts the sign too
  for (const bad of ['', '+', '5', 'm', '5 m', '5min', '-5m']) assert.throws(() => parseGoDuration(bad), bad);
});

test('windowMinutes accepts exactly the divisors of an hour', () => {
  const ok = [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60]; // the same table as window.TestValidate
  for (let m = 1; m <= 60; m++) {
    if (ok.includes(m)) assert.equal(windowMinutes(`${m}m`), m);
    else assert.throws(() => windowMinutes(`${m}m`), `${m}m`);
  }
  for (const bad of ['0s', '30s', '90s', '61m', '2h', '5m30s']) assert.throws(() => windowMinutes(bad), bad);
  assert.equal(windowMinutes('300s'), 5);
  assert.equal(windowMinutes('1h'), 60);
});

test('publishMinutes is every window end plus one minute', () => {
  assert.deepEqual(publishMinutes(5), [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56]);
  assert.deepEqual(publishMinutes(1), Array.from({ length: 60 }, (_, i) => i));
  assert.deepEqual(publishMinutes(15), [1, 16, 31, 46]);
  assert.deepEqual(publishMinutes(60), [1]);
});

test('events: publish in UTC every window, maintenance daily in ETL_SCHEDULE_TZ', () => {
  const [pub, ret, vac] = events({ ETL_WINDOW: '5m', ETL_SCHEDULE_TZ: 'Europe/Moscow' });
  assert.equal(pub.id, 'etlpublish');
  assert.equal(pub.params.script, '#!/bin/sh\nexec etl publish --now "$JOB_NOW"\n');
  assert.equal(pub.timezone, 'UTC');
  assert.deepEqual([pub.catch_up, pub.max_children, pub.retries, pub.retry_delay, pub.timeout], [1, 1, 3, 60, 300]);
  assert.deepEqual([ret.timing, ret.timezone], [{ hours: [11], minutes: [0] }, 'Europe/Moscow']);
  assert.deepEqual([vac.timing, vac.timezone], [{ hours: [11], minutes: [30] }, 'Europe/Moscow']);
  assert.equal(events({}).find((e) => e.id === 'etlretention').timezone, 'UTC');
  assert.throws(() => events({ ETL_WINDOW: '7m' }));
  assert.deepEqual([pub.category, ret.category, vac.category], ['general', 'maintenance', 'maintenance']);
});

test('timeZone accepts IANA names only', () => {
  assert.equal(timeZone('UTC'), 'UTC');
  assert.equal(timeZone('Europe/Moscow'), 'Europe/Moscow');
  assert.throws(() => timeZone('Mars/Olympus'));
  assert.throws(() => events({ ETL_SCHEDULE_TZ: 'nowhere' }));
});
