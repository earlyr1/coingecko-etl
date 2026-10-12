// node --test deploy/cronicle/  (make cronicle-test runs it in the Node image)
'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {
  goTrimSpace, parseGoDuration, windowMinutes, publishMinutes, zoneValidator, timeZone, events,
} = require('./provision.js');

const NS = { us: 1000n, s: 1000000000n, m: 60000000000n, h: 3600000000000n };

test('goTrimSpace trims what Go strings.TrimSpace trims, and nothing else', () => {
  assert.equal(goTrimSpace(' \t\n\v\f\r5m\r\n '), '5m');
  // unicode.IsSpace beyond ASCII: NEL, NBSP, ogham, en quad..hair, LS, PS, NNBSP, MMSP, ideographic.
  for (const c of ['\u0085', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', '　']) {
    assert.equal(goTrimSpace(`${c}5m${c}`), '5m', `U+${c.codePointAt(0).toString(16)}`);
  }
  // Not spaces in Go (JavaScript's trim() would strip U+FEFF).
  for (const c of ['﻿', '​', '᠎']) assert.equal(goTrimSpace(`${c}5m`), `${c}5m`);
  assert.equal(goTrimSpace('5 m'), '5 m');
});

test('parseGoDuration follows time.ParseDuration', () => {
  assert.equal(parseGoDuration('5m'), 5n * NS.m);
  assert.equal(parseGoDuration('1h'), NS.h);
  assert.equal(parseGoDuration('1m30s'), 90n * NS.s);
  assert.equal(parseGoDuration('300s'), 300n * NS.s);
  assert.equal(parseGoDuration('+5m'), 5n * NS.m); // Go accepts one sign
  assert.equal(parseGoDuration('-5m'), -5n * NS.m);
  assert.equal(parseGoDuration('0'), 0n);
  assert.equal(parseGoDuration('-0'), 0n);
  assert.equal(parseGoDuration('.5m'), 30n * NS.s);
  assert.equal(parseGoDuration('5.m'), 5n * NS.m);
  assert.equal(parseGoDuration('0.1h'), 6n * NS.m); // Go's float step for fractions
  assert.equal(parseGoDuration('60000000us'), NS.m);
  assert.equal(parseGoDuration('60000000µs'), NS.m); // micro sign
  assert.equal(parseGoDuration('60000000μs'), NS.m); // Greek mu
  assert.equal(parseGoDuration('9223372036854775807ns'), (1n << 63n) - 1n);
  for (const bad of ['', '+', '-', '5', 'm', '.m', '5 m', ' 5m', '5m ', '5min', '5M', '5mm', '1d',
    '5constructor', '9223372036854775808ns', '2562048h', '--5m', '+-5m']) {
    assert.throws(() => parseGoDuration(bad), undefined, JSON.stringify(bad));
  }
});

test('windowMinutes accepts exactly the divisors of an hour', () => {
  const ok = [1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60]; // the same table as window.TestValidate
  for (let m = 1; m <= 60; m++) {
    if (ok.includes(m)) assert.equal(windowMinutes(`${m}m`), m);
    else assert.throws(() => windowMinutes(`${m}m`), undefined, `${m}m`);
  }
  for (const bad of ['0', '0s', '-5m', '30s', '90s', '61m', '2h', '5m30s', '1.5m']) {
    assert.throws(() => windowMinutes(bad), undefined, bad);
  }
  assert.equal(windowMinutes('300s'), 5);
  assert.equal(windowMinutes('1h'), 60);
  assert.equal(windowMinutes('0.25h'), 15);
});

test('publishMinutes is every window end plus one minute', () => {
  assert.deepEqual(publishMinutes(5), [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56]);
  assert.deepEqual(publishMinutes(1), Array.from({ length: 60 }, (_, i) => i));
  assert.deepEqual(publishMinutes(15), [1, 16, 31, 46]);
  assert.deepEqual(publishMinutes(60), [1]);
});

test('events: the etl-publish timing follows ETL_WINDOW', () => {
  const timing = (w) => events({ ETL_WINDOW: w }).find((e) => e.id === 'etlpublish').timing;
  // Literal minute lists: a timing without minutes runs every minute in Cronicle.
  assert.deepEqual(timing('1m'), { minutes: Array.from({ length: 60 }, (_, i) => i) });
  assert.deepEqual(timing('5m'), { minutes: [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56] });
  assert.deepEqual(timing('15m'), { minutes: [1, 16, 31, 46] });
  assert.deepEqual(timing('60m'), { minutes: [1] });
  for (const w of ['1m', '5m', '15m', '60m']) assert.equal('hours' in timing(w), false, w);
});

test('events: ETL_WINDOW is read as Go reads it', () => {
  const timing = (w) => events({ ETL_WINDOW: w }).find((e) => e.id === 'etlpublish').timing;
  const five = { minutes: [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56] };
  for (const blank of [undefined, '', '  ', '\t\n']) assert.deepEqual(timing(blank), five, JSON.stringify(blank));
  for (const padded of [' 5m', '5m ', '\t5m\n', ' 5m']) assert.deepEqual(timing(padded), five, JSON.stringify(padded));
  assert.deepEqual(timing('60000000μs'), { minutes: Array.from({ length: 60 }, (_, i) => i) });
  assert.deepEqual(timing('60000000µs'), { minutes: Array.from({ length: 60 }, (_, i) => i) });
  for (const bad of ['7m', '5 m', '﻿5m', '-5m']) assert.throws(() => events({ ETL_WINDOW: bad }), undefined, bad);
});

test('events: publish in UTC every window, maintenance daily in ETL_SCHEDULE_TZ', () => {
  const [pub, ret, vac] = events({ ETL_WINDOW: '5m', ETL_SCHEDULE_TZ: 'Europe/Moscow' });
  assert.equal(pub.id, 'etlpublish');
  assert.equal(pub.params.script, '#!/bin/sh\nexec etl publish --now "$JOB_NOW"\n');
  assert.equal(pub.timezone, 'UTC');
  assert.deepEqual([pub.catch_up, pub.max_children, pub.retries, pub.retry_delay, pub.timeout], [1, 1, 3, 60, 300]);
  assert.deepEqual([ret.timing, ret.timezone], [{ hours: [11], minutes: [0] }, 'Europe/Moscow']);
  assert.deepEqual([vac.timing, vac.timezone], [{ hours: [11], minutes: [30] }, 'Europe/Moscow']);
  assert.deepEqual([pub.category, ret.category, vac.category], ['general', 'maintenance', 'maintenance']);
  for (const blank of [undefined, '', '   ']) {
    assert.equal(events({ ETL_SCHEDULE_TZ: blank }).find((e) => e.id === 'etlretention').timezone, 'UTC');
  }
  assert.equal(events({ ETL_SCHEDULE_TZ: ' Europe/Moscow\n' }).find((e) => e.id === 'etlvacuum').timezone, 'Europe/Moscow');
});

test('timeZone accepts the names the scheduler knows, never an offset', () => {
  assert.equal(timeZone('UTC'), 'UTC');
  assert.equal(timeZone('Europe/Moscow'), 'Europe/Moscow');
  assert.throws(() => timeZone('Mars/Olympus'));
  assert.throws(() => events({ ETL_SCHEDULE_TZ: 'nowhere' }));
  // Node's Intl takes offsets; Cronicle's moment-timezone does not.
  for (const offset of ['+03:00', '-05:00', '+0300', '+03']) {
    assert.throws(() => timeZone(offset), /not an IANA time zone name/, offset);
    assert.throws(() => timeZone(offset, () => true), /not an IANA time zone name/, offset);
  }
  // Cronicle v0.9.135 bundles tz data 2021e: a newer name passes Intl but has
  // no zone in moment-timezone, so the scheduler would run the job in UTC.
  const tz2021e = (name) => name !== 'Europe/Kyiv';
  assert.throws(() => timeZone('Europe/Kyiv', tz2021e), /ETL_SCHEDULE_TZ=Europe\/Kyiv/);
  assert.equal(timeZone('Europe/Kiev', tz2021e), 'Europe/Kiev');
  assert.throws(() => events({ ETL_SCHEDULE_TZ: 'Europe/Kyiv' }, { isZone: tz2021e }), /Europe\/Kyiv/);
  assert.equal(events({ ETL_SCHEDULE_TZ: 'Europe/Kiev' }, { isZone: tz2021e })[1].timezone, 'Europe/Kiev');
});

test('zoneValidator uses Cronicle\'s moment-timezone when it is installed, Intl otherwise', () => {
  const asked = [];
  const moment = { tz: { zone: (name) => { asked.push(name); return name === 'Europe/Kiev' ? {} : null; } } };
  const cronicle = zoneValidator(() => moment);
  assert.equal(cronicle('Europe/Kiev'), true);
  assert.equal(cronicle('Europe/Kyiv'), false);
  assert.deepEqual(asked, ['Europe/Kiev', 'Europe/Kyiv']);

  const missing = Object.assign(new Error('Cannot find module'), { code: 'MODULE_NOT_FOUND' });
  const intl = zoneValidator(() => { throw missing; });
  assert.equal(intl('Europe/Kyiv'), true); // Node 24 knows it
  assert.equal(intl('Mars/Olympus'), false);
  // Any other failure to load the library is not hidden behind the fallback.
  assert.throws(() => zoneValidator(() => { throw new SyntaxError('broken'); }), /broken/);
});
