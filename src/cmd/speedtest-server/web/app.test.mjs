import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('./app.js', import.meta.url), 'utf8');
const window = {};
const document = {
  addEventListener() {},
  getElementById() { return null; },
};

vm.runInNewContext(source, {
  window,
  document,
  performance,
  console,
  setInterval,
  clearInterval,
  setTimeout,
  clearTimeout,
}, { filename: 'app.js' });

const core = window.__speedTestCore;
const MiB = 1024 * 1024;

test('drainedBytes excludes queued data without underflow', () => {
  assert.ok(Math.abs(core.drainedBytes(10 * MiB, 6.37 * MiB) - 3.63 * MiB) < 1e-6);
  assert.equal(core.drainedBytes(1 * MiB, 2 * MiB), 0);
});

test('aggregate derives average from total bytes and one phase duration', () => {
  const result = core.aggregate([
    {
      err: null,
      res: {
        totalBytes: 2 * MiB,
        duration: 8,
        avgMbitps: 80,
        peakMbitps: 0,
        upBytes: 0,
        downBytes: 2 * MiB,
        upMbps: 0,
        downMbps: 80,
      },
    },
    {
      err: null,
      res: {
        totalBytes: 6 * MiB,
        duration: 8,
        avgMbitps: 160,
        peakMbitps: 0,
        upBytes: 0,
        downBytes: 6 * MiB,
        upMbps: 0,
        downMbps: 160,
      },
    },
  ], 'down', 8);

  const expectedMbps = 8 * MiB * 8 / 8 / 1e6;
  assert.equal(result.totalBytes, 8 * MiB);
  assert.equal(result.duration, 8);
  assert.ok(Math.abs(result.avgMbitps - expectedMbps) < 1e-9);
  assert.notEqual(result.avgMbitps, 240);
});

test('UDP delivery loss excludes server queue residual', () => {
  const result = core.aggregate([{
    err: null,
    res: {
      totalBytes: 5 * MiB,
      duration: 8,
      avgMbitps: 0,
      peakMbitps: 0,
      upBytes: 0,
      downBytes: 5 * MiB,
      srv: {
        total_bytes: 6 * MiB,
        submitted_bytes: 10 * MiB,
        queued_bytes: 4 * MiB,
        avg_mbitps: 0,
        peak_mbitps: 0,
        truncated: true,
      },
    },
  }], 'down', 8);

  assert.ok(Math.abs(result.lossPct - (100 / 6)) < 1e-9);
  assert.equal(result.srvSubmittedBytes, 10 * MiB);
  assert.equal(result.srvQueuedBytes, 4 * MiB);
  assert.equal(result.truncated, true);
});

test('makeStreamResult uses explicit duration and drained upload bytes', () => {
  const result = core.makeStreamResult({
    rx: 0,
    tx: 10 * MiB,
    peakDown: 0,
    peakUp: 9,
    elapsed() { return 99; },
  }, 'up', 8, 10 * MiB, 6.37 * MiB);

  assert.ok(Math.abs(result.totalBytes - 3.63 * MiB) < 1e-6);
  assert.equal(result.duration, 8);
  assert.equal(result.submittedBytes, 10 * MiB);
  assert.equal(result.queuedBytes, 6.37 * MiB);
  assert.equal(result.truncated, true);
});
