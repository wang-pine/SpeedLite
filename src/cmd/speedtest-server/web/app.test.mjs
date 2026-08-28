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
const context = {
  window,
  document,
  performance,
  console,
  URLSearchParams,
  setInterval,
  clearInterval,
  setTimeout,
  clearTimeout,
};

vm.runInNewContext(source, context, { filename: 'app.js' });

const core = window.__speedTestCore;
const MiB = 1024 * 1024;
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

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

test('TCP upload starts on server start and completes only on result', async (t) => {
  class FakeWebSocket {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSED = 3;
    static instances = [];

    constructor(url) {
      this.url = url;
      this.readyState = FakeWebSocket.CONNECTING;
      this.bufferedAmount = 0;
      this.sent = [];
      FakeWebSocket.instances.push(this);
    }

    send(data) {
      this.sent.push(data);
      if (data && typeof data.byteLength === 'number') this.bufferedAmount += data.byteLength;
    }

    close() {
      if (this.readyState === FakeWebSocket.CLOSED) return;
      this.readyState = FakeWebSocket.CLOSED;
      if (this.onclose) this.onclose({ code: 1000 });
    }
  }
  context.WebSocket = FakeWebSocket;

  const done = [];
  core.runTCPStream({
    dir: 'up',
    streams: 1,
    duration: 0.01,
    packetLen: 1024,
    wsBase: 'ws://test.invalid',
  }, 'tcp-test', (err, result) => done.push({ err, result }));

  const ws = FakeWebSocket.instances[0];
  t.after(() => {
    if (ws.readyState !== FakeWebSocket.CLOSED) {
      ws.onmessage({ data: JSON.stringify({
        type: 'result',
        result: { total_bytes: 0, duration: 0.01, avg_mbitps: 0, peak_mbitps: 0 },
      }) });
      ws.close();
    }
  });

  ws.readyState = FakeWebSocket.OPEN;
  ws.onopen();
  await wait(5);
  assert.equal(ws.sent.length, 0, 'open alone must not start upload traffic');

  ws.onmessage({ data: JSON.stringify({ type: 'start' }) });
  await wait(5);
  assert.ok(
    ws.sent.some((item) => typeof item !== 'string' && typeof item.byteLength === 'number'),
    'start must begin binary traffic',
  );

  await wait(20);
  const stop = ws.sent.find((item) => typeof item === 'string' && JSON.parse(item).type === 'stop');
  assert.ok(stop, 'duration boundary must enqueue an ordered stop message');
  assert.equal(done.length, 0, 'stop alone must not complete a successful stream');

  ws.bufferedAmount = 0;
  ws.onmessage({ data: JSON.stringify({
    type: 'result',
    result: { total_bytes: 16 * MiB, duration: 0.01, avg_mbitps: 0, peak_mbitps: 0 },
  }) });
  await wait(0);
  assert.equal(done.length, 1);
  assert.equal(done[0].err, null);
  assert.ok(done[0].result.srv);
});

test('waitForBufferedDrain returns zero on drain and residual on timeout', async () => {
  const values = [10, 4, 0];
  let index = 0;
  const draining = {
    get bufferedAmount() {
      const value = values[index];
      if (index < values.length - 1) index++;
      return value;
    },
  };
  assert.equal(await core.waitForBufferedDrain(draining, 50, 1), 0);
  assert.equal(await core.waitForBufferedDrain({ bufferedAmount: 7 }, 2, 1), 7);
});

test('UDP upload drains before signaling stop and requires server result', async (t) => {
  class FakeDataChannel {
    constructor() {
      this.readyState = 'connecting';
      this.bufferedAmount = 0;
      this.sent = [];
    }

    send(data) {
      this.sent.push(data);
      this.bufferedAmount += data.byteLength || 0;
    }

    close() {
      if (this.readyState === 'closed') return;
      this.readyState = 'closed';
      if (this.onclose) this.onclose();
    }
  }

  class FakePeerConnection {
    static instances = [];

    constructor() {
      this.dc = new FakeDataChannel();
      FakePeerConnection.instances.push(this);
    }

    createDataChannel() { return this.dc; }
    async createOffer() { return { type: 'offer', sdp: 'offer-sdp' }; }
    async setLocalDescription(offer) { this.localDescription = offer; }
    async setRemoteDescription(answer) { this.remoteDescription = answer; }
    close() { this.closed = true; }
  }

  class FakeSignal {
    static OPEN = 1;
    static CLOSED = 3;
    static instances = [];

    constructor(url) {
      this.url = url;
      this.readyState = FakeSignal.OPEN;
      this.sent = [];
      FakeSignal.instances.push(this);
    }

    send(data) { this.sent.push(data); }
    close() {
      if (this.readyState === FakeSignal.CLOSED) return;
      this.readyState = FakeSignal.CLOSED;
      if (this.onclose) this.onclose({ code: 1000 });
    }
  }

  context.RTCPeerConnection = FakePeerConnection;
  context.WebSocket = FakeSignal;
  const done = [];
  core.runUDPStream({
    dir: 'up',
    duration: 0.01,
    packetLen: 1024,
    wsBase: 'ws://test.invalid',
  }, 'udp-test', (err, result) => done.push({ err, result }));

  const pc = FakePeerConnection.instances[0];
  const dc = pc.dc;
  const signal = FakeSignal.instances[0];
  t.after(() => {
    if (signal.readyState !== FakeSignal.CLOSED) {
      signal.onmessage({ data: JSON.stringify({
        type: 'result',
        result: { total_bytes: 8 * MiB, up_bytes: 8 * MiB, duration: 0.01 },
      }) });
      signal.close();
    }
  });

  await signal.onopen();
  await wait(0);
  signal.onmessage({ data: JSON.stringify({ type: 'answer', sdp: 'answer-sdp' }) });
  await wait(0);
  dc.readyState = 'open';
  dc.onopen();
  await wait(5);
  assert.ok(dc.sent.length > 0, 'open DataChannel must begin upload traffic');

  await wait(10);
  dc.bufferedAmount = 0;
  await wait(10);
  const stop = signal.sent
    .map((item) => JSON.parse(item))
    .find((item) => item.type === 'stop');
  assert.ok(stop, 'drained upload must send stop over reliable signaling');
  assert.equal(stop.queued_bytes, 0);
  assert.equal(done.length, 0, 'stop alone must not complete a successful stream');

  signal.onmessage({ data: JSON.stringify({
    type: 'result',
    result: { total_bytes: 8 * MiB, up_bytes: 8 * MiB, duration: 0.01 },
  }) });
  await wait(0);
  assert.equal(done.length, 1);
  assert.equal(done[0].err, null);
  assert.equal(done[0].result.queuedBytes, 0);
});

test('result rendering distinguishes TCP reconciliation from UDP delivery and queue truncation', () => {
  const rows = [];
  const tbody = {
    appendChild(element) { rows.push(element); },
    querySelectorAll() { return rows; },
  };
  document.querySelector = (selector) => {
    assert.equal(selector, '#resultTable tbody');
    return tbody;
  };
  document.createElement = () => ({ className: '', dataset: {}, innerHTML: '' });

  const base = {
    avgMbitps: 8,
    peakMbitps: 9,
    totalBytes: MiB,
    duration: 1,
    streams: 1,
    upBytes: 0,
    downBytes: MiB,
    downMbps: 8,
    upMbps: 0,
    jitter: 0,
    lossPct: 0,
    srvBytes: MiB,
    srvAvg: 8,
    srvPeak: 9,
  };

  core.addResultRow('tcp', 'down', { ...base, devPct: 1.2 });
  assert.match(rows.map((row) => row.innerHTML).join('\n'), /对账异常/);

  rows.length = 0;
  core.addResultRow('udp', 'down', {
    ...base,
    devPct: 16.7,
    lossPct: 16.7,
    truncated: true,
    srvSubmittedBytes: 10 * MiB,
    srvQueuedBytes: 4 * MiB,
  });
  const html = rows.map((row) => row.innerHTML).join('\n');
  assert.match(html, /交付损失/);
  assert.match(html, /队列截断/);
  assert.match(html, /data-label="抖动">—</);
});

test('peakFromPoints uses synchronized aggregate samples instead of per-stream maxima', () => {
  const points = [
    { t: 0.0, down: 10, up: 1 },
    { t: 0.5, down: 20, up: 2 },
    { t: 1.0, down: 30, up: 3 },
    { t: 2.1, down: 8, up: 4 },
  ];
  assert.equal(core.peakFromPoints(points, 'down'), 20);
  assert.equal(core.peakFromPoints(points, 'up'), 4);
  assert.equal(core.peakFromPoints(points, 'both'), 22);
});

test('aggregate marks partial parallel-stream completion as incomplete', () => {
  const result = core.aggregate([
    {
      err: null,
      res: {
        totalBytes: MiB,
        duration: 1,
        peakMbitps: 1,
        upBytes: 0,
        downBytes: MiB,
      },
    },
    { err: new Error('stream failed'), res: null },
  ], 'down', 1);
  assert.equal(result.streams, 1);
  assert.equal(result.failedStreams, 1);
  assert.equal(result.incomplete, true);
});

test('upload detail row labels uploaded bytes instead of downlink zero', () => {
  const rows = [];
  const tbody = { appendChild(element) { rows.push(element); } };
  document.querySelector = () => tbody;
  document.createElement = () => ({ className: '', dataset: {}, innerHTML: '' });
  core.addResultRow('tcp', 'up', {
    avgMbitps: 8,
    peakMbitps: 8,
    totalBytes: MiB,
    duration: 1,
    streams: 1,
    upBytes: MiB,
    downBytes: 0,
    downMbps: 0,
    upMbps: 8,
    lossPct: 0,
    devPct: 0,
    srvBytes: MiB,
    srvAvg: 8,
    srvPeak: 8,
  });
  const html = rows.map((row) => row.innerHTML).join('\n');
  assert.match(html, /上行 1\.00 MiB/);
  assert.doesNotMatch(html, /下行 0 B/);
});
