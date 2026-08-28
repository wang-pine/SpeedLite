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
