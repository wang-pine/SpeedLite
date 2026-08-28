/* SpeedLite 前端逻辑
 *
 * 架构：前端本地采样统计（下行数收到字节、上行数发送字节，每 100ms 采样）。
 * 服务器只负责搬运数据：
 *  - TCP 测速：WebSocket 打流（下行服务器发 / 上行客户端发）
 *  - UDP 测速：WebRTC DataChannel（ordered:false, maxRetransmits:0，真实走 UDP）
 *              信令经 /ws/signal 交换 SDP
 *
 * 说明：
 *  - UDP(WebRTC unreliable) 展示应用层交付损失：排队残留单独报告，不冒充
 *    IP 层 UDP 丢包；网页抖动不可用，精确值请用 CLI speedlite-cli。
 *  - TCP 使用同一 WebSocket 上的有序 start/stop/result 建立共同统计边界。
 *  - 双向(both)方向：下行与上行同时进行，本地分别计数。
 */
(function () {
'use strict';

const $ = (id) => document.getElementById(id);

/* ---------- 全局状态 ---------- */
const state = {
  running: false,
  t0: 0,
  durations: 0,
  phaseText: '',
  series: [],          // [{id, mode, dir, color, points:[{t, down, up}]}] 缓存所有测试曲线
  curSeries: null,      // 当前正在采样的系列
  streams: new Set(),  // 活跃流句柄（ws / pc）
};

/* ---------- 配置 ---------- */
// 包长档位：固定包长（单值） / 动态包长（媒体混合，按轮换序列切换大小包）。
// 数值取常用 MTU/RTP 值：小包 512B、RTP 1200B、类720p 1400B、类1080p 1500B。
const PACKET_PROFILES = {
  fixed: { mode: 'fixed' },
  'dyn-media': { mode: 'dynamic', sizes: [512, 1200, 1400, 1500] }, // 媒体混合：大小包轮换
};
// 用户自定义包长（写入 select 的 custom-fixed / custom-dyn 选项）。
// custom-fixed: { fixed: N }。custom-dyn: { sizes: [..] }。
let CUSTOM_PACKET = { 'custom-fixed': null, 'custom-dyn': null };

/* 取当前选中包长 option 的元信息（k/label/value），data-k/data-label 定义在 option 上。 */
function packetSelMeta() {
  const sel = $('cfgPacketLen');
  const opt = sel.options[sel.selectedIndex] || sel;
  const value = String(opt.value);
  return { value, kind: opt.dataset.k || 'fixed', label: opt.dataset.label || '' };
}

function getConfig() {
  let server = $('cfgServer').value.trim();
  if (!server) server = location.host;
  server = server.replace(/^https?:\/\//, '').replace(/\/.*$/, '');
  const psm = packetSelMeta();
  const rawKind = psm.kind;                    // fixed | dynamic | custom-fixed | custom-dyn
  let packetKind = rawKind, packetSizes = null;
  let packetLen = clampInt(psm.value, 64, 0x7fffffff, 131072);
  let packetLabel = psm.label || '';

  if (rawKind === 'custom-fixed') {
    const c = CUSTOM_PACKET['custom-fixed'];
    if (c) { packetLen = c; packetLabel = `自定义·固定 ${c}B`; packetKind = 'fixed'; }
  } else if (rawKind === 'custom-dyn') {
    const c = CUSTOM_PACKET['custom-dyn'];
    if (c && c.length) {
      packetSizes = c.slice();
      packetLen = c[0];
      packetKind = 'dynamic';               // 归一为 dynamic，走动态轮换
      packetLabel = `自定义·动态 ${c.join('/')}B`;
    }
  } else if (rawKind === 'dynamic') {
    packetSizes = (PACKET_PROFILES['dyn-media'].sizes).slice();
    packetLen = packetSizes[0];
    packetKind = 'dynamic';
    packetLabel = `动态·媒体混合 ${packetSizes.join('/')}B`;
  } else {
    // fixed
    packetKind = 'fixed';
    packetLabel = `固定·${psm.label || ''} ${packetLen}B`;
  }

  return {
    mode: $('cfgMode').value,          // tcp | udp | both
    dir: $('cfgDir').value,            // down | up | both
    streams: clampInt($('cfgStreams').value, 1, 16, 4),
    duration: clampFloat($('cfgDuration').value, 1, 120, 10),
    packetLen,
    packetKind,             // fixed | dynamic (custom 已归一到这两类)
    packetSizes,
    packetLabel,            // 人类可读的包长描述，用于结果/历史/曲线
    server,
    wsBase: (location.protocol === 'https:' ? 'wss://' : 'ws://') + server,
  };
}
function clampInt(v, lo, hi, def) {
  const n = parseInt(v, 10);
  if (isNaN(n)) return def;
  return Math.max(lo, Math.min(hi, n));
}
function clampFloat(v, lo, hi, def) {
  const n = parseFloat(v);
  if (isNaN(n)) return def;
  return Math.max(lo, Math.min(hi, n));
}

/* 包长迭代器：固定模式复用同一 zero buffer；动态模式按 packetSizes 轮换。 */
function makePacketIterator(cfg) {
  if (cfg.packetKind !== 'dynamic' || !cfg.packetSizes || !cfg.packetSizes.length) {
    const buf = new Uint8Array(cfg.packetLen);
    return () => ({ buf, len: cfg.packetLen });
  }
  const sizes = cfg.packetSizes;
  // 预分配各档 buffer，避免每包重复分配
  const bufs = sizes.map((sz) => new Uint8Array(sz));
  let i = 0;
  return () => {
    const buf = bufs[i];
    const len = sizes[i];
    i = (i + 1) % sizes.length;
    return { buf, len };
  };
}

/* 把 packet_kind / packet_sizes 写入 URL query（动态包长需要服务端轮换发包）。 */
function addPacketParams(q, cfg) {
  if (cfg.packetKind === 'dynamic' && cfg.packetSizes && cfg.packetSizes.length) {
    q.set('packet_kind', 'dynamic');
    q.set('packet_sizes', cfg.packetSizes.join(','));
  }
}

/* ---------- 本地采样器：每个流一个 ---------- */
function makeMeter() {
  return {
    rx: 0, tx: 0,             // 本地已计字节
    lastRX: 0, lastTX: 0,
    createdAt: performance.now(),
    lastT: performance.now(),
    peakDown: 0, peakUp: 0,   // 峰值 Mbit/s（1s 窗口平均）
    win: [],                  // 滑动窗口（近 10 个采样）
    // 上行校正：传入"真实已发字节"（=累计send − bufferedAmount滞留），单调递增
    syncTX(realSent) {
      if (realSent > this.tx) this.tx = realSent;
    },
    addRX(n) { this.rx += n; },
    addTX(n) { this.tx += n; },
    sample() {
      const now = performance.now();
      const dt = (now - this.lastT) / 1000;
      if (dt <= 0) return null;
      const downMbps = ((this.rx - this.lastRX) / dt) * 8 / 1e6;
      const upMbps = ((this.tx - this.lastTX) / dt) * 8 / 1e6;
      this.lastRX = this.rx;
      this.lastTX = this.tx;
      this.lastT = now;
      // 峰值 = 近 1s 滑动窗口均值（消除突发尖峰），TCP/UDP 通用
      this.win.push({ d: downMbps, u: upMbps });
      if (this.win.length > 10) this.win.shift();
      const n = this.win.length;
      const dAvg = this.win.reduce((s, w) => s + w.d, 0) / n;
      const uAvg = this.win.reduce((s, w) => s + w.u, 0) / n;
      if (dAvg > this.peakDown) this.peakDown = dAvg;
      if (uAvg > this.peakUp) this.peakUp = uAvg;
      // 返回滑动窗口均值（1s），而非单窗口瞬时值，让实时曲线平滑呈「爬升→稳定」，
      // 避免因单帧突发/背压造成的单点锯齿。
      return { downMbps: dAvg, upMbps: uAvg };
    },
    elapsed() { return (performance.now() - this.createdAt) / 1000; },
  };
}

function drainedBytes(submitted, queued) {
  return Math.max(0, Number(submitted || 0) - Math.max(0, Number(queued || 0)));
}

async function waitForBufferedDrain(channel, timeoutMs = 3000, pollMs = 10) {
  const deadline = performance.now() + timeoutMs;
  while (channel.bufferedAmount > 0 && performance.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
  return Math.max(0, Number(channel.bufferedAmount || 0));
}

function peakFromPoints(points, dir) {
  const rateOf = (point) => dir === 'up'
    ? Number(point.up || 0)
    : dir === 'down'
      ? Number(point.down || 0)
      : Number(point.down || 0) + Number(point.up || 0);
  let start = 0;
  let sum = 0;
  let peak = 0;
  for (let end = 0; end < points.length; end++) {
    sum += rateOf(points[end]);
    while (start < end && points[start].t < points[end].t - 1) {
      sum -= rateOf(points[start]);
      start++;
    }
    peak = Math.max(peak, sum / (end - start + 1));
  }
  return peak;
}

/* ---------- 全局实时聚合（供大数字/曲线） ---------- */
const live = {
  meters: new Map(),
  // 关键修复：add 接收真实的 meter，而不是内部再造一个空 meter
  add(id, meter) { this.meters.set(id, meter); },
  remove(id) { this.meters.delete(id); },
  reset() { this.meters.clear(); },
};

/* ---------- UI ---------- */
function setRunning(run) {
  state.running = run;
  $('btnStart').disabled = run;
  $('btnStop').disabled = !run;
  $('cfgMode').disabled = run;
  $('cfgDir').disabled = run;
  $('cfgStreams').disabled = run;
  $('cfgDuration').disabled = run;
  $('cfgPacketLen').disabled = run;
}

/* 曲线绘制（支持多条测试系列缓存叠加） */
function drawChart() {
  const canvas = $('chart');
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  const series = state.series;
  if (!series.length) {
    ctx.fillStyle = '#475569';
    ctx.font = Math.max(13, W * 0.014) + 'px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('点击「开始测速」…', W / 2, H / 2);
    renderLegend([]);
    return;
  }
  // 窄屏（手机）压缩左边距，给曲线更多空间
  const pad = { l: W < 480 ? 40 : 52, r: 12, t: 12, b: 28 };
  const iw = W - pad.l - pad.r, ih = H - pad.t - pad.b;

  // 时间轴取所有系列最大时长
  let tMax = 0;
  for (const s of series) for (const d of s.points) if (d.t > tMax) tMax = d.t;
  if (tMax < 1) tMax = 1;

  // Y 轴取所有系列最大速率
  let maxRate = 10;
  for (const s of series) for (const d of s.points) {
    if (d.down > maxRate) maxRate = d.down;
    if (d.up > maxRate) maxRate = d.up;
  }
  maxRate = niceCeil(maxRate);

  // 网格 + 坐标
  ctx.strokeStyle = 'rgba(148,163,184,.15)';
  ctx.fillStyle = '#94a3b8';
  ctx.font = '11px sans-serif';
  ctx.textAlign = 'right';
  ctx.lineWidth = 1;
  const gridY = 5;
  for (let i = 0; i <= gridY; i++) {
    const y = pad.t + ih - (ih * i / gridY);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    ctx.fillText(fmtRate(maxRate * i / gridY), pad.l - 6, y + 4);
  }
  ctx.textAlign = 'center';
  for (let i = 0; i <= 4; i++) {
    const x = pad.l + iw * i / 4;
    ctx.beginPath(); ctx.moveTo(x, pad.t); ctx.lineTo(x, H - pad.b); ctx.stroke();
    ctx.fillText((tMax * i / 4).toFixed(0) + 's', x, H - 8);
  }

  const xy = (t, rate) => [
    pad.l + (t / tMax) * iw,
    pad.t + ih - (rate / maxRate) * ih,
  ];

  // 每个系列：下行实线、上行虚线（同色系标注线型）
  for (const s of series) {
    const drawSerie = (key, style) => {
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 2;
      ctx.setLineDash(style === 'dash' ? [6, 4] : []);
      ctx.beginPath();
      let first = true;
      for (const d of s.points) {
        if (d[key] === undefined) continue;
        const [x, y] = xy(d.t, d[key]);
        if (first) { ctx.moveTo(x, y); first = false; }
        else ctx.lineTo(x, y);
      }
      ctx.stroke();
    };
    drawSerie('down', 'solid');
    drawSerie('up', 'dash');
  }
  ctx.setLineDash([]);

  // 图例：每系列一个色块 + 模式/方向/时间
  renderLegend(series);
}

/* 图例：下行/上行静态说明 + 各测试系列 */
function renderLegend(series) {
  const wrap = $('chartLegend');
  if (!wrap) return;
  let html = '<span class="dot down"></span>下行(实线) <span class="dot up"></span>上行(虚线)';
  for (const s of series) {
    const ts = new Date(s.ts).toLocaleTimeString();
    html += ` <span class="series-tag" style="--sc:${s.color}">■ ${s.mode.toUpperCase()} ${dirText(s.dir)}` +
      (s.packetLabel ? ` · ${s.packetLabel}` : '') + ` ${ts}</span>`;
  }
  wrap.innerHTML = html;
}

function dirText(dir) {
  return dir === 'up' ? '上行' : dir === 'down' ? '下行' : '双向';
}

function niceCeil(v) {
  const p = Math.pow(10, Math.floor(Math.log10(v)));
  const n = v / p;
  if (n <= 1) return p;
  if (n <= 2) return 2 * p;
  if (n <= 5) return 5 * p;
  return 10 * p;
}
function fmtRate(mbps) {
  if (mbps >= 1000) return (mbps / 1000).toFixed(1) + 'G';
  return mbps.toFixed(0) + 'M';
}

/* UI 刷新循环 */
let uiTimer = null;
function startUITimer() {
  stopUITimer();
  uiTimer = setInterval(() => {
    if (!state.running) return;
    // 触发所有 meter 采样（通过 meter.sample() 更新 live）
    let totalDown = 0, totalUp = 0, totalRX = 0, totalTX = 0;
    for (const m of live.meters.values()) {
      if (m.sync) m.sync();  // 上行流：先校正为真实已发（send 缓冲已排空部分）
      const s = m.sample();
      if (s) { totalDown += s.downMbps; totalUp += s.upMbps; }
      totalRX += m.rx;
      totalTX += m.tx;
    }
    $('valDown').textContent = totalDown.toFixed(1);
    $('valUp').textContent = totalUp.toFixed(1);
    // 累计字节 + 进度
    const el = $('liveDetail');
    if (el) {
      const t = Math.min((Date.now() - state.t0) / 1000, state.durations);
      const pct = state.durations > 0 ? Math.round(t / state.durations * 100) : 0;
      el.textContent =
        `${state.phaseText || ''} ${pct}% · 已收 ${fmtBytes(totalRX)} · 已发 ${fmtBytes(totalTX)}`;
      const bar = $('progressBar');
      if (bar) bar.style.width = pct + '%';
    }
    const t = (Date.now() - state.t0) / 1000;
    // 写入当前测试系列（缓存多轮测试曲线）
    if (state.curSeries) state.curSeries.points.push({ t, down: totalDown, up: totalUp });
    drawChart();
  }, 100);
}
function stopUITimer() {
  if (uiTimer) { clearInterval(uiTimer); uiTimer = null; }
}

/* ---------- 单个流的本地统计结果 ---------- */
function makeStreamResult(meter, dir, duration, submitted, queued) {
  // 依据方向取对应字节/速率
  const dt = Math.max(duration || meter.elapsed(), 0.001);
  const submittedBytes = submitted === undefined ? meter.tx : submitted;
  const queuedBytes = Math.max(0, queued || 0);
  const downBytes = meter.rx;
  const upBytes = drainedBytes(submittedBytes, queuedBytes);
  const total = dir === 'up' ? upBytes : dir === 'down' ? downBytes : upBytes + downBytes;
  const downMbps = (downBytes / dt) * 8 / 1e6;
  const upMbps = (upBytes / dt) * 8 / 1e6;
  return {
    totalBytes: total,
    duration: dt,
    avgMbitps: dir === 'up' ? upMbps : dir === 'down' ? downMbps : downMbps + upMbps,
    peakMbitps: dir === 'up' ? meter.peakUp : dir === 'down' ? meter.peakDown : meter.peakDown + meter.peakUp,
    packets: 0, lost: 0, lost_pct: 0, jitter_ms: 0,
    upBytes, downBytes, downMbps, upMbps,
    submittedBytes, queuedBytes, truncated: queuedBytes > 0,
  };
}

/* ---------- TCP 测速（WebSocket） ---------- */
function runTCPStream(cfg, streamId, doneCb) {
  const q = new URLSearchParams({
    mode: 'tcp', dir: cfg.dir, streams: cfg.streams,
    duration: cfg.duration, packet_len: cfg.packetLen,
  });
  addPacketParams(q, cfg);
  const url = `${cfg.wsBase}/ws/test?${q}`;
  const ws = new WebSocket(url);
  ws.binaryType = 'arraybuffer';
  const meter = makeMeter();
  live.add(streamId, meter);
  state.streams.add(ws);
  let sendTimer = null;
  let stopTimer = null;
  let finished = false;
  let opened = false;      // 是否成功建立连接
  let startedAt = null;
  let lastError = null;

  const zeroBuf = new Uint8Array(cfg.packetLen);
  let sentTotal = 0;   // 累计送入 send() 的字节
  // 包长生成器：固定模式复用同一 buffer；动态模式按 packetSizes 轮换（模拟媒体混合包长）。
  const pktNext = makePacketIterator(cfg);

  const finish = (err, res) => {
    if (finished) return;
    finished = true;
    if (sendTimer) clearInterval(sendTimer);
    if (stopTimer) clearTimeout(stopTimer);
    clearTimeout(watchdog);
    live.remove(streamId);
    state.streams.delete(ws);
    try { ws.close(); } catch (e) {}
    doneCb(err, res);
  };

  const watchdog = setTimeout(() => {
    finish(new Error('TCP 测速超时：服务端未返回最终统计'));
  }, (cfg.duration + 10) * 1000);

  const startTransfer = () => {
    if (startedAt !== null) return;
    startedAt = performance.now();
    meter.createdAt = startedAt;
    meter.lastT = startedAt;
    const deadline = startedAt + cfg.duration * 1000;
    let stopSent = false;
    const sendStop = () => {
      if (stopSent || finished) return;
      stopSent = true;
      if (sendTimer) { clearInterval(sendTimer); sendTimer = null; }
      if (stopTimer) { clearTimeout(stopTimer); stopTimer = null; }
      if (ws.readyState === WebSocket.OPEN) {
        // stop 与此前二进制帧共用有序 WebSocket；服务端读到它即确认全部上行负载。
        ws.send(JSON.stringify({ type: 'stop' }));
      }
    };
    if (cfg.dir === 'up' || cfg.dir === 'both') {
      // 到点判定放在打流回调内同步执行：打流 while 可能长时间占住事件循环，
      // 若依赖外层 setTimeout 触发 stop，stop 会被饿死（服务端等满 watchdog 后断开，
      // 前端随即报"TCP 测速超时：服务端未返回最终统计"）。
      sendTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) { clearInterval(sendTimer); sendTimer = null; return; }
        if (performance.now() >= deadline) {
          sendStop();
          return;
        }
        while (ws.readyState === WebSocket.OPEN &&
               ws.bufferedAmount < 2 * 1024 * 1024 &&
               performance.now() < deadline) {
          const pkt = pktNext();
          ws.send(pkt.buf);
          sentTotal += pkt.len;
        }
        if (performance.now() >= deadline) sendStop();
      }, 0);
      meter.sync = () => meter.syncTX(sentTotal - (ws.readyState === WebSocket.OPEN ? ws.bufferedAmount : 0));
      // 兜底：仅当主路径（回调内同步判定）失效时才依赖事件循环；晚 100ms 避免与主路径重复。
      stopTimer = setTimeout(sendStop, cfg.duration * 1000 + 100);
    }
  };

  ws.onmessage = (ev) => {
    if (ev.data instanceof ArrayBuffer) {
      meter.addRX(ev.data.byteLength); // 下行：收到即计数
    } else if (typeof ev.data === 'string') {
      let msg; try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (msg.type === 'error') lastError = new Error(msg.error);
      else if (msg.type === 'start') startTransfer();
      else if (msg.type === 'result' && msg.result) {
        if (startedAt === null) {
          finish(new Error('TCP 测速协议错误：result 早于 start'));
          return;
        }
        const duration = Math.max((performance.now() - startedAt) / 1000, 0.001);
        const res = makeStreamResult(meter, cfg.dir, duration, sentTotal, 0);
        res.srv = msg.result;
        finish(null, res);
      }
    }
  };

  ws.onopen = () => {
    opened = true;
  };

  ws.onclose = (ev) => {
    if (!opened && !finished) {
      // 连接从未建立成功：这是失败，不是 0 速结果
      finish({
        name: 'ConnectionError',
        message: `无法连接测试服务器 (${url})\n请检查服务器地址、端口与网络。` +
          (lastError ? `\n服务器返回: ${lastError.message}` : '') +
          (ev.code ? `\nclose code=${ev.code}` : ''),
      });
      return;
    }
    if (finished) return;
    finish(lastError || new Error(
      `TCP 测速连接在服务端统计返回前关闭${ev && ev.code ? ` (code=${ev.code})` : ''}`,
    ));
  };
  ws.onerror = (e) => { lastError = e && e.error ? e.error : null; };
}

function runTCP(cfg) {
  return new Promise((resolve) => {
    const results = [];
    let pending = cfg.streams;
    let failCount = 0;
    const onDone = (err, res) => {
      pending--;
      if (err) { failCount++; results.push({ err, res: null }); }
      else results.push({ err: null, res });
      if (pending === 0) {
        if (failCount === cfg.streams) {
          // 全部失败：抛出一个汇总错误让 UI 显示原因
          const first = results.find((r) => r.err);
          resolve({ allFailed: true, message: first.err.message });
        } else {
          resolve(results);
        }
      }
    };
    for (let i = 0; i < cfg.streams; i++) runTCPStream(cfg, `tcp-${i}`, onDone);
  });
}

/* ---------- UDP 测速（WebRTC DataChannel） ---------- */
function runUDPStream(cfg, streamId, doneCb) {
  const pc = new RTCPeerConnection({ iceServers: [] });
  const meter = makeMeter();
  live.add(streamId, meter);
  state.streams.add(pc);
  let sendTimer = null;
  let stopTimer = null;
  let finished = false;
  let startedAt = null;
  let localQueued = 0;
  let lastError = null;

  // UDP 测速：WebRTC DataChannel。用 reliable unordered（ordered:false，不设重传限制）。
  // 原因：部分可靠(不重传)在 Chrome↔pion 间会因 SCTP 缺口死锁（上行数据发不出、测速卡住）。
  // 可靠无序通道稳定不死锁，能测出爬升→稳定吞吐；真实 UDP 丢包/掣动请用命令行 speedlite-cli udp。
  const config = { ordered: false };
  const dc = pc.createDataChannel('speedtest-' + streamId, config);
  dc.binaryType = 'arraybuffer';

  // 构造信令 URL，动态包长时附带 packet_kind/packet_sizes 供服务端下行轮换
  const signalQP = `dir=${encodeURIComponent(cfg.dir)}&duration=${cfg.duration}&packet_len=${cfg.packetLen}&stream_id=${encodeURIComponent(streamId)}` +
    (cfg.packetKind === 'dynamic' && cfg.packetSizes && cfg.packetSizes.length
      ? `&packet_kind=dynamic&packet_sizes=${cfg.packetSizes.join(',')}`
      : '');
  const signal = new WebSocket(`${cfg.wsBase}/ws/signal?${signalQP}`);
  const zeroBuf = new Uint8Array(cfg.packetLen);
  let sentTotal = 0;   // 累计送入 dc.send() 的字节
  let uploadingStopped = false;   // stopUpload 幂等守卫（主路径与兜底可能并发/重复）
  // 包长生成器：固定模式复用同一 buffer；动态模式按 packetSizes 轮换。
  const pktNext = makePacketIterator(cfg);

  const finish = (err, res) => {
    if (finished) return;
    finished = true;
    if (sendTimer) clearInterval(sendTimer);
    if (stopTimer) clearTimeout(stopTimer);
    clearTimeout(watchdog);
    try { dc.close(); } catch (e) {}
    try { pc.close(); } catch (e) {}
    live.remove(streamId);
    state.streams.delete(pc);
    try { signal.close(); } catch (e) {}
    doneCb(err, res);
  };

  const watchdog = setTimeout(() => {
    finish(new Error('UDP 测速超时：服务端未返回最终统计'));
  }, (cfg.duration + 10) * 1000);

  const stopUpload = async () => {
    if (uploadingStopped) return;
    uploadingStopped = true;
    if (sendTimer) { clearInterval(sendTimer); sendTimer = null; }
    if (stopTimer) { clearTimeout(stopTimer); stopTimer = null; }
    // 排空等待设为 1.5s（可靠通道数据保证送达；超时则按已统计字节 + 标记截断，避免拖长时长）
    localQueued = await waitForBufferedDrain(dc, 1500, 10);
    if (finished) return;
    meter.syncTX(drainedBytes(sentTotal, localQueued));
    if (signal.readyState === WebSocket.OPEN) {
      signal.send(JSON.stringify({ type: 'stop', queued_bytes: localQueued }));
    } else {
      finish(new Error('UDP 测速信令在 stop 前关闭'));
    }
  };

  signal.onopen = async () => {
    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      signal.send(JSON.stringify({ type: 'offer', sdp: pc.localDescription.sdp }));
    } catch (e) { finish(e); }
  };
  signal.onmessage = async (ev) => {
    let msg; try { msg = JSON.parse(ev.data); } catch (e) { return; }
    if (msg.type === 'answer') {
      try { await pc.setRemoteDescription({ type: 'answer', sdp: msg.sdp }); }
      catch (e) { finish(e); }
    } else if (msg.type === 'error') {
      finish(new Error('UDP 测速: ' + msg.error));
    } else if (msg.type === 'result' && msg.result) {
      if (startedAt === null) {
        finish(new Error('UDP 测速协议错误：result 早于 DataChannel open'));
        return;
      }
      const duration = Math.max((performance.now() - startedAt) / 1000, 0.001);
      const res = makeStreamResult(meter, cfg.dir, duration, sentTotal, localQueued);
      res.srv = msg.result;
      finish(null, res);
    }
  };
  signal.onerror = (e) => { lastError = e && e.error ? e.error : null; };
  signal.onclose = (ev) => {
    if (!finished) {
      finish(lastError || new Error(
        `UDP 信令在服务端统计返回前关闭 (${ev && ev.code !== undefined ? 'code=' + ev.code : '未建立'})`,
      ));
    }
  };

  dc.onopen = () => {
    if (startedAt !== null) return;
    startedAt = performance.now();
    meter.createdAt = startedAt;
    meter.lastT = startedAt;
    if (cfg.dir === 'up' || cfg.dir === 'both') {
      // 与 TCP 相同：到点判定在打流回调内同步执行，避免 while 打流饿死 setTimeout(stop)。
      const deadline = startedAt + cfg.duration * 1000;
      sendTimer = setInterval(() => {
        if (dc.readyState !== 'open') { clearInterval(sendTimer); sendTimer = null; return; }
        if (performance.now() >= deadline) {
          stopUpload();
          return;
        }
        while (dc.readyState === 'open' &&
               dc.bufferedAmount < 2 * 1024 * 1024 &&
               performance.now() < deadline) {
          const pkt = pktNext();
          dc.send(pkt.buf);
          sentTotal += pkt.len;
        }
        if (performance.now() >= deadline) stopUpload();
      }, 0);
      // 采样校正：真实已发 = 累计send − 缓冲滞留
      meter.sync = () => meter.syncTX(sentTotal - (dc.readyState === 'open' ? dc.bufferedAmount : 0));
      // 兜底：仅当主路径失效时才依赖事件循环；晚 100ms 避免与主路径重复
      stopTimer = setTimeout(stopUpload, cfg.duration * 1000 + 100);
    }
  };
  dc.onmessage = (ev) => {
    if (ev.data instanceof ArrayBuffer) {
      meter.addRX(ev.data.byteLength);
    }
  };
  dc.onclose = () => {
    if (!finished) finish(new Error('UDP DataChannel 在服务端统计返回前关闭'));
  };
  dc.onerror = (e) => { lastError = e && e.error ? e.error : lastError; };
}

function runUDP(cfg) {
  return new Promise((resolve) => {
    const results = [];
    let pending = cfg.streams;
    const onDone = (err, res) => {
      pending--;
      results.push({ err, res });
      if (pending === 0) resolve(results);
    };
    for (let i = 0; i < cfg.streams; i++) runUDPStream(cfg, `udp-${i}`, onDone);
  });
}

/* ---------- 结果汇聚 ---------- */
function aggregate(results, dir, phaseDuration, phasePeak, meta) {
  let totalBytes = 0, duration = 0, peakSum = 0;
  let upBytes = 0, downBytes = 0;
  let submittedBytes = 0, queuedBytes = 0, truncated = false;
  // 服务端对账（双重统计）
  let srvBytes = 0, srvAvg = 0, srvPeak = 0, srvCount = 0;
  let srvSubmittedBytes = 0, srvQueuedBytes = 0;
  let srvUpBytes = 0, srvDownBytes = 0;
  let okCount = 0, failedCount = 0;
  for (const r of results) {
    if (!r || !r.res || r.err) { failedCount++; continue; }
    okCount++;
    const x = r.res;
    totalBytes += x.totalBytes || 0;
    duration = Math.max(duration, x.duration || 0);
    peakSum += x.peakMbitps || 0;
    upBytes += x.upBytes || 0;
    downBytes += x.downBytes || 0;
    submittedBytes += x.submittedBytes || 0;
    queuedBytes += x.queuedBytes || 0;
    truncated = truncated || Boolean(x.truncated);
    // 服务端 stats（每个流一个 result，normalize 到相同本质单位后汇总）
    if (x.srv) {
      const s = x.srv;
      srvBytes += s.total_bytes || 0;
      srvAvg += s.avg_mbitps || 0;      // 服务端平均（MB/s→Mbit/s 单位一致）
      srvPeak += s.peak_mbitps || 0;
      srvSubmittedBytes += s.submitted_bytes || s.total_bytes || 0;
      srvQueuedBytes += s.queued_bytes || 0;
      const hasUpBytes = Object.prototype.hasOwnProperty.call(s, 'up_bytes');
      const hasDownBytes = Object.prototype.hasOwnProperty.call(s, 'down_bytes');
      srvUpBytes += hasUpBytes ? (s.up_bytes || 0)
        : (dir === 'up' ? (s.total_bytes || 0) : 0);
      srvDownBytes += hasDownBytes ? (s.down_bytes || 0)
        : (dir === 'down' ? (s.total_bytes || 0) : 0);
      truncated = truncated || Boolean(s.truncated);
      srvCount++;
    }
  }
  if (okCount === 0) return null;
  if (phaseDuration > 0) duration = phaseDuration;
  duration = Math.max(duration, 0.001);
  const upMbps = upBytes * 8 / duration / 1e6;
  const downMbps = downBytes * 8 / duration / 1e6;
  const avgMbitps = dir === 'up' ? upMbps : dir === 'down' ? downMbps : upMbps + downMbps;
  // 对账：TCP 检查共同边界上的字节一致性；UDP 计算应用层交付差额。
  // - down: 服务端已排空 > 客户端收到 → 交付损失
  // - up:   客户端已排空 > 服务端收到 → 交付损失
  // - both: 双方合计比较 → 总偏差
  let lossPct = null, upLossPct = null, downLossPct = null, devPct = null;
  if (srvCount && totalBytes > 0) {
    const big = Math.max(totalBytes, srvBytes);
    devPct = big > 0 ? (Math.abs(totalBytes - srvBytes) / big * 100) : 0;
    if (upBytes > 0) upLossPct = Math.max(0, (upBytes - srvUpBytes) / upBytes * 100);
    if (srvDownBytes > 0) downLossPct = Math.max(0, (srvDownBytes - downBytes) / srvDownBytes * 100);
    if (dir === 'up') lossPct = upLossPct;
    else if (dir === 'down') lossPct = downLossPct;
    else {
      const offered = upBytes + srvDownBytes;
      const missing = Math.max(0, upBytes - srvUpBytes) + Math.max(0, srvDownBytes - downBytes);
      lossPct = offered > 0 ? missing / offered * 100 : 0;
    }
  }
  return {
    totalBytes, duration,
    avgMbitps,
    peakMbitps: Number.isFinite(phasePeak) ? phasePeak : peakSum,
    upBytes, downBytes, upMbps, downMbps,
    submittedBytes, queuedBytes, truncated,
    streams: okCount,
    failedStreams: failedCount,
    incomplete: failedCount > 0,
    jitter: 0,
    // lossPct=UDP 方向性交付损失，devPct=双端总偏差
    srvBytes: srvCount ? srvBytes : null,
    srvAvg: srvCount ? srvAvg : null,
    srvPeak: srvCount ? srvPeak : null,
    srvSubmittedBytes: srvCount ? srvSubmittedBytes : null,
    srvQueuedBytes: srvCount ? srvQueuedBytes : null,
    srvUpBytes: srvCount ? srvUpBytes : null,
    srvDownBytes: srvCount ? srvDownBytes : null,
    lossPct, upLossPct, downLossPct, devPct,
    // 包长元信息（用于按包长分类记录/展示）
    packetLen: meta ? meta.packetLen : null,
    packetLabel: meta ? meta.packetLabel : '',
    packetKind: meta ? meta.packetKind : 'fixed',
    packetSizes: meta ? meta.packetSizes : null,
  };
}

function addResultRow(mode, dir, r) {
  const tbody = document.querySelector('#resultTable tbody');
  const tr = document.createElement('tr');
  tr.className = mode === 'tcp' ? 'row-tcp' : 'row-udp';
  const pktKey = (r.packetKind || 'fixed') + '|' + (r.packetLen || '');
  tr.dataset.key = `${mode}|${dir}|${pktKey}`;
  const dTxt = dirText(dir) + (dir === 'up' ? ' ▲' : dir === 'down' ? ' ▼' : ' ⇅');
  // 链路列简短展示包长（完整描述放明细行），避免重复拼接
  const packetCell = r.packetLen ? `${r.packetLen}B` : '—';
  // 双向时分别展示上下行速率
  let avgCell = `${(r.avgMbitps).toFixed(1)} Mbit/s`;
  if (dir === 'both') {
    avgCell = `▼ ${(r.downMbps).toFixed(1)} / ▲ ${(r.upMbps).toFixed(1)} Mbit/s`;
  }
  const timeCell = new Date().toLocaleTimeString();
  // 网页 UDP 用可靠通道测吞吐，交付损失=0；真实 IP 层丢包用 CLI。
  let lostCell = '0%';
  let lostCls = '';
  const jitterCell = '—';
  const lossLabel = mode === 'udp' ? '交付损失' : '丢包率';
  // 主行：每个 td 带 data-label，手机竖屏下 CSS 卡片化时展示列名
  tr.innerHTML = `
    <td data-label="链路">${mode.toUpperCase()} <span class="pkt">${packetCell}</span></td>
    <td data-label="方向">${dTxt}</td>
    <td data-label="平均">${avgCell}</td>
    <td data-label="峰值">${(r.peakMbitps).toFixed(1)} Mbit/s</td>
    <td data-label="总传输">${fmtBytes(r.totalBytes)}</td>
    <td data-label="${lossLabel}" class="${lostCls}">${lostCell}</td>
    <td data-label="抖动">${jitterCell}</td>
    <td data-label="用时">${r.duration.toFixed(1)} s</td>
    <td data-label="时间" class="time">${timeCell}</td>`;
  tbody.appendChild(tr);
  // 附加信息行：流数 + 包长 + 字节明细 + 服务端对账（偏差分级警示）
  const detail = document.createElement('tr');
  detail.className = 'row-detail';
  detail.dataset.key = `${mode}|${dir}|${pktKey}`;
  const directionBytes = dir === 'up'
    ? `上行 ${fmtBytes(r.upBytes)}`
    : dir === 'down'
      ? `下行 ${fmtBytes(r.downBytes)}`
      : `下行 ${fmtBytes(r.downBytes)} · 上行 ${fmtBytes(r.upBytes)}`;
  let detailTxt = `${r.streams} 条并行流 · ${directionBytes}`;
  if (r.packetLabel) detailTxt += ` · 包长 ${r.packetLabel}`;
  if (r.incomplete) detailTxt += ` · ${r.failedStreams} 条流失败（结果不完整）`;
  if (r.srvBytes !== null && r.srvBytes !== undefined) {
    const dev = mode === 'udp' && r.lossPct !== null && r.lossPct !== undefined
      ? r.lossPct
      : ((r.devPct !== null && r.devPct !== undefined) ? r.devPct : 0);
    let grade, gradeCls;
    if (mode === 'tcp') {
      if (dev < 1) { grade = '对账正常'; gradeCls = 'rec-ok'; }
      else { grade = '对账异常 ⚠'; gradeCls = 'rec-bad'; }
    } else if (r.truncated) {
      grade = '队列截断 ⚠'; gradeCls = 'rec-bad';
    } else if (dev < 5) {
      grade = '交付正常'; gradeCls = 'rec-ok';
    } else if (dev < 30) {
      grade = '交付损失较大'; gradeCls = 'rec-warn';
    } else {
      grade = '严重交付损失 ⚠'; gradeCls = 'rec-bad';
    }
    let warnNote = '';
    if (mode === 'udp') {
      if (dir === 'up') {
        warnNote = ` · 上行: 提交 ${fmtBytes(r.submittedBytes || r.upBytes)} · ` +
          `排空 ${fmtBytes(r.upBytes)} · 接收 ${fmtBytes(r.srvUpBytes ?? r.srvBytes)} · ` +
          `残留 ${fmtBytes(r.queuedBytes || 0)}`;
      } else if (dir === 'down') {
        const submitted = r.srvSubmittedBytes ?? r.srvBytes;
        warnNote = ` · 下行: 提交 ${fmtBytes(submitted)} · ` +
          `排空 ${fmtBytes(r.srvDownBytes ?? r.srvBytes)} · 接收 ${fmtBytes(r.downBytes)} · ` +
          `残留 ${fmtBytes(r.srvQueuedBytes || 0)}`;
      } else {
        warnNote = ` · 上行: 提交 ${fmtBytes(r.submittedBytes || r.upBytes)} · ` +
          `排空 ${fmtBytes(r.upBytes)} · 接收 ${fmtBytes(r.srvUpBytes || 0)} · ` +
          `残留 ${fmtBytes(r.queuedBytes || 0)} · ` +
          `下行: 提交 ${fmtBytes(r.srvSubmittedBytes ?? r.srvDownBytes)} · ` +
          `排空 ${fmtBytes(r.srvDownBytes || 0)} · 接收 ${fmtBytes(r.downBytes)} · ` +
          `残留 ${fmtBytes(r.srvQueuedBytes || 0)}`;
      }
      if (dev >= 5 && !r.truncated) {
        warnNote += ` · 应用层交付损失 ${dev.toFixed(1)}%`;
      }
    } else if (dev >= 1) {
      warnNote = ` · 双端统计相差 ${dev.toFixed(1)}%（TCP 共同边界不一致，请检查异常断链）`;
    }
    detailTxt += ` · <span class="srv-rec">服务器对账: 传输 ${fmtBytes(r.srvBytes)} · ` +
      `平均 ${(r.srvAvg).toFixed(1)} Mbit/s · 峰值 ${(r.srvPeak).toFixed(1)} Mbit/s · ` +
      `<span class="${gradeCls}">${grade}</span>${warnNote}</span>`;
  } else {
    detailTxt += ' · <span class="srv-rec" style="opacity:.6">服务器统计未返回</span>';
  }
  detail.innerHTML = `<td colspan="9" style="color:var(--muted);font-size:12px;text-align:left;padding-top:0">${detailTxt}</td>`;
  tbody.appendChild(detail);
}

// 重新测试同链路+同包长时替换旧结果行（主行+明细行）
function removeResultRow(mode, dir, packetKey) {
  const pktKey = packetKey || '*';
  const tbody = document.querySelector('#resultTable tbody');
  for (const tr of [...tbody.querySelectorAll('tr')]) {
    if (tr.dataset.key === `${mode}|${dir}|${pktKey}`) tr.remove();
  }
}

function fmtBytes(b) {
  if (b >= 1 << 30) return (b / (1 << 30)).toFixed(2) + ' GiB';
  if (b >= 1 << 20) return (b / (1 << 20)).toFixed(2) + ' MiB';
  if (b >= 1 << 10) return (b / (1 << 10)).toFixed(1) + ' KiB';
  return b + ' B';
}

/* 历史记录 */
function saveHistory(entry) {
  const key = 'speedtest-history';
  let hist = [];
  try { hist = JSON.parse(localStorage.getItem(key) || '[]'); } catch (e) {}
  hist.unshift(entry);
  hist = hist.slice(0, 20);
  localStorage.setItem(key, JSON.stringify(hist));
  renderHistory();
}
function renderHistory() {
  const key = 'speedtest-history';
  let hist = [];
  try { hist = JSON.parse(localStorage.getItem(key) || '[]'); } catch (e) {}
  const wrap = $('historyWrap');
  const ul = $('history');
  if (!hist.length) { wrap.hidden = true; return; }
  wrap.hidden = false;
  ul.innerHTML = '';
  for (const h of hist) {
    const li = document.createElement('li');
    li.textContent = `${new Date(h.ts).toLocaleTimeString()} · ${h.mode.toUpperCase()} ${h.dir} · ` +
      (h.packetLabel ? `包长 ${h.packetLabel} · ` : '') +
      `平均 ${h.avgMbitps.toFixed(1)} Mbit/s`;
    ul.appendChild(li);
  }
}

/* ---------- 主流程 ---------- */
const SERIES_COLORS = ['#38bdf8', '#fbbf24', '#a78bfa', '#34d399', '#fb7185', '#f97316', '#22d3ee', '#a3e635'];

function ensureSeries(mode, dir, packetKey, packetLabel) {
  // 同一 (链路+方向+包长) 重新测试 → 替换旧系列；不同项 → 缓存追加
  const key = `${mode}|${dir}|${packetKey || ''}`;
  const now = Date.now();
  for (let i = state.series.length - 1; i >= 0; i--) {
    if (state.series[i].key === key) state.series.splice(i, 1);
  }
  const color = SERIES_COLORS[state.series.length % SERIES_COLORS.length];
  const series = { key, id: now, mode, dir, packetKey, packetLabel, ts: now, color, points: [] };
  state.series.push(series);
  return series;
}

async function start() {
  if (state.running) return;
  const cfg = getConfig();
  $('serverHost').textContent = cfg.server;
  // 结果表：不清空，缓存展示。重新测同项时替换对应行。
  live.reset();
  state.t0 = Date.now();
  state.durations = cfg.duration;
  setRunning(true);
  startUITimer();
  $('valDown').textContent = '0.0';
  $('valUp').textContent = '0.0';

  const modes = cfg.mode === 'both' ? ['tcp', 'udp'] : [cfg.mode];
  const packetKey = `${cfg.packetKind}|${cfg.packetLen}`;
  const packetLabel = cfg.packetLabel;
  try {
    for (const mode of modes) {
      // each 实际链路单独一条缓存曲线；同链路+同包长才替换旧行/旧曲线
      removeResultRow(mode, cfg.dir, packetKey);
      state.curSeries = ensureSeries(mode, cfg.dir, packetKey, packetLabel);
      renderLegend(state.series);
      state.phaseText = `正在测试 ${mode.toUpperCase()} · ${cfg.dir === 'up' ? '上行' : cfg.dir === 'down' ? '下行' : '双向'}`;
      const phaseStartedAt = performance.now();
      const results = (mode === 'tcp') ? await runTCP(cfg) : await runUDP(cfg);
      if (results && results.allFailed) {
        throw new Error(results.message || `所有 ${mode.toUpperCase()} 流均失败`);
      }
      const phaseDuration = Math.max((performance.now() - phaseStartedAt) / 1000, 0.001);
      const phasePeak = peakFromPoints(state.curSeries ? state.curSeries.points : [], cfg.dir);
      const agg = aggregate(results, cfg.dir, phaseDuration, phasePeak, {
        packetLen: cfg.packetLen, packetLabel: cfg.packetLabel,
        packetKind: cfg.packetKind, packetSizes: cfg.packetSizes,
      });
      if (!agg) throw new Error(`所有 ${mode.toUpperCase()} 流均失败`);
      addResultRow(mode, cfg.dir, agg);
      if (!agg.incomplete) {
        saveHistory({ ts: Date.now(), mode, dir: cfg.dir, avgMbitps: agg.avgMbitps, packetLabel: agg.packetLabel, packetLen: agg.packetLen, packetKind: agg.packetKind });
      }
    }
    state.curSeries = null;
    state.phaseText = '测试完成';
  } catch (err) {
    console.error(err);
    const tbody = document.querySelector('#resultTable tbody');
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan="8" style="color:var(--red)">测速失败: ${(err.message || '').replace(/\n/g, '<br>')}</td>`;
    tbody.appendChild(tr);
  } finally {
    for (const s of state.streams) {
      try { if (typeof s.close === 'function') s.close(); } catch (e) {}
    }
    state.streams.clear();
    setRunning(false);
    stopUITimer();
    drawChart();
  }
}

function stop() {
  for (const s of state.streams) {
    try { if (typeof s.close === 'function') s.close(); } catch (e) {}
  }
  state.streams.clear();
  live.reset();
}

/* 刷新清空：手动清掉缓存的曲线与结果（history 本地历史保持） */
function refreshAll() {
  state.series = [];
  document.querySelector('#resultTable tbody').innerHTML = '';
  $('valDown').textContent = '0.0';
  $('valUp').textContent = '0.0';
  const el = $('liveDetail');
  if (el) {
    el.textContent = '就绪';
    const bar = $('progressBar');
    if (bar) bar.style.width = '0%';
  }
  drawChart();  // 内部会 renderLegend([])
}

/* ---------- 画布自适应（手机竖屏） ---------- */
// 桌面保持 1100×300；窄屏（<700px）按容器宽度重设画布实际像素，
// 并保持一个适合竖屏的宽高比，文字随画布重绘而不缩小变形。
function fitChartToViewport() {
  const canvas = $('chart');
  if (!canvas) return;
  const w = canvas.clientWidth || canvas.parentElement.clientWidth;
  if (w < 10) return;
  if (w < 700) {
    const targetW = Math.round(w);
    const targetH = Math.max(170, Math.round(w * 0.62)); // 竖屏比例，避免过矮
    if (canvas.width !== targetW || canvas.height !== targetH) {
      canvas.width = targetW;
      canvas.height = targetH;
      drawChart(); // canvas 尺寸变了，按新尺寸重绘
    }
  } else if (canvas.width !== 1100 || canvas.height !== 300) {
    canvas.width = 1100;
    canvas.height = 300;
    drawChart();
  }
}

/* ---------- 绑定 ---------- */
document.addEventListener('DOMContentLoaded', () => {
  $('serverHost').textContent = location.host;
  $('btnStart').addEventListener('click', start);
  $('btnStop').addEventListener('click', stop);
  $('btnRefresh').addEventListener('click', refreshAll);
  $('btnClearHistory').addEventListener('click', () => {
    localStorage.removeItem('speedtest-history');
    renderHistory();
  });
  $('btnCustomPacket').addEventListener('click', askCustomPacket);
  $('cfgPacketLen').addEventListener('change', () => {
    // 自定义档位未设置时，切过去提示用户先填
    const k = packetSelMeta().kind;
    if ((k === 'custom-fixed' && !CUSTOM_PACKET['custom-fixed']) ||
        (k === 'custom-dyn' && !CUSTOM_PACKET['custom-dyn'])) {
      askCustomPacket();
    }
  });
  renderHistory();
  fitChartToViewport();
  drawChart();
  window.addEventListener('resize', () => { fitChartToViewport(); });
});

/* 自定义包长：固定 → 单个字节数；动态 → 逗号分隔的包长序列。 */
function askCustomPacket() {
  const sel = $('cfgPacketLen');
  const psm = packetSelMeta();
  const kind = psm.kind;   // custom-fixed | custom-dyn
  const opt = sel.options[sel.selectedIndex] || sel;
  const isDyn = kind === 'custom-dyn';
  const hint = isDyn
    ? '动态包长：逗号分隔的多个字节数（如 512,1200,1400,1500）'
    : '固定包长：单个字节数（如 1400）';
  const current = isDyn
    ? (CUSTOM_PACKET['custom-dyn'] || []).join(',')
    : (CUSTOM_PACKET['custom-fixed'] || '1400');
  const input = window.prompt(`${hint}\n当前: ${current}`, current);
  if (input === null) return;   // 取消
  const cleaned = input.trim();
  if (!cleaned) { alert('请输入有效包长'); return; }
  if (isDyn) {
    const sizes = cleaned.split(/[,，;；\s]+/).map(Number).filter((n) => n > 0 && n <= 8 * 1024 * 1024);
    if (!sizes.length) { alert('动态包长请输入大于 0 的字节数，逗号分隔'); return; }
    CUSTOM_PACKET['custom-dyn'] = sizes;
    opt.setAttribute('data-label', `自定义·动态 ${sizes.join('/')}B`);
    opt.textContent = `自定义 · 动态 (${sizes.join('/')}B)`;
  } else {
    const n = Number(cleaned);
    if (!(n > 0 && n <= 8 * 1024 * 1024)) { alert('固定包长请输入 1 个有效的字节数'); return; }
    CUSTOM_PACKET['custom-fixed'] = n;
    sel.value = 'custom-fixed';
    const opt2 = sel.options[sel.selectedIndex] || sel;
    opt2.setAttribute('data-label', `自定义·固定 ${n}B`);
    opt2.textContent = `自定义 · 固定 (${n}B)`;
  }
}

// 调试/测试钩子（对前端无副作用）
if (typeof window !== 'undefined' && window !== undefined) {
  window.__speedTestCore = {
    aggregate, addResultRow, removeResultRow, drainedBytes, waitForBufferedDrain,
    peakFromPoints, makeStreamResult, runTCPStream, runUDPStream,
  };
}

})();
