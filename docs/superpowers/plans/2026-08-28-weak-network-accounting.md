# Weak-Network Accounting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make browser TCP/WebRTC results reconcile under weak-network buffering by using protocol-defined completion boundaries, excluding queued bytes, and computing aggregate rates from aggregate bytes and one elapsed interval.

**Architecture:** WebSocket TCP uses ordered `start → binary data → stop/result` messages so success is declared only after both endpoints share the same boundary. WebRTC keeps its reliable signaling socket open during a bounded DataChannel drain phase and reports submitted, queued, drained, and received bytes separately. Pure frontend accounting helpers and server drain helpers provide deterministic test seams.

**Tech Stack:** Go, Gorilla WebSocket, Pion WebRTC v4, browser JavaScript, Node.js built-in `node:test` and `vm`.

**Spec:** `docs/superpowers/specs/2026-08-28-weak-network-accounting-design.md`

## Global Constraints

- Do not change the native `speedlite-cli` TCP or UDP wire protocols.
- Do not add npm dependencies; frontend tests use Node built-ins only.
- Browser UDP reports application-layer delivery loss, not IP-layer packet loss or jitter.
- A browser stream without a server result is a failed stream, never a successful local-only estimate.
- Normal TCP completion must reconcile application payload bytes within 1%.
- Queue drain timeout is 3 seconds; WebRTC in-flight grace is 800 milliseconds.
- Existing user files outside the listed task files must remain untouched.

---

### Task 1: Pure accounting helpers and aggregate invariants

**Files:**
- Create: `src/cmd/speedlite-server/web/app.test.mjs`
- Modify: `src/cmd/speedlite-server/web/app.js:58-105,264-280,524-580`

**Interfaces:**
- Produces: `drainedBytes(submitted: number, queued: number): number`.
- Produces: `makeStreamResult(meter, dir, duration, submittedBytes?, queuedBytes?): StreamResult`.
- Produces: `aggregate(results, dir, phaseDuration?): AggregateResult`.
- Exposes these functions through `window.__speedliteCore`.

- [ ] **Step 1: Write the frontend harness and failing queued-byte test**

Use `node:test`, `node:assert/strict`, `fs`, and `vm` to load the real `app.js` with a no-op DOM. Assert:

```js
test('drainedBytes excludes queued data without underflow', () => {
  assert.equal(core.drainedBytes(10 * MiB, 6.37 * MiB), 3.63 * MiB);
  assert.equal(core.drainedBytes(1 * MiB, 2 * MiB), 0);
});
```

- [ ] **Step 2: Run RED**

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Expected: FAIL because `core.drainedBytes` is missing.

- [ ] **Step 3: Implement and expose `drainedBytes`**

```js
function drainedBytes(submitted, queued) {
  return Math.max(0, Number(submitted || 0) - Math.max(0, Number(queued || 0)));
}
```

- [ ] **Step 4: Run GREEN**

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Expected: PASS.

- [ ] **Step 5: Write a failing aggregate-average test**

Create two successful stream results totaling 8 MiB over a shared 8-second phase, but give them bogus independent averages. Assert `avgMbitps === 8 * MiB * 8 / 8 / 1e6` and is not their summed average.

- [ ] **Step 6: Run RED**

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Expected: FAIL because current `aggregate()` sums per-stream averages.

- [ ] **Step 7: Compute rates from aggregate bytes and one duration**

Use `phaseDuration` when supplied, otherwise the maximum successful stream duration. Compute directional and combined averages from aggregate bytes. Preserve byte reconciliation and bound deviation to 0–100%.

- [ ] **Step 8: Add delivery-loss tests and run GREEN**

Assert that UDP down with 10 MiB submitted, 4 MiB queued, and 5 MiB received uses 6 MiB drained, reports `1/6 × 100` delivery loss, exposes 4 MiB queued, and sets `truncated=true`.

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Expected: all tests PASS.

- [ ] **Step 9: Commit Task 1**

Run: `git add src/cmd/speedlite-server/web/app.js src/cmd/speedlite-server/web/app.test.mjs && git commit -m "fix: make browser accounting queue-aware"`

---

### Task 2: Ordered WebSocket completion protocol

**Files:**
- Create: `src/internal/wsstream/wsstream_test.go`
- Modify: `src/internal/wsstream/wsstream.go:112-253`
- Modify: `src/cmd/speedlite-server/web/app.js:282-407`
- Modify: `src/cmd/speedlite-server/web/app.test.mjs`

**Interfaces:**
- Consumes: Task 1 accounting helpers.
- Produces: browser control message `{"type":"stop"}`.
- Produces: server result only after the down writer finishes and, for up/both, the reader observes stop.

- [ ] **Step 1: Add a failing TCP-up integration test**

Use `httptest.Server` and Gorilla WebSocket. Read `start`, write three known binary messages, write `{"type":"stop"}`, read result, and assert exact byte equality.

- [ ] **Step 2: Run RED**

Run: `cd src && go test ./internal/wsstream -run TestUpResultWaitsForStop -count=1 -v`

Expected: FAIL because the reader ignores text stop and ends on its timer.

- [ ] **Step 3: Implement stop-aware reading**

Decode text frames into a small control struct. Close `upDone` exactly once on stop; count binary frames until stop. Treat disconnect before stop as failed completion.

- [ ] **Step 4: Wait for both protocol boundaries**

Down waits for its duration-controlled writer. Up waits for `upDone` with a `duration + 10 seconds` watchdog. Both waits for down completion and `upDone`. Only then tick samplers and write result.

- [ ] **Step 5: Run TCP-up GREEN**

Run: `cd src && go test ./internal/wsstream -run TestUpResultWaitsForStop -count=1 -v`

Expected: PASS.

- [ ] **Step 6: Add down ordering and early-disconnect tests**

Assert result follows all binary frames and has the same byte count. Assert closing before stop never yields a successful up result.

- [ ] **Step 7: Run server suite**

Run: `cd src && go test ./internal/wsstream -count=1 -v`

Expected: all tests PASS.

- [ ] **Step 8: Add a failing fake-WebSocket browser test**

Assert elapsed timeout sends stop but does not call `doneCb`; receiving result freezes local bytes after preceding binary messages; close before result returns an error.

- [ ] **Step 9: Implement browser start/stop/result completion**

Start timers only after server start. At duration, stop enqueueing and send stop for up/both. Never create a successful result from the timer. On result, build local result and close; on watchdog/early close, fail.

- [ ] **Step 10: Run frontend and server GREEN**

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Run: `cd src && go test ./internal/wsstream -count=1`

Expected: all tests PASS.

- [ ] **Step 11: Commit Task 2**

Run: `git add src/internal/wsstream/wsstream.go src/internal/wsstream/wsstream_test.go src/cmd/speedlite-server/web/app.js src/cmd/speedlite-server/web/app.test.mjs && git commit -m "fix: align websocket accounting boundaries"`

---

### Task 3: WebRTC drain accounting and signaling stop

**Files:**
- Modify: `src/internal/engine/engine.go:181-190`
- Modify: `src/internal/engine/engine_test.go`
- Create: `src/internal/rtcbridge/accounting_test.go`
- Modify: `src/internal/rtcbridge/rtcbridge.go:32-224`
- Modify: `src/cmd/speedlite-server/web/app.js:409-522`
- Modify: `src/cmd/speedlite-server/web/app.test.mjs`

**Interfaces:**
- Extends `engine.Stats` with `SubmittedBytes`, `QueuedBytes`, `UpBytes`, `DownBytes`, and `Truncated`.
- Produces: `drainSnapshot(submitted, buffered uint64) (drained, queued uint64, truncated bool)`.
- Produces: signaling stop with `queued_bytes` after drain or timeout.

- [ ] **Step 1: Add a failing Stats JSON test**

Marshal a populated Stats and assert keys `submitted_bytes`, `queued_bytes`, `up_bytes`, `down_bytes`, and `truncated`.

- [ ] **Step 2: Run RED**

Run: `cd src && go test ./internal/engine -run TestStatsQueueFieldsJSON -count=1 -v`

Expected: compile failure because the fields are missing.

- [ ] **Step 3: Extend Stats and run GREEN**

Add the five typed fields with backward-compatible JSON tags.

Run: `cd src && go test ./internal/engine -run TestStatsQueueFieldsJSON -count=1 -v`

Expected: PASS.

- [ ] **Step 4: Add failing drain arithmetic tests**

Cover 10 MiB submitted/6.37 MiB buffered, zero buffer, and buffer larger than submitted. Assert no unsigned underflow and truncation iff queued is nonzero.

- [ ] **Step 5: Run RED**

Run: `cd src && go test ./internal/rtcbridge -run TestDrainSnapshot -count=1 -v`

Expected: compile failure because `drainSnapshot` is missing.

- [ ] **Step 6: Implement pure snapshot and bounded drain**

Add `drainSnapshot` plus `waitForDrain(buffered func() uint64, timeout time.Duration)`. Production timeout is 3 seconds; tests inject short timeouts.

- [ ] **Step 7: Make signaling duplex and finalization queue-aware**

Continue reading signaling after SDP. For up/both wait for browser stop, then 800 ms in-flight grace. For down/both stop producing at duration, wait for buffer drain, and populate submitted/queued/down fields. Serialize signal writes and stop transitions.

- [ ] **Step 8: Change browser UDP completion**

Start timing on DataChannel open, stop enqueueing at duration, drain for up to 3 seconds, send signaling stop, keep DataChannel open until result, and treat missing result as failure.

- [ ] **Step 9: Add browser drain and truncation tests**

With fake DataChannel/signaling objects, assert queued bytes are excluded, stop reports residual, result is mandatory, and `doneCb` runs once.

- [ ] **Step 10: Run Task 3 suites**

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Run: `cd src && go test ./internal/engine ./internal/rtcbridge -count=1`

Expected: all tests PASS.

- [ ] **Step 11: Commit Task 3**

Run: `git add src/internal/engine/engine.go src/internal/engine/engine_test.go src/internal/rtcbridge/rtcbridge.go src/internal/rtcbridge/accounting_test.go src/cmd/speedlite-server/web/app.js src/cmd/speedlite-server/web/app.test.mjs && git commit -m "fix: report webrtc drain and delivery accounting"`

---

### Task 4: UI semantics, docs, and full verification

**Files:**
- Modify: `src/cmd/speedlite-server/web/index.html:91-110`
- Modify: `src/cmd/speedlite-server/web/app.js:582-642`
- Modify: `src/cmd/speedlite-server/web/style.css:148-160`
- Modify: `README.md`
- Modify: `docs/用户手册.md`

**Interfaces:**
- Consumes: queue-aware aggregate and extended Stats.
- Produces: TCP status at 1%, UDP “交付损失”, queue truncation warning, and unavailable browser jitter text.

- [ ] **Step 1: Add failing rendering assertions**

Assert TCP 0.5% is normal, TCP 1.2% is abnormal, UDP residual renders delivery loss plus queue truncation, and browser jitter renders `—` instead of `0.0 ms`.

- [ ] **Step 2: Run RED**

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Expected: FAIL on old labels and thresholds.

- [ ] **Step 3: Update table rendering and notes**

Render submitted/drained/received/residual values, use the 1% TCP threshold, rename browser UDP loss, show jitter unavailable, and preserve mobile `data-label` behavior.

- [ ] **Step 4: Run rendering GREEN**

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Expected: PASS.

- [ ] **Step 5: Update README and user manual**

Document ordered TCP boundaries, WebRTC queue semantics, truncation, 1% TCP reconciliation, and the distinction between browser delivery loss and native UDP packet loss/jitter. Remove exact-browser-packet-loss claims.

- [ ] **Step 6: Format and run all verification**

Run: `cd src && gofmt -w internal/engine/engine.go internal/engine/engine_test.go internal/wsstream/wsstream.go internal/wsstream/wsstream_test.go internal/rtcbridge/rtcbridge.go internal/rtcbridge/accounting_test.go`

Run: `cd src && go test ./... -count=1`

Run: `cd src && node --check cmd/speedlite-server/web/app.js`

Run: `cd src && node --test cmd/speedlite-server/web/app.test.mjs`

Run: `cd src && go build -o /tmp/speedlite-server-verify ./cmd/speedlite-server && go build -o /tmp/speedlite-cli-verify ./cmd/speedlite-cli`

Expected: every command exits 0.

- [ ] **Step 7: Run loopback TCP smoke tests**

Start the verified server on unused loopback ports, run ordered up and down clients, and assert client bytes equal result bytes in both directions.

- [ ] **Step 8: Inspect final state**

Run: `git diff --check`

Run: `git status --short`

Run: `rg -n "DEBUG-|TODO|TBD" src README.md docs/用户手册.md`

Expected: no whitespace errors, no temporary instrumentation, and only intended changes.

- [ ] **Step 9: Commit Task 4**

Run: `git add src/cmd/speedlite-server/web/index.html src/cmd/speedlite-server/web/app.js src/cmd/speedlite-server/web/style.css src/cmd/speedlite-server/web/app.test.mjs README.md docs/用户手册.md && git commit -m "docs: explain weak-network delivery accounting"`
