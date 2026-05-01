# Thunder Phase 7 — Production-Ready Full Algorithm + Anthropic Compliance

> **Date**: 2026-05-01
> **Branch**: `feat/thunder` → 8 sub-branches per milestone
> **Author**: Weili (algorithm), Claude (SMG/Rust integration)
> **Goal**: Ship `--policy thunder` with full Python ThunderAgent algorithm + Anthropic-format support to production. Eliminate every gap catalogued in `docs/thunder/algorithm-gap-vs-python.md`.

## 1. Overview

### 1.1 Scope

Phase 7 closes **all 7 algorithm gaps** between SMG's MVP Thunder (HEAD `6cf7970a`) and the Python ThunderAgent reference (`/home/hkang/wl/smg_thunder/ThunderAgent/`), **plus** fixes Python-side problems that are out-of-scope for Python:

- **Gap 5** (capacity leak bug — production-blocker)
- **Gap 6** (streaming bypasses Thunder state — primary user use case)
- **Gap 7** (uncalibrated token estimation — extended: per-program ratio + completion budget + time-decay)
- **Gap 1+2** (proactive pause + victim selection — defining ThunderAgent behavior)
- **Gap 3** (BFD greedy_resume — capacity utilization optimum)
- **Gap 4** (targeted Notify wake — eliminates thundering-herd)
- **Streaming retry × idempotency** (extends D-9 retry policy to streams)
- **Anthropic format compliance** (prompt caching token semantics + cross-protocol calibration — Python doesn't handle these)

Scope explicitly **excludes** Phase 8 items (gRPC validation, profiling endpoints, deployment runbook).

### 1.2 Decomposition: 8 milestones

| M | Title | Gaps | LOC est. (code/test) |
|---|---|---|---|
| M1 | ProgramRequestGuard capacity leak fix | 5 | 30 / 80 |
| M2 | SSE streaming wire-up (3 protocols + rewrite + strip + incremental + guard) | 6 | 390 / 280 |
| M3 | Full token calibration (per-program + global + completion budget + time-decay) | 7 | 90 / 160 |
| M4 | Proactive pause + victim selection + status state machine | 1, 2 | 250 / 250 |
| M5 | BFD greedy_resume (replaces broadcast-then-each-checks) | 3 | 200 / 200 |
| M6 | Targeted Notify (depends on M5 selecting winners) | 4 | 40 / 40 |
| M7 | Streaming retry × idempotency | — (extension of D-9) | 60 / 60 |
| M8 | Anthropic prompt caching + cross-protocol calibration | — (Python gap) | 80 / 70 |
| **Total** | | | **1140 / 1140 ≈ 2280 LOC** |

Wall-clock estimate: **2-3 weeks at 100 LOC/day** including review and iteration.

### 1.3 Dependency graph

```
                ┌── #2 (M2) ──┬── #3 (M3) ──┐
                │              │              │
   #1 (M1) ─────┤              ├── #7 (M7)   ├── #8 (M8)
                │              │              │
                └── #4 (M4) ── #5 (M5) ── #6 (M6)
```

Critical path: #1 → #2 → #3 → #8 (depth 4). Parallelizable: #4 starts after #1; #7 starts after #2.

### 1.4 What "production-ready" means here

After Phase 7 ships, SMG can claim **all** rows in `algorithm-gap-vs-python.md` "What SMG can/cannot legitimately claim" table flip to ✅, with documented intentional divergences for the 5 Python-vs-SMG behavior differences (Section 8).

---

## 2. Reference: Gap inventory

This section refers to `docs/thunder/algorithm-gap-vs-python.md` (HEAD `6666cf83`) as authoritative. The gaps re-mapped to milestones:

| Gap | Reference doc § | Milestone | Severity at MVP |
|---|---|---|---|
| 1. No proactive pause | algorithm-gap §Gap 1 | M4 | high (algorithm fidelity) |
| 2. No victim selection | algorithm-gap §Gap 2 | M4 (paired) | high |
| 3. BFD → least-active | algorithm-gap §Gap 3 | M5 | medium (capacity utilization) |
| 4. Broadcast Notify | algorithm-gap §Gap 4 | M6 | low (engineering) |
| 5. RAII guard incomplete | algorithm-gap §Gap 5 | **M1** | **production-blocker bug** |
| 6. Streaming bypasses state | algorithm-gap §Gap 6 | M2 | high (user use case) |
| 7. Token estimate uncalibrated | algorithm-gap §Gap 7 | M3 | medium |

Plus 2 items not catalogued in the gap doc but raised here as production requirements:

| Item | Source | Milestone |
|---|---|---|
| Streaming retry × idempotency (D-9 extension) | post-mvp-followups Tier 2 row 3 | M7 |
| Anthropic cache_read_input_tokens semantics + cross-protocol calibration | algorithm-gap §Open Question 5 | M8 |

---

## 3. Milestone designs

### 3.1 M1 — ProgramRequestGuard capacity leak fix

**Problem**: `ProgramRequestGuard::Drop` (`policies/thunder.rs:470-501`) decrements `program.in_flight` and broadcasts `notify_waiters()` but never subtracts `estimated_reserved_tokens` from `backend.active_program_tokens`. On any client disconnect the reservation accumulates → backends look saturated → all new TR-mode requests pause until force-resume timeout (30 min default). Production blocker.

**Fix**: Mirror `usage_consumer_task`'s un-reserve logic (`thunder.rs:336-355`) inside Drop's spawned cleanup task, but skip the "+ actual_total_tokens" step (no actual usage event arrived):

```rust
impl Drop for ProgramRequestGuard {
    fn drop(&mut self) {
        if self.completed { return; }
        let Some(state) = self.state.upgrade() else { return; };
        let pid = std::mem::take(&mut self.program_id);
        tokio::spawn(async move {
            let mut guard = state.write().await;

            let (reserved, backend_url) = guard.programs.get(&pid)
                .map(|p| (p.estimated_reserved_tokens, p.backend_url.clone()))
                .unwrap_or((0, None));

            if let Some(url) = backend_url {
                if let Some(b) = guard.backends.get_mut(&url) {
                    b.active_program_tokens = b.active_program_tokens.saturating_sub(reserved);
                }
            }
            if let Some(p) = guard.programs.get_mut(&pid) {
                p.estimated_reserved_tokens = 0;  // prevent double-unreserve
                if p.in_flight > 0 { p.in_flight -= 1; }
            }

            let waiting: Vec<Arc<Notify>> = guard.waiting_events.values().cloned().collect();
            drop(guard);
            for n in &waiting { n.notify_waiters(); }

            trace!(program_id = %pid, reserved_unwound = reserved, "Drop fallback");
        });
    }
}
```

**Why we DON'T also remove the Program** (cf. Python's `force_terminate_program`): in SMG, Programs are long-lived across many requests. ProgramRequestGuard is per-REQUEST. Drop firing on a single request does not mean the Program ended — that's the role of the `release_program` admin endpoint (deferred to Phase 8).

**Idempotency**: protected by `if self.completed { return; }` (line 472). Successful path calls `guard.complete()`; Drop fallback only fires when complete() wasn't called.

**Observability**: `trace!` per drop with `reserved_unwound`. After M2 ships: `warn!` if streaming usage missing (since post-M2 the "no usage event" case should be rare).

**Tests** (added to `mod tests` in `policies/thunder.rs`):
1. `test_drop_unreserves_estimated_tokens` — full happy path: reserve 500, drop, await async cleanup, assert backend back to 0
2. `test_complete_suppresses_drop_unreserve` — success path: reserve 500, complete(), drop, assert no Drop-side cleanup ran
3. `test_drop_with_no_program` — defensive: program already removed; Drop must not panic
4. `test_drop_idempotent_with_saturating_sub` — manually corrupted state where reserved > backend.active_program_tokens; must saturate to 0 not underflow

### 3.2 M2 — SSE streaming wire-up

**Problem**: streaming requests (`router.rs:794-861`) bypass Thunder entirely — no usage extracted, no Program counters updated, no guard wired. 90% of user traffic is streaming (`/v1/messages` + `/v1/responses`).

#### 3.2.1 New module layout

```
model_gateway/src/sse/
├── mod.rs           // public API: SseProtocol, ParsedUsage, ParsedChunk, SseExtractor
├── extractor.rs     // SseExtractor state machine (buffering + line splitting)
├── openai_chat.rs   // OpenAI Chat Completions (/v1/chat/completions) parser
├── anthropic.rs     // Anthropic Messages (/v1/messages) parser
└── responses.rs     // OpenAI Responses (/v1/responses) parser
```

#### 3.2.2 Public API

```rust
// sse/mod.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SseProtocol {
    OpenAiChat,
    AnthropicMessages,
    OpenAiResponses,
}

#[derive(Debug, Clone, Default)]
pub struct ParsedUsage {
    pub total_tokens: u64,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,           // cache_read_input_tokens for Anthropic; cached_tokens for OpenAI
}

#[derive(Debug, Default)]
pub struct ParsedChunk {
    pub forward: Vec<u8>,                     // bytes to send to client (potentially stripped of usage)
    pub usage: Option<ParsedUsage>,           // Some() if usage was finalized in this chunk
    pub token_delta: u64,                     // incremental token estimate since last feed()
}

pub struct SseExtractor {
    protocol: SseProtocol,
    buffer: Vec<u8>,                          // cross-chunk-boundary buffer
    state: ProtocolState,                     // protocol-specific accumulator
    strip_usage_chunk: bool,                  // OpenAI Chat: strip usage chunk if client didn't ask
    last_reported_tokens: u64,
}

impl SseExtractor {
    pub fn new(protocol: SseProtocol, strip_usage_chunk: bool) -> Self;
    pub fn feed(&mut self, chunk: &[u8]) -> ParsedChunk;
    pub fn flush(&mut self) -> ParsedChunk;   // call on stream end to drain buffer
}
```

#### 3.2.3 Per-protocol behavior

| Dimension | OpenAI Chat | Anthropic Messages | OpenAI Responses |
|---|---|---|---|
| Endpoint | `/v1/chat/completions` | `/v1/messages` | `/v1/responses` |
| Usage finalization | last `data:` chunk before `[DONE]` (has `usage` field, `choices=[]`) | after `event: message_stop` (combine `message_start.usage.input_tokens` + last `message_delta.usage.output_tokens`) | `event: response.completed` payload `usage` field |
| Token-counting strategy | each `data:` event with non-empty `delta.content` → +1 (Python heuristic) | read `message_delta.usage.output_tokens` cumulative; delta = current − last_seen | each `event: response.output_text.delta` → +1 |
| Incremental update cadence | every 20 events | every `message_delta` (naturally sparse; ~5-20/stream) | every 20 events |
| Strip target | the `data:` event containing `usage` field (identified by JSON shape: `usage` present + `choices == []`) | none (Anthropic always emits usage events; client expects them) | none (response.completed always emitted; client expects) |
| Request rewrite | force `body.stream_options.include_usage = true`; record original intent for strip decision | none (no equivalent flag) | none (no equivalent flag) |
| Stream end signal | `data: [DONE]\n\n` | `event: message_stop\n` | `event: response.completed\n` followed by upstream connection close |

**Anthropic protocol depth dive**:

```
event: message_start
data: {"type":"message_start","message":{"id":"...","usage":{"input_tokens":25,"cache_read_input_tokens":10}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

... many content_block_delta events ...

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":15}}

event: message_stop
data: {"type":"message_stop"}
```

- `input_tokens` from `message_start` (final value, not updated)
- `output_tokens` cumulative from each `message_delta` (multiple message_delta possible, last one is final)
- `cache_read_input_tokens` from `message_start` (already counted toward input_tokens in Anthropic's accounting; for our calibration use `input_tokens − cache_read_input_tokens` as actual prefill — see M8)

**OpenAI Responses depth dive**:

```
event: response.created
data: {"type":"response.created","response":{"id":"resp_..."}}

event: response.output_item.added
data: {...}

event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"text"}

... many response.output_text.delta events ...

event: response.completed
data: {"type":"response.completed","response":{...,"usage":{"input_tokens":10,"output_tokens":50,"total_tokens":60}}}
```

#### 3.2.4 Request rewrite

In `routers/http/router.rs::send_typed_request` (around line 585), when `is_stream && policy.is_thunder()`:

```rust
let protocol = detect_protocol_from_endpoint(endpoint);
let original_user_explicitly_asked_for_usage = body.get("stream_options")
    .and_then(|s| s.get("include_usage"))
    .map(|v| v == &json!(true))
    .unwrap_or(false);

if matches!(protocol, SseProtocol::OpenAiChat) {
    let stream_opts = body.entry("stream_options").or_insert_with(|| json!({}));
    if let Some(obj) = stream_opts.as_object_mut() {
        obj.insert("include_usage".into(), json!(true));   // force, not setdefault
    }
}

let strip_usage_chunk = matches!(protocol, SseProtocol::OpenAiChat) && !original_user_explicitly_asked_for_usage;
let mut extractor = SseExtractor::new(protocol, strip_usage_chunk);
```

Note: `original_user_explicitly_asked_for_usage` is captured as a local variable, **not** stored in body — avoids polluting the upstream payload.

#### 3.2.5 Streaming spawn + guard wiring

Replace the existing fire-and-forget spawn (`router.rs:823-861`) with:

```rust
let mut guard = policy.create_guard(&program_id);
let usage_sender = policy.usage_sender().cloned();
let progress_sender = policy.streaming_progress_sender().cloned();    // M2 new — see Section 4
let pid_for_spawn = program_id.clone();
let backend_url_for_spawn = backend.url().to_string();

#[expect(clippy::disallowed_methods, reason = "fire-and-forget streaming relay")]
tokio::spawn(async move {
    let mut stream = res.bytes_stream();
    let mut stream_failed = false;
    let mut total_token_estimate: u64 = 0;

    while let Some(chunk_res) = stream.next().await {
        let chunk = match chunk_res {
            Ok(c) => c,
            Err(_) => { stream_failed = true; break; }
        };
        let parsed = extractor.feed(&chunk);

        if !parsed.forward.is_empty() {
            if tx.send(Ok(Bytes::from(parsed.forward))).await.is_err() {
                break;  // client disconnected — guard's Drop will handle un-reserve
            }
        }

        total_token_estimate = total_token_estimate.saturating_add(parsed.token_delta);
        if parsed.token_delta >= INCREMENTAL_TOKEN_INTERVAL {
            if let Some(tx) = &progress_sender {
                let _ = tx.send(StreamingProgressEvent {
                    program_id: pid_for_spawn.clone(),
                    delta_tokens: parsed.token_delta,
                });
            }
        }

        if let Some(usage) = parsed.usage {
            if let Some(tx) = &usage_sender {
                let _ = tx.send(UsageEvent {
                    program_id: Some(pid_for_spawn.clone()),
                    backend_url: backend_url_for_spawn.clone(),
                    prompt_tokens: usage.prompt_tokens.unwrap_or(0) as u32,
                    completion_tokens: usage.completion_tokens.unwrap_or(0) as u32,
                    total_tokens: usage.total_tokens as u32,
                    request_text_chars: prompt_chars,
                    cache_read_input_tokens: usage.cached_tokens.map(|t| t as u32),  // M8
                });
            }
            guard.complete();
            // continue loop: forward remaining chunks (trailing [DONE], etc.)
        }
    }

    // Stream ended — flush extractor's residual buffer
    let final_parsed = extractor.flush();
    if !final_parsed.forward.is_empty() {
        let _ = tx.send(Ok(Bytes::from(final_parsed.forward))).await;
    }
    if let Some(usage) = final_parsed.usage {
        // edge case: usage only revealed on flush (the trailing chunk needs combining
        // with prior buffered partial). Same emit + complete sequence as in-loop case.
        if let Some(tx) = &usage_sender {
            let _ = tx.send(UsageEvent { /* ...same fields... */ });
        }
        guard.complete();
    }

    // metrics + outcome recording (existing logic preserved)
    // ...

    // guard exits scope here; if complete() called → no-op; else → fallback un-reserve (M1)
});
```

#### 3.2.6 New helper: `update_program_tokens_streaming` via channel

Per Section 4 below: `LoadBalancingPolicy::streaming_progress_sender()` returns `Option<&UnboundedSender<StreamingProgressEvent>>`. ThunderPolicy spawns a `progress_consumer_task` that drains and updates `Program.total_tokens`:

```rust
async fn progress_consumer_task(state: Arc<RwLock<RouterState>>, mut rx: UnboundedReceiver<StreamingProgressEvent>) {
    while let Some(event) = rx.recv().await {
        let mut guard = state.write().await;
        if let Some(p) = guard.programs.get_mut(&event.program_id) {
            p.total_tokens = p.total_tokens.saturating_add(event.delta_tokens);
        }
    }
}
```

This isolates lock contention to the consumer task — the streaming spawn just sends to the channel and never awaits the lock. Mirrors P1's `usage_consumer` precedent.

#### 3.2.7 Tests

**Unit tests** (per protocol, in `sse/{openai_chat,anthropic,responses}.rs::tests`): minimum 6 cases each:

1. `feed_complete_stream_oneshot` — full stream in one buffer, usage extracted
2. `feed_partial_chunks_across_event_boundary` — same data split into 3 partials
3. `feed_partial_chunks_split_in_json` — split inside a `{"key":"value"}` JSON
4. `feed_no_usage_chunk_falls_through` — usage never extracted; Parsed.usage = None
5. `feed_strip_usage_chunk_when_enabled` — strip=true; usage chunk absent from forward
6. `feed_keep_usage_chunk_when_disabled` — strip=false; usage chunk present in forward
7. `feed_token_delta_increments` (per-protocol — different token-counting strategy)
8. `feed_handles_keepalive_comments` — `: keepalive\n\n` lines don't break parser

**E2E tests** (`e2e_test/thunder/test_phase7_streaming.py`): minimum 6 cases:

1. `test_openai_chat_streaming_emits_usage_to_thunder`
2. `test_openai_chat_streaming_strips_usage_when_client_didnt_ask`
3. `test_openai_chat_streaming_keeps_usage_when_client_explicitly_asked`
4. `test_anthropic_messages_streaming_emits_usage`
5. `test_openai_responses_streaming_emits_usage`
6. `test_streaming_disconnect_unreserves_via_drop` (validates M1 + M2 jointly)

### 3.3 M3 — Full token calibration

**Problem**: `estimate_request_tokens` (`thunder.rs:756`) hardcodes `chars / 4 + 256`, missing per-program adaptation, time-aging, and completion length signal.

#### 3.3.1 New fields

```rust
// thunder.rs Program
pub struct Program {
    // ...existing fields...
    pub local_char_to_token_ratio: Option<f64>,     // chars per token (prefill-side)
    pub local_completion_fraction: Option<f64>,     // completion_tokens / max_tokens (typical)
    pub last_calibration_at: Option<Instant>,
}

// thunder.rs RouterState
pub struct RouterState {
    // ...existing fields...
    pub global_char_to_token_ratio: Option<f64>,
    pub global_completion_fraction: Option<f64>,
    pub last_global_calibration_at: Option<Instant>,
}
```

#### 3.3.2 Calibration update logic in `usage_consumer_task`

```rust
let now = Instant::now();

// Per-program + global chars/token ratio (excludes cached prefill — see M8)
let actual_prefill = event.prompt_tokens.saturating_sub(event.cache_read_input_tokens.unwrap_or(0));
if event.request_text_chars > 0 && actual_prefill > 0 {
    let observed_ratio = event.request_text_chars as f64 / actual_prefill as f64;
    update_calibration_with_decay(
        &mut p.local_char_to_token_ratio,
        &mut p.last_calibration_at,
        observed_ratio,
        NEUTRAL_RATIO,
        now,
    );
    update_calibration_with_decay(
        &mut guard.global_char_to_token_ratio,
        &mut guard.last_global_calibration_at,
        observed_ratio,
        NEUTRAL_RATIO,
        now,
    );
}

// Per-program + global completion fraction (actual_completion / declared_max_tokens)
if let Some(max_tokens) = event.declared_max_tokens {
    if max_tokens > 0 && event.completion_tokens > 0 {
        let observed_fraction = (event.completion_tokens as f64 / max_tokens as f64).clamp(0.0, 1.0);
        update_calibration_with_decay(
            &mut p.local_completion_fraction,
            &mut p.last_calibration_at,
            observed_fraction,
            NEUTRAL_FRACTION,
            now,
        );
        update_calibration_with_decay(
            &mut guard.global_completion_fraction,
            &mut guard.last_global_calibration_at,
            observed_fraction,
            NEUTRAL_FRACTION,
            now,
        );
    }
}
```

#### 3.3.3 Decay-weighted EMA helper

```rust
const HALF_LIFE: Duration = Duration::from_secs(3600);  // 1 hour
const NEUTRAL_RATIO: f64 = 4.0;                          // chars/token fallback
const NEUTRAL_FRACTION: f64 = 0.5;                       // completion fallback
const EMA_ALPHA: f64 = 0.2;                              // weight of new observation

fn update_calibration_with_decay(
    stored: &mut Option<f64>,
    last_at: &mut Option<Instant>,
    observed: f64,
    neutral: f64,           // NEUTRAL_RATIO for chars/token; NEUTRAL_FRACTION for completion fraction
    now: Instant,
) {
    let decayed = match (*stored, *last_at) {
        (Some(prev), Some(t_old)) => {
            let elapsed = now.saturating_duration_since(t_old).as_secs_f64();
            let half_life_s = HALF_LIFE.as_secs_f64();
            let retain = (-elapsed * f64::consts::LN_2 / half_life_s).exp();
            retain * prev + (1.0 - retain) * neutral
        }
        _ => neutral,
    };

    let new_value = match *stored {
        None => observed,                                       // first observation: direct assign
        Some(_) => EMA_ALPHA * observed + (1.0 - EMA_ALPHA) * decayed,
    };

    *stored = Some(new_value);
    *last_at = Some(now);
}
```

The same helper handles both calibrations by accepting `neutral` as a parameter — callers pass `NEUTRAL_RATIO` (4.0) for char/token ratio, `NEUTRAL_FRACTION` (0.5) for completion fraction.

#### 3.3.4 Three-tier estimate lookup

```rust
fn estimate_request_tokens(
    &self,
    info: &SelectWorkerInfo<'_>,
    state: &RouterState,
    declared_max_tokens: Option<u64>,
) -> u64 {
    let chars = info.request_text.map(str::len).unwrap_or(0) as f64;

    // Per-program → global → 4.0 fallback
    let chars_per_token = info.program_id
        .and_then(|pid| state.programs.get(pid))
        .and_then(|p| p.local_char_to_token_ratio)
        .or(state.global_char_to_token_ratio)
        .unwrap_or(NEUTRAL_RATIO);

    let prompt_estimate = (chars / chars_per_token) as u64;

    // Completion estimate: max_tokens * fraction (per-program → global → 0.5 fallback)
    let completion_fraction = info.program_id
        .and_then(|pid| state.programs.get(pid))
        .and_then(|p| p.local_completion_fraction)
        .or(state.global_completion_fraction)
        .unwrap_or(NEUTRAL_FRACTION);

    let completion_estimate = match declared_max_tokens {
        Some(mt) if mt > 0 => (mt as f64 * completion_fraction) as u64,
        _ => 256,  // legacy fallback when client didn't declare max_tokens
    };

    prompt_estimate.saturating_add(completion_estimate)
}
```

#### 3.3.5 Wiring max_tokens through

`SelectWorkerInfo` already has access to the request body (via `info.request_text` ...) but not directly to `max_tokens`. M3 adds:

```rust
// SelectWorkerInfo<'a>
pub struct SelectWorkerInfo<'a> {
    // ...existing fields...
    pub declared_max_tokens: Option<u64>,
}
```

Routers extract `max_tokens` from the parsed body before calling `select_worker_async`. For each protocol:

- OpenAI Chat: `body.max_tokens` or `body.max_completion_tokens`
- Anthropic Messages: `body.max_tokens` (required field)
- Responses: `body.max_output_tokens`

#### 3.3.6 Tests

1. `test_first_observation_initializes` — None → directly assigned
2. `test_ema_subsequent_event` — Some(4.0), observe 5.0, no time elapsed → 0.2*5 + 0.8*4 = 4.2
3. `test_decay_with_elapsed_time` — Some(8.0) at t=0, observe 4.0 at t=3600s → decayed_old ≈ 0.5*8 + 0.5*4 = 6, then EMA: 0.2*4 + 0.8*6 = 5.2
4. `test_estimate_uses_per_program_first` — program has local 5.0, global 3.0 → use 5.0
5. `test_estimate_falls_through_to_global` — no program local, global 3.0 → use 3.0
6. `test_estimate_falls_through_to_neutral` — neither → 4.0
7. `test_completion_fraction_calibration` — observe 50 completion of max_tokens=100 → 0.5
8. `test_completion_estimate_uses_max_tokens_and_fraction` — max=2000, fraction=0.5 → 1000
9. `test_completion_falls_back_to_256_when_max_tokens_unknown` — None → 256

### 3.4 M4 — Proactive pause + victim selection

**Problem** (`algorithm-gap` Gap 1+2): SMG only checks capacity at admit time. Already-admitted programs run to completion regardless of incoming pressure. Python's `pause_until_safe` (router.py:685-717) does background-tick preemption: when backend over capacity, picks a victim and pauses it.

#### 3.4.1 New types

```rust
// thunder.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramStatus {
    Acting,      // mid-stream (output_text being generated by upstream); cannot interrupt cleanly
    Reasoning,   // request submitted, awaiting first token
    Idle,        // between requests
    Paused,      // not assigned to any backend; waiting for resume
}

pub struct Program {
    // ...existing fields...
    pub status: ProgramStatus,
    pub marked_for_pause: bool,       // true → pause when status transitions out of ACTING
    pub paused_at: Option<Instant>,   // for resume-timeout enforcement
}

impl Default for ProgramStatus {
    fn default() -> Self { ProgramStatus::Idle }
}
```

#### 3.4.2 Status transitions

| From | Trigger | To |
|---|---|---|
| Idle | request admitted (select_worker returns) | Reasoning |
| Reasoning | first byte received from upstream (200 OK header) | Acting |
| Acting | stream end / non-stream response complete | Idle |
| Acting (with marked_for_pause=true) | stream end | Paused (deferred pause taken) |
| Reasoning / Idle | scheduler picks as victim | Paused (immediate) |
| Paused | BFD greedy_resume picks for wake | Reasoning (re-admitted) |

#### 3.4.3 Scheduler tick task

Spawned in `ThunderPolicy::new` alongside capacity-poll, usage-consumer:

```rust
async fn scheduler_tick_task(
    state: Arc<RwLock<RouterState>>,
    interval: Duration,                                    // default 100ms (config.scheduler_tick_ms)
    capacity_freed_signal: Arc<Notify>,                    // M6: external trigger
) {
    let mut tick = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = capacity_freed_signal.notified() => {},   // re-run BFD immediately when capacity frees
        }

        let mut guard = state.write().await;

        // (a) Pause check: any backend over threshold? Pick victims until under.
        proactive_pause_pass(&mut guard);

        // (b) Resume check: BFD greedy_resume for paused programs (M5).
        try_greedy_resume(&mut guard);

        // (c) Force-admit timeout enforcement (existing P5+P6 logic, kept).
        check_force_admit_timeouts(&mut guard);
    }
}

fn proactive_pause_pass(guard: &mut RouterState) {
    let urls: Vec<String> = guard.backends.keys().cloned().collect();
    for url in urls {
        let (over, threshold, capacity) = {
            let b = guard.backends.get(&url).unwrap();
            let cap = b.capacity_tokens;
            let thr = (cap as f64 * (1.0 - capacity_reserved_fraction(b))) as u64;
            (b.active_program_tokens > thr, thr, cap)
        };
        if !over { continue; }

        loop {
            let b = guard.backends.get(&url).unwrap();
            if b.active_program_tokens <= threshold { break; }

            let Some(victim_pid) = pick_victim(&guard.programs, &url) else { break; };

            pause_until_safe(guard, &victim_pid, &url);
        }
    }
}

fn pick_victim(programs: &HashMap<String, Program>, backend_url: &str) -> Option<String> {
    programs.iter()
        .filter(|(_, p)| {
            p.backend_url.as_deref() == Some(backend_url)
            && p.status != ProgramStatus::Paused
            && !p.marked_for_pause
        })
        .min_by_key(|(_, p)| p.step_count)
        .map(|(pid, _)| pid.clone())
}

fn pause_until_safe(guard: &mut RouterState, pid: &str, url: &str) {
    let Some(p) = guard.programs.get_mut(pid) else { return; };

    if p.status == ProgramStatus::Acting {
        // Cannot interrupt mid-stream cleanly — defer
        p.marked_for_pause = true;
        return;
    }

    // Immediate pause (Reasoning or Idle)
    let reserved = p.estimated_reserved_tokens;
    p.status = ProgramStatus::Paused;
    p.paused_at = Some(Instant::now());
    p.estimated_reserved_tokens = 0;

    if let Some(b) = guard.backends.get_mut(url) {
        b.active_program_tokens = b.active_program_tokens.saturating_sub(reserved);
        b.active_programs.remove(pid);
    }

    // Add Notify for this program if not already present (handles re-pause case)
    guard.waiting_events.entry(pid.to_string()).or_insert_with(|| Arc::new(Notify::new()));
}
```

#### 3.4.4 marked_for_pause check points

Whenever a Program transitions Acting → Idle (or stream end), check `marked_for_pause` and apply deferred pause:

```rust
fn check_marked_for_pause_and_pause_if_set(guard: &mut RouterState, pid: &str) {
    let Some(p) = guard.programs.get_mut(pid) else { return; };
    if !p.marked_for_pause { return; }
    if p.status == ProgramStatus::Acting { return; }  // still mid-stream

    let url = p.backend_url.clone();
    p.marked_for_pause = false;
    if let Some(u) = url {
        pause_until_safe(guard, pid, &u);
    }
}
```

Call sites:
- `usage_consumer_task` after applying UsageEvent (non-streaming and streaming both pass through here)
- `ProgramRequestGuard::Drop` Drop fallback
- `streaming spawn` after successful guard.complete()

#### 3.4.5 Tests

1. `test_proactive_pause_picks_least_step_count_victim` — 3 programs at step_count 5, 10, 2 → victim is 2
2. `test_proactive_pause_skips_paused_and_marked` — already-Paused or marked_for_pause excluded
3. `test_acting_program_marked_for_pause_not_paused_immediately`
4. `test_marked_for_pause_taken_after_stream_end`
5. `test_status_transitions_via_select_worker_to_streaming_to_idle`
6. `test_proactive_pause_unreserves_estimated_tokens_from_backend`

### 3.5 M5 — BFD greedy_resume

**Problem** (`algorithm-gap` Gap 3): SMG's broadcast-Notify wake → all paused programs rush `pick_tr` → most fail capacity check → re-pause. Sub-optimal placement, thundering-herd.

#### 3.5.1 try_greedy_resume in scheduler tick

Runs after `proactive_pause_pass` in each tick:

```rust
fn try_greedy_resume(guard: &mut RouterState) {
    // Snapshot paused programs with their estimated tokens AND paused_at (for starvation boost — §3.5.2).
    let now = Instant::now();
    let mut paused: Vec<(String, u64, Option<Instant>)> = guard.programs.iter()
        .filter(|(_, p)| p.status == ProgramStatus::Paused)
        .map(|(pid, p)| (pid.clone(), estimate_resume_tokens(p, guard), p.paused_at))
        .collect();

    // Sort: priority-boosted programs first, then DESC by est within each tier (Python BFD step (a))
    paused.sort_by(|(_, a_est, a_paused_since), (_, b_est, b_paused_since)| {
        let a_priority = a_paused_since
            .map(|t| now.saturating_duration_since(t) > PAUSED_PRIORITY_BOOST_AFTER)
            .unwrap_or(false);
        let b_priority = b_paused_since
            .map(|t| now.saturating_duration_since(t) > PAUSED_PRIORITY_BOOST_AFTER)
            .unwrap_or(false);
        match (a_priority, b_priority) {
            (true, false) => std::cmp::Ordering::Less,    // a wins
            (false, true) => std::cmp::Ordering::Greater, // b wins
            _ => b_est.cmp(a_est),                         // same priority class → DESC by est
        }
    });

    let urls: Vec<String> = guard.backends.keys().cloned().collect();

    'next_program: for (pid, est, _) in paused {
        // Re-fetch sorted backends per iteration (capacities change as we assign)
        let mut backend_caps: Vec<(String, u64)> = urls.iter()
            .map(|u| {
                let b = guard.backends.get(u).unwrap();
                let remaining = b.capacity_tokens.saturating_sub(b.active_program_tokens);
                (u.clone(), remaining)
            })
            .collect();
        backend_caps.sort_by_key(|(_, c)| std::cmp::Reverse(*c));

        for (url, cap) in &backend_caps {
            if *cap >= est {
                wake_program_to(guard, &pid, url, est);
                continue 'next_program;
            }
        }
        // Doesn't fit anywhere — stays Paused for next tick
    }
}

fn estimate_resume_tokens(p: &Program, _guard: &RouterState) -> u64 {
    // Use last-known total_tokens as baseline (Python uses similar)
    // If new program (total_tokens=0), estimate by char ratio heuristic — but typically a paused program has
    // already run at least one step so total_tokens > 0
    p.total_tokens.max(NEUTRAL_RATIO as u64 * 100)  // floor at typical small request
}

fn wake_program_to(guard: &mut RouterState, pid: &str, url: &str, estimated: u64) {
    let Some(p) = guard.programs.get_mut(pid) else { return; };

    p.backend_url = Some(url.to_string());
    p.status = ProgramStatus::Reasoning;
    p.estimated_reserved_tokens = estimated;
    p.paused_at = None;

    if let Some(b) = guard.backends.get_mut(url) {
        b.active_program_tokens = b.active_program_tokens.saturating_add(estimated);
        b.active_programs.insert(pid.to_string());
    }

    if let Some(notify) = guard.waiting_events.get(pid) {
        notify.notify_one();   // ★ M6: targeted, not broadcast
    }
}
```

#### 3.5.2 Starvation mitigation

If a Paused program never fits (always ranked first by total_tokens but no backend has enough capacity), it would starve. Two-tier mitigation:

- **Tier 1** (built into the sort in §3.5.1): if program has been Paused > `PAUSED_PRIORITY_BOOST_AFTER` (default 900s = half of force_resume_timeout), it gets priority-boosted ahead of larger programs in the BFD ordering, giving it first shot at any free backend.
- **Tier 2** (existing P5+P6 logic, kept): if program still hasn't been resumed after `force_resume_timeout` (default 1800s), `force_admit_after_timeout` kicks in to pick least-active backend regardless of capacity.

```rust
const PAUSED_PRIORITY_BOOST_AFTER: Duration = Duration::from_secs(900);  // half of force_resume_timeout
```

The sort logic itself is shown inline in §3.5.1 above.

#### 3.5.3 Tests

1. `test_bfd_assigns_largest_program_to_most_remaining` — 3 programs (80k, 20k, 5k), 2 backends (100k, 30k) → 80→A, 20→B, 5→A
2. `test_bfd_skips_program_that_doesnt_fit` — program 200k, no backend has >100k → stays Paused
3. `test_bfd_capacities_decrement_within_tick` — after assigning 80k to A, A's remaining is 20k for next program
4. `test_starvation_priority_boost_after_threshold` — program paused 901s priority-boosted ahead of bigger programs

### 3.6 M6 — Targeted Notify (depends on M5)

**Problem** (Gap 4): broadcast `notify_waiters()` wakes all paused programs; all rush write lock; only one (or few) actually fit.

**Fix**: M5's `wake_program_to` already uses `notify.notify_one()`. M6 work:

1. **Delete** `notify_waiters()` calls in:
   - `usage_consumer_task` (`thunder.rs:363-368`)
   - `ProgramRequestGuard::Drop` fallback (`thunder.rs:492-497` — set up by M1)
2. **Replace** with sending a `BackendCapacityChanged` signal to the scheduler tick task:
   ```rust
   capacity_freed_signal.notify_one();   // wakes scheduler tick early; runs BFD now instead of waiting up to 100ms
   ```
3. The scheduler tick on receipt re-runs `try_greedy_resume` immediately.

**Pros**: scheduler is the only authority on "who wakes". No thundering-herd. Wake latency = at most one tokio::select! roundtrip (~100μs).

**Cons**: scheduler latency now matters. If scheduler tick is busy, wake is delayed. Mitigation: the `notify_one()` on `capacity_freed_signal` is a wake-up; tokio::select! already supports immediate wake.

#### 3.6.1 Tests

1. `test_no_broadcast_notify_on_capacity_free` — assert no `notify_waiters()` call sites in codebase via grep test
2. `test_capacity_freed_signal_wakes_scheduler` — manually trigger UsageEvent → assert scheduler tick runs within 5ms (vs 100ms tick interval)
3. `test_targeted_notify_only_wakes_chosen_program` — set up 3 paused programs; trigger BFD that picks one; assert only that one's Notify fires (use Notify wait counters or message-passing test harness)

### 3.7 M7 — Streaming retry × idempotency

**Problem**: Existing D-9 retry policy is non-streaming-only (per worklog). Streaming `tokio::spawn` is fire-and-forget; if upstream returns a 5xx before any 200 OK, currently the spawn just records failure and ends. With Thunder, this leaves a phantom in_flight + reservation until Drop fires. Worse: a transient 5xx on a backend should be retryable; but post-200-OK retry would replay chunks the client already saw.

#### 3.7.1 Retry boundary

Rule: **retry permitted only before the streaming relay starts emitting bytes to the client**. Once `tx.send(Ok(first_byte))` succeeds, no retry — the client is already committed to this stream.

#### 3.7.2 Implementation

Wrap the streaming send loop in a retry block that holds the guard across retries:

```rust
async fn route_streaming_with_retry(
    policy: Arc<dyn LoadBalancingPolicy>,
    body: Value,
    program_id: String,
    config: RetryConfig,
    /*...*/
) -> Result<Response, RouterError> {
    let mut guard = policy.create_guard(&program_id);  // ONE guard for the full retry cycle
    let mut retries_left = config.max_retries;
    let mut last_backend_url: Option<String> = None;

    loop {
        let info = SelectWorkerInfo {
            program_id: Some(&program_id),
            avoid_backend: last_backend_url.as_deref(),
            // ...
        };
        let chosen = policy.select_worker_async(workers, &info).await
            .ok_or(RouterError::NoBackend)?;
        let backend_url = chosen.url();

        let res = client.post(backend_url).json(&body).send().await;

        match res {
            Ok(r) if r.status().is_success() => {
                // 200 OK reached — past retry boundary
                let stream_response = spawn_streaming_relay(r, guard, ...);
                return Ok(stream_response);   // guard moves into spawn task
            }
            Ok(r) if r.status().is_server_error() && retries_left > 0 => {
                retries_left -= 1;
                last_backend_url = Some(backend_url.to_string());
                continue;   // retry on a different backend
            }
            Ok(r) => {
                // Non-retryable status (4xx) or exhausted retries
                return Err(RouterError::Upstream { status: r.status() });
            }
            Err(e) if retries_left > 0 => {
                retries_left -= 1;
                last_backend_url = Some(backend_url.to_string());
                continue;
            }
            Err(e) => return Err(RouterError::Network(e)),
            // guard naturally drops on returning Err → fallback un-reserve fires (M1)
        }
    }
}
```

#### 3.7.3 SelectWorkerInfo extension

```rust
pub struct SelectWorkerInfo<'a> {
    // ...existing fields...
    pub avoid_backend: Option<&'a str>,    // M7: don't pick this backend on retry
}
```

ThunderPolicy's `pick_tr` honors this by excluding `avoid_backend` from the candidate set.

#### 3.7.4 Tests

1. `test_streaming_retry_after_5xx_before_200_ok`
2. `test_streaming_no_retry_after_200_ok` — first byte sent → 5xx mid-stream → no retry, client sees broken stream
3. `test_streaming_retry_excludes_failed_backend`
4. `test_streaming_retry_preserves_single_in_flight` — assert in_flight stays 1 across 3 retry attempts
5. `test_streaming_retry_exhaustion_returns_error_and_unreserves` — 3 retries all 5xx → return error, guard drop fires fallback

### 3.8 M8 — Anthropic prompt caching + cross-protocol calibration

#### 3.8.1 Anthropic prompt caching

Anthropic Messages API exposes `cache_creation_input_tokens` and `cache_read_input_tokens` in usage:

```json
{"usage":{"input_tokens":300,"cache_read_input_tokens":250,"output_tokens":50}}
```

`input_tokens=300` includes both fresh prefill (50) and cached (250). For chars-to-tokens calibration, only fresh prefill should count (cached tokens don't have a 1:1 relationship with current request chars — they may correspond to system prompt embedded earlier).

**Fix in calibration**: use `actual_prefill = input_tokens − cache_read_input_tokens` (already shown in 3.3.2).

**Wiring**:

```rust
pub struct UsageEvent {
    // ...existing fields...
    pub cache_read_input_tokens: Option<u32>,    // None for OpenAI Chat / Responses
}
```

Anthropic SSE extractor (sse/anthropic.rs) reads `cache_read_input_tokens` from `message_start.usage` and passes it through to UsageEvent.

#### 3.8.2 Cross-protocol calibration

Same Program may use multiple protocols (e.g., agent uses /v1/chat/completions for tool calls and /v1/messages for reasoning). Different protocols use different tokenizers (OpenAI BPE vs Anthropic's tokenizer). chars-to-tokens ratios differ per protocol.

```rust
// Program field replacement
pub struct Program {
    pub local_char_to_token_ratio_by_protocol: HashMap<SseProtocol, f64>,
    pub local_completion_fraction_by_protocol: HashMap<SseProtocol, f64>,
    pub last_calibration_at_by_protocol: HashMap<SseProtocol, Instant>,
}
```

Estimate lookup becomes: `programs.get(pid).and_then(|p| p.local_char_to_token_ratio_by_protocol.get(&protocol)).copied()` etc.

UsageEvent gains `protocol: SseProtocol` field; `usage_consumer_task` updates the per-protocol entry.

#### 3.8.3 Tests

1. `test_anthropic_calibration_excludes_cache_read_tokens`
2. `test_protocol_specific_ratios_per_program` — same program: OpenAI Chat ratio 4.0 + Anthropic Messages ratio 3.5 don't pollute each other
3. `test_estimate_uses_per_protocol_ratio_when_available`
4. `test_cache_read_tokens_field_optional_for_non_anthropic` — UsageEvent with None doesn't break calibration

---

## 4. Cross-cutting decisions

### 4.1 Trait extension for streaming progress (matches P1 precedent)

Add `streaming_progress_sender` method on `LoadBalancingPolicy` returning `Option<&UnboundedSender<StreamingProgressEvent>>`, mirroring P1's `usage_sender` pattern:

```rust
// model_gateway/src/policies/mod.rs
#[derive(Debug, Clone)]
pub struct StreamingProgressEvent {
    pub program_id: String,
    pub delta_tokens: u64,
}

#[async_trait]
pub trait LoadBalancingPolicy: Send + Sync + Debug {
    // ...existing methods...

    fn streaming_progress_sender(&self) -> Option<&UnboundedSender<StreamingProgressEvent>> {
        None
    }
}
```

ThunderPolicy spawns a `progress_consumer_task` that drains progress events and updates `Program.total_tokens`. Other policies return None.

### 4.2 Mock vLLM extensions

`e2e_test/thunder/mock_vllm.py` gains:

| Mode | Purpose | Stream-mode flag |
|---|---|---|
| OpenAI Chat streaming with usage chunk | M2 | `--stream-mode openai-chat` |
| OpenAI Chat streaming without usage | M2 (verify SMG inject) | `--stream-mode openai-chat-no-usage` |
| Anthropic Messages streaming with cache_read_input_tokens | M2, M8 | `--stream-mode anthropic-messages` |
| Anthropic Messages streaming with no cache | M2 | `--stream-mode anthropic-messages-fresh` |
| OpenAI Responses streaming | M2 | `--stream-mode openai-responses` |
| Broken EOF mid-stream | M1 | `--stream-mode broken-eof` |
| 5xx then 200 (retry success) | M7 | `--stream-mode 5xx-then-200` |
| Capacity-saturated metrics | M4 | `--metrics-capacity-fraction 1.0` |
| Variable max_tokens with sub-fraction completion | M3 | `--completion-fraction 0.3` |

LOC: ~300 LOC added to `mock_vllm.py` (currently ~200).

### 4.3 Observability

| Location | Level | Message | Purpose |
|---|---|---|---|
| `ProgramRequestGuard::Drop` cleanup | trace | `"Drop fallback un-reserved {reserved} tokens"` | request lifecycle |
| `ProgramRequestGuard::Drop` cleanup | warn | `"streaming usage missing pid={pid}; using Drop fallback"` | post-M2 should be rare |
| `SseExtractor::feed` | trace | `"extracted usage: total={t} prompt={p}"` | protocol parse confirmation |
| `usage_consumer` ratio update | debug | `"calibrated ratio: {prev:.2} → {new:.2} (observed {obs:.2})"` | calibration health |
| `progress_consumer` increment | trace | `"progress: pid={pid} delta={d} cumulative={c}"` | incremental tracking |
| `scheduler_tick` proactive pause | info | `"paused victim pid={pid} on backend={url} (load {a}/{cap})"` | pause/resume audit |
| `scheduler_tick` BFD resume | info | `"resumed pid={pid} → backend={url} (est={est})"` | pause/resume audit |
| `route_streaming_with_retry` | warn | `"streaming retry: attempt={n} after status={s}"` | retry visibility |

### 4.4 SMG ↔ Python intentional divergences

Recorded in `algorithm-gap-vs-python.md` "Intentional divergences" section (added by this phase):

| # | Dimension | Python | SMG | Type |
|---|---|---|---|---|
| 1 | `include_usage` injection | setdefault (preserves user) | force override | intentional UX choice |
| 2 | response usage chunk | unconditionally forwarded | stripped if client didn't ask | client transparency |
| 3 | Anthropic incremental token counting | event-count (inaccurate) | cumulative output_tokens (accurate) | **fix Python's bug** |
| 4 | Per-program calibration | not present (Python only has global) | global + per-program two-tier | enhancement |
| 5 | Completion budget calibration | not present | per-program EMA on completion/max_tokens | enhancement |
| 6 | Time-decay on calibration | event-EMA only | event-EMA + wall-time half-life decay | enhancement |
| 7 | Anthropic cache_read_input_tokens | not handled (counted as fresh) | excluded from prefill ratio | **fix Python's bug** |
| 8 | Cross-protocol calibration | not present | per-protocol ratio per program | enhancement |
| 9 | Streaming retry boundary | implicit / undocumented | strict: 200 OK divides retry from no-retry | enhancement |

### 4.5 PR layout

| PR | Title | LOC (code/test) | Depends on |
|---|---|---|---|
| #1 | `fix(policies): ProgramRequestGuard::Drop un-reserves tokens (M1 Gap5)` | 30/80 | — |
| #2 | `feat(sse,thunder): streaming usage extraction across 3 protocols (M2 Gap6)` | 390/280 | #1 |
| #3 | `feat(policies): full token calibration with time-decay (M3 Gap7)` | 90/160 | #2 |
| #4 | `feat(policies): proactive pause + victim selection (M4 Gap1+2)` | 250/250 | #1 |
| #5 | `feat(policies): BFD greedy_resume in scheduler tick (M5 Gap3)` | 200/200 | #4 |
| #6 | `refactor(policies): targeted notify_one replacing broadcast (M6 Gap4)` | 40/40 | #5 |
| #7 | `feat(routers): streaming retry with 200-OK boundary (M7)` | 60/60 | #2 |
| #8 | `feat(thunder): Anthropic prompt caching + per-protocol calibration (M8)` | 80/70 | #2, #3 |

Per-PR conventions:
- Branch: `phase7-mN-<gap>` off `feat/thunder`
- Commit prefix matches Phase 0-6 style (`feat`, `fix`, `refactor`, `test`, `docs`)
- Each PR includes its own e2e test file in `e2e_test/thunder/`
- Worklog entry added per PR documenting D-23..D-37 progress

---

## 5. Worklog decisions (D-23..D-37)

| ID | Decision |
|---|---|
| **D-23** | Phase 7 = full production scope, 8 milestones; no deferrals |
| **D-24** | Critical path #1→#2→#3→#8; #4 parallelizable from #1; #7 from #2 |
| **D-25** | M2 SSE extraction in independent module `model_gateway/src/sse/`, not inline router code |
| **D-26** | M2 incremental token tracking via channel pattern (`StreamingProgressEvent`), mirrors P1 `usage_sender` |
| **D-27** | M2 force `include_usage=true` override (vs Python `setdefault`) + response strip if client didn't ask — both intentional divergence from Python |
| **D-28** | M2 Anthropic uses `message_delta.usage.output_tokens` cumulative (fixes Python's event-count inaccuracy for Anthropic) |
| **D-29** | M3 three-tier estimate lookup: per-program → global → 4.0 default; matches per-protocol after M8 |
| **D-30** | M3 completion budget calibration: per-program EMA on `completion_tokens / max_tokens`; default fraction 0.5; falls back to 256 absolute if max_tokens unknown |
| **D-31** | M3 time-decay: half_life = 3600s; decayed = `decay_weight * stored + (1 - decay_weight) * neutral`; applied before EMA update |
| **D-32** | M4 status state machine: `ProgramStatus { Acting, Reasoning, Idle, Paused }` + `marked_for_pause` flag; transition rules in §3.4.2 |
| **D-33** | M5 BFD greedy_resume in scheduler_tick_task; sort programs DESC by total_tokens, backends DESC by remaining capacity; per-program iteration re-sorts backends after each assignment |
| **D-34** | M5 starvation mitigation: priority boost for paused > 900s; force_admit_after_timeout at 1800s (kept from MVP) |
| **D-35** | M6 deletes `notify_waiters()` broadcasts in `usage_consumer` and `Drop`; replaces with `capacity_freed_signal` to scheduler |
| **D-36** | M7 streaming retry boundary = 200 OK header; no retry post-first-byte-sent (would corrupt client view) |
| **D-37** | M8 Anthropic `cache_read_input_tokens` excluded from prefill ratio; per-protocol calibration via `Program.local_char_to_token_ratio_by_protocol: HashMap<SseProtocol, f64>` |

---

## 6. Risk register

| Risk | Probability | Impact | Mitigation |
|---|---|---|---|
| SSE chunk boundary in UTF-8 multi-byte char | medium | medium | unit tests + `Vec<u8>` buffer (no premature String conversion) |
| Anthropic message_delta semantics unstable across versions | low | high | real Anthropic e2e test before M2 ships |
| Strip mis-removes non-usage chunk | medium | high | per-protocol unit tests with multi-variant chunks |
| `progress_consumer_task` lock contention at high concurrency streaming | low | medium | benchmark first; reduce update interval if needed |
| New trait method breaks downstream `LoadBalancingPolicy` impls | low | low | default implementation (returns None) protects |
| `__smg_*` markers accidentally leaked to upstream | low | high | router-scope variable, not body field (§3.2.4) |
| Scheduler tick at 100ms creates write-lock contention with N=50 backends × M=1000 programs | medium | medium | tick runs read-only decision phase first; write phase batched; optionally raise interval to 200ms |
| `marked_for_pause` check missed at some success path | high | medium | helper `check_marked_for_pause_and_pause_if_set` called from every success site; grep test |
| BFD starvation: large programs never fit | medium | medium | priority-boost + force_admit_after_timeout |
| M6 wake responsibility transfer breaks paused programs | medium | high | `BackendCapacityChanged` signal + scheduler re-runs BFD on each capacity-free event |
| Streaming retry replays already-sent chunks to client | high | high | strict 200-OK boundary; e2e test validates |
| `cache_read_input_tokens` missing on older vLLM/sglang backends | medium | medium | `Option<u32>` field; missing means saturating_sub yields 0 (treats all as fresh); accept slight calibration drift |
| Per-protocol HashMap lookup cost in hot path | low | low | enum-keyed HashMap is O(1) with tiny constant; `enum SseProtocol` has 3 variants, lookup essentially branch |
| Per-program ratio storage memory bloat at 100k+ programs | low | low | each Program adds ~80 bytes for the new fields; 100k programs = 8 MB total, fine |

---

## 7. Test strategy

### 7.1 Unit tests

Each milestone adds tests in the relevant Rust module's `mod tests`. Goal: 80%+ branch coverage on new code paths.

- Total new unit tests across M1-M8: ~50 cases
- Each milestone's tests pass in isolation: `cargo test --workspace -- thunder` per milestone branch

### 7.2 E2E tests

`e2e_test/thunder/` gains one test file per milestone:

| File | Tests | Mock mode |
|---|---|---|
| `test_phase7_m1_drop_unreserves.py` | 3 | broken-eof + capacity-saturated |
| `test_phase7_m2_streaming_3protocols.py` | 9 | openai-chat / anthropic / responses + variants |
| `test_phase7_m3_calibration.py` | 4 | variable completion fraction |
| `test_phase7_m4_proactive_pause.py` | 5 | capacity-saturated triggers preemption |
| `test_phase7_m5_bfd_resume.py` | 4 | multi-program multi-backend capacity scenarios |
| `test_phase7_m6_no_broadcast.py` | 3 | code-grep + capacity-event signal verification |
| `test_phase7_m7_streaming_retry.py` | 5 | 5xx-then-200 + post-200 no-retry |
| `test_phase7_m8_anthropic_cache.py` | 4 | anthropic with cache_read |

### 7.3 Pre-commit / CI

`make pre-commit` (fmt + check + test) clean per PR.
`cargo clippy --all-targets --all-features -- -D warnings` clean per PR.
e2e tests run manually via `pytest e2e_test/thunder/ -v` against real SLURM-hosted sglang for at least M2, M4, M5 (rest can run against mock).

---

## 8. Rollout / production readiness criteria

After all 8 PRs merge to `feat/thunder`, before merging to `main`:

1. ✅ All `algorithm-gap-vs-python.md` "What SMG can/cannot legitimately claim" rows flip to ✓
2. ✅ All 9 SMG↔Python intentional divergences documented in algorithm-gap doc with rationale
3. ✅ Soak test: 100 programs × 10 requests through `--policy thunder --thunder-sub-mode tr` against real SLURM sglang for 8 hours, no requests stuck > 5 min, no Tokio task leak, no memory leak
4. ✅ All 38 unit tests pass + 37 e2e tests pass
5. ✅ `cargo bench` (if added in M5) shows scheduler_tick_task overhead < 5% CPU at 100ms tick + 1000 programs

---

## 9. Cross-references

- `docs/thunder/algorithm-gap-vs-python.md` — gap inventory (input)
- `docs/thunder/handoff-streaming-and-pause-resume.md` — streaming gap discussion
- `docs/thunder/post-mvp-followups.md` — original MVP backlog
- `docs/thunder/worklog.md` — autonomous decision log (D-23..D-37 added by this phase)
- `docs/thunder/03-algorithm.md` — Python algorithm spec (the should-be reference)
- `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/scheduler/router.py` — Python ground truth
- `model_gateway/src/policies/thunder.rs` — current implementation
- `model_gateway/src/sse/` — new module created by M2
