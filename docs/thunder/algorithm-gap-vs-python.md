# SMG Thunder vs Python ThunderAgent — Algorithm Gap Inventory

> **Date**: 2026-05-01
> **Reference Python source**: `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/router.py:685-844` (read-only ground truth)
> **SMG implementation HEAD**: `thunder-policy` `6cf7970a`
>
> **Purpose**: Single canonical inventory of every place where SMG's Thunder implementation differs from the Python ThunderAgent algorithm. This complements but does **not** duplicate:
> - `docs/thunder/handoff-streaming-and-pause-resume.md` — streaming-specific gap analysis + simplified pause/resume scenario walkthrough
> - `docs/thunder/post-mvp-followups.md` — tier-organized backlog with repair LOC estimates
> - `docs/thunder/worklog.md` D-19 ~ D-22 — historical rationale for autonomous trims
>
> Why this doc exists: the user asked "现在 SMG 里面实现的 thunderagent 算法跟 python 版本有差距嘛" — answer is **yes, 7 distinct gaps**, and this is the structured catalog.

---

## TL;DR — The bottom line

**SMG's Thunder is a simplified, partially-faithful port of Python ThunderAgent.** Functionally it routes traffic with capacity awareness; algorithmically it lacks the 4 most distinctive ThunderAgent behaviors:

1. **No proactive pause** (gap 1) — SMG only checks capacity at admit time; doesn't pre-empt running programs to make room
2. **No victim selection** (gap 2) — when capacity is full, SMG can't choose *who* to pause based on age/cost
3. **No BFD optimal placement** (gap 3) — uses least-active heuristic instead of best-fit-decreasing bin-packing
4. **Streaming requests bypass Thunder entirely** (gap 6) — biggest gap for users whose traffic is mostly streaming (Anthropic Messages, OpenAI Responses APIs are typically streaming)

Plus 3 lower-tier gaps: broadcast-vs-targeted Notify (gap 4), incomplete RAII guard causing capacity leak on disconnect (gap 5, real bug), and uncalibrated token estimation (gap 7).

---

## The 7 gaps (catalog)

### Gap 1: Proactive pause — **MISSING**

**Python** (`router.py:685-717` — `pause_until_safe`):
- Background scheduler tick (every 100ms) iterates all backends
- For each backend over capacity threshold, **picks a victim program** running on it and pauses
- Mechanism: changes program status to PAUSED, registers in `waiting_queue`, releases its `active_program_tokens` from the backend

**SMG** (`policies/thunder.rs::pick_tr` only):
- Capacity check happens **only at request admit time**
- Already-admitted requests run to completion; no preemption
- No background scheduler tick exists for pause-decision-making (only the capacity-poll task exists, which is read-only metric refresh)

**User-visible behavior difference**:
- Scenario: 5 long-running programs fill backend; new high-priority program arrives
- Python: scheduler ticks → identifies oldest of the 5 as victim → pauses it → new program admits within 100ms
- SMG: new program waits for one of 5 to *naturally complete*, OR force-resume timeout (default 30 min) — whichever comes first

**Classification**: **Algorithm gap** (true semantic divergence).

**Repair**: ~100 LOC for scheduler tick + pause_until_safe logic. Requires interaction with gap 2 (need victim selection rule) and gap 6 (mark_for_pause for streaming).

---

### Gap 2: Victim selection — **MISSING**

**Python** (`pause_until_safe` body):
1. Find programs running on the over-capacity backend
2. Sort by `step_count` ASC (smallest first — youngest program, least progress wasted by interruption)
3. If victim is ACTING (mid-stream): set `marked_for_pause=true`; pause completes when stream ends naturally
4. If victim is non-ACTING (REASONING/idle): pause immediately; transition to (status, PAUSED)

**SMG**:
- `Program.step_count` field exists (added in P3) but is incremented on assign and **never read for victim choice** (because gap 1 means no victim is ever selected)
- No `marked_for_pause` flag on Program at all
- No status state machine; Program has just `backend_url`, `in_flight`, `total_tokens`, `step_count`, `estimated_reserved_tokens`

**Classification**: **Algorithm gap**, dependent on gap 1.

**Repair**: ~150 LOC — adds `Program.status: ProgramStatus { Acting, Reasoning, Idle, Paused }`, adds `marked_for_pause: bool`, encodes the 4-row state-transition table from `docs/thunder/03-algorithm.md` §4.1.

---

### Gap 3: BFD greedy_resume — **REPLACED with least-active**

**Python** (`router.py:719-844` — `greedy_resume`):
- Best Fit Decreasing bin-packing
- (a) Sort PAUSED programs DESC by `total_tokens` (or recent-step token estimate)
- (b) Sort backends DESC by remaining capacity
- (c) For each program, find first backend that fits (best-fit because it's smallest backend that still fits, due to DESC sort + iteration)
- (d) Programs that don't fit anywhere stay PAUSED for next tick

**SMG** (`pick_tr` calling `select_least_active`):
- When a paused program wakes (via Notify), it picks the backend with **fewest active programs**, regardless of token sizes
- No global coordination across waking programs

**User-visible behavior difference**:
- Scenario: 4 paused programs of sizes [80k, 20k, 10k, 5k] tokens; 2 backends A(120k free) and B(20k free)
- Python BFD: 80k → A (40k left). 20k → A. 10k → B (10k left). 5k → B. All 4 fit perfectly.
- SMG least-active: All 4 wake simultaneously → all see "B has fewer active programs" → all try B → 80k won't fit B → re-pause → next tick same dance.

**Classification**: **Algorithm gap** — affects capacity utilization and fairness.

**Repair**: ~150 LOC. Wire-format-compatible swap: replace `RouterState::select_least_active` with BFD inside `pick_tr`. No struct changes needed.

---

### Gap 4: Notify wake — **broadcast vs targeted**

**Python** (`greedy_resume` body):
- After BFD assigns a program to a backend, calls `program.waiting_event.notify_one()` — wakes that specific program's coroutine
- Other PAUSED programs stay asleep until their turn in a future tick

**SMG** (`usage_consumer_task` after applying UsageEvent):
- Calls `notify_waiters()` on **every** Notify in `RouterState.waiting_events` — wakes all paused programs simultaneously
- Each woken program re-acquires the write lock, rechecks capacity, either admits or re-pauses

**Classification**: **Engineering optimization gap** (not algorithmic — final state is identical, just less efficient transition).

**User-visible behavior difference**:
- Latency: thundering-herd of N waiters all hitting the write lock; serialized through the RwLock means N×~10μs of contention
- Correctness: ✅ identical (each woken program checks capacity again under the lock; only those that fit admit)
- At N≤30 waiters this is invisible; at N=100+ it might be a measurable overhead

**Repair**: ~30 LOC — switch `notify_waiters()` to targeted `notify_one()` in `pick_tr` after BFD selects winner. Requires gap 3 to be fixed first (BFD selects which Notify to wake).

---

### Gap 5: ProgramRequestGuard incomplete — **CAPACITY LEAK BUG**

**Python** (`force_terminate_program(pid)`):
1. Remove `pid` from `programs` dict
2. Remove `pid` from each backend's `active_programs` set
3. **Subtract program's `estimated_reserved_tokens` from `active_program_tokens` on its assigned backend**
4. Wake any waiters (broadcast)
5. Idempotent — calling twice is no-op

**SMG** (`ProgramRequestGuard::Drop`):
1. Decrements `program.in_flight` if positive ✅
2. Calls `notify_waiters()` ✅
3. ❌ **Does NOT subtract `estimated_reserved_tokens` from `active_program_tokens`**
4. ❌ Does NOT remove from `programs` or `backend.active_programs` (lifetimes accumulate)

**Concrete bug scenario**:
- TR-mode admit reserves 1000 tokens on backend A: `A.active_program_tokens += 1000`
- Client connects, sends request, gets 200 with first byte → SMG starts streaming response
- Client hits Ctrl-C / network interrupted → axum drops the request future → `ProgramRequestGuard::Drop` fires
- Drop decrements `in_flight` and broadcasts Notify, but `A.active_program_tokens` is **still +1000**
- Result: backend A's apparent occupancy never decreases for that program. Long-running gateway → accumulates phantom occupancy → TR mode permanently thinks all backends are full → all new requests pause and wait for force-resume timeout (30 min) before being admitted to (actually-empty) backends

**Classification**: **Bug** (regardless of "simplification" intent — Python clearly does it, SMG doesn't). Not algorithm-design gap.

**Repair**: ~30 LOC. In `Drop`'s spawned async task, add:
```rust
let reserved = guard.programs.get(&self.program_id).map(|p| p.estimated_reserved_tokens).unwrap_or(0);
let backend_url = guard.programs.get(&self.program_id).and_then(|p| p.backend_url.clone());
if let Some(url) = backend_url {
    if let Some(b) = guard.backends.get_mut(&url) {
        b.active_program_tokens = b.active_program_tokens.saturating_sub(reserved);
        b.active_programs.remove(&self.program_id);
    }
}
guard.programs.remove(&self.program_id);
```

**Severity**: Production blocker for any non-trivial uptime. Must fix before user runs Thunder against real workload.

---

### Gap 6: Streaming requests bypass Thunder state — **HEAVY**

**Python**: Streaming and non-streaming follow identical state-update paths. Every request:
- Increments `program.in_flight` and `backend.active_program_tokens` at admit
- Emits usage update at completion regardless of streaming
- Eligible for pause/resume

**SMG** (P0+P3+P4 implementation):
- **Admit path** (`select_worker_async` → `pick_tr`): symmetric for stream and non-stream — both increment counters ✅
- **Completion path**: only non-streaming `route_typed_request_once` parses response body for `usage` field and emits `UsageEvent`
- **Streaming path** (`bytes_stream` branches in `routers/http/router.rs:712, 923`): forwards bytes verbatim, **never inspects payload, never emits UsageEvent**, **never calls `complete()` on the guard**

**Concrete consequences** (4):
1. **Capacity gate misses streaming load** — `active_program_tokens` only ever reflects non-streaming history; if 90% of traffic is streaming, the gate is largely unaware of real backend pressure
2. **`Program.in_flight` leaks on streaming** — guard exists but is never `complete()`'d for streams, so guard's Drop fires on stream end... except guard isn't even created on streaming path today (only non-streaming router code creates guards)
3. **`Program.total_tokens` always 0 for streaming-only programs** — breaks any future feature that depends on per-program cumulative token tracking
4. **Force-resume timeout doesn't get short-circuited by streaming completions** — paused program waits up to 30 min even if 5 streaming requests just finished freeing 100k tokens of capacity

**Classification**: **Engineering gap** (deferred implementation) + **algorithm gap** (because Thunder semantics break under streaming-heavy traffic).

**Repair**: ~280 LOC across 3 sub-tasks (per `handoff-streaming-and-pause-resume.md` §2 F1+F2+F3):
- F1: SSE tail extractor for 3 protocols (OpenAI Chat / Anthropic Messages / OpenAI Responses) + `stream_options.include_usage` injection
- F2: Wire `ProgramRequestGuard` to the streaming path so disconnect cleanup works
- F3: Streaming retry × in_flight idempotency (D-9 Option C+C1 extended to streams)

---

### Gap 7: Token estimation — **uncalibrated**

**Python**: Each `Program` maintains a `char_to_token_ratio` (typically ~3.5-4.5 chars/token, but varies by tokenizer). Updated on every UsageEvent: `ratio = (old_ratio * α) + ((actual_tokens / request_chars) * (1-α))`. Used at admit time to estimate how many tokens this request will consume.

**SMG** (`pick_tr::estimate_request_tokens`):
- Hardcoded: `prompt_chars / 4 + 256`
- 4 chars/token is a **rough average**; actual varies 2.5 (CJK) to 5 (English code) depending on content
- 256 completion budget is arbitrary

**User-visible behavior**:
- For Chinese-heavy prompts: SMG underestimates → admits programs that actually consume 1.5× the budget → over-commits backend
- For long-completion programs (max_tokens=8000): SMG severely underestimates → admits when shouldn't
- For short prompts with cached completions: SMG overestimates → over-pauses

**Classification**: **Engineering gap** (no algorithm change, just estimator quality).

**Repair**: ~40 LOC. Add `Program.char_to_token_ratio: f64` (default 0.25 = ¼ chars/token = 4 chars/token). Update in `usage_consumer_task` body:
```rust
if event.request_text_chars > 0 && event.total_tokens > 0 {
    let observed = event.total_tokens as f64 / event.request_text_chars as f64;
    p.char_to_token_ratio = 0.7 * p.char_to_token_ratio + 0.3 * observed;
}
```
Use `program.char_to_token_ratio` in `estimate_request_tokens` if program exists, else fall back to default.

---

## Classification summary

> **Phase 7 status (2026-05-01)**: All 7 gaps closed across 8 milestones (commits `51fd6951`..`23390276`). See `docs/thunder/worklog.md` D-23..D-37 for detailed sign-off.

| Gap | Type | Resolved? | Closed by | LOC actual |
|---|---|---|---|---|
| 1. No proactive pause | **algorithm** | ✅ | M4 (`d3e7b091`) | ~250 |
| 2. No victim selection | **algorithm** | ✅ | M4 (`d3e7b091`) | (paired with 1) |
| 3. BFD → least-active | **algorithm** | ✅ | M5 (`71fe9614`) | ~200 |
| 4. Broadcast vs targeted Notify | engineering | ✅ | M6 (`71fe9614`) | ~40 |
| 5. RAII guard incomplete | **bug** | ✅ | M1 (`51fd6951`) | ~30 |
| 6. Streaming bypasses state | engineering+algorithm | ✅ | M2 (`7c6b5960`) | ~390 |
| 7. Token estimate uncalibrated | engineering | ✅ | M3 (`c100975c`) | ~90 |

---

## What SMG Thunder can/cannot legitimately claim

> **Post-Phase 7** (2026-05-01): all rows flipped to ✅ via M1-M8.

| Claim | Truth status |
|---|---|
| Implements ThunderAgent algorithm | ✅ (with 9 documented intentional divergences below) |
| program-aware sticky routing | ✅ |
| capacity-aware admission gate (non-streaming) | ✅ |
| capacity-aware admission gate (streaming) | ✅ (M2) |
| pause/resume on capacity full | ✅ (M4 proactive + scheduler tick @ 100ms) |
| BFD greedy_resume bin-packing | ✅ (M5 with starvation priority boost) |
| force-resume timeout | ✅ |
| RAII cleanup on client disconnect | ✅ (M1 fix) |
| streaming token tracking | ✅ (M2 incremental + end-of-stream) |
| char_to_token_ratio calibration | ✅ (M3 per-program + global + time-decay + cache_read exclusion) |
| streaming requests participate in scheduling | ✅ (M2) |

## Intentional SMG ↔ Python divergences (Phase 7)

| # | Dimension | Python | SMG | Type |
|---|---|---|---|---|
| 1 | `include_usage` injection on OpenAI Chat | setdefault (preserves user) | force override | UX choice |
| 2 | response usage chunk visibility | unconditionally forwarded | stripped if client didn't ask | client transparency |
| 3 | Anthropic incremental token counting | event-count (inaccurate) | cumulative output_tokens (accurate) | **fixes Python bug** |
| 4 | Per-program calibration | not present | global + per-program two-tier | enhancement |
| 5 | Completion budget calibration | not present | per-program EMA on completion/max_tokens | enhancement |
| 6 | Time-decay on calibration | event-EMA only | event-EMA + wall-time half-life decay | enhancement |
| 7 | Anthropic cache_read_input_tokens | not handled (treated as fresh) | excluded from prefill ratio | **fixes Python bug** |
| 8 | Cross-protocol per-program calibration | not present | single ratio per program (per-protocol HashMap deferred to Tier 2) | partial enhancement |
| 9 | Streaming retry boundary | implicit | strict 200 OK divides retry from no-retry | enhancement |

---

## Repair-order recommendations

### Path A: "Fix critical bugs + unblock streaming use case" (~350 LOC)
Goal: Production-stable for the user's actual workload.
1. **Gap 5** (capacity leak bug) — 30 LOC, must fix before any production
2. **Gap 6** (streaming state participation, F1+F2+F3) — 280 LOC, user's primary workload
3. **Gap 7** (token calibration) — 40 LOC, prevents systemic mis-admission

### Path B: "Faithful Python port" (additionally ~430 LOC = 780 LOC total)
Goal: Algorithmically equivalent to ThunderAgent.
4. **Gap 1+2** (proactive pause + victim selection) — 250 LOC, restores Thunder's defining behavior
5. **Gap 3** (BFD greedy_resume) — 150 LOC, optimal capacity utilization
6. **Gap 4** (targeted Notify) — 30 LOC, eliminates thundering-herd

### Path C: "Functional MVP, defer all" (current state)
Goal: Just keep what's there. Acceptable only if streaming workload is light AND uptime is short (otherwise gap 5 will bite).

---

## Open questions to resolve before deciding repair order

These are flagged for the next session because they affect *which path* is right:

1. **Does the user's actual workload mostly stream?** — From the user's stated "我的很多 use case 要接 /response 和 /messages 格式的请求 which needs to be streaming requests": yes. → Path A is mandatory.
2. **What's the typical concurrent program count?** — If <10, gap 4 (broadcast Notify) is invisible. If >100, gap 4 matters.
3. **What's typical request duration?** — If most programs finish quickly (~1-10s), gap 1 (no proactive pause) is rarely felt because natural completion frees capacity faster than pre-emption would. If long programs (~minutes), gap 1 hurts.
4. **Is fairness a hard requirement?** — If "any program eventually gets served within bounded time" is enough, current SMG works. If "high-priority program preempts long-running one" is required, gap 1+2 needed.
5. **Does the user care about Anthropic prompt caching token semantics?** — Affects how `cache_read_input_tokens` interacts with `active_program_tokens`. (Cross-ref: handoff-streaming-and-pause-resume.md F1 question 5.)

---

## Cross-references

- `docs/thunder/03-algorithm.md` — original Python algorithm spec (the "should-be" reference)
- `docs/thunder/handoff-streaming-and-pause-resume.md` §3 — narrative comparison of completed vs simplified pause/resume with scenario walkthroughs (A/B/C)
- `docs/thunder/post-mvp-followups.md` Tier 1-4 — same gaps organized by priority/trigger
- `docs/thunder/worklog.md` D-22 — original autonomous decision to simplify
- `model_gateway/src/policies/thunder.rs` — current implementation
- `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/router.py:685-844` — Python ground truth (READ-ONLY)
