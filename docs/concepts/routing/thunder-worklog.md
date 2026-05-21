---
title: ThunderAgent — Paper-Parity Worklog
---

# ThunderAgent — Paper-Parity Worklog

This worklog documents corrections applied to SMG's `--policy thunder`
implementation after an audit revealed several deviations from the
[ThunderAgent paper](https://arxiv.org/abs/2602.13692) (Kang et al., 2026) and
its [Python reference implementation](https://github.com/ThunderAgent-org/ThunderAgent).
It also captures one design extension beyond the original Python (a three-tier
restore ordering) and one piece of recommended future work (collapsing the
current single-axis status enum into the paper's orthogonal two-axis model).

> Companion reading: the algorithm itself is described in
> [ThunderAgent — Program-Aware Routing](thunder.md); the user-facing setup
> guide is [Getting Started: ThunderAgent](../../getting-started/thunder.md).

## Background

The paper defines an agentic program with two orthogonal axes:

- **Phase τ ∈ {Reasoning, Acting}** — what the program is doing.
  *Reasoning* means an LLM call is running; *Acting* means the agent is
  between LLM calls (tool execution, orchestration, idle).
- **Status s ∈ {Active, Paused, Terminated}** — where the program sits in the
  scheduler. *Active* on a backend, *Paused* in the global waiting queue, or
  *Terminated* and reclaimed.

The pause score (paper Eq 9) is `S_pause(P) = 1/c_P + 𝕀(τ = Acting)` —
prefer pausing Acting programs (their KV cache is idle), shortest context
first. The restore score (paper Eq 8) is `S_restore(P) = 1/c_P + 𝕀(τ =
Reasoning)` — prefer resuming Reasoning programs (a client is waiting on
the LLM), shortest context first. Token accounting tracks each program's
current context length `c_P`, which is the per-turn `usage.total_tokens`,
not cumulative history.

The Python reference (`ThunderAgent/program/state.py:33-47`) implements the
two axes as separate fields on the `Program` dataclass:

```python
status: ProgramStatus = ProgramStatus.ACTING   # τ — REASONING/ACTING
state: ProgramState = ProgramState.ACTIVE       # s — ACTIVE/PAUSED/TERMINATED
```

This allows the `(state=PAUSED, status=REASONING)` combination, which is
exactly how Python signals "this paused program has a client request
waiting on it — promote it on restore" (`router.py:413-421`).

The original SMG port collapsed both axes into a single
`ProgramStatus = {Idle, Reasoning, Acting, Paused}` enum where the variants
are mutually exclusive. That choice is what motivates the future-work
section at the bottom of this document; the corrections below are layered
on top of it.

## What was wrong, before this work

A side-by-side audit against the paper and the Python reference surfaced
five concrete deviations and one missing feature:

| # | Behavior | Paper / Python | SMG (before) |
|---|---|---|---|
| 1 | Phase model — Acting state | `R→A` on response completion (`router.py:476-477`) | `ProgramStatus::Acting` declared but never set in production code (only in test fixtures). |
| 2 | Pause selection | `S_pause = 1/c_P + 𝕀(τ=A)` — Acting first, shortest context. | `min_by_key(step_count)` — lowest step count, no τ-prefer. |
| 3 | Resume sort | Shortest-context first; paper Eq 8. | DESC by `total_tokens` (largest first). |
| 4 | Token accounting on response | REPLACE: `state.total_tokens = usage.total_tokens`. | `p.total_tokens.saturating_add(...)` plus a second `+=` in the streaming path — strict double-count for any streamed response. |
| 5 | `declared_max_tokens` plumbing | Python: not consulted. SMG's intent: plumb it through. | `estimate_request_tokens` read `info.declared_max_tokens`, but every call site passed `None` — dead code. |
| 6 | Three-tier restore (paper Eq 8) | Python: `reasoning_group → new_program → acting_group`, shortest-first within tier. | Not implemented in SMG or in any earlier port; the paper's τ-indicator on the restore side was missing. |

Item 6 is the "missing feature" rather than a regression — neither SMG nor
the immediate downstream port implemented it. Items 1–5 are real bugs
relative to paper-faithful behavior.

## Changes in this commit

### Phase 1 — Cross-file `declared_max_tokens` plumbing

A new default trait method `declared_max_tokens_hint(&self) -> Option<u32>`
on `GenerationRequest` (`crates/protocols/src/common.rs`), implemented per
protocol:

- **Chat** (`crates/protocols/src/chat.rs`) — `max_completion_tokens.or(max_tokens)` (new OpenAI field with deprecated fallback).
- **Responses** (`crates/protocols/src/responses.rs`) — `max_output_tokens`.
- **Anthropic Messages** (`crates/protocols/src/messages.rs`) — `Some(max_tokens)` (required field).
- **SGLang Generate** (`crates/protocols/src/generate.rs`) — `sampling_params.max_new_tokens`.
- **Completion** (`crates/protocols/src/completion.rs`) — `max_tokens`.
- **Interactions** (`crates/protocols/src/interactions.rs`) — `generation_config.max_output_tokens`.

Call-site wiring:

- `model_gateway/src/routers/http/router.rs` — `select_worker_for_model` gains a `declared_max_tokens: Option<u32>` parameter; `route_typed_request_once` passes `typed_req.declared_max_tokens_hint()`. Streaming `ThunderStreamingCtx` carries the hint into both the chunked-extract and trailing-flush UsageEvent emissions.
- `model_gateway/src/routers/grpc/common/stages/worker_selection.rs` — added a per-`RequestType` match that pulls `declared_max_tokens_hint()` from whichever typed request the stage holds, plumbed into `select_single_worker`.

PD-router paths (`http/pd_router.rs`, the `select_pd_pair` flow in
`worker_selection.rs`) intentionally still pass `None` — ThunderAgent's
prefill-decode mode has its own scheduling story and is not validated
against this fix. See "Known gaps" below.

This is a strict improvement over both prior states: the calibration logic
that already read `declared_max_tokens` from the `UsageEvent` and from the
admission `SelectWorkerInfo` now actually receives real values, not just
literal `None`.

### Phase 2 — Phase transitions wired through Acting

In `model_gateway/src/policies/thunder.rs`:

- `usage_consumer_task` (response-completion path): when `in_flight` drains
  to 0 and the program is in `Reasoning`, flip to `Acting`. This is the
  paper's R→A transition — the program has completed its current LLM call
  and is now (briefly or for the duration of tool execution) idle. The
  paper deliberately wants this state visible so `pick_victim` can pick
  Acting programs preferentially.
- `ProgramRequestGuard::Drop` (error / disconnect path): same transition,
  same gating, so error paths reach the Acting state too. The scheduler
  treats this as an eligible pause candidate per Eq 9.
- `assign()` already handled the reverse transition (`*` → `Reasoning` on
  admission), so the loop closes.
- The doc-comment on `ProgramStatus` was rewritten to describe the
  transitions accurately ("first byte of upstream response received" was
  never wired and has been replaced with the actual trigger).

### Phase 3 — REPLACE token accounting

Added a new `accounted_tokens: u64` field to `Program`. It tracks the
program's portion of `backend.active_program_tokens` that came from
retained KV footprint (as opposed to admission-time `estimated_reserved_tokens`).

`usage_consumer_task` now does:

```rust
b.active_program_tokens = b.active_program_tokens
    .saturating_sub(reserved.saturating_add(previous_accounted));
b.active_program_tokens = b.active_program_tokens
    .saturating_add(event_total_tokens);
p.total_tokens     = event_total_tokens;   // REPLACE
p.accounted_tokens = event_total_tokens;
```

The streaming `progress_consumer_task` still incrementally adds
`delta_tokens` to `p.total_tokens` for best-effort intermediate visibility,
but every full UsageEvent then overwrites it with the authoritative total —
so the streamed-then-finalized path no longer double-counts.

`pause_until_safe` unbookkeeps `reserved + accounted` together on pause and
zeros both fields, mirroring the accounting on admission.

### Phase 4 — `pick_victim` uses paper Eq 9

New helper `context_tokens_for_scoring(p)` returns
`max(total_tokens, accounted_tokens, estimated_reserved_tokens, 1)` so the
score key is monotonic against the largest reservation the scheduler has
committed to. `pick_victim` now uses:

```rust
.min_by_key(|(pid, p)| (
    p.status != ProgramStatus::Acting,        // Acting strictly first
    Self::context_tokens_for_scoring(p),      // shortest context next
    (*pid).clone(),                           // deterministic tie-break
))
```

Acting programs win the boolean tier (false < true), so they're picked
first; within tier, shortest context wins because re-prefilling it later
costs proportional to `c²` (Eq 6).

### Phase 5 — Shortest-first resume sort

`try_greedy_resume` was flipped from DESC-by-`total_tokens` to ASC-by-
`context_tokens_for_scoring`. The starvation boost (programs paused longer
than `PAUSED_PRIORITY_BOOST_AFTER`) still applies on top.

### Phase 6 — Three-tier restore (paper Eq 8 + Python parity, new)

This goes beyond what the previous downstream port had. The paper's
`S_restore = 1/c_P + 𝕀(τ = R)` requires distinguishing "paused with a
request waiting" from "paused while idle" at restore time. Python encodes
this via the orthogonal (state=PAUSED, status=REASONING) combination. SMG's
single enum cannot, so this work added a side-channel counter:

```rust
pub struct Program {
    ...
    pub pending_requests: u32,    // # of pick_tr futures blocked on this program
    ...
}
```

`try_greedy_resume` partitions paused programs into three tiers (lower
number = higher priority), then sorts ASC by context within each:

| Tier | Filter | Meaning |
|---|---|---|
| 0 — Reasoning | `pending_requests > 0 && step_count > 1` | Client request blocked on a program that has a turn history. Resuming unblocks real work. |
| 1 — New | `step_count <= 1` | Admitted at most once (or never). Prevents first-time programs from starving. |
| 2 — Acting | otherwise | Idle between turns with no client waiting. |

The starvation boost is applied **across tiers** so no program waits
forever regardless of which tier it sits in.

This corresponds to Python's `reasoning_group / new_program_group /
acting_group` resume groups (`router.py:854-866`).

### Phase 7 — `PendingRequestGuard` for cancellation safety

Without an RAII guard, `pick_tr` futures cancelled mid-await would leak the
`pending_requests` increment. Over time, a long-running SMG would
accumulate phantom Reasoning-tier candidates on programs whose clients no
longer exist, crowding out real waiters at resume time.

A `PendingRequestGuard` was added next to `ProgramRequestGuard` with the
same Weak<RwLock>/`tokio::spawn` cleanup pattern:

```rust
pub(crate) struct PendingRequestGuard {
    state: Weak<RwLock<RouterState>>,
    program_id: String,
    armed: bool,
}
```

- `consume(self, &mut state)` — synchronous decrement under the held write
  lock; disarms `Drop`. Used at admit-success / timeout / no-backend exits.
- `Drop` — fire-and-forget `tokio::spawn` async decrement when not
  consumed. Catches cancellation.

`pick_tr` now holds `Option<PendingRequestGuard>` across iterations. The
counter increments exactly once (the first time we enter the block path);
every exit path either calls `consume()` or relies on `Drop`.

### Phase 8 — Idle → Paused at first-admit block

A brand-new program (`status == Idle`) that fails its first admission
attempt now enters the Paused set via a four-line addition to `pick_tr`'s
block path:

```rust
if p.status == ProgramStatus::Idle {
    p.status = ProgramStatus::Paused;
    p.paused_at = Some(now);
}
```

Without this, first-admission failures stay `Idle`, are invisible to
`try_greedy_resume`'s `status == Paused` filter, and only wake via the
broadcast `notify_waiters()` that fires on capacity-free events — with no
priority among concurrent newcomers. The four lines above let the three-tier
sort actually fire for the New tier in the common case.

The transition is **gated to Idle only**. Reasoning programs (in-flight)
and Acting programs (with retained accounting) still own backend capacity;
they must go through the scheduler's `pause_until_safe` path for correct
unbookkeeping. Flipping them here would skip the `accounted_tokens`
release.

## Tests

44 → 46 thunder unit tests; full policy suite 154 → 156, all passing.

New / renamed:

- `pick_victim_returns_shortest_context_with_acting_priority` — Eq 9 score check.
- `pause_until_safe_pauses_off_gpu_acting_program` — Acting + `in_flight=0` pauses immediately, accounting cleared. (Replaces the old `pause_until_safe_defers_acting` test that relied on the never-set Acting state.)
- `resume_assigns_shortest_program_first` — Eq 8 sort direction.
- `resume_orders_by_reasoning_new_acting_tier` — full 3-tier ordering with a single-slot backend.
- `resume_within_reasoning_tier_picks_shortest_context` — intra-tier shortest-first.
- `tr_mode_blocked_new_program_is_in_paused_set` — Idle→Paused at first-admit block; counter reaches 0 post-admit.
- `tr_mode_cancellation_releases_pending_request_counter` — Drop fires async decrement on `task.abort()`.

All existing struct-literal test fixtures were updated for the two new
fields (`pending_requests`, `accounted_tokens`).

### Test infrastructure note

While running the new tests, four existing TR-mode tests broke (including
`tr_mode_pauses_then_resumes_on_capacity_free` and
`tr_mode_force_admits_after_timeout`). Root cause: the test fixtures
seeded `backend.capacity_tokens = 100`, but the capacity-poll task's
immediate first tick overwrites that value with the `StubMetrics` default
of 10_000. The old code didn't notice because Idle programs were invisible
to `try_greedy_resume`; the new code correctly exposes the gap.

These tests were updated to use `capacity_tokens = active_program_tokens =
10_000`, so the saturation actually holds after the poll runs.

This is worth noting because it's a real latent issue in the test
infrastructure that may bite future authors. A cleaner fix (deferred) would
be to make `StubMetrics` parameterizable so each test can pick the
capacity it wants — but that's an unrelated cleanup.

## Known gaps not addressed

1. **PD-disaggregated mode** — neither program-id nor declared_max_tokens
   is plumbed into the `select_pd_pair` path. The downstream port we
   referenced made the same choice. ThunderAgent's behavior in PD mode is
   undefined territory and should not be relied on.

2. **Single-enum `ProgramStatus`** — the variants `Idle`, `Reasoning`,
   `Acting`, `Paused` remain mutually exclusive. See "Future work" below.

3. **Pre-pause τ tracking on Paused programs** — currently lost. The Reasoning
   tier at resume is reconstructed from `pending_requests > 0`, which is a
   different signal than Python's "this program was Reasoning before pause,
   and a request is waiting." In practice the pending-request signal is the
   one that matters for unblocking work; the τ history is informational.

4. **`step_count` semantics around the New tier** — `step_count` increments
   in `assign()`, so a program that has never been admitted (`step_count =
   0`) and one that has been admitted exactly once (`step_count = 1`) both
   land in the New tier. This matches Python's intent but may not match a
   reader's intuition.

5. **`update_shared_tokens` equivalent** — the Python reference has a
   `shared_tokens` mechanism for prefix-cache credit that is defined but
   never called (`backend/state.py:129-136`), confirmed by the maintainer as
   intentionally disabled (`ThunderAgent-org/ThunderAgent#36`). SMG never
   implemented this and continues not to. Not a deviation.

## Future work — collapse the single-axis enum into two orthogonal fields

The paper's clean separation of τ (phase) and s (scheduling status) is
worth preserving in the type system. The current single-axis layout
forces side-channel fields (`marked_for_pause`, `pending_requests`,
`accounted_tokens`) to encode information that would be a free
consequence of a two-axis model. A cleaner refactor would look like:

```rust
pub enum Phase {
    Reasoning,
    Acting,
}

pub enum Scheduling {
    Idle,         // never admitted (or all axes uninitialized)
    Active,       // on a backend, doing or waiting to do work
    Paused,       // off-backend, in the global waiting queue
    PausePending, // marked but in-flight work still draining
    Terminated,   // reclaimed
}

pub struct Program {
    pub phase: Phase,
    pub scheduling: Scheduling,
    pub pending_requests: u32,     // count, not a state — keep
    pub in_flight: u32,
    pub total_tokens: u64,
    pub accounted_tokens: u64,
    pub estimated_reserved_tokens: u64,
    // marked_for_pause goes away — folded into Scheduling::PausePending
    pub paused_at: Option<Instant>,
    // ...
}
```

Benefits:

- **Illegal states unrepresentable.** Today `(Paused, in_flight > 0)` is a
  silent bug (a paused program can't be in-flight); the type system can
  enforce it via the `Scheduling::PausePending` intermediate variant.
- **Pre-pause τ preserved.** A program paused while in Acting phase keeps
  its `phase = Acting`; the resume path can read it directly. This matches
  Python's `(state=PAUSED, status=ACTING)` exactly.
- **`marked_for_pause` folds away.** The "in-flight, marked for pause"
  state becomes `Scheduling::PausePending`, removing one side-channel
  boolean.
- **`pick_victim` and `try_greedy_resume` become structural.** Filters
  become exhaustive matches on `Scheduling`; tier scoring becomes a tuple
  over the two enums plus `pending_requests > 0`. Easier to audit, easier
  to test.

Costs:

- **Touches every read/write of `status`** — `pause_until_safe`,
  `pick_victim`, `try_greedy_resume`, `usage_consumer_task`,
  `ProgramRequestGuard::Drop`, `pick_tr`, and all existing struct-literal
  tests. Roughly the same surface area as this commit, but mechanical.
- **Migration risk.** Done carelessly, it would silently change behavior
  at the (Paused, Reasoning) corner that doesn't exist in today's enum.
  Worth doing as a dedicated commit with its own tests rather than mixed
  into other work.

Recommendation: do it as a follow-up on a clean branch, with a focused PR
that touches only `thunder.rs` and its tests. The cross-file phase-1 work
in this commit was a strict addition (new trait method, new field
plumbing) and doesn't conflict with a future axis split.

## File-by-file diff summary

```
 crates/protocols/src/chat.rs                          | +7  (declared_max_tokens_hint impl)
 crates/protocols/src/common.rs                        | +8  (trait method definition)
 crates/protocols/src/completion.rs                    | +4
 crates/protocols/src/generate.rs                      | +4
 crates/protocols/src/interactions.rs                  | +6
 crates/protocols/src/messages.rs                      | +4
 crates/protocols/src/responses.rs                     | +4
 docs/getting-started/thunder.md                       | ±30 (3-tier doc + correct BFD description)
 docs/concepts/routing/thunder-worklog.md              | NEW (this file)
 model_gateway/src/policies/thunder.rs                 | ±540 (most of the algorithmic work + tests)
 model_gateway/src/routers/grpc/common/stages/worker_selection.rs | ±35  (declared_max_tokens wire-up)
 model_gateway/src/routers/http/router.rs              | ±20  (declared_max_tokens wire-up + streaming ctx field)
```

## References

- Paper: [Kang et al. 2026, *ThunderAgent*](https://arxiv.org/abs/2602.13692)
- Python reference: [github.com/ThunderAgent-org/ThunderAgent](https://github.com/ThunderAgent-org/ThunderAgent)
- Paper Eq 8 (restore score): `S_restore(P) = 1/c_P + 𝕀(τ = R)`
- Paper Eq 9 (pause score): `S_pause(P) = 1/c_P + 𝕀(τ = A)`
- Paper Eq 6 (recomputation cost): `Cost_recompute ∝ c²`
- Related upstream discussion: [ThunderAgent-org/ThunderAgent#36](https://github.com/ThunderAgent-org/ThunderAgent/issues/36)
