# Phase 7 Hand-off: Algorithm Verification Before Production Benchmark

> **Date**: 2026-05-01
> **Branch**: `thunder-policy` HEAD `10e6b6ef`
> **Author**: Claude (autonomous Phase 7 execution session)
> **Audience**: Weili (next session)

---

## Status snapshot

Phase 7 shipped all 8 milestones M1-M8 (commits `51fd6951`..`23390276`) plus a
post-review concurrency fix (`10e6b6ef`). All 7 algorithm gaps catalogued in
`docs/thunder/algorithm-gap-vs-python.md` are marked ✅ resolved. 42 thunder
unit tests + 31 SSE unit tests + ~757 lib tests pass; `cargo clippy --workspace
--all-targets --all-features -- -D warnings` is clean.

User-facing docs at `docs/getting-started/thunder.md`. Operator runbook at
`docs/thunder/operations.md`.

What is **NOT yet verified**:

- End-to-end algorithm correctness against Python ThunderAgent reference under
  realistic concurrent load (the unit tests are isolated; they don't reproduce
  the thread interleaving of a real deployment).
- State-machine transitions: Idle → Reasoning → Acting → Idle / Paused under
  real traffic patterns. **The Acting state transition is incompletely wired**
  — see §2.
- Throughput vs `cache_aware` against real sglang.

Before running the throughput benchmark on a SLURM-hosted sglang cluster, the
user wants to **double-check algorithm correctness** against the Python source
of truth.

---

## 1. Verification plan (before benchmark)

The user's stated next-session goal:

> 我们还要再 double check 你的 rust 实现里面 thunderagent 算法的正确性 against
> python version (不同的 state 之间的 transition、是否 pause inflight request
> 等)，在验证算法正确性之后我会再起真实的 sglang server 测 throughput
> performance v.s. cache aware policy

Concrete verification checklist (do these in order):

### 1.1 Read-the-code parity check

Open Python `router.py` and SMG `thunder.rs` side-by-side; compare each of
the following:

| Aspect | Python ref | SMG impl | Verify |
|---|---|---|---|
| Program state enum | `ProgramStatus { REASONING, ACTING }` × `ProgramState { ACTIVE, PAUSED, TERMINATED }` (two orthogonal axes) | Single `ProgramStatus { Idle, Reasoning, Acting, Paused }` enum | Single-axis flattening drops nuance — does Rust correctly handle every Python combo? See §2 |
| Pause trigger | `pause_until_safe` body in `router.py:685-717`: iterate over-cap backends; pick lowest-step_count victim; if ACTING set marked_for_pause else immediately PAUSED | `RouterState::pause_until_safe` in `thunder.rs:306` | Identical control flow? Same victim-selection rule (`min_by_key(step_count)`)? |
| Mark-for-pause check points | Python: every step that transitions ACTING → REASONING calls `_clear_mark_and_pause` | SMG: `check_marked_for_pause` called from `usage_consumer_task` + Drop fallback | Are there any other transition points in SMG (streaming spawn first-byte? non-stream success?) where Python checks but SMG doesn't? |
| Resume algorithm | `greedy_resume` in `router.py:719-844`: sort by total_tokens DESC; per-program sort backends DESC by remaining; first-fit | `RouterState::try_greedy_resume` in `thunder.rs:338` | Same data flow? Same priority-boost behavior under starvation? |
| Wake mechanism | `program.waiting_event.notify_one()` (asyncio.Event) | `notify.notify_one()` (tokio::sync::Notify) | Notify semantics differ subtly — tokio's Notify "notify_one if no waiter pre-registers, else wakes one waiter". Python's asyncio.Event sets a flag once until cleared. Does this matter for pause/resume? |
| Force-resume timeout | Python: `_force_resume_after_timeout` in scheduler tick | SMG: `force_admit_after_timeout` in `pick_tr` | Both target the timing-out program, both pick least-active backend. ✓ |
| Token estimation | `current_ratio = state.context_len / prompt_tokens`; first observation directly assign; subsequent EMA α=0.2 | `update_calibration_with_decay` in `thunder.rs:69`, plus wall-time half-life decay (M3 enhancement) | SMG adds wall-time decay; Python doesn't. SMG's behavior is strict superset. |
| `shared_tokens` | Python tracks `program.shared_tokens` for FORK semantics | SMG: deferred (Q5.3 trim) | Confirm not needed for non-FORK use case. |

### 1.2 The §2 concurrency hot-spot

The biggest open algorithm question: **is `Acting` transition wiring sufficient
in SMG?**

Python flow for streaming:
1. Request admitted: status = REASONING, in_flight=1
2. Upstream first byte received: status = ACTING (`update_program_state_streaming`)
3. Stream end: status = REASONING, then transitions to whatever Python's
   downstream code sets

SMG flow today (post-fix `10e6b6ef`):
1. Request admitted (`assign()`): status = REASONING, in_flight=1
2. Upstream first byte: **no transition** — status stays Reasoning
3. Stream end (usage_consumer): no explicit Acting → Idle transition; check_marked_for_pause runs

**The post-fix `pause_until_safe` defers pause whenever `in_flight > 0`, which
covers both REASONING and ACTING in Python's model.** This works correctly
because:

- A program with in_flight > 0 has a request awaiting upstream completion
- Whether it's Python's REASONING (waiting for first byte) or ACTING
  (mid-stream) is irrelevant for SMG: in either case, clearing the
  reservation would corrupt accounting
- `check_marked_for_pause` fires when in_flight reaches 0 (after stream end
  via usage_consumer, or after disconnect via Drop fallback)

**Question to verify next session**: does Python ever need to distinguish
REASONING vs ACTING for *correctness* (not just observability)? Specifically:

- Python's `pause_until_safe` only sets marked_for_pause for ACTING. For
  REASONING, it pauses immediately. Why? Because in Python a REASONING
  program is "submitted but not generating yet" — it's safe to interrupt the
  upstream call and re-submit later (semantically a pause+resume).
- SMG can't easily abort an in-flight HTTP request; its semantics are
  "request goes through; just don't double-count capacity". So treating both
  as deferred-pause is *more conservative than Python* — programs may stay
  on a too-loaded backend slightly longer than Python would have allowed.

This is a **conservative divergence** but should be documented as such.
Action item for next session: add this as divergence #10 in
`algorithm-gap-vs-python.md` "Intentional SMG ↔ Python divergences" table.

### 1.3 Suggested verification methodology

**Step 1**: Trace through 5 specific scenarios on paper, comparing Python vs
SMG behavior. Pseudo-trace each line by line.

| # | Scenario |
|---|---|
| A | Single program, single request, normal completion (non-streaming) |
| B | Single program, single request, normal completion (streaming) |
| C | Single program, two concurrent requests, both complete cleanly |
| D | Single program, request 1 in-flight, scheduler proactively pauses the program due to backend pressure from OTHER programs |
| E | 5 paused programs, 2 backends with different free capacity, BFD resume |

For each, note: state transitions, when `marked_for_pause` is set/cleared,
when `estimated_reserved_tokens` is non-zero, what backend is in `programs[pid].backend_url`.

**Step 2**: Add a unit test or 2-3 integration tests in
`policies::thunder::tests` that automate the most divergent scenarios. The
existing 42 tests cover individual functions; what's missing is **multi-step
scenarios** (admit → run → preempt → resume on different backend → admit
next request).

**Step 3** (optional): Run a small mock-vLLM-based integration test that
simulates the real deployment loop (multiple programs, multiple backends,
random capacity changes). This catches concurrency hazards the unit tests
miss.

---

## 2. Known incompletely-wired transitions

These were intentional simplifications during Phase 7 execution. Each is a
correctness risk worth checking before production:

### 2.1 Acting state never set

The streaming spawn in `routers/http/router.rs::send_typed_request` (the M2
SSE-aware relay) does **not** transition the program's status to `Acting`
when the first chunk arrives. Status stays `Reasoning` for the entire stream.

**Why this is OK** (post-fix `10e6b6ef`): `pause_until_safe` defers pause for
any `in_flight > 0` program. Whether Python would have called it ACTING or
REASONING is irrelevant; SMG defers either way.

**Why this could be NOT OK**: if a future feature requires distinguishing the
two states (e.g., per-state metrics, per-state CLI controls), it would
silently report wrong values.

**Verification**: grep `pid.status == ProgramStatus::Acting` — confirm the
only check is in `pause_until_safe` and that the in_flight > 0 path subsumes
it. ✓ verified at HEAD `10e6b6ef`.

### 2.2 `streaming_progress_sender` lock contention under load

The progress consumer task acquires `RouterState.write()` once per
`StreamingProgressEvent`. At default `INCREMENTAL_TOKEN_INTERVAL=20`, a
1000-token completion produces ~50 events. With 100 concurrent streams,
that's ~5000 lock acquisitions per second.

Single `RwLock<RouterState>` (D-3 footgun) means these serialize. Could be a
throughput bottleneck.

**Verification**: bench with `cargo flamegraph` on a 100-concurrent-stream
workload; check `RwLock` contention.

**Mitigation if hot**: raise `INCREMENTAL_TOKEN_INTERVAL` to 50 or 100; or
shard `RouterState` per-program (D-3 deferred work).

### 2.3 BFD wake doesn't actually transition Paused → Acting

`wake_program_to` sets status = Reasoning. Per Python, a woken program is
still REASONING (request hasn't started generating yet). When the next pick_tr
admits and forwards to backend, Python would later transition to ACTING when
first byte received.

SMG: pick_tr after wake → `assign()` → keeps status Reasoning. No Acting
transition (per §2.1).

**Why this is OK**: same as §2.1 — the in_flight > 0 guard subsumes the
distinction.

### 2.4 `pick_tr`'s in-flight wait could outlive the program's reservation

If pick_tr for request 2 is paused (waiting on Notify), and meanwhile request
1 (also for same program) completes, the usage_consumer fires
check_marked_for_pause. If marked_for_pause was set by scheduler, the program
goes to Paused state. pick_tr for request 2 wakes and tries to admit on the
no-longer-assigned backend.

Looking at pick_tr code:

```rust
let chosen_url = state.programs.get(&program_id)
    .and_then(|p| p.backend_url.clone())
    .filter(|u| urls.contains(u))
    .or_else(|| state.select_least_active(&urls));
```

If `backend_url` is None (just got paused), falls through to
`select_least_active`. The program effectively gets re-assigned. But the
scheduler may have wanted it Paused.

**Verification**: trace this scenario. May need pick_tr to also check
`status == Paused` and await notify, not just fall through to least_active.

### 2.5 Double-bookkeeping risk on retry

D-9 (retry × pause/resume) mentions the in_flight idempotency concern. The
current implementation (M7) creates a new `ProgramRequestGuard` per retry
attempt because the operation closure inside `RetryExecutor` is re-invoked.
Each guard increments in_flight. After 3 retries, in_flight could read 3
even though only one actually succeeded.

**Verification**: add a unit test in `RetryExecutor` flow: simulate 3 retries
in a row (mock returns 503 twice then 200); confirm final in_flight == 1.

**Mitigation**: refactor `route_typed_request_once` to take a guard from the
caller, so RetryExecutor's wrapper holds one guard across all retries.

---

## 3. What the user wants to test next

After verifying §1 + §2:

1. **Real sglang cluster benchmark**: SMG with `--policy thunder
   --thunder-sub-mode tr` vs `--policy cache_aware` on the SLURM allocation
   per `docs/thunder/slurm-cluster.md` (jobid 30385, 4 H100 80GB workers)
2. **Workload**: agent traffic with `metadata.program_id` set, 100+
   concurrent programs, mix of streaming + non-streaming
3. **Metrics to compare**:
   - Throughput (tokens/sec aggregate)
   - P50 / P99 TTFT (time to first token)
   - P50 / P99 end-to-end latency
   - KV cache hit rate (if sglang exposes it)
   - Request success rate
4. **Pass criterion**: Thunder shows ≥ same throughput as cache_aware,
   higher KV cache reuse (because of program-stickiness), better tail
   latency under bursty load (because of capacity gating).

---

## 4. Quick-start for next session

```bash
cd /home/hkang/wl/smg-wl
git log --oneline -10        # review what shipped in Phase 7
git status                   # confirm clean
cargo test --package smg --lib policies::thunder  # 42 tests pass
cargo test --package smg --lib sse                # 31 tests pass
cargo clippy --workspace --all-targets --all-features -- -D warnings  # clean

# Read the algo gap doc — every divergence catalogued
less docs/thunder/algorithm-gap-vs-python.md

# Read the Python ground truth
less /home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/scheduler/router.py

# Begin §1.1 read-through; for any discrepancy, file a worklog entry
# and either fix or document as intentional divergence
```

If you find a **real bug** (not an intentional divergence), open a
sub-branch `phase7-fix-N-<short-name>` off `thunder-policy`, write a failing
test, fix, ff-merge.

If you find an **intentional divergence** worth keeping, add it to the
"Intentional SMG ↔ Python divergences" table in
`docs/thunder/algorithm-gap-vs-python.md` (currently 9 rows, would become 10
if §2.1 conservative-pause-deferral gets formalized).

---

## 5. Files / commits to anchor on

### Code

- `model_gateway/src/policies/thunder.rs` (~2800 LOC): all of ThunderPolicy.
  Key functions:
    - `RouterState::assign` (line ~268): admit → Reasoning + step_count++
    - `RouterState::pick_victim` (line ~290): scheduler victim selection
    - `RouterState::pause_until_safe` (line ~306): pause body with in_flight
      deferred-fix
    - `RouterState::try_greedy_resume` (line ~338): BFD resume
    - `RouterState::wake_program_to` (line ~395): per-program reassignment
    - `RouterState::check_marked_for_pause` (line ~452): take deferred pause
    - `ThunderPolicy::pick_tr` (line ~830): admission loop with capacity gate
- `model_gateway/src/sse/`: M2 SSE parsers, 5 files
- `model_gateway/src/routers/http/router.rs::send_typed_request`: M2
  streaming spawn wire-up (~250 LOC of Thunder-aware logic)
- `model_gateway/src/policies/mod.rs`: `UsageEvent`,
  `StreamingProgressEvent`, `SelectWorkerInfo`, `LoadBalancingPolicy` trait

### Docs

- `docs/superpowers/specs/2026-05-01-thunder-phase7-production-design.md`
  (1171 LOC spec)
- `docs/thunder/algorithm-gap-vs-python.md` (gap inventory; updated to ✅)
- `docs/thunder/operations.md` (operator runbook)
- `docs/getting-started/thunder.md` (user intro)
- `docs/thunder/worklog.md` (D-23 ~ D-38; sign-off log)

### Commits (Phase 7 chain on `thunder-policy`)

```
10e6b6ef fix(policies): defer pause for in-flight programs (concurrency safety)
190f7e39 docs(thunder): use ThunderAgent for the algorithm; thunder for CLI literals
f801b378 docs(thunder): user-facing getting-started guide + operations runbook
85cb03c4 docs(thunder): Phase 7 closes all 7 algorithm gaps + 9 intentional divergences
23390276 feat(thunder): Anthropic prompt cache calibration validation (Phase 7 M8)
acda026a feat(routers,policies): streaming retry × idempotency hooks (Phase 7 M7)
71fe9614 feat(policies): BFD greedy_resume + targeted notify (Phase 7 M5+M6 Gap3+4)
d3e7b091 feat(policies): proactive pause + victim selection (Phase 7 M4 Gap1+2)
c100975c feat(policies): full token calibration with time-decay (Phase 7 M3 Gap7)
7c6b5960 feat(sse,thunder): streaming usage extraction across 3 protocols (Phase 7 M2 Gap6)
51fd6951 fix(policies): ProgramRequestGuard::Drop un-reserves tokens (Phase 7 M1)
ab2bcbe2 docs(thunder): Phase 7 production-ready design (8 milestones, full Python parity)
```

Python reference (read-only):

- `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/scheduler/router.py`
  (961 LOC; `pause_until_safe` at line 685, `greedy_resume` at line 719)
- `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/scheduler/vllm_request_processor.py`
  (288 LOC; SSE streaming + token-progress callbacks)
- `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/program/state.py`
  (47 LOC; ProgramStatus / ProgramState dataclass — the two-axis state model)

---

## 6. Open questions that may surface during §1 verification

1. **Two-axis vs flat state model**: Python uses ProgramStatus × ProgramState
   (2 orthogonal enums); SMG flattens to one. Are there Python combinations
   that don't map cleanly to SMG? E.g., REASONING + PAUSED = ?
2. **Notify semantics**: Python `asyncio.Event` is "set once, all waiters
   wake"; tokio `Notify::notify_one()` is "wake one waiter or queue one
   permit". Did Phase 7 correctly handle the case where a Notify fires
   *before* a waiter registers (permit queueing)?
3. **Force-resume + BFD interaction**: if BFD already woke a program but
   pick_tr's force-resume timer fires anyway, do both code paths admit? (The
   already_reserved guard in pick_tr should handle this — verify.)
4. **`shared_tokens` truly unused**: Python tracks it for FORK programs.
   Confirm SMG users never invoke FORK (they don't have the API surface).
5. **Cross-protocol calibration**: SMG tracks one chars/token ratio per
   program regardless of protocol. If a program switches protocols
   mid-stream (rare), calibration mixes incompatible tokenizers. Per-protocol
   HashMap was deferred (M8 Tier 2 polish); is the conservative single-ratio
   acceptable?

---

## 7. End-state success definition

You can declare "Phase 7 Thunder ready for production benchmark" when:

- [ ] §1.1 read-through complete; all rows verified or filed as divergences
- [ ] §2.1-§2.5 each either confirmed safe or fixed
- [ ] At least one new multi-step scenario test added to
  `policies::thunder::tests` (Step 2 from §1.3)
- [ ] `cargo test --workspace --lib` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  clean
- [ ] worklog has a D-39 entry signing off the verification round

Then start the SLURM sglang benchmark.

---

## 8. Reading order

If you have 30 minutes:

1. This document (10 min)
2. `algorithm-gap-vs-python.md` (10 min)
3. `operations.md` §6 (architecture diagram, 5 min)
4. Worklog D-38 entry (5 min)

If you have 2 hours:

1. The above (30 min)
2. Open `thunder.rs` and Python `router.py` side-by-side; do §1.1 read-through
3. Open SMG / Python streaming paths side-by-side; verify state transitions

If you have a half day:

1. The above (2h)
2. Add multi-step scenario tests for the 5 cases in §1.3
3. Run them; fix any failures; commit
