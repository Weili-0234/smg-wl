# Thunder Operations & Production Readiness

> **Audience**: SMG operators deploying `--policy thunder` to production.
> **Companion**: [user-facing intro](../getting-started/thunder.md), [algorithm
> design](algorithm-gap-vs-python.md), [Phase 7 spec](../superpowers/specs/2026-05-01-thunder-phase7-production-design.md).

This document covers what's needed to run Thunder reliably in production, what
to watch for, and how to debug when something looks off.

---

## 1. Production readiness checklist

| Item | Status as of 2026-05-01 |
|---|---|
| All 7 algorithm gaps closed (vs Python ThunderAgent reference) | ✅ ([gap inventory](algorithm-gap-vs-python.md)) |
| 9 SMG↔Python intentional divergences documented | ✅ |
| 40 unit tests in `policies::thunder::tests` + 31 in `sse::*::tests` pass | ✅ |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean | ✅ |
| End-to-end soak (100 programs × 10 requests × 8h against real sglang) | ⏳ operator-run |
| Bench: scheduler-tick CPU < 5% under 1000 programs at 100ms tick | ⏳ optional |

The Phase 7 codebase is feature-complete. Steps marked ⏳ are environment-
dependent and must be exercised against your actual cluster before declaring
go-live.

---

## 2. Soak test recipe

Before promoting Thunder to production traffic, run an extended workload
against your target cluster and confirm Thunder behaves correctly under
sustained load.

### 2.1 Setup

1. Provision **at least 2 sglang or vLLM workers** with `/get_server_info`
   exposing `kv_cache_capacity` (or equivalent metric).
2. Start SMG with `--policy thunder --thunder-sub-mode tr`. Enable
   `RUST_LOG=smg::policies::thunder=debug,smg::sse=trace` so per-request
   admission and SSE parsing are observable.
3. Have a load generator that:
    - Sends each request with a unique-per-program `metadata.program_id`.
    - Spreads ~100 distinct programs across the test, each issuing ~10
      requests over the run.
    - Mixes streaming (`/v1/messages` with `stream: true`) and non-streaming
      requests to validate both code paths.

### 2.2 Pass criteria

After 8 hours of continuous traffic at ≥ 50 RPS aggregate:

| Metric | Threshold | How to check |
|---|---|---|
| **No stuck requests** | No request takes > `--thunder-resume-timeout-secs + 60` (default 31 min) | Client P99 latency curve; absence of timeouts in client logs |
| **No tokio task leak** | `cargo run --bin smg ...` RSS stays bounded; `tasks` count from `tokio-console` (if enabled) does not grow unboundedly | Linux `pmap -x <pid>`; tokio-console |
| **No memory leak** | RSS growth ≤ 1MB/hour after warm-up | `pmap -x <pid>` or Prometheus `process_resident_memory_bytes` |
| **Per-program stickiness** | The same `program_id` is routed to the same backend at least 95% of consecutive requests (excluding pause/resume cycles) | Grep `RUST_LOG=trace` for `assign program=...backend=...` and confirm same backend per pid |
| **Capacity gate functional** | `thunder TR pause` log lines appear under burst load and disappear when burst subsides | Log frequency analysis |
| **No phantom occupancy** | After workload stops, all backends report `active_program_tokens == 0` within 10 minutes | `tail -f` SMG logs for `usage applied` lines; eventually `active_program_tokens=0` for each backend |

### 2.3 Failure modes to watch for

- **Pauses lasting longer than ~10s during sustained load**: usually means
  capacity numbers are stale. Lower `--thunder-capacity-poll-interval-secs`
  to 1s temporarily and confirm pauses shorten.
- **Programs all pile onto one backend**: if `select_least_active` ties,
  ordering is by URL string. If your `--worker-urls` are alphabetically
  unbalanced (e.g., `aaa.local` vs `zzz.local`), expect mild bias.
- **`thunder TR force-resume on timeout` log lines**: a paused program
  reached the resume-timeout. This is a safety-net; if frequent, raise
  `--thunder-capacity-reserved-fraction` or add capacity.

---

## 3. Observability

### 3.1 Log keys

Thunder emits structured logs at multiple levels. Set
`RUST_LOG=smg::policies::thunder=debug` in production for
admission/pause/resume audit; bump to `trace` only when debugging.

| Module | Level | Event |
|---|---|---|
| `policies::thunder` | debug | `thunder TR admit` — request admitted (includes `bfd_resumed=true` if scheduler woke this program) |
| `policies::thunder` | debug | `thunder TR pause (full)` — request paused; capacity full |
| `policies::thunder` | warn | `thunder TR force-resume on timeout` — safety net fired |
| `policies::thunder` | trace | `usage applied` — usage_consumer processed a UsageEvent |
| `policies::thunder` | trace | `incremental streaming progress applied` — per-chunk Program.total_tokens update |
| `policies::thunder` | trace | `ProgramRequestGuard drop fallback (no usage)` — client disconnect cleanup |
| `policies::thunder` | trace | `capacity refreshed` — periodic poll of backend `/get_server_info` |
| `policies::thunder` | warn | `capacity fetch failed` — couldn't reach backend metrics endpoint |
| `sse::*` | (none in production by default; tests/dev only) | — |

### 3.2 Recommended log alerts

If you have a log-monitoring system (Loki, Splunk, etc.), alert on:

- **`force-resume on timeout` rate > 1/min sustained**: indicates capacity
  shortfall.
- **`capacity fetch failed` > 5/min for any single worker**: indicates the
  worker's metrics endpoint is unhealthy; Thunder will fall back to
  optimistic admission.
- **`progress: ... cumulative=` token totals growing without `usage applied`
  ever firing**: indicates a streaming response that never finalizes —
  client may be hung or upstream not honoring `include_usage=true`.

### 3.3 Metrics

Thunder reuses SMG's existing Prometheus metrics (`router_*`, `worker_*`).
No Thunder-specific Prometheus counter has been added in Phase 7 — observability
is via structured logs. If you need persistent metrics:

- Use a log-to-metrics pipeline (Vector, Logstash) that counts `thunder TR
  admit` / `pause` / `force-resume` log events.
- A Phase 8 follow-up may add direct Prometheus counters; track
  `docs/thunder/post-mvp-followups.md` Tier 4.

---

## 4. Common issues & runbook

### 4.1 "All requests pause indefinitely"

**Symptom**: every request hits `thunder TR pause (full)`; nothing admits;
eventually `force-resume on timeout` fires for every program.

**Likely cause**: phantom occupancy from prior client disconnects (M1
capacity-leak fix in Phase 7 resolves this). If you're running pre-M1
SMG, every client disconnect would leak `estimated_reserved_tokens`.

**Diagnosis**: `RUST_LOG=trace`; look for `Drop fallback un-reserved` lines.
If they're absent and you've had many client disconnects, you're affected.

**Fix**: upgrade to Phase 7 build (commits `51fd6951` onward).

### 4.2 "Backend looks under-utilized but Thunder won't admit"

**Symptom**: backend KV cache shows ~50% utilization, but Thunder reports
`pause (full)`.

**Likely cause**: capacity poll is stale OR `--thunder-capacity-reserved-fraction`
is too aggressive.

**Diagnosis**: check the timestamp of the most recent `capacity refreshed`
log for the affected backend. If older than 30s, polling is unhealthy.

**Fix**:

```bash
# Reduce poll interval temporarily
--thunder-capacity-poll-interval-secs 1
# Or reduce reserved fraction
--thunder-capacity-reserved-fraction 0.05  # was 0.10
```

### 4.3 "Stream returns empty data; client gets `[DONE]` immediately"

**Symptom**: streaming requests succeed at HTTP level (200 OK) but client
reads zero data from body.

**Likely cause**: SSE parser misidentified the usage chunk and stripped it
along with the content. Should not happen with Phase 7 parsers (see unit
tests in `model_gateway/src/sse/openai_chat.rs::tests`), but if you've
got an exotic upstream that emits non-standard SSE, it could trip.

**Diagnosis**: `RUST_LOG=smg::sse=trace` and inspect the chunk-by-chunk parse.

**Fix**: file an issue with the captured SSE bytes; in the meantime, switch
to `--policy cache_aware` for the affected endpoint while we extend parser
coverage.

### 4.4 "Program tied to a backend that just got removed"

**Symptom**: worker URL was removed from `--worker-urls` (or
`/control_plane` API); programs that were sticky to it pause forever.

**Cause**: scheduler doesn't actively drop assignments when a backend
disappears; it relies on `refresh_backends` at next admission.

**Mitigation**: programs will fail over on their next request because
`pick_tr` calls `select_least_active` when the sticky backend isn't in the
current URL set. Existing in-flight requests will fail when the backend
goes down (handled by retry logic).

**Diagnostic**: check `RUST_LOG=trace` for `refresh_backends`; the URL set
should match `--worker-urls` after the next admission cycle.

---

## 5. Configuration tuning

### 5.1 Backends with very different capacities

Thunder's least-active selection treats all backends equally. If your
backends have skewed capacities (e.g., 80GB H100s + 24GB A10Gs in one pool),
Thunder may bias work toward the larger backends because their "reserved
fraction" allows more concurrent programs.

**Recommendation**: deploy SMG per worker class. Thunder is happiest when
all `--worker-urls` are interchangeable.

### 5.2 Rapid scale events

When workers join or leave the pool:

- Joining: new worker has `capacity_tokens=0` until first poll. Thunder
  admits optimistically (no capacity gate) for newly-discovered backends —
  programs may spread to it before it's polled, which is fine.
- Leaving: programs sticky to the removed worker stay assigned in
  `RouterState.programs` but `refresh_backends` removes the backend entry.
  On the next admission attempt, sticky lookup fails (URL not in current
  set) and `select_least_active` picks a survivor.

### 5.3 Multi-tenant deployments

Thunder doesn't have built-in multi-tenant isolation today. Two tenants'
`program_id` namespaces could collide (e.g., both use `program_id=session-1`).

**Mitigation**: prefix `program_id` with a tenant ID at the client/proxy
layer:

```python
# Client side
metadata = {"program_id": f"{tenant_id}:{session_id}"}
```

A future SMG release will add `--thunder-program-id-prefix-from-header
X-Tenant-ID` (tracked in post-MVP follow-ups).

---

## 6. Architecture summary for operators

```
                       ┌──────────────────────────────┐
                       │  client                      │
                       │  (sends program_id in body)  │
                       └──────────────┬───────────────┘
                                      │
                                      ▼
              ┌─────────────────────────────────────────────────┐
              │ HTTP router                                     │
              │  ├─ extract program_id_hint                     │
              │  ├─ select_worker_async (Thunder)               │
              │  │    ├─ Default mode: sticky-or-least-active   │
              │  │    └─ TR mode: capacity gate + pause/resume  │
              │  ├─ create ProgramRequestGuard (Drop = cleanup) │
              │  ├─ forward to backend                          │
              │  └─ on success: emit UsageEvent                 │
              └─────────────┬───────────────────┬───────────────┘
                            │                   │
                            ▼                   ▼
                  ┌────────────────┐  ┌──────────────────┐
                  │ usage channel  │  │ progress channel │
                  └───────┬────────┘  └────────┬─────────┘
                          │                    │
                          ▼                    ▼
                  ┌────────────────┐  ┌──────────────────┐
                  │ usage_consumer │  │ progress_consumer│
                  │  - un-reserve  │  │  - increment     │
                  │  - calibrate   │  │   total_tokens   │
                  │  - decrement   │  └──────────────────┘
                  │   in_flight    │
                  └────────────────┘

  Background tasks (spawned by ThunderPolicy::new):
  ┌─────────────────────────────────────────────────────────────────────┐
  │ scheduler_tick_task   100ms tick                                    │
  │   ├─ proactive_pause_pass   (M4: pause victims when over capacity)  │
  │   └─ try_greedy_resume      (M5: BFD wake from paused pool)         │
  ├─────────────────────────────────────────────────────────────────────┤
  │ capacity_poll_task    every --thunder-capacity-poll-interval-secs   │
  │   └─ refreshes backend.capacity_tokens via /get_server_info         │
  └─────────────────────────────────────────────────────────────────────┘
```

State:

- `Program` (one per `program_id`): `backend_url`, `in_flight`,
  `total_tokens`, `step_count`, `estimated_reserved_tokens`,
  `local_char_to_token_ratio`, `local_completion_fraction`, `status`,
  `marked_for_pause`, `paused_at`.
- `BackendState` (one per worker URL): `active_programs` (set of pids),
  `active_program_tokens` (sum of all reservations + actual usage),
  `capacity_tokens` (from metrics poll).
- `RouterState`: holds the above two maps + `waiting_events`
  (per-program `Notify` for targeted wake) + global calibration values.

---

## 7. Pre-flight before each release

Before merging a new SMG build to `main`:

```bash
make pre-commit                                                           # fmt + check + test
cargo clippy --workspace --all-targets --all-features -- -D warnings      # clippy strict
cargo test --workspace --lib                                              # all unit tests
pytest e2e_test/thunder/ -v                                               # Thunder e2e (needs mock)
```

For Thunder-specific changes, additionally:

- Verify `algorithm-gap-vs-python.md` "What SMG can/cannot legitimately
  claim" table is still accurate (no rows regressed from ✅).
- Add a worklog entry in `docs/thunder/worklog.md` documenting any new
  intentional divergence from Python ThunderAgent reference.
- Run `cargo test --package smg --lib policies::thunder::tests::` and
  confirm count hasn't dropped.

---

## 8. References

- [User-facing intro](../getting-started/thunder.md)
- [Algorithm gap inventory](algorithm-gap-vs-python.md) — every SMG ↔ Python
  divergence with rationale
- [Phase 7 spec](../superpowers/specs/2026-05-01-thunder-phase7-production-design.md)
  — design of the production-ready ThunderPolicy
- [Worklog](worklog.md) — sign-off log of every autonomous decision (D-1
  through D-37)
- [Post-MVP follow-ups](post-mvp-followups.md) — backlog items not in scope
  for Phase 7
- Python reference (read-only): `/home/hkang/wl/smg_thunder/ThunderAgent/`
