# Thunder Post-MVP Follow-Ups

> Snapshot date: 2026-05-01
> MVP commit: trunk `thunder-policy` HEAD `19d92288` (after ff-merge of P5+P6 combined)

This file consolidates work **deferred** during the autonomous Claude-driven MVP push (P0-P6) so the user has a single place to plan next-session priorities. Each item links to the worklog decision that authorized the deferral.

## What shipped (MVP)

| Phase | Commits | What it delivers |
|---|---|---|
| **P0** | `1ab677ca`-`4a3ef73c` | `/v1/messages` HTTP pass-through; `program_id_hint` trait method; mock backend; e2e baseline |
| **P1** | `546962d8`-`6a306544` | `LoadBalancingPolicy::select_worker_async` + `usage_sender` trait extension; `SelectWorkerInfo.program_id` field; HTTP+gRPC async migration; per-policy parity tests |
| **P2** | `6381c67e`-`208b9aaf` | Mock multi-protocol (`/v1/responses` added); cross-protocol smoke test |
| **P3** | `42c0408e`-`6ba876c0` | `ThunderPolicy` skeleton (Program/BackendState/RouterState); Default sub-mode (sticky least-active-count); `--policy thunder` CLI; factory wiring |
| **P4** | `0d7aa9fc`-`88126c38` | `MetricsClient` trait + HTTP impl; capacity-poll task; `usage_consumer` task; non-streaming `UsageEvent` emission |
| **P5+P6** | `c887a3e1`-`19d92288` | TR sub-mode capacity-aware admission; `Notify`-based pause/resume; force-resume timeout; `ProgramRequestGuard` RAII |

**Tests at MVP**: 12 e2e pass (3 P0 + 4 P2 + 3 P3 + 2 P5+P6); 18+ unit tests in `policies::thunder`; 117 in `policies::*`; 708 workspace-wide. `cargo build --workspace` + `cargo clippy -- -D warnings` clean. xref clean.

**Try it:**

```bash
cd /home/hkang/wl/smg-wl
./target/debug/smg start \
    --host 127.0.0.1 --port 30000 \
    --worker-urls http://localhost:8001 \
    --policy thunder \
    --thunder-sub-mode tr \
    --thunder-resume-timeout-secs 60
```

---

## Deferred work (post-MVP)

### Tier 1 — Algorithmic fidelity (production differentiator)

| Item | Why deferred | Worklog | Effort | When to revisit |
|---|---|---|---|---|
| **BFD greedy_resume** (faithful Python `router.py:719-844` port) | Time pressure; simple broadcast-wake works correctly | D-22 | ~150 LOC | When load tests show >20% capacity wasted by suboptimal placement |
| **`mark_for_pause` for in-flight ACTING programs** | Only matters once BFD picks specific victims | D-22 | ~50 LOC | After BFD lands |
| **`shared_tokens` calc in scheduler** (Q5.3 FORK) | P4 trim — not yet relevant since BFD isn't running | D-20 | ~30 LOC | After BFD lands |
| **`char_to_token_ratio` calibration** (Q5.5) | P4 trim — only useful if estimate accuracy matters | D-20 | ~40 LOC | If `estimate_request_tokens()` causes obvious mis-admissions |

### Tier 2 — Streaming support

| Item | Why deferred | Worklog | Effort | When to revisit |
|---|---|---|---|---|
| **HTTP streaming usage tail extractor** (parse SSE for usage chunk after `[DONE]`) | Complex SSE parsing across vLLM/sglang/sidecar formats | D-20 | ~80 LOC | When users actually use streaming through `--policy thunder` |
| **`stream_options.include_usage = true` injection** on outbound streaming | Pairs with above; injecting on every thunder request shapes upstream behavior | D-20 | ~20 LOC | Same trigger as above |
| **streaming retry × in_flight idempotency** | Retry logic D-9 designed for non-streaming only | D-9 | ~50 LOC | If mid-stream restart becomes a feature |

### Tier 3 — Coverage expansion

| Item | Why deferred | Worklog | Effort | When to revisit |
|---|---|---|---|---|
| **gRPC path validation** (P7 row in `10-phases.md`) | MVP scope; HTTP path covers internal-only deployments | spec §3.3 | ~200 LOC + e2e | When a deployment actually needs gRPC backends |
| **Profiling endpoints** (`/thunder/programs`, `/thunder/profiles`) (P8) | Diagnostic-only; not load-bearing | spec §8 | ~150 LOC | First production deployment / debugging session |
| **CI integration of `pytest e2e_test/thunder/`** | Per D-12 explicit no-CI policy | D-12 | ~30 LOC YAML | When CI capacity is willing to host SMG + mock fixture |

### Tier 4 — Polish (low-priority unless triggered)

| Item | Why deferred | Worklog | Effort | When to revisit |
|---|---|---|---|---|
| **Per-backend RwLock sharding** (vs single `Arc<RwLock<RouterState>>`) | D-3 perf footgun acknowledged; benchmark first | D-3 | ~100 LOC | If thunder.rs `policies::*::stream*` benchmarks show contention >5% CPU |
| **Targeted Notify wake** (vs broadcast) | D-22 broadcast simplicity; thundering-herd bounded | D-22 | ~30 LOC | If wake latency >100ms under load |
| **Replace prod `expect()` calls with `?`-propagation** | P4 introduced one in `HttpMetricsClient::default_client()` | (P4 review) | ~10 LOC | Code review polish round |
| **`--thunder-use-acting-token-decay` toggle** (P9) | Q5.7 footgun docs already note this | Q5.7 | ~30 LOC | If observed token over-counting matters |
| **Deployment runbook** (sidecar setup, mount-path invariant per §10.5) | Manual deployment knowledge | D-13 | ~doc | First production deploy |

### Tier 5 — Code review nits (collected during reviews)

1. **`policies::thunder.rs::pick_default_inner` uses `state.blocking_write()`** when called from sync `select_worker`. Safe outside async runtime, panics inside. Document or wrap in `if let Ok(rt) = tokio::runtime::Handle::try_current() { ... } else { blocking_write() }`.
2. **`extra/auxiliary policy_factory test for thunder`** missing — covered partially by `policies::thunder::tests` but not in `policies::factory::tests`.
3. **`ProgramRequestGuard::Drop` spawns a tokio task** (because Drop is sync but cleanup is async). Risk: if the runtime is shutting down, the spawn fails silently. Acceptable but worth a `tracing::trace!` log.

---

## Autonomous decisions made (D-19 through D-22)

| ID | Decision | Reversal cost |
|---|---|---|
| **D-19** | P3 scope trim: skeleton + Default mode only; usage_consumer / RAII / capacity / pause-resume layered into P4-P6 | None (additive layering succeeded) |
| **D-20** | P4 scope trim: HTTP capacity polling + non-streaming usage; streaming usage tail deferred | None (P4 lands clean; streaming gap is a known follow-up) |
| **D-21** | Combined P5 + P6 into one phase | None (worklog still has "P5+P6" tag; could split retroactively if needed) |
| **D-22** | Simplified pause/resume: broadcast Notify, no BFD, simplified ProgramRequestGuard, force-admit-after-timeout | Partial: BFD can replace `select_least_active` in `pick_tr` without disturbing wire format |

All four decisions have **revisit conditions** documented in their respective worklog entries. None of them ossify a wrong direction; all add up to "first working version" with clear iterative paths.

---

## Recommended next session order

1. **Run `--policy thunder --thunder-sub-mode tr` against a real SLURM-hosted sglang backend** (per `docs/thunder/slurm-cluster.md` jobid 30385) to validate the e2e pause/resume cycle outside the mock. Likely reveals 1-2 new footguns.
2. **Write streaming usage tail extractor** (Tier 2 first item) — biggest user-visible MVP gap.
3. **Run a soak test**: 100 programs × 10 requests each through thunder TR mode against 2 backends with capacity 1024. Validate no requests timeout, no programs lose stickiness.
4. **Review autonomous decisions D-19..D-22**; sign off or override.
5. **Tier 1 BFD port** if soak test reveals capacity waste.
6. **gRPC validation (P7)** if a real deployment needs gRPC.
