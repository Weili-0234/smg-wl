# Thunder Integration Worklog

This file tracks **non-trivial design decisions** made during the thunder integration project, with explicit notes on alternatives considered and conditions that would justify revisiting. Each decision is dated and links to the relevant spec section.

The point of this file: when someone (including future-us) wants to change a behavior and asks "why did we do it this way?" — the answer should be findable here, not buried in commit messages or chat logs.

Format per entry: numbered, dated (YYYY-MM-DD), 4 sections (Context / Options considered / Chosen / Revisit conditions). Append; never delete.

---

## D-9: retry × pause/resume — thunder internal idempotent re-entry

**Date**: 2026-04-30
**Spec ref**: `THUNDER_POLICY_DESIGN.md` §11 (Phase P6 plan), §10.9 (footgun, to be added)
**Task ref**: #37

### Context

SMG's `routers/http/router.rs` runs worker selection inside the retry loop: each retry attempt calls `route_typed_request_once` (line 281), which calls `select_worker_for_model` (line 290), which calls `policy.select_worker_async(...)` (line 175). Concretely, when a thunder-policy program request gets a transient 5xx and retry fires, **thunder is called a second time for the same client request**.

Naive behavior under thunder: second call would re-run `update_program_before_request` (incrementing `step_count`), re-run capacity check (potentially re-pausing), and re-add tokens to `BackendState.active_program_tokens` (double-counting). All three break Python ThunderAgent semantics — Python has no retry layer; one client request = one admit, one step.

### Options considered

| Option | Behavior | Why rejected (or chosen) |
|---|---|---|
| A. Naive — thunder re-enters fully on every retry | step_count drifts; capacity double-counts; possible pause-chain (single client req paused N times in a row across retries) | **Rejected**: violates algorithm semantics. |
| B. Lift `select_worker` out of retry loop (mirror OpenAI router pattern at `openai/chat.rs:65`) | Worker selected once before retry loop; same worker reused for all attempts | **Rejected**: changes retry semantics for ALL policies on HTTP path (cache_aware users currently get retry-on-different-worker tolerance, would lose it). Out of thunder scope. |
| **C. Thunder internal idempotent re-entry** (CHOSEN) | First call admits; subsequent calls in same request detect program already in `(REASONING, ACTIVE)`, skip lifecycle + capacity, only re-pick worker (handles in-retry CB-open) | **Chosen**: faithful to Python (one admit per request); fully scoped to thunder module; preserves retry-on-different-worker for non-thunder policies. |
| D. Disable retry when policy is thunder | Simplest | **Rejected**: regresses SMG's retry resilience. Python ThunderAgent has no retry, but Rust port should not be capped at Python's standalone-proxy capabilities. |

### Chosen design (Option C + sub-option C1)

1. `ThunderPolicy::select_worker_async` checks if `program.status == REASONING && state == ACTIVE` at entry. If true → it's a retry of an already-admitted program; skip `update_program_before_request`, capacity check, and any pause logic. Just `pick_or_repick_backend(pid, workers)` and return.
2. `ProgramRequestGuard` (the RAII cleanup struct from Q5.4 sign-off) carries an `in_flight: bool` flag. While the guard is alive, the program is "in flight" — under retry potentially across multiple `select_worker_async` calls.
3. Scheduler `_pause_until_safe` skips programs with `in_flight == true` (sub-option C1). This prevents the scheduler from picking a program for pause WHILE the router is mid-retry. (Without C1, race: scheduler PAUSEs program X between attempt N and N+1; attempt N+1 re-enters thunder, sees state == PAUSED, falls into admission branch → re-enqueue, double pause.)
4. `pick_or_repick_backend` returns the saved `program.backend_url` if that worker is still in the available list (CB-closed); otherwise re-picks from available, emits `tracing::warn!` + `smg_thunder_retry_repick_total{from, to}` metric.
5. ProgramRequestGuard is held by **the router** for the full retry loop duration (created at first thunder call, dropped when retry loop completes). On Drop without `complete()` → `force_terminate_program` (idempotent).

### Approximate cost

~30 LOC in `policies/thunder.rs` (idempotency check + repick logic) + ~5 LOC in scheduler `_pause_until_safe` (in_flight skip) + 1 new metric `smg_thunder_retry_repick_total`. No changes outside `policies/thunder.rs` and the scheduler tick body. No HTTP router changes.

### Revisit conditions

Revisit this design if any of the following hold in production:

1. **`smg_thunder_retry_repick_total` is high**, indicating frequent CB-open transitions during retry windows. May suggest backend instability or a tighter integration where retry budget is shared with thunder's pause/resume budget.
2. **Retry budget consumed by thunder admission**: if `--retry-max-retries` (default 5) × backoff time ≥ `--thunder-resume-timeout-secs` (default 1800), retries could time out the same window as force-resume — should be impossible with default values but worth a runbook sanity check.
3. **Concurrent in-flight requests under same program_id** (signed-off footgun: ignore + warn): if user lifts the "single in-flight per program" constraint later (e.g., for parallel tool execution within one agent step), the in_flight flag becomes a single-bit insufficient — would need a counter or list.
4. **Streaming retry** ever becomes a thing: today retry only fires on upfront 5xx (status set before body); if SMG ever adds mid-stream restart, the in_flight assumption breaks and we need to revisit how usage_sender events deduplicate across attempts.
5. **A different SMG router** (gRPC, future PD) has different retry semantics that don't follow the "select_worker per attempt inside retry loop" pattern. Each integration phase (P7 gRPC) must verify Option C still applies; if not, document a per-router variant.

### Approved by

Weili Xu, 2026-04-30 session.

---

## D-10: testing topology α — single compute node, everything via `srun`

**Date**: 2026-04-30
**Spec ref**: `THUNDER_POLICY_DESIGN.md` §11 (Phase P2 fixtures), §9 (testing strategy)
**Cluster ref**: `/home/hkang/wl/smg_thunder/slurm_cluster_info.md`

### Context

User selected (β) heavy-e2e testing approach with real vllm + sglang backends + litellm sidecar. Cluster access is restricted: cannot SSH to compute nodes; only `srun --jobid=<id> --overlap --gpus=0 bash -c '<cmd>'` works. Single SLURM allocation: jobid 30385 on `research-secure-31` (8× H100 80G).

Initial proposal contemplated three topologies (α single-node-everything, β backends-on-compute-clients-on-login, γ multi-node-distributed). β requires login → compute TCP reachability, which the user's sandbox forbids probing and which the user has not confirmed. γ requires multi-node SLURM allocation, which the user explicitly declined ("no need for fancy topology yet").

### Options considered

| Option | Description | Why rejected (or chosen) |
|---|---|---|
| α — all on compute node | Backends + litellm sidecars + SMG binary + pytest all run on `research-secure-31` via `srun` | **Chosen** — only topology compatible with no-SSH, no network probing, single allocation |
| β — split login/compute | Compute hosts servers; login runs SMG + pytest | Rejected — login→compute TCP reachability unverified and sandbox-forbidden to probe |
| γ — multi-node | Spread across 2+ allocations | Rejected — user explicitly declined fancy topology for now |

### Chosen design (α)

**Long-running services (start once, persist across iterations)**:

```
research-secure-31 (jobid 30385):
  GPU 0  → vllm-1     localhost:8001
  GPU 1  → vllm-2     localhost:8002
  GPU 2  → sglang-1   localhost:8003
  GPU 3  → sglang-2   localhost:8004
  CPU    → litellm-1  localhost:8011 → forwards to localhost:8001
  CPU    → litellm-2  localhost:8012 → forwards to localhost:8002
  CPU    → litellm-3  localhost:8013 → forwards to localhost:8003
  CPU    → litellm-4  localhost:8014 → forwards to localhost:8004
```

**Per-iteration services (restart per dev cycle)**:

```
  CPU    → smg_thunder localhost:30000  (registers 4 worker URLs: localhost:8011-8014)
```

**Per-test invocation**:

```
  CPU    → pytest e2e_test/thunder/  (sends requests to localhost:30000)
```

All four levels invoked via `srun --jobid=30385 --overlap --gpus=0 bash -c '<cmd>'`. SMG sees 4 distinct backend URLs (8011/8012/8013/8014); thunder's `BackendState` gets 4 separate entries; BFD pause/resume across backends is testable.

Litellm sidecar topology: **one per backend** (signed-off as production-faithful per the prior brainstorm). Drives `/v1/messages` and `/v1/responses` translation.

Model: `Qwen/Qwen3-0.6B` (small enough that 4 instances on 4 H100s leaves 4 GPUs idle as buffer).

Filesystem: shared between login and compute (`/home/hkang/`); edits and `cargo build` run on login, executed binaries run on compute.

### Revisit conditions

1. **Need to test multi-node thunder behavior** (worker on different physical hosts, network latency in routing decisions): switch to topology γ once a second SLURM allocation is available.
2. **CI integration is later requested**: this design assumes local-dev only. CI would need a way to spin up servers automatically; current `srun` approach is interactive.
3. **Login → compute TCP reachability is later confirmed allowed by the cluster operator**: could move pytest back to login node for faster iteration (no `srun` round-trip per `pytest` invocation).
4. **Resource pressure on jobid 30385** (other jobs on same node consuming GPUs/CPU during testing): adjust GPU pinning or move to dedicated allocation.

### Approved by

Weili Xu, 2026-04-30 session.


---

## D-11: testing infrastructure shape (launcher, fixture URL discovery, SMG restart)

**Date**: 2026-04-30
**Spec ref**: `THUNDER_POLICY_DESIGN.md` §11 (Phase P2)

### Context

D-10 locked single-node + everything-via-srun topology. Need concrete shape for: (A) how to start the 8 long-running services; (B) how pytest fixtures discover their URLs; (C) how SMG binary is restarted across dev iterations without restarting backends.

### Chosen design

**(A) Launcher: Python script with uv** — `e2e_test/thunder/scripts/start_backends.py`. Spawns vllm/sglang/litellm via `subprocess.Popen(["srun", "--jobid=30385", "--overlap", "--gpus=N", ...])` with health-check polling between layers (4 backends ready → then 4 sidecars). Bash one-liner alternatives kept as escape hatch documented in scripts/README.md for single-service restart.

Rationale: dependency ordering (litellm depends on backends being live) and health-check polling are clearer in Python than bash. uv is the project's chosen Python toolchain.

**(B) URL discovery: conf file + fixture health-check** — `e2e_test/thunder/test_config.toml` is the single source of truth for ports + GPU pinning. Both the launcher script and pytest fixtures read it. Fixtures health-check before yielding URL; on failure, fail fast with clear message ("run `uv run python scripts/start_backends.py` first"). No env-var indirection.

**(C) SMG restart: dedicated script** — `e2e_test/thunder/scripts/restart_smg.sh`. `pkill smg; sleep 1; nohup ./target/debug/smg start --policy thunder ... &; wait_for_health; echo ready`. Pytest does NOT manage SMG lifecycle — multiple pytest runs share the same SMG instance, which keeps thunder's program registry / scheduler state alive across test invocations (useful for debugging by inspecting `/thunder/programs`).

### Revisit conditions

1. **Cargo iteration becomes too slow**: if `cargo build` + `srun` round-trip per restart ≥ 30s noticeably hurts dev velocity, reconsider topology β (SMG on login node) once login→compute TCP reachability is verified by user/cluster operator.
2. **Test isolation needed**: shared SMG instance across pytest runs means one test's mutations to `/thunder/programs` could leak. If this causes flakes, switch to pytest-managed SMG lifecycle (autouse fixture spawns SMG per session, cleans up on exit).
3. **More than 1 SMG version under test**: if comparing two SMG builds, single-binary at a fixed port doesn't work. Add port-templating in conf file.

### Approved by

Weili Xu, 2026-04-30 session.

---

## D-12: backend simplification — 4× sglang only; mock retained for capacity tier

**Date**: 2026-04-30
**Spec ref**: `THUNDER_POLICY_DESIGN.md` §11 (Phase P2 fixtures + P5/P6 testing)
**Supersedes (in part)**: D-10 backend list (was 2 vllm + 2 sglang; now 4 sglang)

### Context

D-10 specified a heterogeneous mix: 2 vllm + 2 sglang. User revised: simplify to 4× sglang, leveraging sglang's `/flush_cache` endpoint (verified at `python/sglang/srt/entrypoints/http_server.py:762`, supports GET + POST) for per-test KV-cache reset without server restart. sglang's own test fixtures use this exact pattern (`server_fixtures/default_fixture.py:73`).

This raises the question: what about thunder's capacity-overflow scenarios (P5 admission, P6 pause/resume + BFD)? sglang's `--max-num-seqs` is a startup arg; `/flush_cache` clears KV state but does NOT change capacity at runtime. Forcing capacity-full with real sglang requires hack tricks (low `--max-num-seqs` + sustained long-prompt traffic) that are timing-sensitive and flaky.

### Options considered

| Option | Description | Why rejected (or chosen) |
|---|---|---|
| α — pure real sglang for everything | Use `--max-num-seqs 2` + sustained N+1 long prompts to force capacity tests | Rejected: flaky (Qwen-0.6B is too fast; long-prompt traffic doesn't reliably hold KV; kv_cache_usage_perc oscillates as decode batches release KV) |
| **β — real sglang for happy path + mock_xxx.py for capacity tier** (CHOSEN) | 20+ tests use real sglang (multi-protocol, retry, momentum, ProgramRequestGuard, etc.); ~6 capacity-overflow tests use the existing `mock_vllm.py` (rename → `mock_sglang_compat.py`, OpenAI-compat already) with deterministic `/control/capacity` | **Chosen**: deterministic capacity tests; real-backend coverage for everything else; mock cost ≈ 0 (file already exists, ~290 LOC, mostly reusable verbatim) |
| γ — write a thin capacity-cap proxy in front of real sglang | Forwarding proxy exposes `/control/capacity` while passing through real responses | Rejected: 100+ LOC of new code with no benefit over (β) |

### Chosen design

**Real sglang tier** (`e2e_test/thunder/test_real_sglang/`):

```
research-secure-31 (jobid 30385):
  GPU 0  → sglang-1   localhost:8001  (HTTP server + RadixCache)
  GPU 1  → sglang-2   localhost:8002
  GPU 2  → sglang-3   localhost:8003
  GPU 3  → sglang-4   localhost:8004
  CPU    → litellm-1  localhost:8011 → 8001
  CPU    → litellm-2  localhost:8012 → 8002
  CPU    → litellm-3  localhost:8013 → 8003
  CPU    → litellm-4  localhost:8014 → 8004
```

Pytest fixture autouse `flush_caches`:
```python
@pytest.fixture(autouse=True)
def flush_caches(backend_urls):
    for url in backend_urls:
        requests.post(f"{url}/flush_cache", timeout=5).raise_for_status()
    yield
```

P7 (gRPC validation) reuses these instances if sglang exposes both HTTP and gRPC on different ports of the same process; otherwise spawns 4 additional gRPC sglang instances on GPUs 4-7 (still room).

**Mock tier** (`e2e_test/thunder/test_capacity_mock/`):

- 1 instance of renamed `mock_sglang_compat.py` (lifted from `/home/hkang/wl/smg_thunder/e2e_test/thunder/mock_vllm.py`).
- `/control/capacity` end-point accepts `{"capacity_tokens": int}` and `mock_sglang_compat.py` returns synthetic `/get_server_info` reflecting that cap.
- Tests exercise: P5 admission 503 on `capacity=0`; P6 pause/resume cycle (set 0 → admit pauses → set 100k → BFD migrate → unblock); P6 force-timeout (capacity always 0, resume_timeout=5s, expect force-resume metric increment); D-9 retry × pause idempotency.
- SMG started with **only the mock URL registered** when running this tier (different `--worker-urls` flag than real-sglang tier).

**pytest markers**:

```python
@pytest.mark.real_backend   # default; runs against 4 sglang
@pytest.mark.mock_capacity  # runs against mock_sglang_compat
```

Default `pytest e2e_test/thunder/` runs `real_backend`. `pytest -m mock_capacity` runs only the mock tier. Both can run sequentially (different SMG instance, different port; restart_smg.sh accepts a `--mode {real,mock}` flag).

### Revisit conditions

1. **mock_sglang_compat drifts from real sglang behavior**: if sglang upstream adds new `/get_server_info` fields that thunder reads, mock must be updated. Document in mock module header which sglang version it tracks.
2. **Real sglang capacity tests become deterministic** (e.g., sglang upstream adds runtime `--set-max-num-seqs` admin endpoint): retire mock tier.
3. **Tests start failing because flush_cache doesn't fully reset state** (e.g., sglang adds non-cache state that bleeds): switch to per-test process restart, accept slowness.
4. **gRPC backend testing reveals different capacity-overflow path**: P7 phase may need its own mock variant (mock_sglang_grpc.py).

### Approved by

Weili Xu, 2026-04-30 session.

---

## D-13: pre-flight #3 self-verification — `route_typed_request` is protocol-agnostic; 3 small additions to D-6 P0 scope

**Date**: 2026-04-30
**Spec ref**: §5.5b (expanded), §5.5e (new), §10.5 (sidecar mount-path invariant), §10.10/10.11/10.12 (footguns added)

### Context

User pushed back on premature progression to writing-plans before debt list was cleared. Pre-flight item #3 ("manually verify route_typed_request has no OpenAI-specific assumptions blocking T = CreateMessageRequest") had been claimed-done in earlier turn but not actually performed. This entry records the verification and resulting spec patches.

### Verification performed

Read end-to-end (file:line citations) the full call graph of `routers/http/router.rs::route_typed_request<T: GenerationRequest + Serialize + Clone>`:

- `route_typed_request` (line 196) — wraps `route_typed_request_once` in `RetryExecutor`
- `route_typed_request_once` (line 281) — calls `select_worker_for_model` then `send_typed_request`
- `select_worker_for_model` (line 140) — uses `policy.select_worker(...)` with `SelectWorkerInfo.request_text`
- `send_typed_request` (line 835) — `worker.endpoint_url(route)` + `serde_json::to_value(typed_req)` + `worker.prepare_request(json)` + `client.post(...).json(...)` + bytes_stream forwarding
- `worker.endpoint_url(route)` at `worker/worker.rs:444` — `format!("{}{}", base_url, route)`, pure string concat
- `worker.prepare_request(json)` at `worker/worker.rs:471` — only injects `data_parallel_rank` for DP-aware workers, otherwise passthrough
- `route_to_endpoint(route)` at `grpc/utils/metrics.rs:8` — hardcoded `match` whitelist

### Findings

**F-1**: `route_typed_request` core logic is protocol-agnostic. T-specific dependencies are confined to trait methods (`is_stream`, `extract_text_for_routing`, `Serialize`, `program_id_hint`). The body bytes-stream-forward path makes no assumptions about SSE event names; both Anthropic SSE (`event: message_start\ndata: {...}\n\n`) and OpenAI SSE (`data: {...}\n\n`) flow through unchanged.

**F-2**: `extract_text_for_routing` for `CreateMessageRequest` MUST be implemented (it's a trait method; without an impl the code won't compile). Pattern mirrors `crates/protocols/src/chat.rs:598-640`: iterate `system: Option<SystemContent>` and `messages: Vec<InputMessage>`, accumulate `MessageContent::Text` variants into a single buffer (skip Image/ToolUse/ToolResult/Document). Estimated ~30-50 LOC. **D-6 task description was understating P0 scope** by not naming this function explicitly.

**F-3**: `route_to_endpoint(route)` at `grpc/utils/metrics.rs:8` does not match `/v1/messages` → returns `"other"`. The `metrics_labels::ENDPOINT_MESSAGES = "messages"` constant already exists at `observability/metrics.rs:387` but is unwired. P0 must add 1 line. Without this fix, thunder's `/v1/messages` traffic silently buckets into `endpoint="other"` in `smg_router_*` Prometheus series.

**F-4**: `worker.endpoint_url(route)` is `format!("{}{}", base_url, route)` — pure string concat with no path normalization. The litellm sidecar MUST mount its 3 endpoints at the **root** of the registered worker URL (no `/proxy` or `/anthropic` prefix). litellm's default config does this; deployment runbook must verify. Documented as §10.5 mount-path invariant.

### Spec patches applied (this session)

1. **§5.5b expanded**: full method list for `GenerationRequest` impl on `CreateMessageRequest` (is_stream / get_model / extract_text_for_routing / program_id_hint), with line counts and file:line precedents
2. **§5.5e added**: `route_to_endpoint` 1-line wiring
3. **§10.5 expanded**: sidecar mount-path invariant
4. **§10.10 added**: model name rewriting is the deployer's responsibility (sidecar handles)
5. **§10.11 added**: concurrent in-flight per program_id — 503 + warn (signed-off footgun); flagged P6 for impl detail re: distinguishing "retry of same request" vs "concurrent new request"
6. **§10.12 added**: axum/middleware verdict (subagent-audited safe for 1800s await) + open runtime verifications

### D-6 task scope correction

Task #32 description updated. P0 LOC estimate revised from "~50 LOC" to **~65 LOC** (driven primarily by `extract_text_for_routing` impl on `CreateMessageRequest`).

### Revisit conditions

This is a verification entry, not a design decision. No revisit needed unless new pre-flight items surface during implementation.

### Approved by

Weili Xu, 2026-04-30 session ("好").

---

## D-14: CLI flag interaction matrix for thunder

**Date**: 2026-04-30
**Spec ref**: §6.1 (interaction matrix)

### Context

Pre-flight items #8 and #10 (merged — both are about CLI flag interaction). SMG's existing `config/validation.rs::validate_compatibility` checks 3 things (power_of_two needs ≥2 workers, PD+bucket on decode is forbidden, mTLS completeness). Thunder adds one new check + relies on CLI parser whitelist for two paths.

### Chosen rules (full matrix in spec §6.1)

| Combo | Verdict | Mechanism |
|---|---|---|
| `--policy thunder` alone | ✅ | Default behavior; single ThunderPolicy instance |
| `+ --enable-igw` | ✅ | Per-model thunder instance; documented in §10 footgun (independent capacity pools, no cross-model BFD) |
| `+ --service-discovery` | ✅ | Auto-enables IGW; K8s worker churn handled by `subscribe_events` |
| `+ --pd-disaggregation` | ❌ | Hard fail at `validate_compatibility`; reason: "Thunder doesn't support PD in this release" |
| `--prefill-policy thunder` / `--decode-policy thunder` | ❌ | clap value_parser at `main.rs:217 :222` excludes "thunder"; rejected at CLI parse |
| `+` cache_aware-specific flags (cache_threshold, etc.) | ✅ ignore | `tracing::info!` log noting flag is policy-specific and ignored under thunder |

### Why hard-fail PD instead of silent-degrade

`routers/http/pd_router.rs:861` calls sync `policy.select_worker(...)`. Thunder's algorithm lives in async `select_worker_async`; the sync default-impl falls back to a degenerate selection (e.g., first available worker). Silent degrade would let users think thunder is enforcing capacity when it actually is not. Hard fail at startup is correct.

### Revisit conditions

1. Phase plan adds PD support: rewrite this matrix; thunder's PD path needs its own algorithm (BFD across prefill-decode pairs is non-trivial — Python ThunderAgent doesn't have PD).
2. Multi-model deployment with mixed policies (some models thunder, some cache_aware): the per-model PolicyRegistry already supports this via `get_policy_or_default(model_id)`; document properly when first deployed.
3. New SMG-level flag added that interacts with thunder: review here before signing off.

### Approved by

Weili Xu, 2026-04-30 session ("OK的").

---

## D-15: spec hierarchy split — single-file → 11 topic files + INDEX

**Date**: 2026-04-30
**Spec ref**: directory move; `THUNDER_POLICY_DESIGN.md` → `docs/thunder/*.md`

### Context

After ~12 hours of brainstorming + post-compact rebuild + pre-flight verification, the spec had grown to a 1372-LOC single file (`THUNDER_POLICY_DESIGN.md`). User requested splitting into "outline + mapping + 具体分不同文件描述对项目和 codebase 不同方面的理解". The single-file form was getting unwieldy: cross-section navigation required Ctrl-F; PR diffs touching one section bled visual noise into others; reviewers couldn't easily focus on a specific concern.

### Options considered

| Option | Description | Why rejected (or chosen) |
|---|---|---|
| α — thematic split (11 topic files + INDEX) | Each spec section becomes its own file; INDEX.md is the navigation source of truth | **Chosen**: per-topic editorial independence; PR diff hygiene; matches user's "outline + mapping + 不同文件" framing |
| β — audience split (deployer / implementer / reviewer dirs) | Same content reorganized by reader role | Rejected — heavy duplication across audiences; 1-team project doesn't need this taxonomy |
| γ — minimal split (3-4 files) | overview / design / operations / glossary | Rejected — `design.md` would still be ~700 LOC; insufficient improvement |
| δ — keep single file + INDEX.md only | Cosmetic overlay | Rejected — doesn't solve PR diff noise |

### Chosen design (α)

```
docs/thunder/
├── 00-INDEX.md                  ← outline + file map + decision-to-file cross-ref
├── 01-overview.md               ← TL;DR + Mission + Architecture (was §0/§1/§3)
├── 02-decisions.md              ← decision log (was §2)
├── 03-algorithm.md              ← algorithm core + glossary (was §4 + §13)
├── 04-smg-integration.md        ← biggest file: trait/struct/factory/program_id/hooks (was §5)
├── 05-config-cli.md             ← CLI + interaction matrix (was §6 + §6.1)
├── 06-cross-protocol.md         ← sidecar deployment (was §7)
├── 07-observability.md          ← metrics/tracing/admin (was §8)
├── 08-testing.md                ← testing strategy (was §9)
├── 09-footguns.md               ← known limitations (was §10)
├── 10-phases.md                 ← phase plan + sign-off + file inventory (was §11/§12/§15)
├── worklog.md                   ← (moved from worktree root; this file)
├── slurm-cluster.md             ← (copied from /home/hkang/wl/smg_thunder/)
└── legacy/
    └── requirements-brainstorm.md   ← (moved from THUNDER_POLICY_REQUIREMENTS.md)
```

Worktree root gets a 20-LOC `THUNDER.md` pointer for any tool/CI that doesn't know about `docs/thunder/` yet. The original `THUNDER_POLICY_DESIGN.md` is rewritten as a redirect note (does NOT delete — kept reversible until external references are confirmed updated).

A bash sanity script `scripts/check_thunder_xref.sh` checks: (a) markdown links to nonexistent topic files; (b) surviving `§X.Y` in-doc references (warn — tolerated, since users can Ctrl-F by section number); (c) references to legacy `THUNDER_POLICY_DESIGN.md` path (warn); (d) `D-N` references not declared in `worklog.md` or `02-decisions.md` (warn). Run via `bash scripts/check_thunder_xref.sh`. No CI integration (per user's no-CI policy for thunder work).

### Caveats accepted

1. **`§X.Y` cross-references inside topic files are not rewritten to file-relative links**. Reader uses Ctrl-F. Rationale: full link rewrite would touch ~30 sites and add brittle anchor-format dependencies (GitHub-flavored markdown auto-anchors are kebab-case-from-header which breaks if section title is reworded). The script warns about these but doesn't fail.
2. **Worklog entries D-9 through D-14** retain `Spec ref: THUNDER_POLICY_DESIGN.md §X` strings as-is — those references are historical (decision was recorded when spec lived at that path). New worklog entries (D-15+) reference the split paths.
3. **D-1 through D-8** were inlined in the original spec's §2 decision-log table and do NOT have separate worklog entries. They live in `02-decisions.md` as table rows. The xref script's check-4 looks at both `worklog.md` and `02-decisions.md` to recognize them.

### Revisit conditions

1. **External tooling references the legacy path**: keep `THUNDER_POLICY_DESIGN.md` redirect alive until verified gone. Once safe, can be deleted with `rm`.
2. **Topic file grows beyond ~600 LOC**: split that file further. Current largest is `04-smg-integration.md` at 624 LOC — if it crosses ~800, consider splitting into 04a-trait.md + 04b-extraction.md + 04c-hooks.md.
3. **Cross-references break frequently**: invest in proper anchor-based links (e.g., `[§5.5b](04-smg-integration.md#5-5b-generationrequest-trait-extension)`) and add anchor-existence checks to the script.
4. **A new audience (e.g., security review) needs a different cut of the same content**: build that in `docs/thunder/audience/<name>/` rather than reorganizing the topic files.

### Approved by

Weili Xu, 2026-04-30 session ("好，就用 Option α").

---

## D-16: P0 implementation completed — /v1/messages pass-through landed

**Date**: 2026-04-30
**Spec ref**: `docs/thunder/10-phases.md` P0 row, `docs/thunder/04-smg-integration.md` §5.5b/c/d/e

### What landed

- `GenerationRequest::program_id_hint` (default-None) on the trait at `crates/protocols/src/common.rs:40`
- `Metadata.program_id: Option<String>` at `crates/protocols/src/messages.rs:178`
- `impl GenerationRequest for CreateMessageRequest` (4 methods, ~55 LOC) at `crates/protocols/src/messages.rs`
- `"/v1/messages" => ENDPOINT_MESSAGES` arm at `model_gateway/src/routers/grpc/utils/metrics.rs:8`
- `Router::route_messages` pass-through at `model_gateway/src/routers/http/router.rs`
- e2e: `e2e_test/thunder/{__init__.py,conftest.py,mock_vllm.py,test_phase0_messages_passthrough.py}` — 3 tests pass
- Mock backend exposes `/v1/models` + `/version` per Task 7's discovery-fix commit `063ccb64` — required because SMG's `workflow/steps/local/{detect_backend,discover_metadata}.rs` probes these to classify and learn the served model id

### What did NOT change

- No policy code touched (thunder.rs doesn't exist yet)
- No CLI changes (`--policy thunder` still rejected at clap parse)
- No anthropic router changes (3rd-party path out of scope)
- No PD changes
- No gRPC changes (gRPC validation in P7)

### Footgun (production note)

**Production note**: any sidecar fronting an internal backend (litellm-proxy, custom proxies, etc.) must also expose `/v1/models` and `/version`, otherwise SMG worker registration fails with 404 model_not_found. litellm-proxy already does this; custom sidecars need to be checked.

### Revisit conditions

1. If P3 reveals that `extract_text_for_routing` for CreateMessageRequest needs to include ToolResultBlock content (e.g. for cache-aware routing of tool-heavy programs), expand the impl — this is non-breaking.
2. If litellm-proxy is later observed to pass through `metadata.program_id` (current spec §10.5 footgun says it strips), revisit whether the gateway should forward `program_id` as well so backends can use it for KV-cache stickiness hints.

### Approved by

(Pending P0 implementation commit + user review.)

---

## D-17: P1 implementation completed — LoadBalancingPolicy trait extension landed

**Date**: 2026-05-01
**Spec ref**: `docs/thunder/10-phases.md` P1 row, `docs/thunder/04-smg-integration.md` §5.5b/§5.7

### What landed

- `UsageEvent` struct in `model_gateway/src/policies/mod.rs`
- `SelectWorkerInfo.program_id: Option<&'a str>` field in same file
- `#[async_trait]` on `LoadBalancingPolicy` trait + `async fn select_worker_async` default impl + `fn usage_sender` default-None
- New `model_gateway/src/routers/common/program_id.rs` helper module
- Async migration: `routers/http/router.rs::select_worker_for_model` + `routers/grpc/common/stages/worker_selection.rs::select_single_worker`
- 8 per-policy parity tests asserting `select_worker == select_worker_async` for bucket, cache_aware, consistent_hashing, manual, power_of_two, prefix_hash, random, round_robin
- `MinimumTokensPolicy` guard confirming it remains a `DPRankLoadPolicy`-only policy, not a `LoadBalancingPolicy`
- Phase 0 e2e regression: 3/3 still pass

### What did NOT change

- Zero individual policy implementation files modified (`bucket.rs` ... `round_robin.rs` untouched)
- PD path (`routers/grpc/common/stages/worker_selection.rs::select_pd_pair`) deliberately not migrated — PD scope is deferred beyond P1
- `routers/anthropic/`, `routers/openai/`, `routers/gemini/` — 3rd-party path, out of scope
- PD routers were not behaviorally changed; `routers/http/pd_router.rs` only received the required `program_id: None` field default after `SelectWorkerInfo` grew a public field
- `policies/thunder.rs` — does not exist; arrives in P3
- CLI / config / observability / worker / e2e — out of scope

### Footguns surfaced

1. Adding a public field to `SelectWorkerInfo` required updating all struct literals, including a compile-only `program_id: None` default in `routers/http/pd_router.rs`. This did not change PD behavior, but it did touch a file listed as out of scope for behavioral work.
2. The plan listed `dp_min_token` in the parity sweep, but `MinimumTokensPolicy` implements `DPRankLoadPolicy`, not `LoadBalancingPolicy`. P1 kept that production boundary intact and added a dp-rank-only guard instead of a misleading fallback parity test.
3. Clippy flagged fully-qualified `tokio::sync::mpsc::UnboundedSender` usage in the trait default; importing `UnboundedSender` directly keeps the trait extension warning-free under `-D warnings`.

### Revisit conditions

1. If P3 adds a policy that needs async work in selection AND that policy is in the PD path, the deferral above must be reconsidered — the PD `select_pd_pair` will need its own async migration.
2. If `usage_sender` design proves insufficient (e.g., backpressure issues from unbounded channel under high load), revisit channel type — possibly switch to bounded with `try_send` + drop-on-full semantics.
3. If `program_id_hint` becomes performance-critical (millions of QPS), benchmark the `as_deref()` chain in `Metadata` lookup; today it's negligible.

### Approved by

(Pending P1 implementation commit + Claude review + user sign-off.)
