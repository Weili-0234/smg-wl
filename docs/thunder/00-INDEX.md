# Thunder Policy — Documentation Index

> **Status**: Draft v2 (post-pivot, post pre-flight verification, ready for P0 implementation).
> **Source code**: `model_gateway/src/policies/thunder.rs` (P3+ — does not yet exist).
> **Tests**: `e2e_test/thunder/` (P2+ — does not yet exist).
> **Companion repo state**: `worktree=/home/hkang/wl/smg-wl/`, branch `thunder-policy` tracking `lightseekorg-upstream/main` at `04f9b2d6`.

This index is the **single source of truth for navigation** within the Thunder spec. The full spec was originally a 1372-LOC `THUNDER_POLICY_DESIGN.md` single file; it was split into 11 topic files (per worklog D-15) for editorial independence and PR diff hygiene. **No content lives only in this INDEX.md** — every claim has a topic file. INDEX is just a router.

---

## File map

| File | Purpose | LOC | Original spec section |
|---|---|---|---|
| [`01-overview.md`](01-overview.md) | TL;DR + Mission + Architecture (deployment shape, request flow, out-of-scope, background subscriptions) | 134 | §0, §1, §3 |
| [`02-decisions.md`](02-decisions.md) | Sign-off table linking each algorithmic deviation to a task ID and FAITHFUL/FORK tag | 30 | §2 |
| [`03-algorithm.md`](03-algorithm.md) | Algorithm core (Program lifecycle, BackendState, scheduler tick, BFD, pause_until_safe, momentum, sub-modes) + Glossary | 231 | §4, §13 |
| [`04-smg-integration.md`](04-smg-integration.md) | The biggest file. Trait extension diff, SelectWorkerInfo extension, ThunderPolicy struct, PolicyConfig + Factory wiring, per-router program_id extraction (with §5.5b GenerationRequest extension and §5.5c-e Phase-0 unblocker diffs), HTTP path streaming usage tail extractor, hook mechanism, scheduler task lifecycle, Notify integration, KvEventMonitor + WorkerRegistry events, concurrency model + perf footgun | 621 | §5 |
| [`05-config-cli.md`](05-config-cli.md) | CLI flags + interaction matrix (thunder × enable_igw / service_discovery / pd_disaggregation / cache_aware-specific flags) | 41 | §6, §6.1 |
| [`06-cross-protocol.md`](06-cross-protocol.md) | Sidecar deployment shape + why cross-protocol counting "just works" under internal-only scope | 25 | §7 |
| [`07-observability.md`](07-observability.md) | Existing-channel metric emission, new `smg_thunder_*` series, tracing fields, admin endpoints (`/thunder/programs`, `/thunder/profiles`) | 54 | §8 |
| [`08-testing.md`](08-testing.md) | Pytest fixtures (sketch), per-phase test plan, what's NOT covered in e2e | 26 | §9 |
| [`09-footguns.md`](09-footguns.md) | All 12 known limitations with trigger / observable / inspection / mitigation | 99 | §10 |
| [`10-phases.md`](10-phases.md) | Phase plan (P0..P9) + per-PR sign-off rules + file-level change inventory | 79 | §11, §12, §15 |

Companion docs in this directory (NOT split-from-spec; independently authored):

- [`worklog.md`](worklog.md) — Non-trivial design decisions with `Context / Options considered / Chosen / Revisit conditions` per entry. **Append-only, never delete.** Currently D-9 through D-14 (D-1..D-8 were inline in the original spec's decision log §2 = `02-decisions.md`).
- [`slurm-cluster.md`](slurm-cluster.md) — Active SLURM allocation info (jobid, node, srun access pattern). Updated each session as allocations change.
- [`legacy/requirements-brainstorm.md`](legacy/requirements-brainstorm.md) — The original 570-LOC requirements dump from the prior brainstorm session (pre-policy-pivot). Kept for archaeological purposes; the live design lives in the topic files above. Older terminology may not match current spec.

---

## Decision → file map

When a worklog `D-N` entry is referenced, find its impact in the topic files via this map:

| Worklog | Topic | Lives in |
|---|---|---|
| (D-1..D-8 — legacy from initial brainstorm) | Various | inlined in `02-decisions.md` decision-log table |
| `D-9` retry × pause idempotency (Option C+C1) | Footgun §10.9 + Phase P6 plan | `09-footguns.md`, `10-phases.md` |
| `D-10` testing topology α | Phase P2 setup | `10-phases.md`, `slurm-cluster.md` |
| `D-11` testing infrastructure shape (launcher / fixture / restart) | Phase P2 details | `10-phases.md`, `08-testing.md` |
| `D-12` 4× sglang only + flush_cache + mock tier | Phase P2/P5/P6 | `10-phases.md`, `08-testing.md` |
| `D-13` route_typed_request protocol-agnostic verification | §5.5b GenerationRequest impl + §5.5e route_to_endpoint + §10.5/.10/.11/.12 | `04-smg-integration.md`, `09-footguns.md` |
| `D-14` CLI flag interaction matrix | §6.1 | `05-config-cli.md` |

---

## Quick start (post-compact / new-developer onboarding)

1. **Want to know the high-level picture?** Read `01-overview.md`.
2. **Want to know "why did we choose X?"** Read `worklog.md` (latest entries first); cross-ref to topic file for "what we chose to do".
3. **Implementing P0?** Start at `04-smg-integration.md` §5.5b/c/d/e + `10-phases.md` P0 row.
4. **Reviewing a PR?** Check the per-PR sign-off rules in `10-phases.md` §12 + the relevant topic file.
5. **Hit an unexpected behavior in production?** Check `09-footguns.md` for known footguns indexed by trigger.

---

## Spec version history

- **v1** (THUNDER_POLICY_DESIGN.md, 1372 LOC, single file): drafted 2026-04-30 morning during post-compact rebuild after the policy-pivot brainstorm.
- **v2** (this directory, 11 topic files + companions): split 2026-04-30 evening per worklog D-15. No semantic changes vs v1 — only structural reorganization. v1 has been deleted from the worktree (replaced by the root `THUNDER.md` pointer).

---

## References

- **Python source (read-only)**: `/home/hkang/wl/smg_thunder/ThunderAgent/ThunderAgent/`
- **Original (superseded) RoutingMode-based spec**: `/home/hkang/wl/smg_thunder/THUNDER_PHASE5PLUS_DESIGN.md` — kept for content reuse (algorithm semantics, BFD pseudocode, footguns)
- **SMG architecture overview**: `docs/concepts/architecture/overview.md`
- **LoadBalancingPolicy trait**: `model_gateway/src/policies/mod.rs:44`
- **cache_aware mirror pattern**: `model_gateway/src/policies/cache_aware.rs:71-595`
- **PolicyFactory dispatch**: `model_gateway/src/policies/factory.rs:17`
- **WorkerSelector (3rd-party path, NOT used by thunder)**: `model_gateway/src/routers/common/worker_selection.rs:73`
- **WorkerRegistry events**: `model_gateway/src/worker/registry.rs:143-151`
- **KvEventMonitor**: `model_gateway/src/worker/kv_event_monitor.rs`
- **Metrics descriptors**: `model_gateway/src/observability/metrics.rs:151-336`
- **Build app (axum routes + middleware stack)**: `model_gateway/src/server.rs:901-1153`
- **CLI args struct**: `model_gateway/src/main.rs:135-1461`
- **Validation**: `model_gateway/src/config/validation.rs:772 validate_compatibility`
