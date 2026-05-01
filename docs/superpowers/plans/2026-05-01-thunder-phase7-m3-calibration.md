# Phase 7 M3 — Full token calibration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans.

**Goal:** Replace `chars / 4 + 256` hardcoded estimate with three-tier calibrated lookup: per-program → global → 4.0 default for chars/token ratio; per-program → global → 0.5 default for completion-fraction. EMA updates on each UsageEvent with wall-time half-life decay (3600s) toward neutral values.

**Spec reference:** `docs/superpowers/specs/2026-05-01-thunder-phase7-production-design.md` §3.3

---

### Task 1: Extend Program + RouterState + UsageEvent for calibration storage

- [ ] Add `local_char_to_token_ratio: Option<f64>`, `local_completion_fraction: Option<f64>`, `last_calibration_at: Option<Instant>` to Program
- [ ] Add `global_char_to_token_ratio: Option<f64>`, `global_completion_fraction: Option<f64>`, `last_global_calibration_at: Option<Instant>` to RouterState
- [ ] Add `declared_max_tokens: Option<u32>` to UsageEvent
- [ ] Update Program/UsageEvent test sites to include defaults

### Task 2: Calibration helper

- [ ] Add `update_calibration_with_decay` helper in `policies/thunder.rs` that handles decay-then-EMA
- [ ] Add NEUTRAL_RATIO=4.0, NEUTRAL_FRACTION=0.5, EMA_ALPHA=0.2, HALF_LIFE=3600s constants

### Task 3: Wire calibration update into usage_consumer_task

- [ ] After existing program update logic, call helper twice (per-program + global) for chars/token ratio
- [ ] Then twice for completion fraction (gated on declared_max_tokens being Some)

### Task 4: Three-tier lookup in estimate_request_tokens

- [ ] Add `state: &RouterState` and `declared_max_tokens: Option<u64>` parameters
- [ ] Replace hardcoded with three-tier lookup
- [ ] Update callers (pick_default_inner, pick_tr) to pass them

### Task 5: Wire declared_max_tokens through SelectWorkerInfo

- [ ] Add `declared_max_tokens: Option<u32>` to SelectWorkerInfo
- [ ] Routers extract from typed_req body and populate

### Task 6: Tests + worklog + commit

- [ ] 9 unit tests per spec §3.3.6
- [ ] cargo clippy clean
- [ ] worklog D-29 ~ D-31 entries
- [ ] commit `feat(policies): full token calibration with time-decay (Phase 7 M3 Gap7)`
- [ ] ff-merge to thunder-policy
