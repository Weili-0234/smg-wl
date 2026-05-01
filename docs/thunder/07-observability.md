> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Observability

## 8. Observability

### 8.1 Existing-channel emission

Every `select_worker_async` call site is observed via SMG's existing per-router metrics (zero new dashboards needed):

```rust
// In routers (existing code, unchanged by thunder):
Metrics::record_router_request(router_type, model_id, streaming);
Metrics::record_router_duration(router_type, model_id, streaming, status, duration);
Metrics::record_router_error(router_type, model_id, streaming, error_kind);
```

`router_type` stays as the router (`openai_chat`, `anthropic_messages`, etc.) — thunder doesn't pretend to be a router. Existing dashboards correctly attribute traffic.

### 8.2 New thunder-specific Prometheus series

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `smg_thunder_programs_total` | gauge | `state`, `status` | Count of programs by lifecycle bucket |
| `smg_thunder_program_id_missing_total` | counter | — | Q5.2 fallback count |
| `smg_thunder_pause_total` | counter | `reason` | `capacity_overflow`, `kv_swing`, `decay_eviction` |
| `smg_thunder_resume_total` | counter | — | BFD greedy_resume placements |
| `smg_thunder_force_resume_total` | counter | — | Q5.1 timeout-driven |
| `smg_thunder_resume_wait_seconds` | histogram | — | Latency from pause → resume (or → timeout) |
| `smg_thunder_scheduler_tick_duration_seconds` | histogram | — | Tick critical-section duration (footgun §10) |
| `smg_thunder_backend_shared_tokens` | gauge | `backend_url` | Q5.3 tracker — see footgun §10 |
| `smg_thunder_backend_active_program_tokens` | gauge | `backend_url` | Used by BFD |
| `smg_thunder_char_to_token_ratio` | gauge | — | Momentum value |

All registered via `describe_*!` in `observability/metrics.rs:151+` (add a new "Layer 7: Thunder metrics" section).

### 8.3 Tracing fields

Every program lifecycle transition emits `tracing::info!` with structured fields: `program_id`, `from_state`, `to_state`, `backend_url`. OTel span stays open across pause/resume (`span.continue()`-style — verify OTel crate supports cross-await persistence).

### 8.4 Admin endpoints

Mounted in `build_app` only when active policy is thunder. Pattern:

```rust
// in server.rs:build_app
if let Some(thunder) = policy_registry.active_policy_as_thunder() {  // downcast helper
    admin_routes = admin_routes
        .route("/thunder/programs", get(thunder_programs_handler).with_state(thunder.clone()))
        .route("/thunder/profiles", get(thunder_profiles_handler).with_state(thunder.clone()));
}
```

Where `active_policy_as_thunder()` lives in `policies/registry.rs` and does the `as_any().downcast_ref::<ThunderPolicy>()` once at startup (cached `Arc<ThunderPolicy>` in registry).

Endpoints return JSON snapshots of `RouterState.programs` (read guard) and per-program profile timing (Phase P8).

---
