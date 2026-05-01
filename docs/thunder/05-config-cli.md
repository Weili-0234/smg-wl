> Part of the [Thunder Policy spec](00-INDEX.md). Companion: [worklog](worklog.md) — design decisions with revisit conditions.

# Thunder — Configuration & CLI

## 6. Configuration / CLI surface

Deployer-facing flags (added to `main.rs` under new `Help heading = "Thunder Policy"`):

| Flag | Default | Range | Notes |
|---|---|---|---|
| `--policy thunder` | (none — uses cache_aware unless flipped) | discrete | Adds "thunder" to value_parser whitelist at `main.rs:152` ONLY (not `:217` `:222` — see §6.1 below) |
| `--thunder-sub-mode {default,tr}` | `default` | discrete | TR enables scheduler + admission |
| `--thunder-scheduler-interval-secs` | `5` | u64 ≥1 | Ignored when sub_mode=default |
| `--thunder-resume-timeout-secs` | `1800` | u64 ≥10 | Force-resume after this; Q5.1 |
| `--thunder-tool-coefficient` | `0.5` | f64 in [0,1] | ACTING tokens weight |
| `--thunder-use-acting-token-decay` | `false` | bool | Phase P9 toggle |

PolicyConfig::Thunder serde shape:

```yaml
policy:
  type: thunder
  sub_mode: tr
  scheduler_interval_secs: 5
  resume_timeout_secs: 1800
  tool_coefficient: 0.5
  use_acting_token_decay: false
```

### 6.1 CLI flag interaction matrix (D-14)

SMG validates flag combinations at startup via `config/validation.rs::validate_compatibility` (line 772). Thunder adds one new check there. Full matrix:

| Combination | Behavior | Implementation |
|---|---|---|
| `--policy thunder` (alone) | ✅ Single-router mode (default `enable_igw=false`); single global `ThunderPolicy` instance, single `RouterState`, single scheduler task | Default; nothing special |
| `--policy thunder` + `--enable-igw` | ✅ Per-model dispatch via `RouterManager`; **each model gets its own `ThunderPolicy` instance** (PolicyRegistry is per-model `Arc<dyn LoadBalancingPolicy>`); independent `RouterState` and scheduler task per model | Allowed; documented in §10 footgun (per-model independent capacity pools, no cross-model BFD) |
| `--policy thunder` + `--service-discovery` | ✅ Auto-enables `enable_igw` (`main.rs:1421-1424`); workers churn via K8s pod label discovery; thunder's `WorkerRegistry::subscribe_events` handler reconciles (§3.4) | Allowed; same as `enable_igw` semantics |
| `--policy thunder` + `--pd-disaggregation` | ❌ **Rejected at startup** | `validate_compatibility` adds: `if PolicyConfig::Thunder + RoutingMode::PrefillDecode → ConfigError::IncompatibleConfig{ reason: "Thunder policy does not support PD-disaggregation in this release. Use --policy cache_aware, or specify --prefill-policy and --decode-policy explicitly." }` |
| `--prefill-policy thunder` or `--decode-policy thunder` | ❌ **Rejected at CLI parse** | `main.rs:217` and `:222` `value_parser` arrays do NOT include `"thunder"` — clap rejects with "invalid value" before SMG starts |
| `--policy thunder` + cache_aware-specific tuning flags (`--cache-threshold`, `--balance-abs-threshold`, etc.) | ✅ Allowed; thunder ignores them | At startup, after parsing, log `tracing::info!("flag --{} is specific to {} policy, ignored under thunder", flag_name, policy_name)` for each cache_aware/manual/prefix_hash flag passed |

**Why reject PD instead of silent-degrade**: PD path goes through `pd_router.rs:861` which calls sync `policy.select_worker(...)`. Thunder's algorithm logic is in the async variant (`select_worker_async`); the sync default-impl falls back to a degenerate selection (e.g., first available worker) without program tracking, BFD, or pause/resume. Silent degrade would let user think thunder is working when capacity-aware behavior is actually disabled. Hard fail at startup is correct.

---
