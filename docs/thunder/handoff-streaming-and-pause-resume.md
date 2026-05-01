# Thunder Handoff — 流式支持 + 简化 pause/resume 详细记录

> **日期**: 2026-05-01
> **作者**: Claude (autonomous MVP push 之后)
> **触发原因**: 用户明确反馈："我的很多 use case 要接 `/v1/responses` 和 `/v1/messages` 格式的请求，which needs to be streaming requests" — 而当前 MVP 的 streaming 路径**不更新 ThunderPolicy 状态**，意味着流式请求享受不到 capacity-aware backpressure。

---

## 一、当前 streaming 状况：到底缺什么？

### 1.1 现状（HEAD `9d69cc5c`）

**流式请求是能跑通的**（pass-through），但是从 ThunderPolicy 角度看流式请求是**透明的**——它不知道这些请求消耗了多少 token，也不知道何时完成：

| 请求类型 | 路由 | program_id 提取 | 容量门 (TR mode) | 选 backend | 响应 forward | **token 计入 ThunderPolicy** | **请求完成时通知 ThunderPolicy** |
|---|---|---|---|---|---|---|---|
| 非流式 `/v1/messages` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 从响应 body 中 parse `usage` 字段 | ✅ usage_consumer 减 in_flight |
| 非流式 `/v1/chat/completions` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同上 | ✅ 同上 |
| 非流式 `/v1/responses` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ 同上 | ✅ 同上 |
| **流式** `/v1/messages` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ **不计入** | ❌ **不通知** |
| **流式** `/v1/chat/completions` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ **不计入** | ❌ **不通知** |
| **流式** `/v1/responses` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ **不计入** | ❌ **不通知** |

### 1.2 后果（如果不修，对你的 use case 有什么影响？）

由于你的大部分 use case 是流式的：

1. **TR mode 的 capacity gate 在流式下"漏算"**
   - `BackendState.active_program_tokens` 只包括非流式请求的 tokens
   - 一个 backend 可能实际上已经被流式请求占满 KV cache，但 ThunderPolicy 看着是空的，继续放新请求进来
   - 后端真正过载时返回 5xx → 客户端拿到错误，而不是被 thunder 优雅 pause

2. **`Program.in_flight` 在流式下不递减**
   - 流式请求的 `ProgramRequestGuard` 在请求完成时是 `Drop` 触发减 1（因为 `complete()` 没被调）
   - 但因为流式 path 根本没创建 guard（只有非流式 router path 创建），所以连这个兜底都没有
   - 结果：流式 program 的 in_flight 永远 +1 但不减，长期跑会让所有流式 program 看起来"全在 in-flight"，影响 sticky routing 的统计

3. **`Program.total_tokens` 在流式下永远是 0**
   - 没有 UsageEvent 发出
   - 任何依赖 `total_tokens` 的功能（未来的 BFD victim selection、`smg_thunder_*` metrics 暴露）都会缺数据

4. **force-resume timeout 不会因流式完成而提前唤醒**
   - 假设 5 个流式请求把 backend 占满，第 6 个请求 pause 等容量
   - 5 个流式请求陆续完成，但**没有 UsageEvent 触发 broadcast**
   - 第 6 个请求一直等到 30 分钟 force-resume timeout 才进
   - 实际上后端早就空了

### 1.3 三个协议的流式响应格式差异（这是为什么我把它推迟）

这是流式 usage tail extractor 复杂的根本原因：

| 协议 | SSE 事件格式 | usage 在哪里 | 终止 marker |
|---|---|---|---|
| **OpenAI Chat** (`/v1/chat/completions`) | `data: {"id":"chatcmpl-...","choices":[{"delta":...}]}\n\n` | 最后一个 chunk 之前的 `usage` 字段（**仅当 `stream_options.include_usage=true` 时**） | `data: [DONE]\n\n` |
| **Anthropic Messages** (`/v1/messages`) | `event: message_delta\ndata: {"usage":{...}}\n\n` 后跟 `event: message_stop` | 单独的 `message_delta` 事件携带 `usage` | `event: message_stop` 然后流关 |
| **OpenAI Responses** (`/v1/responses`) | `event: response.output_text.delta` 等多种事件类型 | `event: response.completed` 中的 `response.usage` | `event: response.completed` 然后流关 |

每个协议都需要**独立的 SSE parser**。litellm-proxy sidecar 可能会做归一化（把 Anthropic/Responses 翻成 OpenAI 格式），但 thunder 在 sidecar **之前**看到原始字节流，不能假设 sidecar 转过。

**而且**，对于 OpenAI Chat 的 usage tail，必须在请求里**注入** `stream_options.include_usage = true`，否则 vLLM/sglang 默认不在最后一个 chunk 里发 usage。这意味着 thunder 还要**改写流式请求的 body**，引入额外的失败模式（如果用户已经设了 `stream_options.include_usage=false` 显式拒绝，thunder 应该尊重还是 override？）。

---

## 二、需要后续讨论实现的功能（按优先级）

### Tier 1 — 必须做（streaming use case 阻塞）

#### **F1: HTTP 流式 usage tail extractor + `stream_options.include_usage` 注入**

**Scope**:
- 在 `routers/http/router.rs::route_typed_request` 的流式分支（`bytes_stream` 调用处，line 712 / 923）包裹原始 `Body::from_stream` 用一个 wrapper stream，边 forward 给客户端边 sniff usage
- 三种协议各写一个 SSE parser，识别该协议的 usage chunk + 终止 marker
- 在请求体上注入 `stream_options.include_usage = true`（仅 OpenAI Chat 需要，Anthropic/Responses 默认就发 usage）
- 解析出 usage 后通过 `policy.usage_sender()` 发出 UsageEvent

**预估**: ~150 LOC parser + ~30 LOC injection + ~50 LOC wrapper stream + ~40 LOC tests

**Risks / 需要和你讨论的设计点**:
1. **`stream_options.include_usage` 注入策略** — 用户显式设了 `false` 时，thunder 是 override 还是尊重并放弃 usage tracking？我倾向**尊重用户**，但注入一个 warn log 提示 "thunder 无法跟踪此请求 token 用量"。
2. **如果某个 chunk 解析失败应该怎样** — 应该（a）仍然 forward 给客户端但 silently drop UsageEvent，还是（b）记 metric `smg_thunder_streaming_parse_failure_total{protocol}`？我倾向 (b)。
3. **流式过程中客户端 disconnect 怎么办** — 当前 ProgramRequestGuard 的 Drop 会清理 in_flight，但在流式下 guard 是否被建立？这块需要先把 guard wire 到流式 path。
4. **Thinking blocks / tool_use blocks 的 token 是否也应该计入** — 三种协议对此的处理不一致，建议简化：只看最终的 `usage.{prompt,completion,total}_tokens`，不细分。
5. **Anthropic streaming 的 `cache_read_input_tokens`** — Anthropic 单独 report 缓存命中的 prompt token 数。是否应该影响 ThunderPolicy 的 capacity 统计？我倾向**不计入** active_program_tokens（因为缓存命中不实际占 KV slot），但需要你确认。

**讨论问题**: 你的 use case 中有没有用 Anthropic prompt caching？如果有，且需要 thunder 正确账记缓存命中的 token，需要多一个 field 在 UsageEvent 中。

---

#### **F2: 流式 path 创建 ProgramRequestGuard**

**Scope**:
- 在 `routers/http/router.rs::route_typed_request` 流式分支（与 F1 同位置）也调用 `ThunderPolicy::create_guard(program_id)` 并把 guard 跟 stream 的 lifetime 绑定
- 当 stream 被 drop（客户端 disconnect 或正常完成且没经过 `complete()` 路径），Drop 自动减 in_flight + broadcast Notify
- F1 的 SSE parser 解析到终止 marker 时调用 `guard.complete()`

**预估**: ~30 LOC

**Risks**:
- guard 的 lifetime 绑定到 axum response stream 的 future — 需要把 guard `move` 到 wrapper stream 的状态里。`ProgramRequestGuard` 当前是 `Send`，应该 OK，但需要验证。

---

### Tier 2 — 强烈建议做（之后会暴露问题）

#### **F3: 流式 retry × in_flight idempotency**

当前 D-9 (`Option C+C1`) 的 retry idempotency **只针对非流式**。如果流式请求的连接在客户端建立后、但首字节之前断开导致 SMG 触发 retry：

- 当前会 admit 一个新的 program request（in_flight += 1），但旧的 stream wrapper 也还在（D-9 没考虑 stream 的 in-flight 状态）
- 结果：in_flight 计数错乱

**Scope**:
- `route_typed_request` 流式分支用同一个 ProgramRequestGuard 跨 retry（per D-9 Option C），重 entry 时 thunder 检测 `program.in_flight > 0` 跳过准入逻辑
- 单测覆盖 retry 场景

**预估**: ~50 LOC + 1 e2e test

---

### Tier 3 — 可推迟（性能优化）

#### **F4: BFD greedy_resume 完整 port**

**Scope**: 把 Python `router.py:719-844` 的 BFD greedy_resume 算法忠实 port 到 Rust，替换 `pick_tr` 中当前的 `select_least_active`。

**为什么推迟**: 简化版（broadcast wake + least-active）**正确**，只是不**最优**。要真正发挥 BFD 价值需要做 load test 看到 capacity 浪费才值得做。

**预估**: ~150 LOC

---

#### **F5: `mark_for_pause` for in-flight ACTING programs**

**Scope**: 当 BFD 决定要 pause 一个 ACTING（流式中）的 program 时，先标记 `marked_for_pause=true`，等流结束再真正 pause（避免中断流式响应）。

**为什么推迟**: 没 BFD 选 victim 这个就用不上。BFD port 完后再做。

**预估**: ~50 LOC

---

#### **F6: `char_to_token_ratio` 校准**

**Scope**: usage_consumer 收到 UsageEvent 后用 `actual_tokens / request_text_chars` 更新 program 的 char→token 比例，下次 admit 用 program-specific 比例估算（替代当前固定 4 chars/token）。

**为什么推迟**: 当前固定 4 chars/token 估算偏粗，但只影响 TR mode 的 admission 决策。如果发现 mis-admit 再做。

**预估**: ~40 LOC

---

#### **F7: `shared_tokens` calc**

**Scope**: BFD 需要算 backend 间共享 token（同一个 program 跨 backend 时如何记账）。Q5.3 spec 已 sign-off 但没实现。

**为什么推迟**: 没 BFD 不需要。

**预估**: ~30 LOC

---

### Tier 4 — 后续 phase 工作

| 工作 | Phase | 何时做 |
|---|---|---|
| gRPC 路径验证 | P7 | 部署需要 gRPC 后端时 |
| Profiling 端点 (`/thunder/programs`, `/thunder/profiles`) | P8 | 第一次生产部署时 |
| `--thunder-use-acting-token-decay` | P9 | 看到 token 高估时 |
| Per-backend RwLock sharding | P9 | benchmark 显示 contention 时 |
| 部署 runbook | P9 | 第一次生产部署时 |

---

## 三、"简化 pause/resume" — 详细解释

> 你问"你说的简化 pause/resume 是什么意思也要记录在上面" — 这部分是 D-22 的展开，澄清**简化版**和**spec 描述的完整版**之间的精确差距。

### 3.1 完整版（spec 描述的、Python ThunderAgent 实现的）

Python `router.py:685-844` 的完整 pause/resume 包含 4 个组件：

#### (1) `pause_until_safe(scheduler_tick_state)` — 选 victim 的逻辑
- **每个 scheduler tick**（默认 100ms）调用一次
- 遍历所有 backend，看哪些 backend 已经超过 `(1 - reserved_fraction) * capacity_tokens`
- 对每个超容 backend，从其上面运行的 program 里**选一个 victim** pause 掉
- victim 选择规则：取最近 admit 的、step_count 最小的（最"年轻"的 program）
- 如果 victim 在 ACTING 状态（流式中），**不立刻 pause**，而是设 `marked_for_pause=true`，等 stream 结束再 pause（避免中断流）
- 如果 victim 在 REASONING 或非 acting 状态，立刻 pause

#### (2) `greedy_resume(scheduler_tick_state)` — BFD bin-packing 算法
- 取所有 PAUSED programs，按 token 数 **DESC** 排序（大的优先）
- 取所有 backends，按剩余容量 **DESC** 排序
- BFD 主循环：依次选大的 program → 找第一个能装下它的 backend（best fit）→ resume 到那个 backend
- 装不下的 program 继续 PAUSED，下一 tick 重试

#### (3) Per-program `Notify` + 状态机
- 每个 program 有一个 `waiting_event: Arc<Notify>`
- pause 时把 program 入队 `waiting_queue`
- resume 时找到该 program 的 Notify 调 `notify_one()`（精准唤醒，**不是广播**）

#### (4) ProgramRequestGuard 完整 RAII
- guard 持有 `in_flight` 标志位
- `complete()` 标记正常完成
- `Drop` 没调 complete → 触发 `force_terminate_program(pid)`：
  - 从 `programs` 中移除条目
  - 从 backend 的 `active_programs` 中移除
  - 减 `active_program_tokens` 当前 program 占用部分
  - broadcast 唤醒所有 waiters
  - **幂等**：可以多次调而无副作用（D-9 Option C+C1 retry 场景）

### 3.2 简化版（我的 D-22 实现的）

| 组件 | 完整版 | 简化版 | 取舍 |
|---|---|---|---|
| **何时检查容量** | scheduler tick 每 100ms 跑 `pause_until_safe`，主动找 victim pause | **只在 admit 时**检查；已 admit 的请求不会被中途 pause | 不会主动腾出 capacity 给等待中的 program；要等已 admit 的请求自然完成 |
| **选 victim** | 按 step_count 找最年轻的 program | **不选 victim**（已 admit 的不动） | 简化大幅，但意味着永远是"先到先得" — 高优先级 program 排在长 prompt 之后会等 |
| **BFD bin-packing** | DESC 排序 program × backend，best-fit | **least-active backend**（默认模式的逻辑） | 可能在 backend 间分配不均；多个 paused program 同时唤醒后可能挤一个 backend 然后再次溢出 |
| **唤醒** | 精准 `notify_one()` 选定的 program | **广播 `notify_waiters()`** 唤醒所有等待者 | 惊群问题；但 waiter 数量有限（≤ 几十），重新检查容量门只 ~10us |
| **ACTING program pause 中断流** | 检测后设 `marked_for_pause` 等流结束 | **流式不参与 pause/resume**（流式 path 不创容量门，见 F1/F2 缺口） | 这是当前最大坑——你的 use case 流式占大头 |
| **ProgramRequestGuard Drop** | 调 `force_terminate_program` 完整清理 + 幂等 | **简化**：只减 `in_flight`，broadcast Notify，不清 `programs` 也不清 `active_programs` 集合 | 客户端 disconnect 不会立刻释放 backend 的容量给其他 program；只有等 usage_consumer 收 UsageEvent 才释放（但 disconnect 没 usage 事件） — **泄漏风险** |
| **Force-resume timeout** | 等 30min（`resume_timeout_secs`），到点跳过容量门 force-admit | **完全相同** | 一致 |

### 3.3 简化版导致的具体行为差异

#### 场景 A：5 个流式请求填满 backend，第 6 个请求到达
- **完整版**: 第 6 个 pause；scheduler tick 看到 5 个已运行的，选最年轻的 victim pause 掉（标记 marked_for_pause），让第 6 个 resume；5 中年轻那个流结束后真正 pause
- **简化版**: 第 6 个 pause；5 个继续跑（流式不会被中途 pause）；第 6 个等 5 个里**任何一个**自然结束（或等 30 分钟 timeout 强制 admit）

#### 场景 B：4 个 program 在 PAUSED queue，2 个 backend 各空出 100k
- **完整版**: BFD 按 program token 数 DESC 排序，把最大的 program 放到剩余最大的 backend，然后第二大的放第二大的 backend；剩 2 个 program 继续 PAUSED
- **简化版**: 广播唤醒所有 4 个；它们竞争同一个 backend（least-active 选同一个）；前 2 个 admit，后 2 个再次 PAUSED；下一轮再竞争

#### 场景 C：客户端在请求 admit 后立刻 ctrl-C
- **完整版**: ProgramRequestGuard Drop → `force_terminate_program` → 立刻释放 backend 的容量 → broadcast 唤醒等待者
- **简化版**: ProgramRequestGuard Drop → 只减 `in_flight` + broadcast → backend 的 `active_program_tokens` **没释放**（等 usage_consumer 来减，但永远等不到）→ **capacity 泄漏**

### 3.4 简化版的"反悔成本"（重要，决定优先做哪个）

| 改动 | LOC | 反悔成本 | 影响 |
|---|---|---|---|
| 加 BFD bin-packing | ~150 | **0**（替换 `pick_tr` 中的 `select_least_active`，wire 不动） | 公平性 + 容量利用率 |
| 加 scheduler tick `pause_until_safe` | ~100 | **小**（新建后台 task；guard 集成不动） | 解决场景 A |
| 修 ProgramRequestGuard 漏释放容量 | ~30 | **0**（在现有 Drop 里多 release `estimated_reserved_tokens`） | 解决场景 C 容量泄漏 |
| 流式 path 加容量门 + UsageEvent (F1+F2) | ~250 | **小** | 解决场景 A 在 streaming 下不工作 |

---

## 四、推荐你 wakeup 后的优先顺序（修订版，反映 streaming use case）

1. **看本文档 § 1 + § 3** 理解当前流式的 gap 和"简化"具体是什么
2. **决策 § 2 中各 F-N 的 5 个讨论问题**（特别是 F1 的 `stream_options.include_usage` override 策略 + Anthropic prompt caching 行为）
3. **F1 + F2 + F3 是 streaming-MVP**（~280 LOC + e2e）— 我建议下个 session 优先做这三个，而不是去做 BFD（因为没 streaming 支持，再优秀的 BFD 也跑不起来）
4. **跑一次 SLURM 真后端 streaming 测试** 确认 mock 行为和真后端行为对得上
5. **F-修复 ProgramRequestGuard 容量泄漏**（30 LOC）— 小但关键
6. **再看 BFD/scheduler tick**（F4+F5）— 看 load test 决定

---

## 五、当前 commit 状态参考

- 分支: `thunder-policy`
- HEAD: `9d69cc5c`（post-MVP-followups doc 提交后）
- 47 commits ahead of `04f9b2d6` upstream
- 触发 streaming-MVP 的下一个分支推荐: `thunder-policy-streaming` (新 phase 标 P5.5 或 P4.5 都行)

要继续做的话，下个 session 起点就是这份文档 + `docs/thunder/post-mvp-followups.md`。
