# async-task-outbox

本地可验证的异步任务执行与可靠投递链路：FastAPI + SQLite（WAL），无外部中间件、无真实凭据。

## 任务状态机

```
                 submit
                   │
                   ▼
        ┌────── PENDING ──────────────┐
        │claim  │                     │ cancel
        ▼       │                     ▼
     RUNNING    │                CANCELLED (终态)
   ┌────┼───────┼────────┐            ▲
   │ok  │retryable fail  │timeout /   │ cancel 请求被任务感知
   ▼    ▼                ▼ business fail / 重试耗尽
SUCCEEDED PENDING(退避)  DEAD ──replay(人工)──▶ PENDING
 (终态)   (attempt+1)
```

- 终态：`SUCCEEDED` / `CANCELLED`，任何进一步跃迁一律拒绝。
- `RUNNING` 任务携带租约（`lease_owner` + `lease_expires_at`）；进程崩溃后租约过期，任务被其他执行者回收（`lease expired, reclaimed`），不丢不重。
- 所有跃迁写入 `task_events` 审计表（追加写、永不修改），通过 `GET /tasks/{id}/events` 查询完整状态流水。

## 长任务支持

### 进度上报与查询

- 两参签名的 handler `handler(payload, ctx)` 会获得 `TaskContext`：
  - `ctx.report_progress(percent, step)`：上报完成百分比（自动截断到 0–100）与当前步骤描述，覆盖式更新，只保留最新快照；
  - 旧的一参 handler `handler(payload)` 完全兼容，无需修改。
- 查询：`GET /tasks/{id}/progress`。
  - 从未上报过 → `{"reported": false, "percent": null, ...}`（未知），**不报错**；
  - 已上报 → `{"reported": true, "percent": 40.0, "step": "...", "updated_at": "..."}`。
- 进度随“一次运行”生命周期：每次被抢占开始新一轮执行时清零；人工重放（replay）同样清零，**上一轮的进度不会带到下一轮**。
- 仅当前租约持有者可上报进度；租约被回收后旧执行者的上报会被拒绝（`invalid_transition`），防止写脏数据。

### 执行期间的独占规则

- 执行器在 handler 运行期间按 `lease_seconds / 3` 周期自动**心跳续租**：只要任务还在正常执行，租约就不会过期，**不会因为耗时超过默认租约而被回收或交给第二个执行者**。
- 续租是条件更新（`status=RUNNING AND lease_owner=自己`）；若续租失败（例如进程被外部干预导致租约丢失），执行结果会被丢弃而不是覆盖既成事实，保证恰好执行一次。
- 该行为已被并发测试验证：0.9s 任务 / 0.2s 租约 / 3 个执行者竞争，任务恰好执行一次，事件流水中无 `reclaimed`。

### 运行中取消的语义与边界

取消是两阶段过程，全程可追溯：

1. `POST /tasks/{id}/cancel`：
   - `PENDING` → 直接落 `CANCELLED`（终态）；
   - `RUNNING` → 置 `cancel_requested` 标记，状态仍为 `RUNNING`，事件流水追加 `cancel requested, waiting for task to stop`；
2. 执行中的任务通过 `ctx.throw_if_cancelled()`（协作式取消点，抛 `TaskCancelled`）或 `ctx.is_cancel_requested()` 尽快感知；执行器心跳线程按租约周期轮询取消标记并通知 handler。
3. 任务停下后由持有者收敛到 `CANCELLED`，事件流水追加 `cancel requested acknowledged, task stopped`。handler 感知取消后即使正常返回（未抛异常），同样收敛到 `CANCELLED`。

边界与可区分的拒绝（均为 `409` + 不同 `code`）：

| 场景 | code | 说明 |
|---|---|---|
| 重复取消（RUNNING 且已置标记） | `cancel_already_requested` | 取消请求已在途中 |
| 终态（SUCCEEDED/CANCELLED）再取消 | `task_already_terminal` | 已结束，不可再取消 |
| DEAD 取消 | `invalid_transition` | 死信只能重放，不能取消 |

取消请求与进度上报并发发生时，所有状态修改在同一锁内完成，任务状态不会被带乱（有并发测试覆盖）。

### 对现有短任务的兼容范围

- 一参 handler、幂等提交、退避重试、死信与重放、执行超时、崩溃恢复行为完全不变；
- 短任务不调用 `ctx` 即不受任何影响；`PENDING` 直接取消的语义不变；
- 唯一的行为变化：`RUNNING` 状态取消由此前的 `invalid_transition` 拒绝，变为“置取消标记、两阶段收敛”。

## 幂等与重试规则

- **幂等提交**：`idempotency_key` 唯一约束。重复提交返回原任务（`created=false`），不产生新的执行记录。
- **并发抢占**：`claim` 使用条件 UPDATE，同一任务任一时刻只有一个执行者持有租约。
- **退避重试**：指数退避 `base * 2^(attempt-1)`，封顶 `max_seconds`；`attempt >= max_attempts` 后进入 `DEAD`。
- **失败分类**：
  - 普通异常 → 可重试，回 `PENDING`（带退避）；
  - `BusinessFailure` → 业务明确失败，不重试，直接 `DEAD`；
  - 执行超时（`execution_timeout`）→ 直接 `DEAD`；
  - 任何失败都写入事件流水与 `last_error`，不被静默吞掉。
- **取消**：`PENDING` 直接取消；`RUNNING` 走两阶段取消（见“长任务支持”）。终态重复取消 → `409 task_already_terminal`；重复的运行中取消 → `409 cancel_already_requested`；死信取消 → `409 invalid_transition`，原因均可区分。

## 死信与人工重放

- `GET /tasks?status=DEAD` 查询死信。
- `POST /tasks/{id}/replay`：`DEAD → PENDING`，`attempt` 归零、清空租约与错误；历史事件完整保留并追加 `manual replay` 记录，操作者（actor）可追溯。
- 非 `DEAD` 任务重放 → `409 invalid_transition`。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 提交任务（幂等键去重） |
| GET | `/tasks/{id}` | 查询任务状态（含 `cancel_requested`） |
| GET | `/tasks?status=DEAD` | 按状态过滤 |
| GET | `/tasks/{id}/events` | 状态流水（判定依据） |
| GET | `/tasks/{id}/progress` | 进度查询（未上报返回 `reported=false`） |
| POST | `/tasks/{id}/cancel` | 请求取消（非法情形返回 409 + 可区分 code） |
| POST | `/tasks/{id}/replay` | 死信人工重放（进度与取消标记一并清零） |

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -q -s   # 运行测试，-s 可看到任务状态流水与判定依据
uv run python main.py        # 启动服务（127.0.0.1:8000，内置 echo / long 两种任务类型）
```

手工验证长任务（另开一个终端）：

```bash
# 1) 提交一个约 10s 的长任务
curl -s -X POST 127.0.0.1:8000/tasks -H 'Content-Type: application/json' \
  -d '{"task_type":"long","payload":{"steps":10,"step_seconds":1.0},"idempotency_key":"demo-1"}'
# 2) 执行中随时查进度（默认租约 5s，任务耗时远超租约也不会被回收）
curl -s 127.0.0.1:8000/tasks/<id>/progress
# 3) 请求取消，观察两阶段收敛
curl -s -X POST 127.0.0.1:8000/tasks/<id>/cancel     # 返回 RUNNING + cancel_requested=true
curl -s 127.0.0.1:8000/tasks/<id>                    # 稍后变为 CANCELLED
curl -s 127.0.0.1:8000/tasks/<id>/events             # 完整取消过程可追溯
# 4) 重复取消 -> 409 task_already_terminal
```

测试覆盖：幂等提交、5 执行者并发抢占 30 任务恰好执行一次、执行中途崩溃后的租约回收（重启恢复）、重试耗尽/业务失败/超时三种死信路径、死信重放后历史可追溯、取消与非法跃迁的可区分拒绝；长任务新增覆盖：进度上报/查询/未知处理/重放清零、耗时远超租约时 3 执行者竞争下恰好执行一次且无回收、运行中取消被任务感知并收敛到 CANCELLED（含 handler 自抛 TaskCancelled 与提前正常返回两种路径）、重复取消/终态取消/死信取消三种可区分拒绝、取消与进度上报并发下状态不紊乱。
