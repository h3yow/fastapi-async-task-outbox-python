# async-task-outbox

本地可验证的异步任务执行与可靠投递链路：FastAPI + SQLite（WAL），无外部中间件、无真实凭据。支持短任务与长耗时任务（进度上报、执行期独占、运行中取消）。

## 任务状态机

```
                 submit
                   │
                   ▼
        ┌────── PENDING ──────────────┐
        │claim  │                     │ cancel
        ▼       │                     ▼
     RUNNING    │                CANCELLED (终态)
   ┌────┼───────┼────────┬───────────┘
   │ok  │retryable fail  │timeout / business fail / 重试耗尽
   ▼    ▼                ▼
SUCCEEDED PENDING(退避)  DEAD ──replay(人工)──▶ PENDING
 (终态)   (attempt+1)
```

- 终态：`SUCCEEDED` / `CANCELLED`，任何进一步跃迁一律拒绝。
- `RUNNING` 任务携带租约（`lease_owner` + `lease_expires_at`）；进程崩溃后租约过期，任务被其他执行者回收（`lease expired, reclaimed`），不丢不重。
- 运行中取消：`RUNNING → RUNNING`（置取消标记，事件流水记录）→ 执行者感知后 `RUNNING → CANCELLED`。
- 所有跃迁写入 `task_events` 审计表（追加写、永不修改），通过 `GET /tasks/{id}/events` 查询完整状态流水。

## 长任务支持

### 进度上报与查询

- handler 声明第二个参数即可获得 `TaskContext`（声明一个参数的短任务 handler 完全不用改）：

  ```python
  def long_handler(payload, ctx):
      for i in range(10):
          ctx.throw_if_cancelled()          # 感知取消请求
          ...                               # 干活
          ctx.report_progress(percent=(i+1)*10, step=f"step-{i+1}/10")
  ```

- `GET /tasks/{id}/progress` 查询当前执行纪元的最新进度：
  - 已上报：`{"reported": true, "percent": 40.0, "step": "step-4/10", ...}`；
  - 从未上报：`{"reported": false}`（**未知**，返回 200 不报错）。
- 每次抢占（claim）开启新的**执行纪元**（epoch）。人工重放（replay）会清掉旧进度，重新执行后进度从零开始，上一轮的进度不会被带过来。

### 执行期间的独占规则

- 执行器在 handler 运行期间按心跳间隔（默认租约时长的 1/3）自动续租：**只要任务还在正常执行，即使耗时远超默认租约时长，也不会被系统回收或交给第二个执行者**。
- 只有执行者真正崩溃（心跳停止）、租约过期后，任务才会被回收重跑——与短任务的崩溃恢复语义一致。
- 进度上报、租约续期、终态收敛都携带执行纪元校验：来自上一纪元的迟到写入一律被拒绝，旧执行者无法污染新一轮执行。

### 运行中取消的语义与边界

- `POST /tasks/{id}/cancel`：
  - `PENDING` → 立即取消，落 `CANCELLED`；
  - `RUNNING` → 置取消标记（返回 200 且 `cancel_requested=true`），执行者感知后收敛到 `CANCELLED`；重复请求幂等，只记录一条事件；
  - 终态（`SUCCEEDED`/`CANCELLED`）→ `409 task_already_terminal`；
  - `DEAD` → `409 invalid_transition`（死信只能重放，不能取消）。
- 取消是**协作式**的：handler 通过 `ctx.throw_if_cancelled()` / `ctx.is_cancel_requested()` 感知并停止；handler 不感知时，任务会在执行超时（`execution_timeout`）时被强制收敛——若已置取消标记则落 `CANCELLED`，否则进 `DEAD`。
- handler 退出时若取消标记已置位，取消请求优先于 handler 自身结局（成功/失败），统一收敛到 `CANCELLED`，保证取消与进度上报并发时状态不被带乱。
- 全程可追溯：`cancel requested by <actor>, waiting for worker ...` 与 `... acknowledged, worker stopped execution` 两条事件完整记录请求方与执行者的动作。

### 对现有短任务行为的兼容范围

- 单参数 handler 签名不变；幂等提交、并发抢占恰好一次、退避重试、业务失败/超时进死信、死信重放、`PENDING` 直接取消等语义全部不变。
- 唯一的行为变化：`POST /tasks/{id}/cancel` 对 `RUNNING` 任务从“拒绝（409）”变为“置取消标记（200）”；仓储层 `repo.cancel()` 保留旧的严格语义（仅 `PENDING` 可取消）。

## 幂等与重试规则

- **幂等提交**：`idempotency_key` 唯一约束。重复提交返回原任务（`created=false`），不产生新的执行记录。
- **并发抢占**：`claim` 使用条件 UPDATE，同一任务任一时刻只有一个执行者持有租约。
- **退避重试**：指数退避 `base * 2^(attempt-1)`，封顶 `max_seconds`；`attempt >= max_attempts` 后进入 `DEAD`。
- **失败分类**：
  - 普通异常 → 可重试，回 `PENDING`（带退避）；
  - `BusinessFailure` → 业务明确失败，不重试，直接 `DEAD`；
  - 执行超时（`execution_timeout`）→ 直接 `DEAD`（已置取消标记时收敛 `CANCELLED`）；
  - 任何失败都写入事件流水与 `last_error`，不被静默吞掉。
- **取消**：`PENDING` 直接取消；`RUNNING` 置取消标记待执行者收敛；终态重复取消 → `409 task_already_terminal`；`DEAD` 取消 → `409 invalid_transition`，原因可区分。

## 死信与人工重放

- `GET /tasks?status=DEAD` 查询死信。
- `POST /tasks/{id}/replay`：`DEAD → PENDING`，`attempt` 归零、清空租约/错误/取消标记/旧进度；历史事件完整保留并追加 `manual replay` 记录，操作者（actor）可追溯。
- 非 `DEAD` 任务重放 → `409 invalid_transition`。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 提交任务（幂等键去重） |
| GET | `/tasks/{id}` | 查询任务状态（含 `cancel_requested`） |
| GET | `/tasks?status=DEAD` | 按状态过滤 |
| GET | `/tasks/{id}/progress` | 查询当前执行纪元进度（未上报返回 `reported=false`） |
| GET | `/tasks/{id}/events` | 状态流水（判定依据） |
| POST | `/tasks/{id}/cancel` | 请求取消（非法状态返回 409 + 可区分 code） |
| POST | `/tasks/{id}/replay` | 死信人工重放 |

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -q -s   # 运行测试，-s 可看到任务状态流水与判定依据
uv run python main.py        # 启动服务（127.0.0.1:8000，内置 echo / long 任务类型）
```

手工验证长任务（另开一个终端）：

```bash
# 提交一个 10 步、每步 1 秒的长任务
curl -s -X POST 127.0.0.1:8000/tasks -H 'Content-Type: application/json' \
  -d '{"task_type":"long","payload":{"steps":10,"step_seconds":1},"idempotency_key":"demo-1"}'
# 执行中随时查进度（未上报时返回 reported=false）
curl -s 127.0.0.1:8000/tasks/<task_id>/progress
# 运行中请求取消，随后观察任务收敛到 CANCELLED
curl -s -X POST 127.0.0.1:8000/tasks/<task_id>/cancel
curl -s 127.0.0.1:8000/tasks/<task_id>/events
```

测试覆盖：幂等提交、5 执行者并发抢占 30 任务恰好执行一次、执行中途崩溃后的租约回收（重启恢复）、重试耗尽/业务失败/超时三种死信路径、死信重放后历史可追溯、取消与非法跃迁的可区分拒绝；长任务新增：进度上报与查询（未上报=未知、重放后清零）、运行 1.2s 超过 0.3s 租约仍恰好执行一次（3 执行者并发实测）、运行中取消被感知并收敛 `CANCELLED`、重复取消幂等、终态/死信取消的可区分拒绝、取消与高频进度上报并发下状态一致性。
