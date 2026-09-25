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
   ┌────┼───────┼────────┐
   │ok  │retryable fail  │timeout / business fail / 重试耗尽
   ▼    ▼                ▼
SUCCEEDED PENDING(退避)  DEAD ──replay(人工)──▶ PENDING
 (终态)   (attempt+1)
```

- 终态：`SUCCEEDED` / `CANCELLED`，任何进一步跃迁一律拒绝。
- `RUNNING` 任务携带租约（`lease_owner` + `lease_expires_at`）；进程崩溃后租约过期，任务被其他执行者回收（`lease expired, reclaimed`），不丢不重。
- 所有跃迁写入 `task_events` 审计表（追加写、永不修改），通过 `GET /tasks/{id}/events` 查询完整状态流水。

## 幂等与重试规则

- **幂等提交**：`idempotency_key` 唯一约束。重复提交返回原任务（`created=false`），不产生新的执行记录。
- **并发抢占**：`claim` 使用条件 UPDATE，同一任务任一时刻只有一个执行者持有租约。
- **退避重试**：指数退避 `base * 2^(attempt-1)`，封顶 `max_seconds`；`attempt >= max_attempts` 后进入 `DEAD`。
- **失败分类**：
  - 普通异常 → 可重试，回 `PENDING`（带退避）；
  - `BusinessFailure` → 业务明确失败，不重试，直接 `DEAD`；
  - 执行超时（`execution_timeout`）→ 直接 `DEAD`；
  - 任何失败都写入事件流水与 `last_error`，不被静默吞掉。
- **取消**：仅 `PENDING` 可取消。终态重复取消 → `409 task_already_terminal`；`RUNNING` 取消 → `409 invalid_transition`，原因可区分。

## 死信与人工重放

- `GET /tasks?status=DEAD` 查询死信。
- `POST /tasks/{id}/replay`：`DEAD → PENDING`，`attempt` 归零、清空租约与错误；历史事件完整保留并追加 `manual replay` 记录，操作者（actor）可追溯。
- 非 `DEAD` 任务重放 → `409 invalid_transition`。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/tasks` | 提交任务（幂等键去重） |
| GET | `/tasks/{id}` | 查询任务状态 |
| GET | `/tasks?status=DEAD` | 按状态过滤 |
| GET | `/tasks/{id}/events` | 状态流水（判定依据） |
| POST | `/tasks/{id}/cancel` | 取消（非法跃迁返回 409 + code） |
| POST | `/tasks/{id}/replay` | 死信人工重放 |

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest tests/ -q -s   # 运行测试，-s 可看到任务状态流水与判定依据
uv run python main.py        # 启动服务（127.0.0.1:8000，内置 echo 任务类型）
```

测试覆盖：幂等提交、5 执行者并发抢占 30 任务恰好执行一次、执行中途崩溃后的租约回收（重启恢复）、重试耗尽/业务失败/超时三种死信路径、死信重放后历史可追溯、取消与非法跃迁的可区分拒绝。
