# 异步任务执行与可靠投递链路（FastAPI + SQLite 事务发件箱）

在不依赖任何外部中间件（无 Redis / MQ / 真实凭据）的前提下，提供一套
**本地可验证**的异步任务执行与可靠投递链路，保证任务在并发执行、
进程重启、连续失败三类场景下都有**确定且可解释**的结果。

- 任务提交 / 状态查询 / 取消 / 死信人工重放（HTTP API）
- 多执行者并发，单任务任意时刻只被一个执行者实际执行（租约 + 行级互斥）
- 有上限的指数退避重试；执行超时、连续失败、业务明确失败分别落入可查询死信
- 事务发件箱（transactional outbox）：任务终态与待投递消息同事务落库
- append-only 状态流水：每次状态跃迁、每次失败原因均可审计，失败绝不静默
- 崩溃恢复：执行/投递中途进程被 `kill -9`，重启后靠过期租约回收，不丢任务
- 全部能力由 pytest 覆盖，退避采用确定性策略（无随机抖动），测试可重复

## 目录结构

```
app/
  models.py     领域模型：状态机、退避策略、错误码、异常
  store.py      SQLite 持久化：tasks / task_events / task_outbox（WAL + BEGIN IMMEDIATE）
  handlers.py   任务处理器与投递出口 sink 的注册表
  worker.py     任务执行者：抢占租约、限时执行、协作取消、重试/死信
  deliverer.py  发件箱投递者：同构的抢占/退避/死信模型
  service.py    门面：装配 store、注册表、后台 worker/deliverer
  api.py        HTTP 路由与错误码映射
  schemas.py    请求/响应模型
main.py         进程入口（FastAPI lifespan 启停后台执行者）
tests/          并发抢占 / 重启恢复 / 幂等取消 / 死信重放 / HTTP API 测试
scripts/        真实多进程 kill -9 重启验证脚本
```

## 一、任务状态机

```
                 领取(claim)                成功 + 同事务写发件箱
 PENDING ───────────────────────► RUNNING ───────────────────────► SUCCEEDED
    ▲                              │  │                              │
    │ 退避重试(RETRY_BACKOFF)       │  │ 业务明确失败/超时用尽/重试用尽 │ 投递重试用尽
    └──────────────────────────────┘  ▼                              ▼
       （backoff_until 到期可再领） DEAD_LETTER ◄────────────────────┘
    ▲                                 │
    │ 人工重放 REPLAYED（保留历史，     │ 协作取消（cancel_requested）
    │ replay_seq+1，attempt 归零）     ▼
    └────────────────────────────  CANCELLED
 PENDING 也可被立即取消（直接到 CANCELLED）
```

- `SUCCEEDED`、`CANCELLED` 为不可重放终态；
- `DEAD_LETTER` 为**可重放**终态，`dead_letter_reason` 区分进入原因；
- 任务成功后若投递阶段最终失败，任务会从 SUCCEEDED 被反压标记为
  `DEAD_LETTER(reason=DELIVERY_RETRIES_EXHAUSTED)`，历史中的 SUCCEEDED
  事件保留，重放时**只重新投递、不再执行处理器**。

### 死信原因（可查询、可区分）

| reason | 触发条件 |
| --- | --- |
| `RETRIES_EXHAUSTED` | 处理器连续瞬时失败，达到 `max_attempts` |
| `BUSINESS_REJECTED` | 处理器抛 `PermanentFailure`，**不重试立即死信** |
| `TIMEOUT` | 执行超过 `task_timeout`，按瞬时失败重试，最后一次仍超时则以此原因死信 |
| `DELIVERY_RETRIES_EXHAUSTED` | 发件箱投递 sink 持续失败到上限（或 sink 抛 `PermanentDeliveryError`） |

## 二、幂等与取消规则

### 提交幂等

- 每个任务必须携带 `idempotency_key`；
- 相同 key 再次提交：**不产生新执行记录**，返回原任务且 `deduplicated=true`；
- 相同 key 但 `task_type` 或 `payload` 不同：返回 `409 IDEMPOTENCY_CONFLICT`。

数据库中 `tasks.idempotency_key` 为唯一约束，并发重复提交由数据库裁决。

### 取消规则（非法跃迁一律拒绝，原因可区分）

| 当前状态 | 取消行为 |
| --- | --- |
| `PENDING` | 立即收敛为 `CANCELLED` |
| `RUNNING` | 置 `cancel_requested=true`（协作式取消），执行者在执行前后检查并收敛为 `CANCELLED`，返回 200 |
| `CANCELLED` | 拒绝：`409 ALREADY_CANCELLED`（重复取消） |
| `SUCCEEDED` / `DEAD_LETTER` | 拒绝：`409 TASK_TERMINAL`（已结束） |
| 任务不存在 | `404 TASK_NOT_FOUND` |

## 三、重试与可靠投递规则

- **确定性指数退避**：第 n 次失败后等待
  `min(cap, base * factor^(n-1))`；默认 `base=0.05s, factor=2, cap=1s`，
  `max_attempts=5`。不引入随机抖动，保证测试可重复。
- **执行互斥**：执行者通过单条 `BEGIN IMMEDIATE` 事务的
  `UPDATE ... SELECT` 抢占任务，写租约 `lease_owner + lease_expires_at`。
  多执行者 / 多进程打开同一个 DB 文件时，由 SQLite 写锁保证同时只有一个成功。
- **崩溃恢复**：执行者运行期间按 `lease/3` 间隔心跳续租。进程死亡后心跳
  停止，租约到期后下一个执行者把该任务作为 `LEASE_RECLAIMED` 重新领取
  （`attempt+1`）。因此任务执行语义是 **at-least-once**——处理器必须自行
  保证业务副作用幂等；发件箱投递把 `idempotency_key` 传给 sink，由 sink
  实现去重以达到外部副作用的 effectively-once。
- **事务发件箱**：任务标记 SUCCEEDED 与插入 `task_outbox` 在**同一事务**，
  不会出现“成功了但没有投递消息”。投递者以同样的租约/退避模型把消息
  送往 sink；投递成功才置 DELIVERED。

## 四、死信重放方式

```bash
# 任务处于 DEAD_LETTER 时（可先 GET 查 dead_letter_reason 与事件流水）
curl -X POST http://127.0.0.1:8000/tasks/<task_id>/replay
```

- 重放仅对 `DEAD_LETTER` 合法，其它状态返回 `409 NOT_REPLAYABLE`；
- 原有事件流水**原样保留**，只追加 `REPLAYED` 事件（含 `replay_seq`、
  操作者 actor、是否仅重投递）；
- 执行类死信：`attempt` 归零、重新进入 PENDING 走完整执行+投递；
- 投递类死信：任务不再执行，发件箱消息回到 PENDING 重新投递。

## 五、HTTP API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /tasks` | 提交任务（body: `task_type`, `payload`, `idempotency_key`, `max_attempts?`） |
| `GET /tasks/{id}` | 查询任务状态（含 attempt、死信原因、结果、replay_seq） |
| `GET /tasks/{id}/events` | append-only 状态流水 |
| `POST /tasks/{id}/cancel` | 取消 / 协作取消 |
| `POST /tasks/{id}/replay` | 死信人工重放 |

错误响应统一为 `{"detail": "...", "code": "<ERROR_CODE>"}`。

## 六、本地验证方法

需要 Python 3.14（或按 `requires-python` 调整）与 [uv](https://docs.astral.sh/uv/)。

### 1. 运行单元 / 集成测试

```bash
uv sync --dev
uv run pytest                      # 17 个测试，约 3 秒，可重复
uv run pytest -s --log-cli-level=INFO   # 打印每个任务的状态流水与判定依据
```

测试场景对应需求：

| 测试文件 | 覆盖场景 |
| --- | --- |
| `tests/test_concurrency.py` | 4 个执行者并发处理 20 个任务：每任务恰好执行一次、投递一次 |
| `tests/test_recovery.py` | 执行中途退出后重启的租约回收；投递中途退出后重投 + sink 幂等去重 |
| `tests/test_idempotency_cancel.py` | 幂等去重、载荷冲突、重复取消、终态取消的错误码区分 |
| `tests/test_dead_letter_replay.py` | 三类执行死信 + 投递死信、超时重试、重放保留历史且可追溯 |
| `tests/test_http_api.py` | HTTP 错误码、RUNNING 中协作取消端到端 |

### 2. 真实多进程 kill -9 重启演示

```bash
./scripts/run_restart_demo.sh
```

脚本依次启动 4 个**相互独立的进程**：提交即退出 → 执行中 `os._exit(1)`
强杀 → 新进程打开同一 DB 回收过期租约并完成执行与投递 → 打印全部状态流水。

### 3. 启动 HTTP 服务

```bash
TASK_DB_PATH=tasks.db uv run python main.py
# 自定义处理器/出口在代码中通过 service.register_handler / register_sink 注册；
# 未注册 sink 的任务类型按“本地空出口”确认投递，仅用于无外部系统的本地验证。
```

## 设计取舍说明

- 选择 SQLite（WAL + `BEGIN IMMEDIATE` + busy_timeout）作为唯一基础设施，
  换取零依赖的本地可验证性；其写互斥语义同样适用于多进程执行者。
  生产环境若替换为 Postgres，只需改写 `TaskStore`，状态机与 worker/deliverer
  的协作协议（租约、退避、发件箱、事件流水）保持不变。
- “任何失败都不得被静默吞掉”：执行失败、退避、死信、投递失败/重试/死信
  全部写入 `task_events`，并在 worker/deliverer 日志中输出。
