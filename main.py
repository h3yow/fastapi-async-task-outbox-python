"""应用装配：内存库 + 后台执行器 + HTTP API。本地直接运行即可验证。"""
import contextlib
import time

import uvicorn
from fastapi import FastAPI

from app.api import create_router
from app.db import Database
from app.executor import Executor, TaskContext
from app.repository import TaskRepository


def echo_handler(payload: dict) -> None:
    print(f"[echo] {payload}")


def long_handler(payload: dict, ctx: TaskContext) -> None:
    """演示长任务：分步执行、持续上报进度、响应取消。

    payload 可传 {"steps": 10, "step_seconds": 1.0}。
    """
    steps = int(payload.get("steps", 10))
    step_seconds = float(payload.get("step_seconds", 1.0))
    for i in range(steps):
        ctx.throw_if_cancelled()  # 协作式取消点
        time.sleep(step_seconds)
        ctx.report_progress((i + 1) * 100.0 / steps, f"step {i + 1}/{steps}")


def create_app(db_path: str = ":memory:", start_worker: bool = True) -> FastAPI:
    db = Database(db_path)
    repo = TaskRepository(db)
    executor = Executor(repo, {"echo": echo_handler, "long": long_handler},
                        worker_id="worker-1")

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_worker:
            executor.start()
        yield
        executor.stop()
        db.close()

    app = FastAPI(title="async-task-outbox", lifespan=lifespan)
    app.state.repo = repo
    app.state.executor = executor
    app.include_router(create_router(repo))
    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
