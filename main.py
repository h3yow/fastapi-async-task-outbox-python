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
    """演示长任务：分 10 步上报进度，每步检查取消请求。"""
    steps = int(payload.get("steps", 10))
    for i in range(1, steps + 1):
        ctx.throw_if_cancelled()
        time.sleep(float(payload.get("step_seconds", 1.0)))
        ctx.report_progress(percent=i / steps * 100, step=f"step-{i}/{steps}")
        print(f"[long] {ctx.task_id} progress {i}/{steps}")


def create_app(db_path: str = ":memory:", start_worker: bool = True) -> FastAPI:
    db = Database(db_path)
    repo = TaskRepository(db)
    executor = Executor(repo, {"echo": echo_handler, "long": long_handler},
                        worker_id="worker-1", execution_timeout=3600)

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
