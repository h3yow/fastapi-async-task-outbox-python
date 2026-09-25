"""应用装配：内存库 + 后台执行器 + HTTP API。本地直接运行即可验证。"""
import contextlib

import uvicorn
from fastapi import FastAPI

from app.api import create_router
from app.db import Database
from app.executor import Executor
from app.repository import TaskRepository


def echo_handler(payload: dict) -> None:
    print(f"[echo] {payload}")


def create_app(db_path: str = ":memory:", start_worker: bool = True) -> FastAPI:
    db = Database(db_path)
    repo = TaskRepository(db)
    executor = Executor(repo, {"echo": echo_handler}, worker_id="worker-1")

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
