"""进程入口：组装服务、挂载路由、随生命周期启停后台执行者。"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.api import build_router
from app.config import Settings, default_settings
from app.service import TaskService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def create_app(
    db_path: str | None = None, settings: Settings | None = None
) -> FastAPI:
    settings = settings or default_settings(
        db_path or os.environ.get("TASK_DB_PATH", "tasks.db")
    )
    service = TaskService(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await service.start_background()
        try:
            yield
        finally:
            await service.stop_background()

    app = FastAPI(title="async-task-outbox", lifespan=lifespan)
    app.state.service = service
    app.include_router(build_router(service))
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
