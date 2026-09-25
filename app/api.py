"""HTTP API：任务提交 / 查询 / 取消 / 死信重放 / 状态流水。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .models import (
    DomainError,
    InvalidTransitionError,
    Task,
    TaskAlreadyTerminalError,
    TaskEvent,
    TaskNotFoundError,
    TaskStatus,
)
from .repository import TaskRepository


class SubmitRequest(BaseModel):
    task_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str
    max_attempts: int = 3


class TaskView(BaseModel):
    id: str
    idempotency_key: str
    task_type: str
    status: str
    attempt: int
    max_attempts: int
    last_error: str | None

    @classmethod
    def of(cls, t: Task) -> "TaskView":
        return cls(id=t.id, idempotency_key=t.idempotency_key, task_type=t.task_type,
                   status=t.status.value, attempt=t.attempt,
                   max_attempts=t.max_attempts, last_error=t.last_error)


class EventView(BaseModel):
    from_status: str | None
    to_status: str
    reason: str
    actor: str
    created_at: str

    @classmethod
    def of(cls, e: TaskEvent) -> "EventView":
        return cls(from_status=e.from_status.value if e.from_status else None,
                   to_status=e.to_status.value, reason=e.reason, actor=e.actor,
                   created_at=e.created_at.isoformat())


def _to_http(exc: DomainError) -> HTTPException:
    if isinstance(exc, TaskNotFoundError):
        return HTTPException(404, detail={"code": exc.code, "message": str(exc)})
    if isinstance(exc, (TaskAlreadyTerminalError, InvalidTransitionError)):
        return HTTPException(409, detail={"code": exc.code, "message": str(exc)})
    return HTTPException(400, detail={"code": exc.code, "message": str(exc)})


def create_router(repo: TaskRepository) -> APIRouter:
    router = APIRouter()

    @router.post("/tasks", status_code=201)
    def submit(req: SubmitRequest):
        task, created = repo.submit(req.task_type, req.payload,
                                    req.idempotency_key, req.max_attempts)
        view = TaskView.of(task)
        return {"created": created, "task": view}

    @router.get("/tasks/{task_id}")
    def get(task_id: str):
        try:
            return TaskView.of(repo.get(task_id))
        except DomainError as exc:
            raise _to_http(exc) from exc

    @router.get("/tasks")
    def list_tasks(status: TaskStatus | None = None):
        return [TaskView.of(t) for t in repo.list_tasks(status)]

    @router.get("/tasks/{task_id}/events")
    def events(task_id: str):
        try:
            repo.get(task_id)
        except DomainError as exc:
            raise _to_http(exc) from exc
        return [EventView.of(e) for e in repo.list_events(task_id)]

    @router.post("/tasks/{task_id}/cancel")
    def cancel(task_id: str):
        try:
            return TaskView.of(repo.cancel(task_id))
        except DomainError as exc:
            raise _to_http(exc) from exc

    @router.post("/tasks/{task_id}/replay")
    def replay(task_id: str):
        try:
            return TaskView.of(repo.replay(task_id))
        except DomainError as exc:
            raise _to_http(exc) from exc

    return router
