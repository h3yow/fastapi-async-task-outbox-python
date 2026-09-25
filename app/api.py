"""FastAPI 路由：提交 / 查询 / 取消 / 重放 / 事件流水。"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from .models import ErrorCode, Task, TaskError, TaskEvent
from .schemas import EventResponse, SubmitRequest, SubmitResponse, TaskResponse
from .service import TaskService

_HTTP_STATUS = {
    ErrorCode.TASK_NOT_FOUND: 404,
    ErrorCode.IDEMPOTENCY_CONFLICT: 409,
    ErrorCode.ALREADY_CANCELLED: 409,
    ErrorCode.TASK_TERMINAL: 409,
    ErrorCode.TASK_RUNNING: 409,
    ErrorCode.NOT_REPLAYABLE: 409,
}


def _task_to_resp(task: Task) -> TaskResponse:
    return TaskResponse(
        id=task.id,
        idempotency_key=task.idempotency_key,
        task_type=task.task_type,
        payload=task.payload,
        status=task.status.value,
        attempt=task.attempt,
        max_attempts=task.max_attempts,
        cancel_requested=task.cancel_requested,
        replay_of=task.replay_of,
        replay_seq=task.replay_seq,
        dead_letter_reason=task.dead_letter_reason,
        dead_letter_detail=task.dead_letter_detail,
        result=task.result,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )


def _event_to_resp(event: TaskEvent) -> EventResponse:
    return EventResponse(
        seq=event.seq,
        at=event.at,
        actor=event.actor,
        event_type=event.event_type,
        from_status=event.from_status.value if event.from_status else None,
        to_status=event.to_status.value if event.to_status else None,
        attempt=event.attempt,
        detail=event.detail,
    )


def _error(exc: TaskError) -> JSONResponse:
    return JSONResponse(
        status_code=_HTTP_STATUS[exc.code],
        content={"detail": exc.message, "code": exc.code.value},
    )


def build_router(service: TaskService) -> APIRouter:
    router = APIRouter(prefix="/tasks", tags=["tasks"])

    @router.post("", response_model=SubmitResponse, status_code=201)
    async def submit(body: SubmitRequest) -> JSONResponse | SubmitResponse:
        try:
            task, created = await service.submit(
                body.task_type,
                body.payload,
                body.idempotency_key,
                max_attempts=body.max_attempts,
            )
        except TaskError as exc:
            return _error(exc)
        return SubmitResponse(
            task_id=task.id,
            status=task.status.value,
            idempotency_key=task.idempotency_key,
            deduplicated=not created,
        )

    @router.get("/{task_id}", response_model=TaskResponse)
    async def get_task(task_id: str):
        try:
            task = await service.get(task_id)
        except TaskError as exc:
            return _error(exc)
        return _task_to_resp(task)

    @router.get("/{task_id}/events", response_model=list[EventResponse])
    async def get_events(task_id: str):
        try:
            events = await service.events(task_id)
        except TaskError as exc:
            return _error(exc)
        return [_event_to_resp(e) for e in events]

    @router.post("/{task_id}/cancel", response_model=TaskResponse)
    async def cancel_task(task_id: str):
        try:
            task = await service.cancel(task_id)
        except TaskError as exc:
            return _error(exc)
        return _task_to_resp(task)

    @router.post("/{task_id}/replay", response_model=TaskResponse)
    async def replay_task(task_id: str):
        try:
            task = await service.replay(task_id)
        except TaskError as exc:
            return _error(exc)
        return _task_to_resp(task)

    return router
