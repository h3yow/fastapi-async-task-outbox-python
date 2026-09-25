"""HTTP 请求/响应 schema。"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SubmitRequest(BaseModel):
    task_type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(min_length=1)
    max_attempts: int | None = Field(default=None, ge=1, le=100)


class SubmitResponse(BaseModel):
    task_id: str
    status: str
    idempotency_key: str
    deduplicated: bool


class TaskResponse(BaseModel):
    id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    status: str
    attempt: int
    max_attempts: int
    cancel_requested: bool
    replay_of: str | None
    replay_seq: int
    dead_letter_reason: str | None
    dead_letter_detail: str | None
    result: Any
    created_at: datetime
    updated_at: datetime


class EventResponse(BaseModel):
    seq: int
    at: datetime
    actor: str
    event_type: str
    from_status: str | None
    to_status: str | None
    attempt: int
    detail: dict[str, Any]


class Message(BaseModel):
    detail: str
    code: str
