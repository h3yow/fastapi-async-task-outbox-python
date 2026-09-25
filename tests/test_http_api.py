"""HTTP API 层：错误码可区分性 + 运行中协作取消。"""
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from tests.conftest import fast_settings


@pytest.fixture
def client(tmp_path):
    settings = fast_settings(str(tmp_path / "api.db"))
    return create_app(settings=settings)


async def test_api_error_codes_are_distinguishable(client):
    async with client.router.lifespan_context(client):
        transport = ASGITransport(app=client)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            # 404
            r = await c.get("/tasks/nope")
            assert r.status_code == 404
            assert r.json()["code"] == "TASK_NOT_FOUND"

            # 幂等冲突
            await c.post("/tasks", json={
                "task_type": "echo", "payload": {"a": 1},
                "idempotency_key": "api-1"})
            r = await c.post("/tasks", json={
                "task_type": "echo", "payload": {"a": 2},
                "idempotency_key": "api-1"})
            assert r.status_code == 409
            assert r.json()["code"] == "IDEMPOTENCY_CONFLICT"

            # 取消不存在
            r = await c.post("/tasks/nope/cancel")
            assert r.status_code == 404

            # 重放非死信
            r0 = await c.post("/tasks", json={
                "task_type": "echo", "payload": {}, "idempotency_key": "api-2"})
            tid = r0.json()["task_id"]
            r = await c.post(f"/tasks/{tid}/cancel")
            assert r.status_code == 200 and r.json()["status"] == "CANCELLED"
            r = await c.post(f"/tasks/{tid}/cancel")
            assert r.status_code == 409
            assert r.json()["code"] == "ALREADY_CANCELLED"
            r = await c.post(f"/tasks/{tid}/replay")
            assert r.status_code == 409
            assert r.json()["code"] == "NOT_REPLAYABLE"


async def test_cooperative_cancel_during_running(client):
    entered = asyncio.Event()

    async def slow_job(payload):
        entered.set()
        for _ in range(100):
            await asyncio.sleep(0.01)
        return "should-not-return"

    client.state.service.register_handler("slow", slow_job)
    async with client.router.lifespan_context(client):
        transport = ASGITransport(app=client)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post("/tasks", json={
                "task_type": "slow", "payload": {}, "idempotency_key": "api-3"})
            tid = r.json()["task_id"]
            await asyncio.wait_for(entered.wait(), timeout=2)
            # RUNNING 时取消：先返回 RUNNING+cancel_requested，执行者随后收敛
            r = await c.post(f"/tasks/{tid}/cancel")
            assert r.status_code == 200
            assert r.json()["cancel_requested"] is True

            for _ in range(200):
                await asyncio.sleep(0.02)
                g = await c.get(f"/tasks/{tid}")
                if g.json()["status"] == "CANCELLED":
                    break
            assert g.json()["status"] == "CANCELLED"

            ev = await c.get(f"/tasks/{tid}/events")
            kinds = [e["event_type"] for e in ev.json()]
            assert "CANCEL_REQUESTED" in kinds and "CANCELLED" in kinds
