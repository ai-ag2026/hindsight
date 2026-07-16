"""local(tars) 2026-07-16: recall access tracking (Vollaudit G-2).

``access_count`` existed since the initial schema but had no writer anywhere
(upstream's ``access_count_update`` task type is comment/test-only). These
tests cover the new fire-and-forget bump on the recall path.
"""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from hindsight_api.engine.memory_engine import MemoryEngine


def _bare_engine() -> MemoryEngine:
    eng = MemoryEngine.__new__(MemoryEngine)
    eng._access_bump_tasks = set()
    return eng


@pytest.mark.asyncio
async def test_bump_executes_single_update_with_ids(monkeypatch):
    eng = _bare_engine()
    conn = AsyncMock()
    fake_pool = object()

    async def fake_get_pool():
        return fake_pool

    @asynccontextmanager
    async def fake_acquire(pool):
        assert pool is fake_pool
        yield conn

    eng._get_pool = fake_get_pool
    monkeypatch.setattr(
        "hindsight_api.engine.memory_engine.acquire_with_retry", fake_acquire
    )
    ids = ["11111111-1111-1111-1111-111111111111",
           "22222222-2222-2222-2222-222222222222"]
    await eng._bump_access_counts(ids)
    assert conn.execute.await_count == 1
    sql, passed_ids = conn.execute.await_args.args
    assert "access_count = access_count + 1" in sql
    assert passed_ids == ids


@pytest.mark.asyncio
async def test_bump_failure_never_raises(monkeypatch):
    eng = _bare_engine()

    async def broken_pool():
        raise RuntimeError("pool down")

    eng._get_pool = broken_pool
    await eng._bump_access_counts(["11111111-1111-1111-1111-111111111111"])


@pytest.mark.asyncio
async def test_schedule_holds_reference_and_cleans_up():
    eng = _bare_engine()
    done = asyncio.Event()

    async def fake_bump(ids):
        done.set()

    eng._bump_access_counts = fake_bump
    eng._schedule_access_bump(["11111111-1111-1111-1111-111111111111"])
    assert len(eng._access_bump_tasks) == 1
    await asyncio.wait_for(done.wait(), timeout=2)
    await asyncio.sleep(0)  # let done_callback run
    assert len(eng._access_bump_tasks) == 0


@pytest.mark.asyncio
async def test_schedule_noop_on_empty_ids():
    eng = _bare_engine()
    eng._schedule_access_bump([])
    assert eng._access_bump_tasks == set()
