"""Pure-unit coverage of scheduler RPC timeout provenance and cancellation."""

import asyncio
from unittest.mock import MagicMock

import pytest

from first_gateway.controllers.worker import Heartbeat
from first_gateway.controllers.workers import pilot_job_observer
from first_gateway.controllers.workers.pilot_job_observer import PilotJobObserver


def _observer() -> tuple[PilotJobObserver, MagicMock]:
    observer = PilotJobObserver.__new__(PilotJobObserver)
    heartbeat = MagicMock(spec=Heartbeat)
    observer.hb = heartbeat
    return observer, heartbeat


async def test_rpc_preserves_inner_timeout_identity_and_details() -> None:
    observer, heartbeat = _observer()
    inner = TimeoutError("Timeout expired while waiting for Compute task task-123")

    async def adapter_timeout() -> None:
        raise inner

    with pytest.raises(TimeoutError) as caught:
        await observer._rpc(adapter_timeout())

    assert caught.value is inner
    assert "task-123" in str(caught.value)
    assert "60s" not in str(caught.value)
    assert heartbeat.beat.call_count == 2


async def test_rpc_names_its_own_expired_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer, heartbeat = _observer()
    monkeypatch.setattr(pilot_job_observer, "_RPC_TIMEOUT", 0.01)
    cancelled = asyncio.Event()

    async def stalled_adapter() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    with pytest.raises(
        TimeoutError, match=r"stalled_adapter RPC timed out after 0.01s"
    ) as caught:
        await observer._rpc(stalled_adapter())

    assert isinstance(caught.value.__cause__, TimeoutError)
    assert cancelled.is_set()
    assert heartbeat.beat.call_count == 2


async def test_rpc_preserves_caller_cancellation() -> None:
    observer, heartbeat = _observer()
    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def stalled_adapter() -> None:
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    task = asyncio.create_task(observer._rpc(stalled_adapter()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert cancelled.is_set()
    assert heartbeat.beat.call_count == 2


async def test_rpc_preserves_success() -> None:
    observer, heartbeat = _observer()

    async def result() -> str:
        return "scheduler-result"

    assert await observer._rpc(result()) == "scheduler-result"
    assert heartbeat.beat.call_count == 2


async def test_rpc_preserves_other_error_identity() -> None:
    observer, heartbeat = _observer()
    inner = RuntimeError("qstat failed")

    async def failed_adapter() -> None:
        raise inner

    with pytest.raises(RuntimeError) as caught:
        await observer._rpc(failed_adapter())

    assert caught.value is inner
    assert heartbeat.beat.call_count == 2
