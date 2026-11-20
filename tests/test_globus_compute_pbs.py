import asyncio
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from globus_compute_sdk.errors import TaskExecutionFailed, TaskPending

from first_common.schema.base_scheduler import JobSubmitPayload
from first_gateway.platforms.schedulers import globus_compute_pbs as gcpbs
from first_gateway.platforms.schedulers import scheduler_rpc
from first_gateway.platforms.schedulers.globus_compute_pbs import (
    FuncRegistry,
    GlobusComputePBSAdapter,
)

_FUNCS = FuncRegistry(
    qsub="f-qsub",
    qstat="f-qstat",
    qdel="f-qdel",
    list_files="f-ls",
    put_file="f-put",
    read_file="f-read",
)
_QSTAT_RESULT = {
    "1.pbs": {
        "job_state": "R",
        "Job_Name": "job-a",
        "ctime": "Mon Jun 22 18:24:00 2026",
        "Resource_List": {"walltime": "01:00:00"},
    }
}


class FakeComputeClient:
    """
    Minimal stand-in for globus_compute_sdk.Client. Each submitted task stays
    pending until a test resolves it with `complete()` or `fail()`.
    """

    def __init__(self) -> None:
        self.submitted: list[tuple[str, str, dict[str, Any]]] = []
        self.outcomes: dict[str, Any] = {}
        self.reason = "waiting-for-ep"
        # Mirrors the SDK's private cache: pending and successful tasks are
        # stored; failed ones raise before being stored.
        self._task_status_table: dict[str, dict[str, Any]] = {}

    def run(self, *, endpoint_id: str, function_id: str, **kwargs: Any) -> str:
        del endpoint_id
        task_id = f"task-{len(self.submitted)}"
        self.submitted.append((task_id, function_id, kwargs))
        return task_id

    def get_result(self, task_id: str) -> Any:
        if task_id not in self.outcomes:
            self._cache(task_id, {"pending": True})
            raise TaskPending(self.reason)  # type: ignore[no-untyped-call]
        outcome = self.outcomes[task_id]
        if isinstance(outcome, BaseException):
            raise outcome
        self._cache(task_id, {"pending": False, "result": outcome})
        return outcome

    def _cache(self, task_id: str, status: dict[str, Any]) -> None:
        table = getattr(self, "_task_status_table", None)
        if isinstance(table, dict):
            table[task_id] = status

    def complete(self, task_id: str, result: Any) -> None:
        self.outcomes[task_id] = result

    def fail(self, task_id: str, exc: BaseException) -> None:
        self.outcomes[task_id] = exc


@pytest.fixture(autouse=True)
def _clear_in_flight() -> Iterator[None]:
    gcpbs._in_flight.clear()
    gcpbs._warned_no_status_table = False
    with (
        patch.object(gcpbs, "_POLL_TIMEOUT", 0.05),
        patch.object(gcpbs, "_POLL_INTERVAL", 0.01),
    ):
        yield
    gcpbs._in_flight.clear()


@pytest.fixture
def fake() -> FakeComputeClient:
    return FakeComputeClient()


@pytest.fixture
def adapter(fake: FakeComputeClient) -> GlobusComputePBSAdapter:
    return GlobusComputePBSAdapter(fake, "ep-123", _FUNCS)  # type: ignore[arg-type]


def _job(name: str) -> JobSubmitPayload:
    return JobSubmitPayload(
        name=name,
        account="acct",
        queue="q",
        num_nodes=1,
        gpus_per_node=8,
        walltime_min=30,
        log_path=Path("/logs/x.log"),
        script_path=Path("/scripts/x.sh"),
        scheduler_flags="",
    )


async def _call(
    adapter: GlobusComputePBSAdapter, func: gcpbs.FuncName, **kwargs: Any
) -> Any:
    return await adapter._call(func, timeout=0.05, interval=0.01, **kwargs)


async def test_timeout_reports_pending_reason(
    adapter: GlobusComputePBSAdapter,
) -> None:
    with pytest.raises(TimeoutError) as exc:
        await _call(adapter, "qstat")

    msg = str(exc.value)
    assert "task-0" in msg
    assert "ep-123" in msg
    assert "'waiting-for-ep'" in msg
    assert "no user endpoint has picked it up" in msg


async def test_result_after_pending(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    async def finish_soon() -> None:
        await asyncio.sleep(0.02)
        fake.complete("task-0", _QSTAT_RESULT)

    task = asyncio.create_task(finish_soon())
    statuses = await adapter._call("qstat", timeout=5, interval=0.01)
    await task
    assert statuses == _QSTAT_RESULT
    assert not gcpbs._in_flight


async def test_unfinished_read_is_reused_not_resubmitted(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    """Repeated polls of an unresponsive endpoint don't pile up tasks."""
    for _ in range(3):
        with pytest.raises(TimeoutError):
            await _call(adapter, "qstat")
    assert len(fake.submitted) == 1


async def test_reused_read_accepted_when_fresh(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    with pytest.raises(TimeoutError):
        await _call(adapter, "qstat")
    fake.complete("task-0", _QSTAT_RESULT)

    assert await _call(adapter, "qstat") == _QSTAT_RESULT
    assert len(fake.submitted) == 1


async def test_reused_read_discarded_when_stale(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    """A read last seen pending long ago may have run long ago: resubmit."""
    with pytest.raises(TimeoutError):
        await _call(adapter, "qstat")
    fake.complete("task-0", {"stale": {}})

    with patch.object(gcpbs, "_READ_FRESHNESS_SEC", 0.0):
        with pytest.raises(TimeoutError) as exc:
            await _call(adapter, "qstat")
    assert len(fake.submitted) == 2
    assert "task-1" in str(exc.value)

    fake.complete("task-1", _QSTAT_RESULT)
    assert await _call(adapter, "qstat") == _QSTAT_RESULT


async def test_read_abandoned_after_cap(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    with pytest.raises(TimeoutError):
        await _call(adapter, "qstat")
    with patch.object(gcpbs, "_READ_ABANDON_SEC", 0.0):
        with pytest.raises(TimeoutError):
            await _call(adapter, "qstat")
    assert len(fake.submitted) == 2


async def test_reads_with_different_args_are_separate(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    for job_id in ("1.pbs", "2.pbs", "1.pbs"):
        with pytest.raises(TimeoutError):
            await _call(adapter, "qstat", job_id=job_id)
    assert [kw for _, _, kw in fake.submitted] == [
        {"job_id": "1.pbs"},
        {"job_id": "2.pbs"},
    ]


async def test_pending_qsub_is_never_resubmitted(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    """A late qsub result still counts, and no duplicate job is submitted."""
    job = _job("job-a")
    with (
        patch.object(gcpbs, "_READ_ABANDON_SEC", 0.0),
        patch.object(gcpbs, "_READ_FRESHNESS_SEC", 0.0),
    ):
        for _ in range(3):
            with pytest.raises(TimeoutError, match="will not resubmit"):
                await asyncio.wait_for(adapter.submit_job(job), 5)
        assert len(fake.submitted) == 1

        fake.complete("task-0", "123.pbs\n")
        result = await adapter.submit_job(job)

    assert result.scheduler_id == "123.pbs"
    assert len(fake.submitted) == 1
    assert not gcpbs._in_flight


async def test_different_qsubs_are_independent(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    for name in ("job-a", "job-b"):
        with pytest.raises(TimeoutError):
            await adapter.submit_job(_job(name))
    assert len(fake.submitted) == 2


async def test_failed_task_is_forgotten(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    with pytest.raises(TimeoutError):
        await _call(adapter, "qdel", job_id="1.pbs")
    fake.fail("task-0", TaskExecutionFailed("boom"))
    with pytest.raises(TaskExecutionFailed):
        await _call(adapter, "qdel", job_id="1.pbs")
    assert not gcpbs._in_flight

    with pytest.raises(TimeoutError):
        await _call(adapter, "qdel", job_id="1.pbs")
    assert len(fake.submitted) == 2


async def test_in_flight_is_shared_across_adapter_instances(
    fake: FakeComputeClient,
) -> None:
    """Controllers build a new adapter per reconcile; dedup must survive that."""
    for _ in range(2):
        a = GlobusComputePBSAdapter(fake, "ep-123", _FUNCS)  # type: ignore[arg-type]
        with pytest.raises(TimeoutError):
            await _call(a, "qstat")
    assert len(fake.submitted) == 1

    other_ep = GlobusComputePBSAdapter(fake, "ep-456", _FUNCS)  # type: ignore[arg-type]
    with pytest.raises(TimeoutError):
        await _call(other_ep, "qstat")
    assert len(fake.submitted) == 2


async def test_hung_get_result_is_reported(fake: FakeComputeClient) -> None:
    client = MagicMock(wraps=fake)

    def hang(task_id: str) -> None:
        del task_id
        time.sleep(0.2)

    client.get_result.side_effect = hang
    adapter = GlobusComputePBSAdapter(client, "ep-123", _FUNCS)
    with patch.object(gcpbs, "_RPC_TIMEOUT", 0.01):
        with pytest.raises(TimeoutError, match="get_result call did not return"):
            await _call(adapter, "qstat")


async def test_scheduler_rpc_preserves_inner_timeout_message() -> None:
    """A TimeoutError raised by the adapter itself is not relabeled."""

    async def inner() -> None:
        raise TimeoutError("Compute task abc: last seen 'waiting-for-ep'")

    with pytest.raises(TimeoutError, match="waiting-for-ep"):
        await scheduler_rpc(inner(), timeout=5)


async def test_scheduler_rpc_names_call_on_own_timeout() -> None:
    async def slow_call() -> None:
        await asyncio.sleep(3600)

    with pytest.raises(TimeoutError, match=r"slow_call RPC timed out after 0\.05s"):
        await scheduler_rpc(slow_call(), timeout=0.05)


async def test_sdk_task_cache_pruned_after_success(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    with pytest.raises(TimeoutError):
        await _call(adapter, "qstat")
    assert "task-0" in fake._task_status_table
    fake.complete("task-0", _QSTAT_RESULT)

    assert await _call(adapter, "qstat") == _QSTAT_RESULT
    assert fake._task_status_table == {}


async def test_sdk_task_cache_pruned_for_stale_and_abandoned_reads(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    with pytest.raises(TimeoutError):
        await _call(adapter, "qstat")
    fake.complete("task-0", {"stale": {}})
    with patch.object(gcpbs, "_READ_FRESHNESS_SEC", 0.0):
        with pytest.raises(TimeoutError):
            await _call(adapter, "qstat")
    assert "task-0" not in fake._task_status_table

    with patch.object(gcpbs, "_READ_ABANDON_SEC", 0.0):
        with pytest.raises(TimeoutError):
            await _call(adapter, "qstat")
    assert "task-1" not in fake._task_status_table


async def test_sdk_task_cache_pruned_after_failure(
    adapter: GlobusComputePBSAdapter, fake: FakeComputeClient
) -> None:
    with pytest.raises(TimeoutError):
        await _call(adapter, "qdel", job_id="1.pbs")
    fake.fail("task-0", TaskExecutionFailed("boom"))
    with pytest.raises(TaskExecutionFailed):
        await _call(adapter, "qdel", job_id="1.pbs")
    assert fake._task_status_table == {}


@pytest.mark.parametrize("table", [None, ["not", "a", "dict"]])
async def test_missing_sdk_task_cache_warns_once(
    fake: FakeComputeClient, table: Any
) -> None:
    """If the SDK changes its private cache, calls still work; we warn once."""
    if table is None:
        del fake._task_status_table
    else:
        fake._task_status_table = table
    adapter = GlobusComputePBSAdapter(fake, "ep-123", _FUNCS)  # type: ignore[arg-type]

    with patch.object(gcpbs, "logger") as logger:
        for i in range(2):
            fake.complete(f"task-{i}", _QSTAT_RESULT)
            assert await _call(adapter, "qstat") == _QSTAT_RESULT

    warnings = [
        c for c in logger.warning.call_args_list if "_task_status_table" in c.args[0]
    ]
    assert len(warnings) == 1
