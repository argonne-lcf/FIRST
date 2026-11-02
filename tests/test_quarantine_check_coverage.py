"""A passing screen may withdraw only quarantine reasons it actually checked."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from first_pilot.admission import pbs_quarantine

from .test_pilot_node_admission import (
    _calls,
    _fake_pbs,
    _quarantine_config,
    _quarantine_job,
)


@pytest.fixture(autouse=True)
def _service_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "first_pilot.admission.pwd.getpwuid", lambda _uid: MagicMock(pw_name="svc")
    )
    monkeypatch.setenv("PBS_JOBID", "7.pbs")


@pytest.mark.parametrize(
    ("reason", "checks", "withdraw"),
    [
        ("gpu_count", ["gpu_count"], True),
        ("tcp_self_connect", ["gpu_count"], False),
        ("tcp_self_connect", ["gpu_count", "tcp_self_connect"], True),
        ("gpu_count+tcp_self_connect", ["gpu_count"], False),
        (
            "gpu_count+tcp_self_connect",
            ["gpu_count", "tcp_self_connect"],
            True,
        ),
        ("unknown_check", ["gpu_count", "tcp_self_connect"], False),
        ("gpu_count+unknown_check", ["gpu_count"], False),
        (None, ["gpu_count", "tcp_self_connect"], False),
        ("", ["gpu_count", "tcp_self_connect"], False),
    ],
)
def test_withdrawal_requires_every_recorded_check(
    tmp_path: Path, reason: str | None, checks: list[str], withdraw: bool
) -> None:
    pbs = _fake_pbs({"1.pbs": _quarantine_job("dev", "n1", reason=reason)})
    config = _quarantine_config(tmp_path, checks=checks, tcp_probe_port=20005)

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(config, ["n1.lab"], {}, None)

    assert _calls(pbs, "qdel") == ([["qdel", "1.pbs"]] if withdraw else [])
    assert _calls(pbs, "qsub") == []


def test_coverage_does_not_withdraw_foreign_or_still_failing_requests(
    tmp_path: Path,
) -> None:
    pbs = _fake_pbs(
        {
            "1.pbs": _quarantine_job("prod", "n1", reason="gpu_count"),
            "2.pbs": _quarantine_job("dev", "n1", "other", reason="gpu_count"),
            "3.pbs": _quarantine_job("dev", "n1", reason="gpu_count"),
        }
    )
    config = _quarantine_config(tmp_path, checks=["gpu_count"])

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(config, ["n1"], {"n1": ["gpu_count: still missing"]}, None)

    assert _calls(pbs, "qdel") == []
    assert _calls(pbs, "qsub") == []  # The owned request still deduplicates.


def test_uncovered_quarantine_still_counts_toward_the_cap(tmp_path: Path) -> None:
    pbs = _fake_pbs({"1.pbs": _quarantine_job("dev", "n1", reason="tcp_self_connect")})
    config = _quarantine_config(tmp_path, checks=["gpu_count"], max_quarantined_nodes=1)

    with patch("first_pilot.admission._pbs", pbs):
        pbs_quarantine(config, ["n1", "n2"], {"n2": ["gpu_count: missing GPU"]}, None)

    assert _calls(pbs, "qdel") == []
    assert _calls(pbs, "qsub") == []
