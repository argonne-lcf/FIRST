"""The quarantine guard also precedes initialization on reused pilot jobs."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from first_pilot.admission import AdmissionDeferredKnownQuarantine
from first_pilot.replica_manager import ReplicaManager


@pytest.mark.parametrize(
    "error",
    [
        AdmissionDeferredKnownQuarantine("admission_deferred_known_quarantine"),
        RuntimeError("native PBS lookup failed: infrastructure error"),
    ],
)
def test_reused_pilot_guard_precedes_reservation_and_model_initialization(
    error: RuntimeError,
) -> None:
    manager = ReplicaManager.__new__(ReplicaManager)
    manager.config = SimpleNamespace()  # type: ignore[assignment]
    manager.node_hostnames = ["x4820c0s4b0n0"]
    healthy = object()
    manager._replicas = {"healthy": healthy}  # type: ignore[dict-item]
    with (
        patch(
            "first_pilot.admission.guard_quarantined_nodes", side_effect=error
        ) as guard,
        patch.object(manager, "_validate_request") as validate,
        patch("first_pilot.replica_manager.Replica") as initialize,
    ):
        with pytest.raises(type(error), match=str(error)):
            manager.start_replica(SimpleNamespace())  # type: ignore[arg-type]
    guard.assert_called_once_with(manager.config, manager.node_hostnames)
    validate.assert_not_called()
    initialize.assert_not_called()
    assert manager._replicas == {"healthy": healthy}
