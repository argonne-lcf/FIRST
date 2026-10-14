"""Admission policy is configurable per site without weakening Tara defaults."""

import pytest
from pydantic import ValidationError

from first_common.schema.types import NodeAdmission


def test_admission_memory_defaults_preserve_existing_policy() -> None:
    admission = NodeAdmission()
    assert admission.minimum_free_memory_fraction == 0.95
    assert admission.memory_reserve_mib == 256


@pytest.mark.parametrize("fraction", [0, -0.01, 1.01, float("inf"), float("nan")])
def test_memory_fraction_must_be_positive_and_at_most_one(fraction: float) -> None:
    with pytest.raises(ValidationError):
        NodeAdmission(minimum_free_memory_fraction=fraction)


def test_memory_reserve_must_be_nonnegative() -> None:
    with pytest.raises(ValidationError):
        NodeAdmission(memory_reserve_mib=-1)


def test_site_can_configure_walltime_and_quarantine_budget() -> None:
    admission = NodeAdmission(
        quarantine_walltime_min=14 * 24 * 60, max_quarantined_nodes=12
    )
    assert admission.quarantine_walltime_min == 14 * 24 * 60
    assert admission.max_quarantined_nodes == 12


@pytest.mark.parametrize(
    "policy", [{"quarantine_walltime_min": 0}, {"max_quarantined_nodes": 0}]
)
def test_walltime_and_quarantine_budget_must_remain_positive(
    policy: dict[str, int],
) -> None:
    with pytest.raises(ValidationError):
        NodeAdmission.model_validate(policy)
