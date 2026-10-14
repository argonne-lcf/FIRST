"""PBS request validation is independent of a site's grouping resource names."""

from typing import Any

import pytest

from first_common.pbs_quarantine import exclusive_placement, execution_vnodes


@pytest.mark.parametrize(
    "place",
    [
        "exclhost",
        "excl",
        "scatter:excl",
        "pack:exclhost",
        "free:exclhost",
        "vscatter:exclhost",
        "scatter:excl:group=tier0",
        "scatter:exclhost:group=rack_id",
        "group=host:exclhost:pack",
        "exclhost:group=placement_set",
    ],
)
def test_pbs_exclusivity_accepts_site_groups_and_option_order(place: str) -> None:
    assert exclusive_placement(place)


@pytest.mark.parametrize(
    "place",
    [
        None,
        2,
        "",
        "shared",
        "scatter",
        "scatter:shared:group=host",
        "scatter:exclhost:shared",
        "scatter:pack:exclhost",
        "excl:exclhost",
        "exclhost:exclhost",
        "exclhost:group=host:group=rack",
        "exclhost:",
        "exclhost:group=",
        "exclhost:group=rack+other",
        "exclhost:unknown",
        "exclhost\n",
        "exclhost" + " " * 4096,
    ],
)
def test_pbs_exclusivity_rejects_missing_conflicting_or_malformed_options(
    place: Any,
) -> None:
    assert not exclusive_placement(place)


def test_native_execution_parser_preserves_site_resource_values() -> None:
    assert execution_vnodes("(gpu-a:ncpus=64:ngpus=8:accelerator=h100)") == [
        ("gpu-a", {"ncpus": "64", "ngpus": "8", "accelerator": "h100"})
    ]


@pytest.mark.parametrize(
    "value",
    [
        "(gpu-a[0]:ncpus=32:ngpus=4+gpu-a[1]:ncpus=32:ngpus=4)",
        "(gpu-a.example:ncpus=64:ngpus=8)",
    ],
)
def test_native_execution_parser_does_not_guess_physical_mapping(value: str) -> None:
    with pytest.raises(RuntimeError, match="physical|noncanonical"):
        execution_vnodes(value)
