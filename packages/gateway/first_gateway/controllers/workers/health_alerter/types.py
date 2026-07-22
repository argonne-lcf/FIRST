from dataclasses import dataclass

from first_common.schema.resources.runtime import (
    AlertGroup,
    Severity,
    StagedTransition,
)


@dataclass(frozen=True)
class Observation:
    """
    A single health check observation.

    - `key` is stable identity.
    - The system alerts whenever `status` changes from what was last sent to Slack.
    """

    key: str
    status: str
    summary: str
    severity: Severity
    owner: str = ""
    group: AlertGroup = "Other"


@dataclass
class CheckResult:
    """
    The result of a check function, which returns many observations or fails.
    """

    check_function: str
    success: bool
    error_msg: str | None
    observations: list[Observation]


@dataclass
class FlushPlan:
    """
    Matured transitions (stable status) that are ready to post to Slack.
    """

    degradations: list[StagedTransition]  # status != ""
    recoveries: list[str]  # just the keys where status == ""
