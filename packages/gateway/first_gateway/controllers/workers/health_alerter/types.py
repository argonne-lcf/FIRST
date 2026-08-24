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

    - `key` is stable internal identity
    - The system alerts whenever `status` changes from what was last sent to
      Slack, so `status` must be stable across benign churn (e.g. a rising
      retry count belongs in `summary`, not `status`).
    - `summary` is the human-readable degradation line shown in Slack.
    - `display_name` is the human-readable resource name alone, used to render the
      recovery line
    """

    key: str
    status: str
    summary: str
    severity: Severity
    display_name: str = ""
    recovery_hint: str = ""
    debounce_s: float | None = None
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
    recoveries: list[StagedTransition]  # status == ""
