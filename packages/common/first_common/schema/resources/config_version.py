from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ConfigVersionSummary(BaseModel):
    """
    Audit history of applied configuration changes.
    """

    uid: int
    applied_at: datetime
    applied_by: str


class ConfigVersion(ConfigVersionSummary):
    """
    Audit history of applied configuration changes, with detailed diff.
    """

    changes: dict[str, Any]
