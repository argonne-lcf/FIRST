from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ConfigVersionSummary(BaseModel):
    uid: int
    applied_at: datetime
    applied_by: str


class ConfigVersion(ConfigVersionSummary):
    changes: dict[str, Any]
