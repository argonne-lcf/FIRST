from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ConfigVersion(BaseModel):
    uid: int
    applied_at: datetime
    applied_by: str
    changes: dict[str, Any]
