from pathlib import Path
from typing import Literal

from ninja import Schema


class Sam3Request(Schema):
    inference_type: Literal["single-image", "batch"]
    data_uri: str
    single_image_prompt: str | None
    weights_dir_override: Path | None = None
