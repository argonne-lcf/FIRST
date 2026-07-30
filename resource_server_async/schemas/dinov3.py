from ninja import Schema


class DINOv3Request(Schema):
    """
    Mirrors the DINOv3 Globus Compute ``Payload`` TypedDict, minus ``output_dir``.
    """

    input_dir: str
    project: str | None = None
    checkpoint: str | None = None
    save_overlay: bool | None = None
    batch_size: int | None = None
    num_workers: int | None = None
