from ninja import Schema


class D3TritonRequest(Schema):
    model_name: str
    input_path: str
    output_path: str
    outputs: list[str] | None = None
