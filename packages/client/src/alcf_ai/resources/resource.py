from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alcf_ai.client import InferenceClient


class ClientResource:
    def __init__(self, name: str, client: "InferenceClient") -> None:
        self.name = name
        self._client = client

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
