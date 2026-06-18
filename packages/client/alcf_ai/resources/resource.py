from typing import TYPE_CHECKING

from httpx import Response

from first_common.errors import FirstError

if TYPE_CHECKING:
    from alcf_ai.client import InferenceClient


def raise_for_status(response: Response) -> None:
    if response.status_code < 400:
        return

    try:
        error = response.json()["error"]
    except:
        error = None

    if error:
        message = error.pop("message", "")
        raise FirstError(f"HTTP Error {response.status_code}: {message}\n {error}\n")
    else:
        response.raise_for_status()


class ClientResource:
    def __init__(self, name: str, client: "InferenceClient") -> None:
        self.name = name
        self._client = client

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
