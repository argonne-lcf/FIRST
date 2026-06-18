from httpx import Response

from first_common.errors import FirstError


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
