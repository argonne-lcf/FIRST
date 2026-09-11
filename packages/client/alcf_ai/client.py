import os
from typing import TYPE_CHECKING, Any, Generator

from alcf_tokens.auth import ServiceName, get_access_token
from dotenv import load_dotenv
from httpx import Auth, Client, Request, Response, Timeout

from first_common.schema.auth import UserAuthEvent

from ._http import raise_for_status
from .catalog import CatalogAPI
from .dinov3 import DINOv3API
from .inference import InferenceAPI
from .sam3 import Sam3API
from .staging import StagingAPI

if TYPE_CHECKING:
    from anthropic import Anthropic
    from openai import OpenAI

load_dotenv()

# The alcf-tokens service whose scope the gateway accepts
INFERENCE_SERVICE = ServiceName.inference

DEFAULT_BASE_URL = os.environ.get(
    "inference_base_url", "https://inference-api.alcf.anl.gov"
)

# LLM completions can legitimately take minutes; only the connect phase is short.
DEFAULT_TIMEOUT = Timeout(10.0, read=600.0)


class AutoGlobusAuth(Auth):
    """httpx auth flow that attaches a fresh Globus access token to every request."""

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        request.headers["Authorization"] = (
            f"Bearer {get_access_token(INFERENCE_SERVICE)}"
        )
        yield request


class InferenceClient(Client):
    """
    Authenticated httpx client for the ALCF Inference Gateway.

    - ``client.catalog``: discover Models, deployments, clusters, access groups.
    - ``client.inference``: federated inference (the gateway picks a backend).
    - ``client.deployment(name)``: inference pinned to a single deployment.
    - ``client.staging``: Globus data staging in and out of the clusters.
    - ``client.sam3`` / ``client.dinov3``: segmentation task-model wrappers.
    - ``client.openai`` / ``client.anthropic``: official SDK clients wired to
      the gateway's OpenAI-compatible and Anthropic-compatible endpoints.

    Extra keyword arguments are forwarded to ``httpx.Client`` (e.g. ``transport``
    or ``auth=None`` for tests).
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: Timeout = DEFAULT_TIMEOUT,
        **httpx_kwargs: Any,
    ) -> None:
        httpx_kwargs.setdefault("auth", AutoGlobusAuth())
        super().__init__(
            base_url=base_url or DEFAULT_BASE_URL, timeout=timeout, **httpx_kwargs
        )
        self.catalog = CatalogAPI(self)
        self.inference = InferenceAPI(self)
        self.staging = StagingAPI(self)
        self.sam3 = Sam3API(self)
        self.dinov3 = DINOv3API(self)

    def __repr__(self) -> str:
        return f"InferenceClient({self.base_url})"

    def deployment(self, name: str) -> InferenceAPI:
        """Inference API scoped to one deployment (by resource name or slug)."""
        return InferenceAPI(self, deployment=name)

    def whoami(self) -> UserAuthEvent:
        resp = self.get("/whoami")
        raise_for_status(resp)
        return UserAuthEvent.model_validate(resp.json())

    @property
    def openai(self) -> "OpenAI":
        """
        An ``openai.OpenAI`` client that reuses this client's auth and targets
        the federated ``/federated/v1`` routes.
        """
        from openai import OpenAI

        return OpenAI(
            api_key="unused",  # replaced by AutoGlobusAuth on every request
            base_url=f"{str(self.base_url).rstrip('/')}/federated/v1",
            http_client=self,
        )

    @property
    def anthropic(self) -> "Anthropic":
        """
        An ``anthropic.Anthropic`` client targeting the federated Anthropic
        Messages route.  Requires ``pip install anthropic``.  The current access
        token is used as the bearer token; construct a new client after it
        expires.
        """
        try:
            from anthropic import Anthropic
        except ImportError as exc:
            raise ImportError(
                "The anthropic SDK is not installed: pip install anthropic"
            ) from exc

        return Anthropic(
            auth_token=get_access_token(INFERENCE_SERVICE),
            base_url=f"{str(self.base_url).rstrip('/')}/federated",
        )
