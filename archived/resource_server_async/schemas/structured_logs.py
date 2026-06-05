import ast
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from logging import getLogger
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from django.conf import settings
from django.http import HttpResponse, StreamingHttpResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    computed_field,
    field_validator,
)

_user_slog = getLogger("resource_server_async.structured.user")
_access_slog = getLogger("resource_server_async.structured.access_log")
_request_slog = getLogger("resource_server_async.structured.request_log")
_request_metrics_slog = getLogger("resource_server_async.structured.request_metrics")
_batch_slog = getLogger("resource_server_async.structured.batch_log")
_batch_metrics_slog = getLogger("resource_server_async.structured.batch_metrics")

MAX_LEN = 1800


def _truncate_str(value: str) -> str:
    if len(value) <= MAX_LEN:
        return value
    return value[:MAX_LEN] + f"...<truncated {len(value) - MAX_LEN} chars>"


TruncatedStr = Annotated[str, PlainSerializer(_truncate_str)]


@dataclass(slots=True)
class UsageTokens:
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


class UserPydantic(BaseModel):
    id: str
    name: str
    username: str
    user_group_uuids: list[str]
    idp_id: str
    idp_name: str
    auth_service: str

    def emit(self) -> None:
        """
        Emit user info to log
        """
        _user_slog.info(
            "authenticated",
            extra={
                **self.model_dump(mode="json", exclude={"name"}),
                "user.name": self.name,
            },
        )


class AccessLogPydantic(BaseModel):
    id: str
    timestamp_request: datetime
    api_route: str
    origin_ip: str | None
    timestamp_response: datetime | None = None
    status_code: int | None = None
    error: TruncatedStr | None = None
    authorized_groups: str | None = None

    def emit(
        self, user: UserPydantic | None, response: HttpResponse | StreamingHttpResponse
    ) -> None:
        """
        Emit access log after view returns response.
        """
        self.timestamp_response = datetime.now(timezone.utc)
        self.status_code = response.status_code

        if response.status_code >= 400:
            if isinstance(response, StreamingHttpResponse):
                self.error = "<streaming response error>"
            else:
                self.error = response.content.decode(errors="ignore")

        _access_slog.info(
            "created",
            extra={
                **self.model_dump(mode="json"),
                "user.id": user.id if user else None,
            },
        )


class RequestLogPydantic(BaseModel):
    id: str
    access_log_id: str
    user_id: str
    cluster: str
    framework: str
    model: str
    openai_endpoint: str
    prompt: TruncatedStr
    timestamp_compute_request: datetime
    status_code: int | None = None
    timestamp_compute_response: datetime | None = None
    result: TruncatedStr | None = None
    task_uuid: str | None = None

    def emit(self, response_body: str, status_code: int | None) -> None:
        """
        Log an LLM prompt request and results.

        Large prompt/result payloads exceeding MAX_LEN will be written to the
        filesystem.
        """
        self.status_code = status_code
        self.result = response_body

        if self.timestamp_compute_response is None:
            self.timestamp_compute_response = datetime.now(timezone.utc)

        _request_slog.info(
            "created",
            extra=self.model_dump(mode="json"),
        )

        if len(self.prompt) > MAX_LEN or len(self.result) > MAX_LEN:
            full = {"prompt": self.prompt, "result": self.result}
            prompt_dir = Path(settings.PROMPT_STORAGE_DIR)
            prompt_file = Path(prompt_dir) / f"{self.id}.json"
            try:
                prompt_file.write_text(json.dumps(full, indent=2))
            except FileNotFoundError:
                prompt_file.parent.mkdir(parents=True, exist_ok=True)
                prompt_file.write_text(json.dumps(full, indent=2))

    async def emit_metrics(self, usage: UsageTokens | None = None) -> None:
        """
        Log LLM prompt request metrics.  If usage is None, attempts to
        extract token metrics from self.result.

        Call emit(response) to set the result before calling emit_metrics().
        Otherwise, uses the provided token usage data.
        """
        if usage is None:
            usage = extract_usage(self.result) if self.result else UsageTokens()

        metrics = RequestMetricsPydantic(
            request_id=self.id,
            cluster=self.cluster,
            framework=self.framework,
            model=self.model,
            timestamp_compute_request=self.timestamp_compute_request,
            timestamp_compute_response=self.timestamp_compute_response,
            status_code=self.status_code,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens,
        )

        _request_metrics_slog.info("upserted", extra=metrics.model_dump(mode="json"))

        from resource_server_async.endpoints import BaseEndpoint

        endpoint = await BaseEndpoint.load_adapter(
            self.cluster, self.framework, self.model
        )
        endpoint.record_token_usage(self.user_id, int(usage.total_tokens or 0))


class RequestMetricsPydantic(BaseModel):
    request_id: str
    cluster: str
    framework: str
    model: str
    timestamp_compute_request: datetime
    timestamp_compute_response: datetime | None = None
    status_code: int | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | float | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def response_time_sec(self) -> float | None:
        if self.timestamp_compute_request and self.timestamp_compute_response:
            start = self.timestamp_compute_request
            end = self.timestamp_compute_response
            return (end - start).total_seconds()
        return None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def throughput_tokens_per_sec(self) -> float | None:
        if (
            isinstance(self.total_tokens, (int, float))
            and isinstance(self.response_time_sec, (int, float))
            and self.response_time_sec > 1e-9
        ):
            return self.total_tokens / self.response_time_sec
        return None


class BatchLogPydantic(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    access_log_id: str
    user_id: str

    input_file: str
    output_folder_path: str | None = None
    cluster: str | None = None
    framework: str | None = None
    model: str

    globus_batch_uuid: str | None = None
    task_ids: str | None = None
    result: TruncatedStr | None = Field(default="")

    status: str | None = None
    in_progress_at: datetime | None = None
    completed_at: datetime | None = None
    failed_at: datetime | None = None

    @field_validator("id", "access_log_id", "user_id", mode="before")
    @classmethod
    def coerce_uuid(cls, v: Any) -> Any:
        if isinstance(v, UUID):
            return str(v)
        return v

    def emit(self, action: str) -> None:
        _batch_slog.info(action, extra=self.model_dump(mode="json"))

    def emit_metrics(
        self,
        total_tokens: int | None,
        num_responses: int | None,
        response_time_sec: float | None,
        throughput_tokens_per_sec: float | None,
    ) -> None:
        defaults = {
            "cluster": self.cluster,
            "framework": self.framework,
            "model": self.model,
            "status": self.status,
            "total_tokens": total_tokens,
            "num_responses": num_responses,
            "response_time_sec": response_time_sec,
            "throughput_tokens_per_sec": throughput_tokens_per_sec,
            "completed_at": self.completed_at,
        }
        _batch_metrics_slog.info("upserted", extra={"batch_id": self.id, **defaults})


def _parse_dict(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return ast.literal_eval(raw)


def _get_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    value: dict[str, Any] | None = data.get(key)
    return value if isinstance(value, dict) else {}


def _get_int(data: dict[str, Any], key: str) -> int | None:
    value = data.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def extract_usage(result: str) -> UsageTokens:
    """
    Attempt to parse token usage counts from a JSON response body.

    Handles three shapes:

    - OpenAI chat/completions, completions, embeddings::
        {"usage": {"prompt_tokens": int,
                   "completion_tokens": int,
                   "total_tokens": int}}
    - OpenAI Responses API::
        {"usage": {"input_tokens": int,
                   "output_tokens": int,
                   "total_tokens": int}}
    - Anthropic Messages API::
        {"usage": {"input_tokens": int,
                   "output_tokens": int}}  # no total_tokens

    Also honours a top-level ``metrics.total_tokens`` if present (the compute
    function attaches that to non-streaming responses).  When the upstream
    only reports input/output tokens, total_tokens is computed as their sum
    so token-rate-limit accounting still works.
    """
    try:
        data = json.loads(result)
        if isinstance(data, str):
            data = _parse_dict(data)
        assert isinstance(data, dict)
    except Exception:
        return UsageTokens()

    usage = _get_dict(data, "usage")
    metrics = _get_dict(data, "metrics")

    prompt_tokens = _get_int(usage, "prompt_tokens") or _get_int(usage, "input_tokens")
    completion_tokens = _get_int(usage, "completion_tokens") or _get_int(
        usage, "output_tokens"
    )
    total_tokens = _get_int(usage, "total_tokens") or _get_int(metrics, "total_tokens")

    # Anthropic does not report total_tokens; derive it from input/output so
    # TPM accounting still charges the right amount.
    if total_tokens is None and (
        prompt_tokens is not None or completion_tokens is not None
    ):
        total_tokens = (prompt_tokens or 0) + (completion_tokens or 0)

    return UsageTokens(
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
    )
