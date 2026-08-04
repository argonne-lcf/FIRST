"""Request-scoped identity derivation for Minerva replica affinity and cache isolation."""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import TYPE_CHECKING

from django.conf import settings

if TYPE_CHECKING:
    from resource_server_async.logging import RequestContext


SESSION_HEADER = "X-ALCF-Session-ID"
MAX_SESSION_ID_BYTES = 128
AFFINITY_HEADER = "X-Minerva-Affinity-Key"
REQUEST_ID_HEADER = "X-Request-ID"

# vLLM 0.26.0 request protocols whose validated JSON schemas expose cache_salt.
# Anthropic messages, health, and metrics intentionally remain absent.
CACHE_SALT_ENDPOINTS = frozenset(
    {
        "chat/completions",
        "completions",
        "responses",
        "embeddings",
        "pooling",
        "classify",
        "score",
    }
)


class MinervaAffinityConfigurationError(RuntimeError):
    pass


def validate_session_id(value: str | None) -> str | None:
    """Validate an optional visible-ASCII conversation/branch identifier."""
    if value is None or value == "":
        return None
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError(
            f"{SESSION_HEADER} must contain at most {MAX_SESSION_ID_BYTES} visible ASCII bytes"
        ) from exc
    if len(encoded) > MAX_SESSION_ID_BYTES or any(
        byte < 0x21 or byte > 0x7E for byte in encoded
    ):
        raise ValueError(
            f"{SESSION_HEADER} must contain at most {MAX_SESSION_ID_BYTES} visible ASCII bytes"
        )
    return value


def affinity_secret() -> bytes:
    value = getattr(settings, "MINERVA_AFFINITY_HMAC_KEY", None)
    if isinstance(value, str):
        secret = value.encode("utf-8")
    elif isinstance(value, bytes):
        secret = value
    else:
        secret = b""
    if len(secret) < 32:
        raise MinervaAffinityConfigurationError(
            "MINERVA_AFFINITY_HMAC_KEY must contain at least 32 bytes"
        )
    return secret


def hmac_base64url(secret: bytes, fields: tuple[str, ...]) -> str:
    message = b"\0".join(field.encode("utf-8") for field in fields)
    digest = hmac.new(secret, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def derive_cache_salt(secret: bytes, user_id: str, model_slug: str) -> str:
    return hmac_base64url(secret, ("minerva-cache-v1", user_id, model_slug))


def derive_affinity_key(
    secret: bytes,
    user_id: str,
    model_slug: str,
    session_id: str | None,
) -> str:
    return hmac_base64url(
        secret,
        (
            "minerva-affinity-v1",
            user_id,
            model_slug,
            session_id if session_id is not None else "user-default",
        ),
    )


def derive_minerva_request_values(
    context: RequestContext,
    model_slug: str,
) -> tuple[dict[str, str], str]:
    if context.user is None:
        raise MinervaAffinityConfigurationError(
            "authenticated user identity is unavailable for Minerva affinity"
        )
    secret = affinity_secret()
    user_id = context.user.id
    affinity_key = derive_affinity_key(
        secret, user_id, model_slug, context.minerva_session_id
    )
    cache_salt = derive_cache_salt(secret, user_id, model_slug)
    headers = {
        AFFINITY_HEADER: affinity_key,
        REQUEST_ID_HEADER: context.access_log.id,
    }
    return headers, cache_salt
