"""Mask secrets in text bound for Slack.

Health alerts carry free text (tracebacks, remote stderr, health-check detail)
that could in principle echo a credential. Every outgoing Slack message passes
through `Redactor.redact_blocks` (see `HealthAlerter._post_message`). The
admin system-health API keeps the unredacted text.
"""

import os
import re
from collections.abc import Mapping
from typing import Any

from dotenv import dotenv_values
from pydantic import BaseModel, SecretStr

# Shorter values would shred ordinary text.
MIN_SECRET_LEN = 8

# Environment variables whose values are treated as secrets (e.g. static
# backend API keys resolved via SecretRef("env_var://...")).
_SECRET_ENV_NAME = re.compile(r"KEY|SECRET|TOKEN|PASSWORD")

_URL_PASSWORD = re.compile(r"://[^/\s:@]*:([^\s@]+)@")
_SECRET_KEY = r"[\w-]*(?:password|passwd|secret|token|api[_-]?key)"

# (pattern, replacement), applied in order after exact-value masking.
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # PEM block; then the halves of one cut by truncation.
    (
        re.compile(r"-----BEGIN [A-Z0-9 ]+-----.*?-----END [A-Z0-9 ]+-----", re.S),
        "***PEM***",
    ),
    (re.compile(r"(?:[A-Za-z0-9+/=]{16,}\s+)+-----END [A-Z0-9 ]+-----"), "***PEM***"),
    (re.compile(r"-----BEGIN [A-Z0-9 ]+-----(?:\s+[A-Za-z0-9+/=]+)*"), "***PEM***"),
    (re.compile(r"\bBearer\s+\S+", re.I), "Bearer ***"),
    (_URL_PASSWORD, "://***:***@"),
    # password=..., client_secret=..., api_key=...
    (re.compile(rf"\b({_SECRET_KEY})=[^\s&\"',;)]+", re.I), r"\1=***"),
    # "password": "...", 'api_key': '...'
    (re.compile(rf"([\"']{_SECRET_KEY}[\"']\s*:\s*[\"'])[^\"']+", re.I), r"\1***"),
]


def _settings_secrets(obj: Any, path: str, out: dict[str, str]) -> None:
    """Collect `{value: field path}` for every SecretStr and URL password."""
    if isinstance(obj, BaseModel):
        for name in type(obj).model_fields:
            _settings_secrets(getattr(obj, name), f"{path}{name}.", out)
    elif isinstance(obj, Mapping):
        for key, value in obj.items():
            _settings_secrets(value, f"{path}{key}.", out)
    elif isinstance(obj, (list, tuple)):
        for i, value in enumerate(obj):
            _settings_secrets(value, f"{path}{i}.", out)
    elif isinstance(obj, (SecretStr, str)):
        name = path.rstrip(".")
        value = obj.get_secret_value() if isinstance(obj, SecretStr) else obj
        if isinstance(obj, SecretStr):
            out.setdefault(value, name)
        for match in _URL_PASSWORD.finditer(value):
            out.setdefault(match.group(1), f"{name}.password")


def _env_secrets(environ: Mapping[str, str | None], out: dict[str, str]) -> None:
    for name, value in environ.items():
        upper = name.upper()
        if value and _SECRET_ENV_NAME.search(upper) and not upper.endswith("_FILE"):
            out.setdefault(value, name)


class Redactor:
    """Masks known secret values (by name) and secret-shaped patterns."""

    def __init__(self, secrets: Mapping[str, str]) -> None:
        values = sorted(
            (v for v in secrets if len(v) >= MIN_SECRET_LEN), key=len, reverse=True
        )
        self._names = {v: secrets[v] for v in values}
        # One alternation, longest first: a value containing another is masked
        # whole, and replacements are never re-scanned.
        self._exact = (
            re.compile("|".join(re.escape(v) for v in values)) if values else None
        )

    @classmethod
    def from_settings(
        cls, settings: Any, environ: Mapping[str, str | None] | None = None
    ) -> "Redactor":
        """Secrets from `settings` plus secret-named environment variables.

        By default the environment is read like `SecretRef` resolves it:
        `.env`, `.env.secret`, then the process environment.
        """
        if environ is None:
            environ = {
                **dotenv_values(".env"),
                **dotenv_values(".env.secret"),
                **os.environ,
            }
        secrets: dict[str, str] = {}
        _settings_secrets(settings, "", secrets)
        _env_secrets(environ, secrets)
        return cls(secrets)

    def redact(self, text: str) -> str:
        if self._exact is not None:
            text = self._exact.sub(lambda m: f"***{self._names[m.group()]}***", text)
        for pattern, replacement in _PATTERNS:
            text = pattern.sub(replacement, text)
        return text

    def redact_blocks(self, blocks: Any) -> Any:
        """Copy of Slack Block Kit `blocks` with every `text` string redacted."""
        if isinstance(blocks, list):
            return [self.redact_blocks(b) for b in blocks]
        if isinstance(blocks, dict):
            return {
                k: (
                    self.redact(v)
                    if k == "text" and isinstance(v, str)
                    else self.redact_blocks(v)
                )
                for k, v in blocks.items()
            }
        return blocks
