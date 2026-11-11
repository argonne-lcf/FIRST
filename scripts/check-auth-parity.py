#!/usr/bin/env python3
"""Compare V1 dotenv gates with a rendered V2 Compose configuration (read-only)."""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, TextIO

from dotenv.parser import parse_stream

# V1 key -> (V2 Globus field, V1 encoding, default).
GATES = {
    "GLOBUS_POLICIES": ("policies", "text", ""),
    "GLOBUS_GROUPS": ("user_groups", "text", ""),
    "AUTHORIZED_IDP_DOMAINS": ("authorized_idp_domains", "text", ""),
    "AUTHORIZED_GROUPS_PER_IDP": ("authorized_groups_per_idp", "groups", "{}"),
    "AUTHORIZED_GLOBUS_SERVICE_USERNAMES": (
        "authorized_service_usernames",
        "json",
        "[]",
    ),
}


def read_dotenv(stream: TextIO) -> dict[str, str]:
    """Never source the file, interpolate ambient variables or print its contents."""
    env = {}
    for binding in parse_stream(stream):
        if binding.error:
            raise ValueError("invalid V1 dotenv syntax")
        if binding.key is not None:
            env[binding.key] = binding.value or ""
    return env


def string_list(value: Any) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError("expected a list of strings")
    return value


def normalize(value: Any, field: str) -> Any:
    if field == "authorized_groups_per_idp":
        if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
            raise ValueError("expected a domain-to-groups mapping")
        return {key: sorted(set(string_list(groups))) for key, groups in value.items()}
    values = string_list(value)
    if field in {"policies", "authorized_idp_domains"} and not values:
        raise ValueError("required auth gate must not be empty")
    return sorted(set(values))


def v1_gates(env: dict[str, str]) -> dict[str, Any]:
    gates = {}
    for key, (field, encoding, default) in GATES.items():
        try:
            raw = env.get(key, default)
            if "${" in raw:
                raise ValueError("unresolved dotenv interpolation")
            value: Any
            if encoding == "text":
                value = [v for v in re.split(r"[\s;]+", raw.strip()) if v]
            else:
                value = json.loads(raw)
                if encoding == "groups":
                    if not isinstance(value, dict) or not all(
                        isinstance(v, str) for v in value.values()
                    ):
                        raise ValueError("expected V1 comma-separated group strings")
                    value = {
                        k: [g.strip() for g in groups.split(",")]
                        for k, groups in value.items()
                    }
            gates[field] = normalize(value, field)
        except (ValueError, TypeError):
            raise ValueError(f"invalid V1 gate {key}") from None
    return gates


def v2_gates(compose: Any, service: str) -> dict[str, Any]:
    try:
        env = compose["services"][service]["environment"]
        if not isinstance(env, dict):
            raise ValueError()
        env = {k.lower(): v for k, v in env.items()}
        # Pydantic accepts a JSON parent setting, overridden by nested keys.
        parent = json.loads(env.get("first_globus", "{}"))
        if not isinstance(parent, dict):
            raise ValueError()
    except (KeyError, TypeError, AttributeError, ValueError):
        raise ValueError("missing or invalid Compose service environment") from None

    gates = {}
    for field, _, _ in GATES.values():
        try:
            key = "first_globus__" + field
            if any(k.startswith(key + "__") for k in env):
                raise ValueError("nested gate overrides require resolved JSON")
            value = (
                json.loads(env[key])
                if key in env
                else parent.get(
                    field, {} if field == "authorized_groups_per_idp" else []
                )
            )
            gates[field] = normalize(value, field)
        except (ValueError, TypeError):
            raise ValueError(f"invalid V2 gate FIRST_GLOBUS__{field.upper()}") from None
    return gates


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v1-env", required=True, type=Path)
    parser.add_argument(
        "--v2-compose",
        required=True,
        help="docker compose config --format json output; '-' reads stdin",
    )
    parser.add_argument("--service", default="inference-gateway")
    args = parser.parse_args()
    try:
        with args.v1_env.open(encoding="utf-8") as stream:
            old = v1_gates(read_dotenv(stream))
        if args.v2_compose == "-":
            compose = json.load(sys.stdin)
        else:
            with Path(args.v2_compose).open(encoding="utf-8") as stream:
                compose = json.load(stream)
        new = v2_gates(compose, args.service)
    except OSError:
        print("error: unable to read parity input", file=sys.stderr)
        return 2
    except (ValueError, UnicodeError) as exc:
        # JSON decoder errors may include input; report only our safe messages.
        detail = (
            "invalid Compose JSON"
            if isinstance(exc, json.JSONDecodeError)
            else str(exc)
        )
        if isinstance(exc, UnicodeError):
            detail = "parity input must be UTF-8"
        print(f"error: {detail}", file=sys.stderr)
        return 2

    differences = [field for field in old if old[field] != new[field]]
    for field in differences:
        print(f"DIFF: {field}")
    if not differences:
        print("No differences in the five auth gates.")
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
