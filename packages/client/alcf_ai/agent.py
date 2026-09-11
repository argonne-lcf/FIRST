"""
Generate coding-agent configuration (opencode, pi, codex, claude) for the gateway.

A single provider points at the federated routes; per-model entries carry the
context window and capabilities published by the catalog.
"""

import copy
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Literal, Protocol

import httpx
import tomlkit
from pydantic import BaseModel, ConfigDict
from tomlkit.items import InlineTable

from first_common.schema.resources.read import ModelSummary

logger = logging.getLogger(__name__)

Agent = Literal["opencode", "codex", "pi", "claude"]
AGENTS: tuple[Agent, ...] = ("opencode", "codex", "pi", "claude")

PROVIDER_KEY = "alcf-inference-service"
PROVIDER_NAME = "ALCF Inference Service"

# Downstream backends (e.g. Minerva) route requests sharing this header to the
# same replica for prompt-cache reuse; agents fill it from $ALCF_SESSION_ID.
SESSION_HEADER = "X-ALCF-Session-ID"
SESSION_ENV = "ALCF_SESSION_ID"

# Endpoints an agent can drive; a model qualifies if it serves any of them.
_OPENAI_ENDPOINTS = frozenset({"chat/completions", "responses"})
AGENT_ENDPOINTS: dict[Agent, frozenset[str]] = {
    "opencode": _OPENAI_ENDPOINTS,
    "codex": _OPENAI_ENDPOINTS,
    "pi": _OPENAI_ENDPOINTS,
    "claude": frozenset({"messages"}),
}

# Effort levels pi understands, in ascending order.
_PI_LEVELS = ("minimal", "low", "medium", "high", "xhigh", "max")

# npm package (alcf-ai-opencode-plugin) that injects a refreshed token.
OPENCODE_PLUGIN_PACKAGE = "alcf-ai-opencode-plugin"

# Command-based provider auth (auto token refresh) requires codex >= 0.118.0.
_CODEX_AUTH_COMMAND_VERSION = (0, 118, 0)


# --- Capability metadata ------------------------------------------------------------


class Reasoning(BaseModel):
    model_config = ConfigDict(extra="ignore")
    supported: bool = False
    default_effort: str | None = None
    effort_levels: list[str] = []
    separate_output: bool = False


class ToolCalling(BaseModel):
    model_config = ConfigDict(extra="ignore")
    supported: bool = False


class ModelCapabilities(BaseModel):
    """
    The client-neutral capability metadata a Model may publish in its
    ``capabilities`` object.  Unknown keys are ignored so the catalog can grow.
    """

    model_config = ConfigDict(extra="ignore")
    input_modalities: list[str] = ["text"]
    tool_calling: ToolCalling = ToolCalling()
    reasoning: Reasoning = Reasoning()
    max_output_tokens: int | None = None


@dataclass(frozen=True)
class AgentModel:
    """What an agent needs to know about one Model."""

    id: str
    name: str
    context_window: int | None
    endpoints: tuple[str, ...]
    capabilities: ModelCapabilities

    @classmethod
    def from_summary(cls, m: ModelSummary) -> "AgentModel":
        return cls(
            id=m.name,
            name=m.display_name or m.name,
            context_window=m.max_model_len,
            endpoints=tuple(m.supported_endpoints),
            capabilities=ModelCapabilities.model_validate(m.capabilities),
        )

    @property
    def supports_responses(self) -> bool:
        return "responses" in self.endpoints

    @property
    def supports_chat(self) -> bool:
        return "chat/completions" in self.endpoints

    @property
    def supports_messages(self) -> bool:
        return "messages" in self.endpoints


def select_models(
    models: list[ModelSummary],
    names: list[str],
    endpoints: frozenset[str] = _OPENAI_ENDPOINTS,
) -> list[AgentModel]:
    """
    Models serving any of ``endpoints``, optionally restricted to ``names``
    (matched against name or alias).  Models come back in the order ``names``
    lists them, else sorted by name; the first is the agent's default.
    """

    def matches(n: str, m: ModelSummary) -> bool:
        return n == m.name or n in m.aliases

    if names:
        missing = [n for n in names if not any(matches(n, m) for m in models)]
        if missing:
            raise ValueError(f"Unknown or inaccessible model(s): {', '.join(missing)}")
        models = sorted(
            (m for m in models if any(matches(n, m) for n in names)),
            key=lambda m: next(i for i, n in enumerate(names) if matches(n, m)),
        )
    else:
        models = sorted(models, key=lambda m: m.name)
    return [
        AgentModel.from_summary(m)
        for m in models
        if endpoints & set(m.supported_endpoints)
    ]


def _supported_modalities(modalities: list[str], allowed: tuple[str, ...]) -> list[str]:
    """``modalities`` deduplicated and restricted to those an agent's schema knows."""
    return [m for m in dict.fromkeys(modalities) if m in allowed]


def federated_url(base_url: str) -> str:
    return f"{base_url.rstrip('/')}/federated/v1"


def token_helper_command() -> list[str] | None:
    """
    Command that silently refreshes and prints an inference access token.

    Returns None when no way to run the helper was found.
    """
    # uv prunes its cache, so an executable in there is not a durable helper.
    cache = Path(os.environ.get("UV_CACHE_DIR", Path.home() / ".cache" / "uv"))
    if uv := shutil.which("uv"):
        try:
            output = subprocess.check_output(
                [uv, "cache", "dir"], text=True, stderr=subprocess.DEVNULL
            )
            cache = Path(output.strip())
        except (OSError, subprocess.SubprocessError):
            pass

    def durable(exe: str | Path) -> bool:
        return cache.resolve() not in Path(exe).resolve().parents

    if (exe := shutil.which("alcf-tokens")) and durable(exe):
        return [exe, "get-token", "inference"]
    if (exe := shutil.which("alcf-ai")) and durable(exe):
        return [exe, "auth", "get-access-token"]
    argv0 = Path(sys.argv[0])
    if argv0.name.startswith("alcf-ai") and argv0.is_file() and durable(argv0):
        return [str(argv0.resolve()), "auth", "get-access-token"]
    if uvx := shutil.which("uvx"):
        return [uvx, "alcf-tokens", "get-token", "inference"]
    return None


def _version_tuple(version: str) -> tuple[int, ...]:
    # Drop any prerelease suffix: "0.120.0-alpha.1" -> (0, 120, 0).
    return tuple(int(p) for p in version.split("-")[0].split(".") if p.isdigit())


# --- Config file helpers ------------------------------------------------------------


class ConfigLoadError(RuntimeError):
    """An existing agent config file could not be read or parsed."""

    def __init__(self, path: Path, reason: object) -> None:
        super().__init__(
            f"Refusing to modify {path}: the existing file could not be loaded "
            f"({reason}). Fix or move it aside, then re-run this command."
        )
        self.path = path


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open() as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ConfigLoadError(path, err) from err
    if not isinstance(data, dict):
        raise ConfigLoadError(path, "top-level value is not a JSON object")
    return data


def _load_toml(path: Path) -> tomlkit.TOMLDocument:
    try:
        with path.open() as f:
            return tomlkit.load(f)
    except FileNotFoundError:
        return tomlkit.TOMLDocument()
    except (OSError, UnicodeDecodeError, tomlkit.exceptions.TOMLKitError) as err:
        raise ConfigLoadError(path, err) from err


def _write_config_file(path: Path, content: str) -> None:
    """
    Atomically write ``content`` to ``path``.

    If the file already exists with different contents, a copy is first saved
    alongside it as ``<name>.bak.<timestamp>``.
    """
    # Write through symlinks (e.g. dotfile managers) rather than replacing them.
    target = path.resolve()
    target.parent.mkdir(exist_ok=True, parents=True)

    try:
        existing: bytes | None = target.read_bytes()
    except FileNotFoundError:
        existing = None
    if existing == content.encode():
        return
    if existing is not None:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = target.with_name(f"{target.name}.bak.{timestamp}")
        shutil.copy2(target, backup)
        logger.info(f"Backed up {path} to {backup}")

    fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        if existing is not None:
            shutil.copymode(target, tmp)
        os.replace(tmp, target)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def _write_json(path: Path, config: dict[str, Any]) -> None:
    _write_config_file(path, json.dumps(config, indent=2) + "\n")


def _write_toml(path: Path, config: tomlkit.TOMLDocument) -> None:
    _write_config_file(path, tomlkit.dumps(config))


# --- opencode ---------------------------------------------------------------------


def opencode_model_entry(model: AgentModel) -> dict[str, Any]:
    caps = model.capabilities
    entry: dict[str, Any] = {"name": model.name, "timeout": False}
    if model.context_window:
        entry["limit"] = {
            "context": model.context_window,
            "output": caps.max_output_tokens or 0,
        }
    entry["modalities"] = {"input": caps.input_modalities}
    if caps.tool_calling.supported:
        entry["tool_call"] = True
    if caps.reasoning.supported:
        entry["reasoning"] = True
        if caps.reasoning.default_effort and caps.reasoning.effort_levels:
            entry["options"] = {"reasoningEffort": caps.reasoning.default_effort}
            entry["variants"] = {
                lvl: {"reasoningEffort": lvl} for lvl in caps.reasoning.effort_levels
            }
        if caps.reasoning.separate_output:
            entry["interleaved"] = {"field": "reasoning_content"}
    return entry


def opencode_v2_model_entry(model: AgentModel) -> dict[str, Any]:
    """A model entry in opencode 2's native config schema."""
    caps = model.capabilities
    entry: dict[str, Any] = {"name": model.name}
    capabilities: dict[str, Any] = {"tools": caps.tool_calling.supported}
    if caps.input_modalities:
        capabilities["input"] = caps.input_modalities
    if caps.reasoning.supported:
        controls: dict[str, Any] = {"supported": True}
        if caps.reasoning.effort_levels:
            controls["effort"] = caps.reasoning.effort_levels
        capabilities["reasoning"] = controls
        if caps.reasoning.default_effort:
            entry["settings"] = {"reasoningEffort": caps.reasoning.default_effort}
        if caps.reasoning.separate_output:
            entry["compatibility"] = {"reasoningField": "reasoning_content"}
    entry["capabilities"] = capabilities
    if model.context_window:
        entry["limit"] = {"context": model.context_window}
    return entry


def opencode_version() -> str:
    """The installed opencode CLI version (e.g. "1.18.35")."""
    opencode = shutil.which("opencode")
    if opencode is None:
        raise RuntimeError("opencode CLI is not installed")
    output = subprocess.check_output([opencode, "--version"], text=True).strip()
    match = re.search(r"\d+(?:\.\d+)+", output)
    if match is None:
        raise ValueError(f"unrecognized version output: {output!r}")
    return match.group()


def _opencode_plugin_id(entry: Any) -> str:
    """The npm package name of an opencode plugin entry, without any version."""
    if isinstance(entry, str):
        package = entry
    elif isinstance(entry, list):
        package = entry[0] if entry else ""
    elif isinstance(entry, dict):
        package = entry.get("package", "")
    else:
        package = ""
    return re.sub(r"@[\w.\-+]+$", "", str(package))


def _without_plugin(plugins: list[Any]) -> list[Any]:
    return [p for p in plugins if _opencode_plugin_id(p) != OPENCODE_PLUGIN_PACKAGE]


def _clear_opencode_plugin_cache() -> None:
    """
    Drop opencode's cached copy of the plugin so it installs the current release;
    opencode otherwise keeps reusing the cached package.
    """
    for cache in ("packages", "npm"):
        for path in (Path.home() / ".cache" / "opencode" / cache).glob(
            f"{OPENCODE_PLUGIN_PACKAGE}*"
        ):
            shutil.rmtree(path, ignore_errors=True)
            logger.info(f"Cleared the cached opencode plugin at {path}")


def _configure_opencode_v1(
    config: dict[str, Any],
    base_url: str,
    api_key: str,
    models: list[AgentModel],
    token_helper: list[str] | None,
) -> None:
    providers = config.setdefault("provider", {})
    providers[PROVIDER_KEY] = {
        "name": PROVIDER_NAME,
        "npm": "@ai-sdk/openai-compatible",
        "options": {
            "baseURL": federated_url(base_url),
            "apiKey": "alcf-ai-auto-refresh" if token_helper else api_key,
            "headers": {SESSION_HEADER: f"{{env:{SESSION_ENV}}}"},
        },
        "models": {m.id: opencode_model_entry(m) for m in models},
    }

    # opencode installs this npm plugin itself; it injects a refreshed token.
    plugins = _without_plugin(config.get("plugin", []))
    if token_helper:
        plugins.append([OPENCODE_PLUGIN_PACKAGE, {"tokenHelper": token_helper}])
    config["plugin"] = plugins


def _configure_opencode_v2(
    config: dict[str, Any],
    base_url: str,
    api_key: str,
    models: list[AgentModel],
    token_helper: list[str] | None,
) -> None:
    providers = config.setdefault("providers", {})
    providers[PROVIDER_KEY] = {
        "name": PROVIDER_NAME,
        "package": "@opencode/ai/providers/openai-compatible",
        "settings": {
            "baseURL": federated_url(base_url),
            "apiKey": "alcf-ai-auto-refresh" if token_helper else api_key,
            "headerTimeout": False,
            "chunkTimeout": False,
        },
        "headers": {SESSION_HEADER: f"{{env:{SESSION_ENV}}}"},
        "models": {m.id: opencode_v2_model_entry(m) for m in models},
    }

    # Drop entries an earlier run wrote in the V1 schema.
    legacy_providers = config.get("provider")
    if isinstance(legacy_providers, dict):
        for key in [k for k in legacy_providers if k.startswith(PROVIDER_KEY)]:
            del legacy_providers[key]
        if not legacy_providers:
            del config["provider"]

    # opencode installs this npm plugin itself; it injects a refreshed token.
    plugins = _without_plugin(config.get("plugins", []))
    if token_helper:
        plugins.append(
            {
                "package": OPENCODE_PLUGIN_PACKAGE,
                "options": {"tokenHelper": token_helper},
            }
        )
    config["plugins"] = plugins

    legacy_plugins = config.get("plugin")
    if isinstance(legacy_plugins, list):
        if kept := _without_plugin(legacy_plugins):
            config["plugin"] = kept
        else:
            del config["plugin"]


def configure_opencode(
    base_url: str,
    api_key: str,
    models: list[AgentModel],
    *,
    token_helper: list[str] | None = None,
    version: str | None = None,
) -> Path:
    """
    Register the gateway as an opencode provider, in opencode 2's native
    schema when that version (or newer) is installed.
    """
    path = Path.home() / ".config" / "opencode" / "opencode.jsonc"
    config = _load_json(path)

    if version is None:
        try:
            version = opencode_version()
        except (OSError, RuntimeError, subprocess.SubprocessError, ValueError) as err:
            logger.warning(
                f"Could not detect the installed opencode version ({err}); writing "
                "the V1 configuration schema, which opencode 2 also reads."
            )
    if version is not None and _version_tuple(version) >= (2,):
        _configure_opencode_v2(config, base_url, api_key, models, token_helper)
    else:
        _configure_opencode_v1(config, base_url, api_key, models, token_helper)

    _write_json(path, config)
    _clear_opencode_plugin_cache()
    return path


# --- pi ---------------------------------------------------------------------------


def pi_model_entry(model: AgentModel) -> dict[str, Any]:
    caps = model.capabilities
    entry: dict[str, Any] = {
        "id": model.id,
        "name": model.name,
        "api": "openai-responses" if model.supports_responses else "openai-completions",
    }
    # pi's models.json schema knows only text/image
    if inputs := _supported_modalities(caps.input_modalities, ("text", "image")):
        entry["input"] = inputs
    if model.context_window:
        entry["contextWindow"] = model.context_window
    if caps.max_output_tokens:
        entry["maxTokens"] = caps.max_output_tokens
    if caps.reasoning.supported:
        entry["reasoning"] = True
        if caps.reasoning.effort_levels:
            entry["thinkingLevelMap"] = {
                lvl: lvl if lvl in caps.reasoning.effort_levels else None
                for lvl in _PI_LEVELS
            }
            entry["compat"] = {"supportsReasoningEffort": True}
    return entry


def configure_pi(
    base_url: str,
    api_key: str,
    models: list[AgentModel],
    *,
    token_helper: list[str] | None = None,
) -> Path:
    path = Path.home() / ".pi" / "agent" / "models.json"
    config = _load_json(path)
    providers = config.setdefault("providers", {})
    providers[PROVIDER_KEY] = {
        "baseUrl": federated_url(base_url),
        "api": "openai-completions",
        # pi runs a "!"-prefixed value as a shell command.
        "apiKey": f"!{shlex.join(token_helper)}" if token_helper else api_key,
        # pi resolves "$VAR" headers strictly (errors when unset), so expand
        # the session id in a shell that supplies a default.
        "headers": {SESSION_HEADER: f"!echo ${{{SESSION_ENV}:-default}}"},
        "compat": {"supportsDeveloperRole": False},
        "models": [pi_model_entry(m) for m in models],
    }
    _write_json(path, config)
    return path


# --- codex ------------------------------------------------------------------------

CatalogFetcher = Callable[[str], tuple[dict[str, Any], str]]


def codex_version() -> str:
    """The installed Codex CLI version (e.g. "0.149.1")."""
    codex = shutil.which("codex")
    if codex is None:
        raise RuntimeError("codex CLI is not installed")
    output = subprocess.check_output([codex, "--version"], text=True).strip()
    return output.rsplit(maxsplit=1)[-1].lstrip("v")


def fetch_codex_catalog(version: str) -> tuple[dict[str, Any], str]:
    """The upstream model catalog and base prompt matching a Codex version."""
    base = (
        "https://raw.githubusercontent.com/openai/codex/"
        f"rust-v{version}/codex-rs/models-manager"
    )
    with httpx.Client(timeout=httpx.Timeout(60)) as client:
        catalog = client.get(f"{base}/models.json")
        catalog.raise_for_status()
        prompt = client.get(f"{base}/prompt.md")
        prompt.raise_for_status()
    upstream: dict[str, Any] = catalog.json()
    return upstream, prompt.text


def build_codex_catalog(
    upstream: dict[str, Any],
    prompt_text: str,
    model: AgentModel,
    version: str,
) -> dict[str, Any]:
    """Clone a version-matched upstream catalog entry and adapt it to ``model``."""
    template = next(
        (m for m in upstream["models"] if m.get("slug") == "gpt-5.4"),
        upstream["models"][0],
    )
    entry = copy.deepcopy(template)
    context = model.context_window or 65536
    # Codex's catalog accepts only text/image/audio modalities.
    input_modalities = _supported_modalities(
        model.capabilities.input_modalities, ("text", "image", "audio")
    )
    entry.update(
        {
            "slug": model.id,
            "display_name": model.name,
            "description": f"{model.id} served through the {PROVIDER_NAME}",
            "visibility": "list",
            "supported_in_api": True,
            "priority": 0,
            "minimal_client_version": version,
            "availability_nux": None,
            "upgrade": None,
            "context_window": context,
            "max_context_window": context,
            "auto_compact_token_limit": None,
            "effective_context_window_percent": 90,
            "comp_hash": None,
            "default_reasoning_level": None,
            "supported_reasoning_levels": [],
            "supports_reasoning_summary_parameter": False,
            "supports_reasoning_summaries": False,
            "default_reasoning_summary": "none",
            "reasoning_summary_format": "none",
            "support_verbosity": False,
            "default_verbosity": None,
            "shell_type": "shell_command",
            "apply_patch_tool_type": "freeform",
            "supports_search_tool": False,
            "web_search_tool_type": "text",
            "experimental_supported_tools": [],
            "input_modalities": input_modalities or ["text"],
            "supports_image_detail_original": False,
            "prefer_websockets": False,
            "use_responses_lite": False,
            "tool_mode": None,
            "multi_agent_version": None,
            "auto_review_model_override": None,
            "model_specialty": None,
            "service_tiers": [],
            "additional_speed_tiers": [],
            "default_service_tier": None,
            "include_skills_usage_instructions": True,
            "include_plugin_usage_instructions": False,
            "include_apps_usage_instructions": False,
        }
    )
    messages = entry.setdefault("model_messages", {})
    messages["instructions_template"] = prompt_text
    messages["instructions_variables"] = None
    entry["base_instructions"] = prompt_text
    for key, value in (
        ("supports_parallel_tool_calls", False),
        ("node_repl_disabled", True),
        ("node_repl_auto_review_required", False),
    ):
        if key in entry:
            entry[key] = value
    return {"models": [entry]}


def _codex_session_headers() -> InlineTable:
    # Codex sends the header only when the env var is set.
    headers = tomlkit.inline_table()
    headers[SESSION_HEADER] = SESSION_ENV
    return headers


def configure_codex(
    base_url: str,
    api_key: str,
    models: list[AgentModel],
    *,
    token_helper: list[str] | None = None,
    version: str | None = None,
    fetch_catalog: CatalogFetcher = fetch_codex_catalog,
) -> Path:
    """
    Register the gateway as Codex model providers and generate a version-matched
    model catalog plus a config file per model.

    Codex fixes the wire API per provider: Responses-capable models go under
    ``alcf-inference-service`` and chat-only models under
    ``alcf-inference-service-chat`` (Codex > 0.94 no longer supports chat).

    With a ``token_helper``, Codex >= 0.118.0 runs it to refresh the token;
    older versions get ``api_key`` embedded instead.
    """
    codex_home = Path.home() / ".codex"
    path = codex_home / "config.toml"
    config = _load_toml(path)
    version = version or codex_version()
    responses_only = _version_tuple(version) > (0, 94, 0)

    auth: dict[str, Any]
    if token_helper and _version_tuple(version) >= _CODEX_AUTH_COMMAND_VERSION:
        auth = {"auth": {"command": token_helper[0], "args": token_helper[1:]}}
    else:
        if token_helper:
            logger.warning(
                f"Codex {version} does not support provider auth commands; the "
                "embedded token will not auto-refresh. Upgrade to codex >= 0.118.0."
            )
        auth = {"experimental_bearer_token": api_key}

    responses_models = [m for m in models if m.supports_responses]
    chat_models = [m for m in models if not m.supports_responses]
    if responses_only and chat_models:
        logger.warning(
            f"Codex {version} only supports the Responses API; skipping "
            f"{', '.join(m.id for m in chat_models)}"
        )
        chat_models = []

    providers = config.get("model_providers", tomlkit.table())
    assignments: list[tuple[AgentModel, str]] = []
    if responses_models:
        providers[PROVIDER_KEY] = {
            "name": PROVIDER_NAME,
            "base_url": federated_url(base_url),
            **auth,
            "wire_api": "responses",
            "env_http_headers": _codex_session_headers(),
        }
        assignments += [(m, PROVIDER_KEY) for m in responses_models]
    if chat_models:
        providers[f"{PROVIDER_KEY}-chat"] = {
            "name": f"{PROVIDER_NAME} (Chat Completions)",
            "base_url": federated_url(base_url),
            **auth,
            "wire_api": "chat",
            "env_http_headers": _codex_session_headers(),
        }
        assignments += [(m, f"{PROVIDER_KEY}-chat") for m in chat_models]
    config["model_providers"] = providers

    # Pick a default model only when the user hasn't already configured one.
    if assignments and "model" not in config:
        config["model"] = assignments[0][0].id
        config["model_provider"] = assignments[0][1]
    _write_toml(path, config)

    try:
        upstream, prompt_text = fetch_catalog(version)
    except (httpx.HTTPError, OSError, ValueError) as err:
        logger.warning(f"Skipping codex model catalog generation: {err}")
        return path

    for model, provider in assignments:
        slug = model.id.replace("/", "-")
        catalog_path = codex_home / "catalogs" / f"{slug}.json"
        _write_json(
            catalog_path, build_codex_catalog(upstream, prompt_text, model, version)
        )
        profile = tomlkit.TOMLDocument()
        profile["model_provider"] = provider
        profile["model"] = model.id
        profile["model_catalog_json"] = str(catalog_path)
        _write_toml(codex_home / f"alcf-{slug}.config.toml", profile)
    return path


# --- claude -----------------------------------------------------------------------


def configure_claude(
    base_url: str,
    api_key: str,
    models: list[AgentModel],
    *,
    token_helper: list[str] | None = None,
) -> Path:
    """
    Point Claude Code at the federated Anthropic Messages route, with the
    first of ``models`` as the default for every model tier.

    Claude Code settings cannot expand env vars, so the session header is not
    written here: export ``ANTHROPIC_CUSTOM_HEADERS="X-ALCF-Session-ID: ..."``
    to send one.
    """
    path = Path.home() / ".claude" / "settings.json"
    config = _load_json(path)
    default = models[0]

    env = config.setdefault("env", {})
    env.update(
        {
            # Claude Code appends /v1/messages.
            "ANTHROPIC_BASE_URL": f"{base_url.rstrip('/')}/federated",
            "ANTHROPIC_MODEL": default.id,
            "ANTHROPIC_DEFAULT_OPUS_MODEL": default.id,
            "ANTHROPIC_DEFAULT_SONNET_MODEL": default.id,
            "ANTHROPIC_DEFAULT_HAIKU_MODEL": default.id,
        }
    )
    # Claude Code assumes a 200k window for models it doesn't know.
    if default.context_window:
        env["CLAUDE_CODE_MAX_CONTEXT_TOKENS"] = str(default.context_window)
    else:
        env.pop("CLAUDE_CODE_MAX_CONTEXT_TOKENS", None)
    if token_helper:
        env.pop("ANTHROPIC_AUTH_TOKEN", None)
        config["apiKeyHelper"] = shlex.join(token_helper)
    else:
        env["ANTHROPIC_AUTH_TOKEN"] = api_key
        config.pop("apiKeyHelper", None)

    _write_json(path, config)
    return path


class Configurator(Protocol):
    def __call__(
        self,
        base_url: str,
        api_key: str,
        models: list[AgentModel],
        *,
        token_helper: list[str] | None = None,
    ) -> Path: ...


CONFIGURATORS: dict[str, Configurator] = {
    "opencode": configure_opencode,
    "pi": configure_pi,
    "codex": configure_codex,
    "claude": configure_claude,
}
