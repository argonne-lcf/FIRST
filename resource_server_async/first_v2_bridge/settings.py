import os

# Mapping of V2 deployment-name prefix -> (V1 cluster, V1 framework).
PrefixMap = dict[str, tuple[str, str]]


def _default_prefix_map() -> PrefixMap:
    return {"tara/": ("tara", "api")}


class BridgeSettings:
    """Bridge configuration snapshot, resolved from the environment."""

    def __init__(self) -> None:
        self.redis_url = os.environ.get("FIRST_V2_REDIS_URL") or None
        self.ca_cert_path = os.environ.get("FIRST_V2_CA_CERT_PATH") or None
        self.client_cert_path = os.environ.get("FIRST_V2_CLIENT_CERT_PATH") or None
        self.client_key_path = os.environ.get("FIRST_V2_CLIENT_KEY_PATH") or None

        # Optional SOCKS/HTTP proxy (e.g. socks5h://localhost:1080).
        self.proxy_url = os.environ.get("FIRST_V2_PROXY_URL") or None
        self.check_hostname = _env_bool("FIRST_V2_CHECK_HOSTNAME", False)
        self.trust_env = False
        self.poll_interval_sec = int(os.environ.get("FIRST_V2_POLL_INTERVAL_SEC", "10"))
        self.prefix_map: PrefixMap = _default_prefix_map()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")
