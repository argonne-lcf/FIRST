import ipaddress
import logging
import os
import re
import signal
import socket
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from textwrap import dedent
from typing import NamedTuple

from cryptography import x509
from jinja2 import Template

from first_common.schema.pilot import PilotClientRole, PilotRuntimeConfig
from first_common.schema.types import RESOURCE_NAME_PATTERN, normalize_proxy_path

logger = logging.getLogger(__name__)

_conf_template_str = """
    worker_processes 2;

    events {
        worker_connections 4096;
        multi_accept on;
    }

    pid {{nginx_tmpdir}}/nginx.pid;
    error_log {{nginx_tmpdir}}/nginx-error.log warn;

    http {
        # All temp paths must be writable
        client_body_temp_path {{nginx_tmpdir}}/client_body;
        proxy_temp_path {{nginx_tmpdir}}/proxy;
        fastcgi_temp_path {{nginx_tmpdir}}/fastcgi;
        uwsgi_temp_path {{nginx_tmpdir}}/uwsgi;
        scgi_temp_path {{nginx_tmpdir}}/scgi;

        # access logs are retained after job shutdown
        access_log {{access_log_path}} combined buffer=64k flush=5s;

        # Control-plane audit: one JSON line per request
        map $uri $audit_control {
            default 0;
            "~^{{control_path}}" 1;
        }
        log_format control_audit escape=json
            '{"time":"$time_iso8601"'
            ',"remote_addr":"$remote_addr"'
            ',"client_dn":"$ssl_client_s_dn"'
            ',"client_verify":"$ssl_client_verify"'
            ',"request":"$request"'
            ',"uri":"$uri"'
            ',"status":$status'
            ',"body_bytes_sent":$body_bytes_sent'
            ',"request_time":$request_time}';
        access_log {{audit_log_path}} control_audit buffer=64k flush=5s if=$audit_control;

        tcp_nodelay on;                 # push token frames immediately
        gzip off;                       # never compress SSE (it buffers tokens)
        reset_timedout_connection on;

        # Gateway-facing keepalive. Must outlive the gateway/httpx idle expiry so
        # the gateway is always the side that closes an idle connection first.
        keepalive_timeout  300s;
        keepalive_requests 10000;

        # Authorization by client certificate identity. Match on $uri (the
        # normalized path), never $request_uri, so "..", "//", and
        # percent-encoding cannot smuggle a request past a prefix rule.
        map $ssl_client_s_dn $role {
            {% for role in roles -%}
            "CN={{role.value}}" {{role.name}};
            {% endfor -%}
            default none;
        }
        map "$role:$request_method:$uri" $authorized {
            default 0;
            "~^{{roles.control.name}}:" 1;
            "~^{{roles.router.name}}:[A-Z]+:/replicas/" 1;
            "~^{{roles.router.name}}:GET:{{control_path}}logs/" 1;
            "~^{{roles.metrics.name}}:GET:/replicas/.+/metrics$" 1;
        }

        upstream control_api {
            server unix:{{config.control_uds_path.as_posix()}};
            keepalive 8;
        }

        {% for replica in replicas %}
        # Pooled connections to the local vLLM replica over its Unix socket.
        # Keyed by loop index: replica.name contains '/' and '.', invalid in an
        # upstream identifier. Both loops iterate `replicas` in order, so indices align.
        upstream replica_{{loop.index}} {
            server unix:{{replica.uds}};
            keepalive 32;
            keepalive_requests 10000;
            keepalive_timeout 60s;      # < vLLM/uvicorn keep-alive (raise vLLM's; default 5s defeats this)
        }
        {% endfor %}

        server {
            # Bind only the configured interfaces, never the wildcard address.
            {% for ip in listen_ips -%}
            listen {{ip}}:{{config.external_port}} ssl;
            {% endfor -%}
            ssl_protocols TLSv1.3;
            server_name _;
            ssl_certificate {{server_crt_path}};
            ssl_certificate_key {{server_key_path}};

            # Client Authentication (mTLS)
            ssl_client_certificate {{ca_crt_path}};
            ssl_verify_client on;
            ssl_verify_depth 1;

            # Default deny: every location inherits the role policy above.
            if ($authorized = 0) {
                return 403;
            }

            # Prompts can be MBs of JSON; keep them out of disk spool.
            client_max_body_size    32m;
            client_body_buffer_size 1m;

            location {{control_path}} {
                {% for ip in config.ip_allowlist -%}
                allow {{ip}};
                {% endfor -%}
                allow 127.0.0.1;
                deny all;
                proxy_pass http://control_api/;
                # Let the controller's 180s stop read deadline expire first.
                proxy_read_timeout 185s;
            }

            # Anything not pinned by an exact-match location below does not exist.
            location / {
                return 404;
            }

            # One exact-match location per path the replica's model declares
            # (plus its metrics path). proxy_pass names the full upstream path,
            # so no client-supplied remainder is ever forwarded.
            {% for replica in replicas %}
            {% set upstream = loop.index %}
            {% for path in replica.paths %}
            location = /replicas/{{replica.name}}{{path}} {
                {% for ip in config.ip_allowlist -%}
                allow {{ip}};
                {% endfor -%}
                allow 127.0.0.1;
                deny all;
                proxy_pass http://replica_{{upstream}}{{path}};

                # Upstream keepalive prerequisites
                proxy_http_version 1.1;
                proxy_set_header Connection "";

                # Streaming correctness: relay tokens the instant they arrive
                proxy_buffering off;
                proxy_request_buffering off;

                # Timeouts sit ABOVE the gateway's so the gateway times out first
                proxy_connect_timeout 5s;
                proxy_send_timeout    60s;
                proxy_read_timeout    920s;

                proxy_socket_keepalive on;
                proxy_next_upstream off;
            }
            {% endfor %}
            {% endfor %}
        }
    }
"""

conf_template = Template(dedent(_conf_template_str).lstrip())

_SAFE_REPLICA_NAME = re.compile(RESOURCE_NAME_PATTERN)


def check_server_cert_expiry(
    server_crt: str, walltime_min: int, now: datetime | None = None
) -> None:
    """
    Refuse to start a pilot whose server certificate would expire mid-job.
    """
    not_after = x509.load_pem_x509_certificate(server_crt.encode()).not_valid_after_utc
    job_end = (now or datetime.now(timezone.utc)) + timedelta(minutes=walltime_min)
    if not_after < job_end:
        raise RuntimeError(
            f"Pilot server certificate expires at {not_after.isoformat()}, before "
            f"the end of the {walltime_min} minute walltime ({job_end.isoformat()}). "
            "Re-issue the pilot certificates before submitting this job."
        )


class ReplicaUpstream(NamedTuple):
    name: str
    uds: str
    # Upstream paths (leading slash) to expose; see ReplicaStartRequest.proxy_paths.
    paths: tuple[str, ...]


class NginxManager:
    control_path = "/control/"

    def __init__(
        self, config: PilotRuntimeConfig, tmpdir: str | Path, interface_ips: list[str]
    ) -> None:
        self.pilot_config = config
        self.listen_ips = [
            str(ipaddress.IPv4Address(ip))
            for ip in dict.fromkeys(["127.0.0.1", *interface_ips])
        ]
        self.tmpdir = Path(tmpdir).resolve()
        self.tmpdir.mkdir(parents=True, exist_ok=True)

        self.access_log_path = config.audit_dir / f"{config.job_name}.access.log"
        self.audit_log_path = config.audit_dir / (
            f"{config.job_name}.control-access.jsonl"
        )
        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)

        # Materialize cert/key PEMs from the config
        self.ca_crt_path = self._write_secret("ca.crt", config.ca_crt)
        self.server_crt_path = self._write_secret("server.crt", config.server_crt)
        self.server_key_path = self._write_secret("server.key", config.server_key)

        self.config_path = self.tmpdir / "nginx.conf"
        self.config_path.write_text(self.render_config(replicas=[]))
        self._nginx: subprocess.Popen[bytes] | None = None

    def _write_secret(self, name: str, content: str) -> Path:
        path = self.tmpdir / name
        path.touch(0o600)
        path.chmod(0o600)
        with path.open("w") as fp:
            fp.write(content.strip() + "\n")
        return path

    def render_config(self, replicas: list[ReplicaUpstream]) -> str:
        # replica.name and replica.paths are interpolated verbatim into
        # `location` directives. ReplicaStartRequest already enforces these
        # patterns; re-check here so the template can never render a value
        # that escapes its directive.
        for replica in replicas:
            if _SAFE_REPLICA_NAME.fullmatch(replica.name) is None:
                raise ValueError(
                    f"refusing to render NGINX config: unsafe replica name "
                    f"{replica.name!r}"
                )
            for path in replica.paths:
                if path != f"/{normalize_proxy_path(path)}":
                    raise ValueError(
                        f"refusing to render NGINX config: unsafe replica path {path!r}"
                    )

        return conf_template.render(
            config=self.pilot_config,
            listen_ips=self.listen_ips,
            nginx_tmpdir=self.tmpdir.as_posix().rstrip("/"),
            replicas=replicas,
            control_path=self.control_path,
            roles=PilotClientRole,
            ca_crt_path=self.ca_crt_path.as_posix(),
            server_crt_path=self.server_crt_path.as_posix(),
            server_key_path=self.server_key_path.as_posix(),
            access_log_path=self.access_log_path.as_posix(),
            audit_log_path=self.audit_log_path.as_posix(),
        )

    def start(self) -> None:
        args = [
            self.pilot_config.nginx_path.as_posix(),
            "-e",
            f"{self.tmpdir}/nginx-error.log",
            "-c",
            self.config_path.as_posix(),
            "-g",
            "daemon off;",
        ]
        self._nginx = subprocess.Popen(
            args,
            cwd=self.tmpdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    def stop(self) -> None:
        if self._nginx is None or self._nginx.poll() is not None:
            return

        self._nginx.send_signal(signal.SIGQUIT)
        try:
            self._nginx.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self._nginx.kill()

    def reload(self, replicas: list[ReplicaUpstream]) -> None:
        if self._nginx is None:
            raise RuntimeError("NGINX process is not set yet; must call start() first.")

        new_config = self.tmpdir / "nginx.conf.new"
        new_config.write_text(self.render_config(replicas))

        test = subprocess.run(
            [
                self.pilot_config.nginx_path,
                "-t",
                "-e",
                f"{self.tmpdir}/nginx-error.log",
                "-c",
                new_config.as_posix(),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if test.returncode:
            raise RuntimeError(
                f"NGINX configuration test failed: {test.stderr.strip()}"
            )

        os.replace(new_config, self.config_path)
        self._nginx.send_signal(signal.SIGHUP)

    def _read_error_log(self) -> str:
        error_log = self.tmpdir / "nginx-error.log"
        if error_log.exists():
            return error_log.read_text()
        return "(nginx-error.log not found)"

    def wait_until_healthy(self, timeout: float = 10.0, interval: float = 0.2) -> None:
        if self._nginx is None:
            raise RuntimeError("NGINX process is not set yet; must call start() first.")

        port = self.pilot_config.external_port
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            if self._nginx.poll() is not None:
                error_log = self._read_error_log()
                logger.error(
                    "nginx exited with code %s\n%s", self._nginx.returncode, error_log
                )
                raise RuntimeError(f"nginx exited with code {self._nginx.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                time.sleep(interval)

        error_log = self._read_error_log()
        logger.error(
            "nginx not ready on port %d after %ss\n%s", port, timeout, error_log
        )
        raise TimeoutError(f"nginx not ready on port {port} after {timeout}s")
