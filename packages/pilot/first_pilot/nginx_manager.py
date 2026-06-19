import os
import signal
import subprocess
from pathlib import Path
from textwrap import dedent
from typing import NamedTuple, Self

from jinja2 import Template

from .config import Config

_conf_template_str = """
    worker_processes 2;

    events {
        worker_connections 2048;
    }

    pid {{nginx_tmpdir}}/nginx.pid;
    error_log {{nginx_tmpdir}}/nginx-error.log;

    http {
        # All temp paths must be writable
        client_body_temp_path {{nginx_tmpdir}}/client_body;
        proxy_temp_path {{nginx_tmpdir}}/proxy;
        fastcgi_temp_path {{nginx_tmpdir}}/fastcgi;
        access_log {{nginx_tmpdir}}/access.log;
    }

    server {
        listen 8000 ssl;
        ssl_protocols TLSv1.2 TLSv1.3;
        server_name _;
        ssl_certificate {{config.server_crt_path}};
        ssl_certificate_key {{config.server_key_path}};

        # Client Authentication (mTLS)
        ssl_client_certificate {{config.ca_crt_path}};
        ssl_verify_client on;
        ssl_verify_depth 1;

        {% for model in models %}
        location /{{model.slug}}/ {
            {% for ip in config.ip_allowlist -%}
            allow {{ip}};
            {% endfor -%}
            deny all;
            proxy_pass http://127.0.0.1:{{model.port}};
        }
        {% endfor %}
    }
"""

conf_template = Template(dedent(_conf_template_str).lstrip())


class ModelPort(NamedTuple):
    slug: str
    port: int


class NginxManager:
    def __init__(self, tmpdir: str | Path, pilot_config: Config) -> None:
        self.pilot_config = pilot_config
        self.tmpdir = Path(tmpdir)
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        self.config_path = self.tmpdir / "nginx.conf"
        self.config_path.write_text(self.render_config(models=[]))
        self._nginx = None

    def render_config(self, models: list[ModelPort]):
        return conf_template.render(
            config=self.pilot_config,
            nginx_tmpdir=self.tmpdir.as_posix().rstrip("/"),
            models=models,
        )

    def start(self) -> None:
        args = [
            self.pilot_config.nginx_path.as_posix(),
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

    def reload(self, models: list[ModelPort]) -> None:
        if self._nginx is None:
            raise RuntimeError("NGINX process is not set yet; must call start() first.")

        new_config = self.tmpdir / "nginx.conf.new"
        new_config.write_text(self.render_config(models))

        test = subprocess.run(
            [
                self.pilot_config.nginx_path,
                "-t",
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

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        self.stop()
