import os
import socket

from uvicorn.workers import UvicornWorker as _UvicornWorker

# "host:port" each worker listens on with its own SO_REUSEPORT socket.
WORKER_BIND_ENV = "FIRST_GATEWAY_WORKER_BIND"


def _reuseport_socket(bind: str, backlog: int) -> socket.socket:
    host, _, port = bind.rpartition(":")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind((host or "0.0.0.0", int(port)))
    sock.listen(backlog)
    sock.setblocking(False)
    sock.set_inheritable(False)
    return sock


class UvicornWorker(_UvicornWorker):
    """Uvicorn worker that can listen on its own SO_REUSEPORT socket.

    Gunicorn's workers share the one listening socket the master created, and
    the kernel hands most new connections on it to whichever worker polled
    last.  The proxy in front keeps its connections open, so one worker ends
    up serving most of the requests and saturates while the others idle.

    With ``FIRST_GATEWAY_WORKER_BIND=host:port`` set, each worker binds its own
    socket with SO_REUSEPORT instead, and the kernel spreads new connections
    evenly over the workers (by a hash of the connection's addresses).  The
    master must then bind somewhere else (``-b unix:...``): its socket is not
    served.  Connections still queued on a worker's socket when that worker
    dies are reset.

    Unset, the worker serves gunicorn's shared socket as usual.
    """

    CONFIG_KWARGS = {"loop": "uvloop", "http": "httptools"}

    def run(self) -> None:
        bind = os.environ.get(WORKER_BIND_ENV)
        if bind:
            self.sockets = [_reuseport_socket(bind, self.config.backlog)]
        super().run()
