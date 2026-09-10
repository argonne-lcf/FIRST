import atexit
import logging
import logging.config
from datetime import date, datetime, timezone
from typing import Any

from pythonjsonlogger.json import JsonFormatter

from first_gateway.apiserver.context import get_request_id


class GatewayJsonFormatter(JsonFormatter):
    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)

        log_record["timestamp"] = datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).isoformat()
        log_record["level"] = record.levelname
        log_record["logger"] = record.name
        log_record["pid"] = record.process
        log_record["lineno"] = record.lineno

        # Stamped by RequestIdFilter at enqueue time (request thread/context)
        if rid := getattr(record, "request_id", None):
            log_record["request_id"] = rid

    @staticmethod
    def json_default(obj: Any) -> str:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return str(obj)


class RequestIdFilter(logging.Filter):
    """Stamp ``record.request_id`` from the request contextvar.

    Attached to the QueueHandler, this runs synchronously at ``logger.info()``
    call time (the request's own thread/context, where the contextvar is set),
    so the attribute survives the queue hand-off to the listener thread.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if (rid := get_request_id()) is not None:
            record.request_id = rid
        return True


class TracebackOnly(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.exc_info is not None and record.exc_info[1] is not None


class UvicornAccessFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple) and len(record.args) >= 5:
            record.client_addr = record.args[0]
            record.method = record.args[1]
            record.path = record.args[2]
            record.http_version = record.args[3]
            record.status_code = record.args[4]
            record.msg = ""
            record.args = None
        return True


LOGGING: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {"()": "first_gateway.log_config.GatewayJsonFormatter"},
        "plain": {"format": "\n%(message)s\n"},
    },
    "filters": {
        "uvicorn_access_fields": {"()": "first_gateway.log_config.UvicornAccessFilter"},
        "traceback_only": {"()": "first_gateway.log_config.TracebackOnly"},
        "request_id": {"()": "first_gateway.log_config.RequestIdFilter"},
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "json",
        },
        "stderr_crash": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "formatter": "plain",
            "filters": ["traceback_only"],
        },
        "queue": {
            "class": "logging.handlers.QueueHandler",
            "handlers": ["stdout", "stderr_crash"],
            "respect_handler_level": True,
            "filters": ["request_id"],
        },
    },
    "loggers": {
        "uvicorn.error": {"handlers": ["queue"], "level": "INFO", "propagate": False},
        "uvicorn.access": {
            "handlers": ["queue"],
            "level": "INFO",
            "propagate": False,
            "filters": ["uvicorn_access_fields"],
        },
        "gunicorn.error": {
            "handlers": ["queue"],
            "level": "INFO",
            "propagate": False,
        },
        "gunicorn.access": {
            "handlers": ["queue"],
            "level": "INFO",
            "propagate": False,
        },
        "first_common": {
            "handlers": ["queue"],
            "level": "INFO",
            "propagate": False,
        },
        "first_gateway": {
            "handlers": ["queue"],
            "level": "INFO",
            "propagate": False,
        },
    },
    "root": {"level": "WARNING", "handlers": ["queue"]},
}


def config_logging(log_level: str) -> None:
    for logger in LOGGING["loggers"].values():
        logger["level"] = log_level
    logging.config.dictConfig(LOGGING)
    listener = logging.getHandlerByName("queue").listener  # type: ignore[union-attr]
    listener.start()
    # atexit is a backstop for non-graceful exits; the lifespan drains explicitly
    # on graceful shutdown via drain_logs() so in-flight usage rows aren't lost.
    atexit.register(drain_logs)


def drain_logs() -> None:
    """Flush and stop the logging QueueListener so every enqueued record is
    written before the process exits.

    ``QueueListener.stop()`` enqueues a sentinel and joins the listener thread,
    which guarantees all records already on the queue (notably in-flight
    ``InferenceLog`` usage rows) are handled first.
    """
    handler = logging.getHandlerByName("queue")
    listener = getattr(handler, "listener", None)
    if listener is not None and listener._thread is not None:
        listener.stop()
