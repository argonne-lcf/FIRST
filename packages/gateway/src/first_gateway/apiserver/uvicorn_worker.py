from uvicorn.workers import UvicornWorker as _UvicornWorker


class UvicornWorker(_UvicornWorker):
    CONFIG_KWARGS = {"loop": "uvloop", "http": "httptools"}
