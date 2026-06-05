from uvicorn.workers import UvicornWorker


class InferenceUvicornWorker(UvicornWorker):
    CONFIG_KWARGS = {
        "loop": "asyncio",
        "http": "h11",
        "lifespan": "off",
        "log_config": None,
    }
