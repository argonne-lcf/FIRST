# This is strictly for typechecking (configured django mypy plugin)
# and is never used at runtime.  However, it makes static analysis more robust.
from .settings import *  # noqa: F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}
