import logging

import pytest


@pytest.fixture
def gateway_caplog(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> pytest.LogCaptureFixture:
    """caplog that sees first_gateway records.

    The apiserver lifespan (run by other tests) calls config_logging(), which
    stops first_gateway from propagating to the root logger caplog listens on.
    """
    monkeypatch.setattr(logging.getLogger("first_gateway"), "propagate", True)
    return caplog
