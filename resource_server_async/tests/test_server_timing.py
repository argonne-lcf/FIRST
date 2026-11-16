import asyncio
import time

from django.http import HttpRequest, HttpResponse, StreamingHttpResponse
from django.test import RequestFactory, SimpleTestCase

from resource_server_async.server_timing import (
    ServerTimingMiddleware,
    record_upstream_time,
)


def _parse(response: HttpResponse | StreamingHttpResponse) -> dict[str, float]:
    """Parse ``name;dur=<ms>[;desc=...]`` entries into {name: ms}."""
    out: dict[str, float] = {}
    for entry in response["Server-Timing"].split(","):
        name, *params = (part.strip() for part in entry.split(";"))
        out[name] = next(float(p[4:]) for p in params if p.startswith("dur="))
    return out


class ServerTimingMiddlewareTests(SimpleTestCase):
    async def test_reports_gateway_and_upstream_time(self) -> None:
        async def view(request: HttpRequest) -> HttpResponse:
            await asyncio.sleep(0.02)  # gateway work before the backend call
            t0 = time.perf_counter()
            await asyncio.sleep(0.05)  # the backend call
            record_upstream_time(time.perf_counter() - t0)
            return HttpResponse("ok")

        response = await ServerTimingMiddleware(view)(RequestFactory().get("/"))
        timing = _parse(response)
        self.assertGreaterEqual(timing["upstream"], 50)
        self.assertGreaterEqual(timing["gateway"] - timing["upstream"], 20)
        self.assertIn('desc="attempts=1"', response["Server-Timing"])
        self.assertNotIn("queue", timing)

    async def test_omits_upstream_when_no_backend_was_called(self) -> None:
        async def view(request: HttpRequest) -> HttpResponse:
            return HttpResponse("ok")

        response = await ServerTimingMiddleware(view)(RequestFactory().get("/"))
        self.assertEqual(set(_parse(response)), {"gateway"})

    async def test_queue_time_from_proxy_stamp(self) -> None:
        async def view(request: HttpRequest) -> HttpResponse:
            return HttpResponse("ok")

        cases = [
            (f"t={time.time() - 0.25:.3f}", True),
            ("t=garbage", False),
            (f"t={time.time() + 60:.3f}", False),  # proxy clock ahead of ours
        ]
        for stamp, expect_queue in cases:
            with self.subTest(stamp=stamp):
                request = RequestFactory().get("/", headers={"X-Request-Start": stamp})
                timing = _parse(await ServerTimingMiddleware(view)(request))
                self.assertEqual("queue" in timing, expect_queue)
                if expect_queue:
                    self.assertGreaterEqual(timing["queue"], 240)
                    self.assertLess(timing["queue"], 5000)

    async def test_recording_outside_a_request_is_a_noop(self) -> None:
        record_upstream_time(1.0)
