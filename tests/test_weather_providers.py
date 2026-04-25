from __future__ import annotations

import threading
from datetime import date

from forecast import forecast_engine
from precipitation import precip_forecast


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, payload: dict | None = None, headers: dict | None = None, url: str = "https://example.test"):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}
        self.url = url

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise forecast_engine.requests.HTTPError(
                f"{self.status_code} Client Error",
                response=self,
            )

    def json(self) -> dict:
        return self._payload


def test_openmeteo_precip_forecast_request_uses_explicit_date_range_without_forecast_days(monkeypatch):
    captured: dict[str, object] = {}

    def fake_get(url: str, *, params: dict, timeout: int):
        captured["url"] = url
        captured["params"] = dict(params)
        captured["timeout"] = timeout
        return _FakeResponse(payload={"daily": {"precipitation_sum": [1.25, 2.5]}})

    monkeypatch.setattr(precip_forecast.requests, "get", fake_get)

    total = precip_forecast._get_om_precip_range(
        22.308,
        113.9185,
        date(2026, 4, 25),
        date(2026, 4, 30),
        "mm",
        archive=False,
    )

    assert total == 3.75
    assert captured["url"] == "https://api.open-meteo.com/v1/forecast"
    assert captured["timeout"] == 15
    assert captured["params"]["start_date"] == "2026-04-25"
    assert captured["params"]["end_date"] == "2026-04-30"
    assert "forecast_days" not in captured["params"]


def test_visual_crossing_temperature_forecast_retries_after_rate_limit(monkeypatch):
    responses = [
        _FakeResponse(status_code=429, headers={"Retry-After": "1"}, url="https://weather.visualcrossing.com/rate-limit"),
        _FakeResponse(payload={"days": [{"tempmax": 30.2}]}),
    ]
    sleeps: list[float] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        assert "timeline/6.5774,3.3214/2026-04-27" in url
        assert params["elements"] == "tempmax"
        assert timeout == 10
        return responses.pop(0)

    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)
    monkeypatch.setattr(forecast_engine.time, "sleep", lambda delay: sleeps.append(delay))

    value = forecast_engine.get_visual_crossing_forecast_max_temp("lagos", date(2026, 4, 27))

    assert value == 30.2
    assert sleeps == [1.0]
