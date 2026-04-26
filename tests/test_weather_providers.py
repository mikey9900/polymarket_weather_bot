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


def test_openmeteo_temperature_forecast_retries_after_rate_limit(monkeypatch):
    responses = [
        _FakeResponse(
            status_code=429,
            headers={"Retry-After": "2"},
            url="https://api.open-meteo.com/v1/forecast?latitude=6.5774&longitude=3.3214",
        ),
        _FakeResponse(payload={"daily": {"temperature_2m_max": [31.4]}}),
    ]
    sleeps: list[float] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        assert url == "https://api.open-meteo.com/v1/forecast"
        assert params["daily"] == "temperature_2m_max"
        assert params["start_date"] == "2026-04-27"
        assert params["end_date"] == "2026-04-27"
        assert timeout == 10
        return responses.pop(0)

    monkeypatch.setattr(forecast_engine, "OPENMETEO_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(forecast_engine, "_openmeteo_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)
    monkeypatch.setattr(forecast_engine.time, "sleep", lambda delay: sleeps.append(delay))

    value = forecast_engine.get_openmeteo_forecast_max_temp("lagos", date(2026, 4, 27))

    assert value == 31.4
    assert sleeps == [2.0]


def test_visual_crossing_temperature_forecast_disables_after_auth_failure_without_leaking_key(monkeypatch, capsys):
    api_key = "super-secret-key"
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params)))
        return _FakeResponse(
            status_code=401,
            url=f"https://weather.visualcrossing.com/timeline?key={api_key}&unitGroup=metric",
        )

    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_API_KEY", api_key)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_auth_failed", False)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_visual_crossing_forecast_max_temp("lagos", date(2026, 4, 27))
    second = forecast_engine.get_visual_crossing_forecast_max_temp("jakarta", date(2026, 4, 27))
    output = capsys.readouterr().out

    assert first is None
    assert second is None
    assert len(calls) == 1
    assert api_key not in output
    assert "disabling VC for this run" in output
