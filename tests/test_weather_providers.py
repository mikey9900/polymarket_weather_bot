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
        self.text = "{}" if payload is None else str(payload)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise forecast_engine.requests.HTTPError(
                f"{self.status_code} Client Error",
                response=self,
            )

    def json(self) -> dict:
        return self._payload


class _FakeDate(date):
    @classmethod
    def today(cls) -> "_FakeDate":
        return cls(2026, 4, 26)


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
        _FakeResponse(payload={"days": [{"datetime": "2026-04-27", "tempmax": 30.2}]}),
    ]
    sleeps: list[float] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        assert "timeline/6.5774,3.3214" in url
        assert params["elements"] == "tempmax"
        assert timeout == 10
        return responses.pop(0)

    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_visual_crossing_daily_cache", {})
    monkeypatch.setattr(forecast_engine, "_visual_crossing_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_auth_failed", False)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)
    monkeypatch.setattr(forecast_engine.time, "sleep", lambda delay: sleeps.append(delay))

    value = forecast_engine.get_visual_crossing_forecast_max_temp("lagos", _FakeDate(2026, 4, 27))

    assert value == 30.2
    assert sleeps == [1.0]


def test_visual_crossing_temperature_forecast_reuses_cached_window_for_neighboring_dates(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params)))
        return _FakeResponse(
            payload={
                "days": [
                    {"datetime": "2026-04-27", "tempmax": 30.2},
                    {"datetime": "2026-04-28", "tempmax": 31.1},
                ]
            }
        )

    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_FORECAST_WINDOW_DAYS", 15)
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_CACHE_TTL_SECONDS", 7200.0)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_visual_crossing_daily_cache", {})
    monkeypatch.setattr(forecast_engine, "_visual_crossing_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_auth_failed", False)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_visual_crossing_forecast_max_temp("lagos", _FakeDate(2026, 4, 27))
    second = forecast_engine.get_visual_crossing_forecast_max_temp("lagos", _FakeDate(2026, 4, 28))

    assert first == 30.2
    assert second == 31.1
    assert len(calls) == 1
    assert calls[0][0].endswith("/timeline/6.5774,3.3214")


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
    monkeypatch.setattr(forecast_engine, "OPENMETEO_MIN_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_openmeteo_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_last_request_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_openmeteo_daily_cache", {})
    monkeypatch.setattr(forecast_engine, "_openmeteo_window", lambda target_date: (target_date, target_date))
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)
    monkeypatch.setattr(forecast_engine.time, "sleep", lambda delay: sleeps.append(delay))

    value = forecast_engine.get_openmeteo_forecast_max_temp("lagos", date(2026, 4, 27))

    assert value == 31.4
    assert sleeps == [2.0]


def test_openmeteo_temperature_forecast_reuses_cached_window_for_neighboring_dates(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params)))
        return _FakeResponse(
            payload={
                "daily": {
                    "time": ["2026-04-27", "2026-04-28"],
                    "temperature_2m_max": [31.4, 32.1],
                }
            }
        )

    monkeypatch.setattr(forecast_engine, "OPENMETEO_MIN_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(forecast_engine, "OPENMETEO_CACHE_TTL_SECONDS", 900.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_openmeteo_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_last_request_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_openmeteo_daily_cache", {})
    monkeypatch.setattr(
        forecast_engine,
        "_openmeteo_window",
        lambda target_date: (date(2026, 4, 27), date(2026, 4, 28)),
    )
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_openmeteo_forecast_max_temp("lagos", date(2026, 4, 27))
    second = forecast_engine.get_openmeteo_forecast_max_temp("lagos", date(2026, 4, 28))

    assert first == 31.4
    assert second == 32.1
    assert len(calls) == 1
    assert calls[0][1]["start_date"] == "2026-04-27"
    assert calls[0][1]["end_date"] == "2026-04-28"


def test_openmeteo_temperature_forecast_reuses_persistent_cache_after_memory_reset(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params)))
        return _FakeResponse(
            payload={
                "daily": {
                    "time": ["2026-04-27", "2026-04-28"],
                    "temperature_2m_max": [31.4, 32.1],
                }
            }
        )

    monkeypatch.setattr(forecast_engine, "OPENMETEO_MIN_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(forecast_engine, "OPENMETEO_CACHE_TTL_SECONDS", 900.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_openmeteo_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_last_request_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_openmeteo_daily_cache", {})
    monkeypatch.setattr(
        forecast_engine,
        "_openmeteo_window",
        lambda target_date: (date(2026, 4, 27), date(2026, 4, 28)),
    )
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_openmeteo_forecast_max_temp("lagos", date(2026, 4, 27))

    assert first == 31.4
    assert len(calls) == 1

    monkeypatch.setattr(forecast_engine, "_openmeteo_daily_cache", {})
    monkeypatch.setattr(
        forecast_engine.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Open-Meteo should have used persistent cache")),
    )

    second = forecast_engine.get_openmeteo_forecast_max_temp("lagos", date(2026, 4, 28))

    assert second == 32.1


def test_openmeteo_temperature_forecast_enters_global_cooldown_after_rate_limit(monkeypatch, capsys):
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params)))
        return _FakeResponse(
            status_code=429,
            url="https://api.open-meteo.com/v1/forecast?latitude=6.5774&longitude=3.3214",
        )

    monkeypatch.setattr(forecast_engine, "OPENMETEO_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(forecast_engine, "OPENMETEO_MIN_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(forecast_engine, "OPENMETEO_RATE_LIMIT_COOLDOWN_SECONDS", 60.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_openmeteo_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_last_request_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_openmeteo_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_openmeteo_daily_cache", {})
    monkeypatch.setattr(forecast_engine, "_openmeteo_window", lambda target_date: (target_date, target_date))
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_openmeteo_forecast_max_temp("lagos", date(2026, 4, 27))
    second = forecast_engine.get_openmeteo_forecast_max_temp("jakarta", date(2026, 4, 27))
    output = capsys.readouterr().out

    assert first is None
    assert second is None
    assert len(calls) == 1
    assert "cooling down" in output


def test_visual_crossing_temperature_forecast_enters_global_cooldown_after_rate_limit(monkeypatch, capsys):
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params)))
        return _FakeResponse(
            status_code=429,
            headers={"Retry-After": "1"},
            url="https://weather.visualcrossing.com/rate-limit",
        )

    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_RATE_LIMIT_COOLDOWN_SECONDS", 60.0)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_visual_crossing_daily_cache", {})
    monkeypatch.setattr(forecast_engine, "_visual_crossing_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_auth_failed", False)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_visual_crossing_forecast_max_temp("lagos", date(2026, 4, 27))
    second = forecast_engine.get_visual_crossing_forecast_max_temp("jakarta", date(2026, 4, 27))
    output = capsys.readouterr().out

    assert first is None
    assert second is None
    assert len(calls) == 1
    assert "cooling down" in output


def test_wu_temperature_forecast_uses_v3_daily_endpoint_for_future_dates(monkeypatch):
    calls: list[tuple[str, dict, int]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params), timeout))
        return _FakeResponse(
            payload={
                "validTimeLocal": [
                    "2026-04-26T07:00:00-0400",
                    "2026-04-27T07:00:00-0400",
                    "2026-04-28T07:00:00-0400",
                ],
                "calendarDayTemperatureMax": [85, 87, 86],
            }
        )

    monkeypatch.setattr(forecast_engine, "WU_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "_wu_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_wu_daily_cache", {})
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    value = forecast_engine.get_wu_forecast_max_temp("miami", _FakeDate(2026, 4, 27))

    assert value == 87.0
    assert calls == [
        (
            "https://api.weather.com/v3/wx/forecast/daily/5day",
            {
                "geocode": "25.7959,-80.287",
                "units": "e",
                "language": "en-US",
                "format": "json",
                "apiKey": "test-key",
            },
            10,
        )
    ]


def test_wu_temperature_forecast_reuses_cached_window_for_neighboring_dates(monkeypatch):
    calls: list[tuple[str, dict, int]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params), timeout))
        return _FakeResponse(
            payload={
                "validTimeLocal": [
                    "2026-04-26T07:00:00-0400",
                    "2026-04-27T07:00:00-0400",
                    "2026-04-28T07:00:00-0400",
                ],
                "calendarDayTemperatureMax": [84, 85, 86],
            }
        )

    monkeypatch.setattr(forecast_engine, "WU_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "WU_CACHE_TTL_SECONDS", 900.0)
    monkeypatch.setattr(forecast_engine, "_wu_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_wu_daily_cache", {})
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_wu_forecast_max_temp("miami", _FakeDate(2026, 4, 27))
    second = forecast_engine.get_wu_forecast_max_temp("miami", _FakeDate(2026, 4, 28))

    assert first == 85.0
    assert second == 86.0
    assert len(calls) == 1


def test_weatherapi_temperature_forecast_skips_when_key_missing(monkeypatch):
    def fake_get(url: str, *, params: dict, timeout: int):
        raise AssertionError("WeatherAPI should not be called without a key")

    monkeypatch.setattr(forecast_engine, "WEATHERAPI_KEY", "")
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_ENABLE_TEMPERATURE", True)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    assert forecast_engine.get_weatherapi_forecast_max_temp("miami", date(2026, 4, 27)) is None


def test_weatherapi_temperature_forecast_returns_none_outside_free_horizon(monkeypatch):
    def fake_get(url: str, *, params: dict, timeout: int):
        raise AssertionError("WeatherAPI should not be called beyond the free forecast horizon")

    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_ENABLE_TEMPERATURE", True)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    assert forecast_engine.get_weatherapi_forecast_max_temp("lagos", _FakeDate(2026, 4, 29)) is None


def test_weatherapi_temperature_forecast_parses_fahrenheit_and_celsius(monkeypatch):
    calls: list[dict] = []
    responses = [
        _FakeResponse(
            payload={
                "forecast": {
                    "forecastday": [
                        {"date": "2026-04-27", "day": {"maxtemp_f": 86.0, "maxtemp_c": 30.0}},
                    ]
                }
            }
        ),
        _FakeResponse(
            payload={
                "forecast": {
                    "forecastday": [
                        {"date": "2026-04-27", "day": {"maxtemp_f": 86.0, "maxtemp_c": 30.0}},
                    ]
                }
            }
        ),
    ]

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append(dict(params))
        return responses.pop(0)

    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_ENABLE_TEMPERATURE", True)
    monkeypatch.setattr(forecast_engine, "_weatherapi_daily_cache", {})
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    miami = forecast_engine.get_weatherapi_forecast_max_temp("miami", _FakeDate(2026, 4, 27))
    lagos = forecast_engine.get_weatherapi_forecast_max_temp("lagos", _FakeDate(2026, 4, 27))

    assert miami == 86.0
    assert lagos == 30.0
    assert calls[0]["q"] == "25.7959,-80.287"
    assert calls[1]["q"] == "6.5774,3.3214"


def test_weatherapi_temperature_forecast_reuses_cached_window_for_neighboring_dates(monkeypatch):
    calls: list[dict] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append(dict(params))
        return _FakeResponse(
            payload={
                "forecast": {
                    "forecastday": [
                        {"date": "2026-04-26", "day": {"maxtemp_f": 84.0, "maxtemp_c": 29.0}},
                        {"date": "2026-04-27", "day": {"maxtemp_f": 85.0, "maxtemp_c": 29.5}},
                        {"date": "2026-04-28", "day": {"maxtemp_f": 86.0, "maxtemp_c": 30.0}},
                    ]
                }
            }
        )

    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_ENABLE_TEMPERATURE", True)
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_CACHE_TTL_SECONDS", 3600.0)
    monkeypatch.setattr(forecast_engine, "_weatherapi_daily_cache", {})
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_weatherapi_forecast_max_temp("miami", _FakeDate(2026, 4, 27))
    second = forecast_engine.get_weatherapi_forecast_max_temp("miami", _FakeDate(2026, 4, 28))

    assert first == 85.0
    assert second == 86.0
    assert len(calls) == 1


def test_noaa_temperature_forecast_reuses_persistent_cache_after_memory_reset(monkeypatch):
    calls: list[str] = []

    def fake_get(url: str, *, headers: dict, timeout: int):
        calls.append(url)
        if "/points/" in url:
            return _FakeResponse(payload={"properties": {"forecast": "https://api.weather.gov/gridpoints/TEST/1,1/forecast"}})
        return _FakeResponse(
            payload={
                "properties": {
                    "periods": [
                        {
                            "isDaytime": True,
                            "startTime": "2026-04-27T06:00:00-04:00",
                            "temperature": 85,
                        },
                        {
                            "isDaytime": True,
                            "startTime": "2026-04-28T06:00:00-04:00",
                            "temperature": 86,
                        },
                    ]
                }
            }
        )

    monkeypatch.setattr(forecast_engine, "NOAA_CACHE_TTL_SECONDS", 900.0)
    monkeypatch.setattr(forecast_engine, "_noaa_daily_cache", {})
    monkeypatch.setattr(forecast_engine, "_noaa_grid_cache", {})
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_noaa_forecast_max_temp("miami", date(2026, 4, 27))

    assert first == 85.0
    assert len(calls) == 2

    monkeypatch.setattr(forecast_engine, "_noaa_daily_cache", {})
    monkeypatch.setattr(
        forecast_engine.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("NOAA should have used persistent cache")),
    )

    second = forecast_engine.get_noaa_forecast_max_temp("miami", date(2026, 4, 28))

    assert second == 86.0


def test_visual_crossing_review_temperature_disabled_by_default(monkeypatch):
    def fake_get(url: str, *, params: dict, timeout: int):
        raise AssertionError("Visual Crossing should not be called for review mode by default")

    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_ENABLE_TEMPERATURE", True)
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_ENABLE_REVIEW_TEMPERATURE", False)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    assert forecast_engine.get_visual_crossing_forecast_max_temp(
        "lagos",
        date(2026, 4, 27),
        provider_context="review",
    ) is None


def test_temperature_review_context_horizons_short_circuit_provider_calls(monkeypatch):
    def fake_get(url: str, *args, **kwargs):
        raise AssertionError("Provider request should have been skipped by the review horizon")

    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "WU_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "WEATHERAPI_ENABLE_TEMPERATURE", True)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    assert forecast_engine.get_wu_forecast_max_temp("miami", _FakeDate(2026, 5, 2), provider_context="review") is None
    assert forecast_engine.get_openmeteo_forecast_max_temp("lagos", _FakeDate(2026, 5, 5), provider_context="review") is None
    assert forecast_engine.get_noaa_forecast_max_temp("miami", _FakeDate(2026, 5, 5), provider_context="review") is None
    assert forecast_engine.get_weatherapi_forecast_max_temp("lagos", _FakeDate(2026, 4, 29), provider_context="review") is None


def test_get_both_bucket_probabilities_logs_compact_provider_status(monkeypatch, capsys):
    monkeypatch.setattr(forecast_engine, "date", _FakeDate)
    monkeypatch.setattr(forecast_engine, "WU_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine, "_visual_crossing_auth_failed", True)
    monkeypatch.setattr(forecast_engine, "_openmeteo_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "get_wu_forecast_max_temp", lambda city_slug, target_date: 85.0)
    monkeypatch.setattr(forecast_engine, "get_openmeteo_forecast_max_temp", lambda city_slug, target_date: None)
    monkeypatch.setattr(forecast_engine, "get_visual_crossing_forecast_max_temp", lambda city_slug, target_date: None)
    monkeypatch.setattr(forecast_engine, "get_noaa_forecast_max_temp", lambda city_slug, target_date: 84.0)

    forecast_engine.get_both_bucket_probabilities(
        "miami",
        _FakeDate(2026, 4, 27),
        [{"label": "84-85°F", "low": 84, "high": 85}],
    )

    output = capsys.readouterr().out

    assert "Sources:" in output
    assert "WU(KMIA) 85.0" in output
    assert "OM unavailable" in output
    assert "VC auth-disabled" in output
    assert "NOAA 84.0" in output


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
    monkeypatch.setattr(forecast_engine, "_visual_crossing_daily_cache", {})
    monkeypatch.setattr(forecast_engine, "_visual_crossing_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = forecast_engine.get_visual_crossing_forecast_max_temp("lagos", date(2026, 4, 27))
    second = forecast_engine.get_visual_crossing_forecast_max_temp("jakarta", date(2026, 4, 27))
    output = capsys.readouterr().out

    assert first is None
    assert second is None
    assert len(calls) == 1
    assert api_key not in output
    assert "disabling VC for this run" in output


def test_visual_crossing_precipitation_reuses_cached_month(monkeypatch):
    calls: list[tuple[str, dict]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params)))
        return _FakeResponse(
            payload={
                "days": [
                    {"datetime": "2026-04-01", "precip": 0.4},
                    {"datetime": "2026-04-02", "precip": 0.3},
                ]
            }
        )

    class _PrecipDate(date):
        @classmethod
        def today(cls) -> "_PrecipDate":
            return cls(2026, 4, 2)

    monkeypatch.setattr(precip_forecast, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(precip_forecast, "VISUAL_CROSSING_PRECIP_CACHE_TTL_SECONDS", 43200.0)
    monkeypatch.setattr(precip_forecast, "_visual_crossing_precip_cache", {})
    monkeypatch.setattr(precip_forecast, "date", _PrecipDate)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_gate", threading.BoundedSemaphore(1))
    monkeypatch.setattr(forecast_engine, "_visual_crossing_disabled_until_monotonic", 0.0)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_cooldown_notice_sent", False)
    monkeypatch.setattr(forecast_engine, "_visual_crossing_auth_failed", False)
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    first = precip_forecast.get_vc_monthly_precip("lagos", 2026, 4)
    second = precip_forecast.get_vc_monthly_precip("lagos", 2026, 4)

    assert first is not None
    assert second == first
    assert len(calls) == 1


def test_visual_crossing_precipitation_can_be_disabled_by_config(monkeypatch):
    def fake_get(url: str, *, params: dict, timeout: int):
        raise AssertionError("Visual Crossing should be skipped when precip is disabled")

    monkeypatch.setattr(precip_forecast, "VISUAL_CROSSING_ENABLE_PRECIP", False)
    monkeypatch.setattr(precip_forecast, "VISUAL_CROSSING_API_KEY", "test-key")
    monkeypatch.setattr(forecast_engine.requests, "get", fake_get)

    assert precip_forecast.get_vc_monthly_precip("lagos", 2026, 4) is None
