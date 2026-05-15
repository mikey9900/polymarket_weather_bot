from __future__ import annotations

import json
from datetime import datetime, timezone

from scanner import weather_event_scanner as scanner


def test_missing_event_cache_keeps_only_past_slugs():
    today = datetime(2026, 5, 15, tzinfo=timezone.utc).date()

    filtered = scanner._filter_cache_slugs(
        [
            "highest-temperature-in-nyc-on-may-14-2026",
            "highest-temperature-in-nyc-on-may-15-2026",
            "highest-temperature-in-nyc-on-may-16-2026",
            "legacy-non-weather-slug",
        ],
        today,
    )

    assert "highest-temperature-in-nyc-on-may-14-2026" in filtered
    assert "highest-temperature-in-nyc-on-may-15-2026" not in filtered
    assert "highest-temperature-in-nyc-on-may-16-2026" not in filtered
    assert "legacy-non-weather-slug" in filtered


def test_future_missing_events_are_rechecked_instead_of_cached(tmp_path, monkeypatch):
    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 5, 15, 12, 0, tzinfo=timezone.utc)

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return []

    cache_file = tmp_path / "seen_events.json"
    monkeypatch.setattr(scanner, "CACHE_FILE", str(cache_file))
    monkeypatch.setattr(scanner, "NORTH_AMERICA_CITIES", ["nyc"])
    monkeypatch.setattr(scanner, "datetime", FakeDatetime)
    monkeypatch.setattr(scanner.requests, "get", lambda *args, **kwargs: FakeResponse())

    assert scanner.fetch_weather_events(market_scope="north_america") == []

    cached = json.loads(cache_file.read_text(encoding="utf-8"))
    assert "highest-temperature-in-nyc-on-may-14-2026" in cached
    assert "highest-temperature-in-nyc-on-may-15-2026" not in cached
    assert "highest-temperature-in-nyc-on-may-16-2026" not in cached
