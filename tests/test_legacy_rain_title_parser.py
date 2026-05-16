from __future__ import annotations

from datetime import datetime

import parser.title_parser as title_parser


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 5, 15, 10, 30, 0)
        if tz is not None:
            return value.replace(tzinfo=tz)
        return value


def test_parse_rain_title_extracts_month_day_and_location(monkeypatch):
    monkeypatch.setattr(title_parser, "datetime", FrozenDateTime)

    parsed = title_parser.parse_rain_title("Will it rain in New York City on April 14?")

    assert parsed is not None
    assert parsed["location"] == "new york city"
    assert parsed["window_start_local"] == datetime(2026, 4, 14, 0, 0, 0)
    assert parsed["window_end_local"] == datetime(2026, 4, 14, 23, 59, 59)


def test_parse_rain_title_extracts_tomorrow_location(monkeypatch):
    monkeypatch.setattr(title_parser, "datetime", FrozenDateTime)

    parsed = title_parser.parse_rain_title("Will it rain at JFK Airport tomorrow?")

    assert parsed is not None
    assert parsed["location"] == "jfk airport"
    assert parsed["window_start_local"] == datetime(2026, 5, 16, 0, 0, 0)


def test_parse_rain_title_rejects_missing_date(monkeypatch):
    monkeypatch.setattr(title_parser, "datetime", FrozenDateTime)

    assert title_parser.parse_rain_title("Will there be significant rainfall in NYC soon?") is None
