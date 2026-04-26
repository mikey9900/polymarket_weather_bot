from __future__ import annotations

import pytest

from weather_bot import persistent_weather_cache


@pytest.fixture(autouse=True)
def _isolated_weather_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "weather_cache.db"
    persistent_weather_cache.close_weather_cache()
    monkeypatch.setattr(persistent_weather_cache, "CACHE_DB_PATH", cache_path)
    yield
    persistent_weather_cache.close_weather_cache()
