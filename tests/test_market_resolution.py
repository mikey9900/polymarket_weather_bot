from __future__ import annotations

from weather_bot import runtime


class _FakeResponse:
    def __init__(self, payload, *, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


def test_market_resolution_uses_slug_endpoint_and_outcome_prices(monkeypatch):
    calls: list[str] = []

    def fake_get(url, *, timeout):
        calls.append(url)
        return _FakeResponse(
            {
                "closed": True,
                "resolutionPrice": None,
                "umaResolutionStatus": "resolved",
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["0", "1"]',
            }
        )

    monkeypatch.setattr(runtime.requests, "get", fake_get)

    assert runtime.get_market_resolution("highest-temperature-in-denver-on-may-10-2026-66-67f") == "NO"
    assert calls == ["https://gamma-api.polymarket.com/markets/slug/highest-temperature-in-denver-on-may-10-2026-66-67f"]


def test_market_resolution_waits_until_market_is_closed(monkeypatch):
    def fake_get(url, *, timeout):
        return _FakeResponse(
            {
                "closed": False,
                "outcomes": '["Yes", "No"]',
                "outcomePrices": '["1", "0"]',
            }
        )

    monkeypatch.setattr(runtime.requests, "get", fake_get)

    assert runtime.get_market_resolution("open-market") is None
