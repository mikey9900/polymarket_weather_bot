from __future__ import annotations

from polymarket import polymarket_prices


class _FakeResponse:
    def __init__(self, *, status_code: int = 200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else []

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise polymarket_prices.requests.HTTPError(f"{self.status_code} Client Error", response=self)

    def json(self):
        return self._payload


def test_get_yes_price_fetches_market_by_slug_query(monkeypatch):
    calls: list[tuple[str, dict, int]] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append((url, dict(params), timeout))
        return _FakeResponse(payload=[{"outcomePrices": ["0.42", "0.58"]}])

    monkeypatch.setattr(polymarket_prices, "_price_cache", {})
    monkeypatch.setattr(polymarket_prices.requests, "get", fake_get)

    value = polymarket_prices.get_yes_price(
        "https://polymarket.com/event/highest-temperature-in-miami-on-april-26-2026-84-85f"
    )

    assert value == 0.42
    assert calls == [
        (
            "https://gamma-api.polymarket.com/markets",
            {"slug": "highest-temperature-in-miami-on-april-26-2026-84-85f"},
            10,
        )
    ]


def test_get_yes_price_reuses_recent_cache(monkeypatch):
    calls: list[str] = []

    def fake_get(url: str, *, params: dict, timeout: int):
        calls.append(str(params["slug"]))
        return _FakeResponse(payload=[{"outcomePrices": "[\"0.33\", \"0.67\"]"}])

    monkeypatch.setattr(polymarket_prices, "PRICE_CACHE_TTL_SECONDS", 60.0)
    monkeypatch.setattr(polymarket_prices, "_price_cache", {})
    monkeypatch.setattr(polymarket_prices.requests, "get", fake_get)

    first = polymarket_prices.get_yes_price("highest-temperature-in-denver-on-april-27-2026-56-57f")
    second = polymarket_prices.get_yes_price("highest-temperature-in-denver-on-april-27-2026-56-57f")

    assert first == 0.33
    assert second == 0.33
    assert calls == ["highest-temperature-in-denver-on-april-27-2026-56-57f"]
