from __future__ import annotations

from weather_bot.execution.models import ShadowOrderIntent
from weather_bot.execution.shadow_fill import enrich_shadow_intent_with_fill_rehearsal
from parser.weather_parser import parse_temperature_buckets_for_event


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _intent(**overrides) -> ShadowOrderIntent:
    values = {
        "intent_kind": "entry",
        "execution_mode": "paper_shadow",
        "signal_key": "sig-1",
        "market_type": "temperature",
        "market_slug": "market-1",
        "event_slug": "event-1",
        "city_slug": "nyc",
        "event_date": "2026-05-07",
        "label": "70-71F",
        "direction": "YES",
        "order_action": "BUY",
        "outcome_side": "YES",
        "order_intent": "BUY_LONG",
        "order_type": "LIMIT",
        "time_in_force": "GTC",
        "manual_order_indicator": "AUTOMATIC",
        "target_price": 0.50,
        "reference_price": 0.45,
        "shares": 100.0,
        "notional_usd": 45.0,
        "estimated_fee_paid": 0.0,
        "payload": {"clob_token_ids": ["yes-token-fill", "no-token-fill"]},
    }
    values.update(overrides)
    return ShadowOrderIntent(**values)


def test_shadow_fill_rehearsal_records_full_fill_against_book(monkeypatch):
    def fake_get(url, params, timeout):
        assert url == "https://clob.polymarket.com/book"
        assert params == {"token_id": "yes-token-fill"}
        return _FakeResponse(
            {
                "asset_id": "yes-token-fill",
                "timestamp": "2026-05-07T05:00:00Z",
                "bids": [{"price": "0.44", "size": "25"}],
                "asks": [
                    {"price": "0.45", "size": "60"},
                    {"price": "0.50", "size": "50"},
                    {"price": "0.55", "size": "100"},
                ],
                "min_order_size": "1",
                "tick_size": "0.01",
            }
        )

    monkeypatch.setattr("weather_bot.execution.shadow_fill.requests.get", fake_get)

    enriched = enrich_shadow_intent_with_fill_rehearsal(_intent())

    assert enriched.clob_token_id == "yes-token-fill"
    assert enriched.simulated_fill_status == "full_fill"
    assert enriched.simulated_fill_shares == 100.0
    assert enriched.simulated_avg_fill_price == 0.47
    assert enriched.simulated_unfilled_shares == 0.0
    assert enriched.book_best_bid == 0.44
    assert enriched.book_best_ask == 0.45
    assert enriched.book_depth_at_target_shares == 110.0
    assert enriched.payload["shadow_fill_rehearsal"]["top_levels"]["asks"][0]["price"] == 0.45


def test_shadow_fill_rehearsal_marks_missing_token_without_fetch(monkeypatch):
    def fail_get(*_args, **_kwargs):
        raise AssertionError("missing-token intents should not call CLOB")

    monkeypatch.setattr("weather_bot.execution.shadow_fill.requests.get", fail_get)

    enriched = enrich_shadow_intent_with_fill_rehearsal(_intent(payload={}))

    assert enriched.simulated_fill_status == "missing_token_id"
    assert enriched.execution_error


def test_temperature_parser_preserves_clob_token_ids_for_shadow_execution():
    buckets = parse_temperature_buckets_for_event(
        [
            {
                "question": "Highest temperature in NYC on May 7?",
                "groupItemTitle": "70-71F",
                "groupItemThreshold": "70",
                "slug": "nyc-high-70-71",
                "outcomePrices": '["0.42", "0.58"]',
                "liquidityNum": "250",
                "clobTokenIds": '["yes-token-parser", "no-token-parser"]',
            }
        ]
    )

    assert len(buckets) == 1
    assert buckets[0]["clob_token_ids"] == ["yes-token-parser", "no-token-parser"]
    assert buckets[0]["yes_token_id"] == "yes-token-parser"
    assert buckets[0]["no_token_id"] == "no-token-parser"
