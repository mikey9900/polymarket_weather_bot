from __future__ import annotations

from datetime import datetime, timedelta, timezone

from weather_bot.execution.models import ShadowOrderIntent
from weather_bot.execution.shadow_execution import ShadowExecutionEngine, ShadowExecutionRuntimeConfig
from weather_bot.models import ForecastSnapshot, WeatherDecision, WeatherSignal
from weather_bot.tracker import WeatherTracker


def _intent(**overrides) -> ShadowOrderIntent:
    values = {
        "intent_kind": "entry",
        "execution_mode": "paper_shadow",
        "signal_key": "shadow-exec-1",
        "market_type": "temperature",
        "market_slug": "market-shadow-exec-1",
        "event_slug": "event-shadow-exec-1",
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
        "clob_token_id": "yes-token-shadow-exec",
        "simulated_fill_status": "no_fill",
        "payload": {"clob_token_ids": ["yes-token-shadow-exec", "no-token-shadow-exec"]},
        "created_at": "2026-05-07T12:00:00+00:00",
    }
    values.update(overrides)
    return ShadowOrderIntent(**values)


def _engine(tracker: WeatherTracker, *, config: ShadowExecutionRuntimeConfig | None = None, book_fetcher=None) -> ShadowExecutionEngine:
    return ShadowExecutionEngine(
        tracker=tracker,
        config=config
        or ShadowExecutionRuntimeConfig(
            enabled=True,
            entry_ttl_seconds=1800,
            exit_ttl_seconds=300,
            queue_fill_fraction=0.5,
            rest_fallback_seconds=5,
            show_taker_exit_estimate=False,
        ),
        book_fetcher=book_fetcher or (lambda _token: None),
    )


def _paper_position_id(tracker: WeatherTracker) -> int:
    signal = WeatherSignal(
        signal_key="shadow-exec-paper",
        market_type="temperature",
        event_title="Highest temperature in NYC on May 7",
        market_slug="market-shadow-exec-paper",
        event_slug="event-shadow-exec-paper",
        city_slug="nyc",
        event_date="2026-05-07",
        label="70-71F",
        direction="YES",
        market_prob=0.40,
        forecast_prob=0.80,
        edge=0.40,
        edge_abs=0.40,
        edge_size="large",
        confidence="confirmed",
        source_count=3,
        liquidity=500.0,
        time_to_resolution_s=8 * 3600.0,
        source_dispersion_pct=0.02,
        score=0.84,
        forecast_snapshot=ForecastSnapshot(
            market_type="temperature",
            city_slug="nyc",
            event_date="2026-05-07",
            unit="F",
        ),
        raw_payload={"clob_token_ids": ["yes-token-shadow-exec", "no-token-shadow-exec"]},
        created_at="2026-05-07T12:00:00+00:00",
    )
    signal_id = tracker.log_signal(signal)
    decision_id = tracker.log_decision(
        signal_id,
        WeatherDecision(
            signal_key=signal.signal_key,
            accepted=True,
            reason="test",
            final_score=0.84,
            policy_action="paper_trade",
        ),
    )
    tracker.ensure_paper_capital(1000.0)
    position = tracker.create_paper_position(
        signal_id=signal_id,
        decision_id=decision_id,
        signal=signal,
        stake_usd=10.0,
        decision_final_score=0.84,
    )
    return int(position.id)


def test_immediate_rehearsal_full_fill_opens_realistic_shadow_position(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    intent = _intent(
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.45,
        simulated_notional_usd=45.0,
        simulated_unfilled_shares=0.0,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    intent_id = tracker.record_shadow_order_intent(intent)

    order_id = engine.mirror_intent(intent_id, intent)

    order = tracker.get_shadow_exec_order(order_id)
    positions = tracker.get_shadow_exec_positions(limit=None)
    fills = tracker.get_recent_shadow_exec_fills(limit=None)
    assert order["status"] == "filled"
    assert order["filled_shares"] == 100.0
    assert len(positions) == 1
    assert positions[0]["status"] == "open"
    assert positions[0]["open_shares"] == 100.0
    assert positions[0]["avg_entry_price"] == 0.45
    assert fills[0]["liquidity_source"] == "immediate_book"


def test_resting_trade_fill_uses_trade_side_price_and_queue_haircut(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    intent = _intent()
    intent_id = tracker.record_shadow_order_intent(intent)
    engine.mirror_intent(intent_id, intent)

    wrong_side = engine.apply_trade_event(
        clob_token_id="yes-token-shadow-exec",
        side="BUY",
        price=0.49,
        size=80.0,
        traded_at="2026-05-07T12:05:00+00:00",
    )
    fills = engine.apply_trade_event(
        clob_token_id="yes-token-shadow-exec",
        side="SELL",
        price=0.49,
        size=80.0,
        traded_at="2026-05-07T12:06:00+00:00",
    )

    positions = tracker.get_shadow_exec_positions(limit=None)
    assert wrong_side == []
    assert len(fills) == 1
    assert fills[0]["shares"] == 40.0
    assert positions[0]["open_shares"] == 40.0
    assert positions[0]["avg_entry_price"] == 0.49


def test_market_channel_last_trade_price_message_fills_resting_order(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    intent = _intent()
    intent_id = tracker.record_shadow_order_intent(intent)
    engine.mirror_intent(intent_id, intent)

    fills = engine.apply_market_channel_message(
        {
            "event_type": "last_trade_price",
            "asset_id": "yes-token-shadow-exec",
            "side": "SELL",
            "price": "0.50",
            "size": "50",
            "timestamp": "1778155500000",
            "market": "0xmarket",
        }
    )

    assert len(fills) == 1
    assert fills[0]["shares"] == 25.0
    assert fills[0]["evidence"]["event_type"] == "last_trade_price"


def test_shadow_entry_order_expires_after_ttl_when_unfilled(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    intent = _intent()
    intent_id = tracker.record_shadow_order_intent(intent)
    order_id = engine.mirror_intent(intent_id, intent)
    expiry = datetime.fromisoformat("2026-05-07T12:00:00+00:00") + timedelta(seconds=1801)

    expired = tracker.expire_shadow_exec_orders(now=expiry.astimezone(timezone.utc).isoformat())

    order = tracker.get_shadow_exec_order(order_id)
    assert expired == 1
    assert order["status"] == "expired"
    assert tracker.get_shadow_exec_positions(limit=None) == []


def test_exit_order_closes_realistic_position_without_reopening_paper(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    position_id = _paper_position_id(tracker)
    entry = _intent(
        position_id=position_id,
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.40,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    entry_id = tracker.record_shadow_order_intent(entry)
    engine.mirror_intent(entry_id, entry)
    exit_intent = _intent(
        position_id=position_id,
        intent_kind="exit",
        order_action="SELL",
        order_intent="SELL_LONG",
        time_in_force="IOC",
        target_price=0.55,
        reference_price=0.55,
        shares=100.0,
        simulated_fill_status="no_fill",
        created_at="2026-05-07T12:10:00+00:00",
    )
    exit_id = tracker.record_shadow_order_intent(exit_intent)
    engine.mirror_intent(exit_id, exit_intent)

    fills = engine.apply_trade_event(
        clob_token_id="yes-token-shadow-exec",
        side="BUY",
        price=0.56,
        size=300.0,
        traded_at="2026-05-07T12:11:00+00:00",
    )

    positions = tracker.get_shadow_exec_positions(limit=None)
    assert fills
    assert positions[0]["status"] == "closed"
    assert positions[0]["realized_pnl"] == 16.0
    assert positions[0]["total_pnl"] == 16.0


def test_exit_without_realistic_entry_is_marked_no_position(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    position_id = _paper_position_id(tracker)
    exit_intent = _intent(
        position_id=position_id,
        intent_kind="exit",
        order_action="SELL",
        order_intent="SELL_LONG",
        target_price=0.50,
        shares=100.0,
    )
    exit_id = tracker.record_shadow_order_intent(exit_intent)

    order_id = engine.mirror_intent(exit_id, exit_intent)

    order = tracker.get_shadow_exec_order(order_id)
    assert order["status"] == "no_position"
    assert "No realistic shadow position" in order["status_reason"]


def test_shadow_cycle_requeues_exit_for_stuck_open_shadow_position(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(
        tracker,
        book_fetcher=lambda _token: {
            "hash": "empty-exit-book",
            "timestamp": "2026-05-07T12:30:00+00:00",
            "bids": [],
            "asks": [],
        },
    )
    position_id = _paper_position_id(tracker)
    entry = _intent(
        position_id=position_id,
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.40,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    entry_id = tracker.record_shadow_order_intent(entry)
    engine.mirror_intent(entry_id, entry)
    tracker.conn.execute(
        """
        UPDATE paper_positions
        SET exit_reason = ?, mark_price = ?, mark_final_score = ?, mark_updated_at = ?
        WHERE id = ?
        """,
        ("Remaining model edge is at or below the near-fair threshold.", 0.42, 0.5, "2026-05-07T12:30:00+00:00", position_id),
    )
    tracker.conn.commit()

    summary = engine.run_cycle()
    exit_orders = [order for order in tracker.get_recent_shadow_exec_orders(limit=None) if order["intent_kind"] == "exit"]

    assert summary["exit_retries_queued"] == 1
    assert len(exit_orders) == 1
    assert exit_orders[0]["status"] == "resting"
    assert exit_orders[0]["target_price"] == 0.42
    assert exit_orders[0]["payload"]["shadow_intent"]["payload"]["shadow_execution_retry"]["shadow_position_id"]

    second_summary = engine.run_cycle()
    exit_orders = [order for order in tracker.get_recent_shadow_exec_orders(limit=None) if order["intent_kind"] == "exit"]
    assert second_summary["exit_retries_queued"] == 0
    assert len(exit_orders) == 1


def test_shadow_cycle_requeues_after_retry_exit_expires(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(
        tracker,
        config=ShadowExecutionRuntimeConfig(
            enabled=True,
            entry_ttl_seconds=1800,
            exit_ttl_seconds=300,
            queue_fill_fraction=0.5,
            rest_fallback_seconds=5,
            show_taker_exit_estimate=False,
        ),
        book_fetcher=lambda _token: {
            "hash": "empty-exit-book",
            "timestamp": "2026-05-07T12:30:00+00:00",
            "bids": [],
            "asks": [],
        },
    )
    position_id = _paper_position_id(tracker)
    entry = _intent(
        position_id=position_id,
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.40,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    entry_id = tracker.record_shadow_order_intent(entry)
    engine.mirror_intent(entry_id, entry)
    tracker.conn.execute(
        """
        UPDATE paper_positions
        SET exit_reason = ?, mark_price = ?, mark_updated_at = ?
        WHERE id = ?
        """,
        ("Remaining model edge is at or below the near-fair threshold.", 0.42, "2026-05-07T12:30:00+00:00", position_id),
    )
    tracker.conn.commit()

    assert engine.run_cycle()["exit_retries_queued"] == 1
    first_exit = [order for order in tracker.get_recent_shadow_exec_orders(limit=None) if order["intent_kind"] == "exit"][0]
    expires_after = datetime.fromisoformat(first_exit["expires_at"]) + timedelta(seconds=1)
    tracker.expire_shadow_exec_orders(now=expires_after.astimezone(timezone.utc).isoformat())

    assert engine.run_cycle()["exit_retries_queued"] == 1
    exit_orders = [order for order in tracker.get_recent_shadow_exec_orders(limit=None) if order["intent_kind"] == "exit"]
    assert len(exit_orders) == 2
    assert exit_orders[0]["status"] == "resting"
    assert exit_orders[1]["status"] == "expired"


def test_resolved_market_settles_open_shadow_position_instead_of_retrying(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(
        tracker,
        book_fetcher=lambda _token: {
            "hash": "empty-resolved-book",
            "timestamp": "2026-05-07T12:30:00+00:00",
            "bids": [],
            "asks": [],
        },
    )
    position_id = _paper_position_id(tracker)
    market_slug = "market-shadow-exec-paper"
    event_slug = "event-shadow-exec-paper"
    entry = _intent(
        position_id=position_id,
        market_slug=market_slug,
        event_slug=event_slug,
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.40,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    entry_id = tracker.record_shadow_order_intent(entry)
    engine.mirror_intent(entry_id, entry)
    exit_intent = _intent(
        intent_kind="exit",
        position_id=position_id,
        market_slug=market_slug,
        event_slug=event_slug,
        order_action="SELL",
        order_intent="SELL_TO_CLOSE",
        target_price=0.42,
        reference_price=0.42,
        shares=100.0,
        notional_usd=42.0,
        simulated_fill_status="no_fill",
        created_at="2026-05-07T12:30:00+00:00",
    )
    exit_id = tracker.record_shadow_order_intent(exit_intent)
    exit_order_id = engine.mirror_intent(exit_id, exit_intent)

    outcome = tracker.settle_market(market_slug, "YES")

    exit_order = tracker.get_shadow_exec_order(exit_order_id)
    positions = tracker.get_shadow_exec_positions(limit=None)
    paper = tracker.conn.execute("SELECT * FROM paper_positions WHERE id = ?", (position_id,)).fetchone()
    assert outcome.resolved_positions == 1
    assert exit_order["status"] == "resolved"
    assert positions[0]["status"] == "closed"
    assert positions[0]["open_shares"] == 0.0
    assert positions[0]["mark_price"] == 1.0
    assert positions[0]["realized_pnl"] == 60.0
    assert paper["status"] == "resolved"
    assert paper["exit_reason"] == "resolved:YES"


def test_shadow_only_resolved_market_still_settles(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    entry = _intent(
        simulated_fill_status="full_fill",
        simulated_fill_shares=50.0,
        simulated_avg_fill_price=0.30,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    entry_id = tracker.record_shadow_order_intent(entry)
    engine.mirror_intent(entry_id, entry)

    outcome = tracker.settle_market("market-shadow-exec-1", "NO")

    positions = tracker.get_shadow_exec_positions(limit=None)
    assert outcome.resolved_positions == 0
    assert positions[0]["status"] == "closed"
    assert positions[0]["mark_price"] == 0.0
    assert positions[0]["realized_pnl"] == -15.0


def test_shadow_settlement_does_not_double_count_capital_for_already_resolved_paper(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(tracker)
    position_id = _paper_position_id(tracker)
    market_slug = "market-shadow-exec-paper"
    event_slug = "event-shadow-exec-paper"
    entry = _intent(
        position_id=position_id,
        market_slug=market_slug,
        event_slug=event_slug,
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.40,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    engine.mirror_intent(tracker.record_shadow_order_intent(entry), entry)
    partial_exit = _intent(
        intent_kind="exit",
        position_id=position_id,
        market_slug=market_slug,
        event_slug=event_slug,
        order_action="SELL",
        order_intent="SELL_TO_CLOSE",
        target_price=0.50,
        reference_price=0.50,
        shares=100.0,
        notional_usd=50.0,
        simulated_fill_status="partial_fill",
        simulated_fill_shares=50.0,
        simulated_avg_fill_price=0.50,
        execution_checked_at="2026-05-07T12:30:00+00:00",
        created_at="2026-05-07T12:30:00+00:00",
    )
    engine.mirror_intent(tracker.record_shadow_order_intent(partial_exit), partial_exit)
    tracker.conn.execute(
        """
        UPDATE paper_positions
        SET status = 'resolved',
            resolution = 'YES',
            realized_pnl = 30.0,
            exit_reason = 'resolved:YES',
            resolved_at = '2026-05-07T18:00:00+00:00'
        WHERE id = ?
        """,
        (position_id,),
    )
    tracker.set_setting("paper_capital", {"initial": 1000.0, "available": 1035.0})
    tracker.conn.commit()

    tracker.settle_market(market_slug, "YES")

    _, available = tracker.get_paper_capital()
    paper = tracker.conn.execute("SELECT realized_pnl FROM paper_positions WHERE id = ?", (position_id,)).fetchone()
    positions = tracker.get_shadow_exec_positions(limit=None)
    assert available == 1035.0
    assert paper["realized_pnl"] == 35.0
    assert positions[0]["status"] == "closed"
    assert positions[0]["realized_pnl"] == 35.0


def test_entry_bid_improvement_lifts_only_to_edge_cap_and_resizes_budget(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(
        tracker,
        book_fetcher=lambda _token: {
            "hash": "book-entry-cap",
            "timestamp": "2026-05-07T12:00:00+00:00",
            "asks": [{"price": "0.14", "size": "100"}],
            "bids": [],
        },
    )
    intent = _intent(
        target_price=0.10,
        reference_price=0.10,
        shares=100.0,
        notional_usd=10.0,
        simulated_fill_status="no_fill",
        payload={
            "clob_token_ids": ["yes-token-shadow-exec", "no-token-shadow-exec"],
            "forecast_prob": 0.35,
            "entry_edge_floor": 0.20,
        },
    )
    intent_id = tracker.record_shadow_order_intent(intent)

    order_id = engine.mirror_intent(intent_id, intent)

    order = tracker.get_shadow_exec_order(order_id)
    fills = tracker.get_recent_shadow_exec_fills(limit=None)
    positions = tracker.get_shadow_exec_positions(limit=None)
    assert order["target_price"] == 0.15
    assert order["requested_shares"] == 66.666667
    assert order["status"] == "filled"
    assert fills[0]["price"] == 0.14
    assert positions[0]["avg_entry_price"] == 0.14
    assert positions[0]["open_shares"] == 66.666667
    pricing = order["payload"]["shadow_intent"]["payload"]["shadow_execution_pricing"]
    assert pricing["preserved_edge_abs"] == 0.2


def test_entry_bid_improvement_does_not_chase_past_edge_cap(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(
        tracker,
        book_fetcher=lambda _token: {
            "hash": "book-entry-too-high",
            "timestamp": "2026-05-07T12:00:00+00:00",
            "asks": [{"price": "0.16", "size": "100"}],
            "bids": [],
        },
    )
    intent = _intent(
        target_price=0.10,
        reference_price=0.10,
        shares=100.0,
        notional_usd=10.0,
        simulated_fill_status="no_fill",
        payload={
            "clob_token_ids": ["yes-token-shadow-exec", "no-token-shadow-exec"],
            "forecast_prob": 0.35,
            "entry_edge_floor": 0.20,
        },
    )
    intent_id = tracker.record_shadow_order_intent(intent)

    order_id = engine.mirror_intent(intent_id, intent)

    order = tracker.get_shadow_exec_order(order_id)
    assert order["target_price"] == 0.15
    assert order["status"] == "resting"
    assert tracker.get_shadow_exec_positions(limit=None) == []


def test_exit_sell_ladder_reprices_after_wait_and_fills_buy_trade(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(
        tracker,
        config=ShadowExecutionRuntimeConfig(
            enabled=True,
            entry_ttl_seconds=1800,
            exit_ttl_seconds=300,
            queue_fill_fraction=0.5,
            rest_fallback_seconds=5,
            show_taker_exit_estimate=False,
            exit_ladder_step_seconds=60,
            exit_concession_steps=(0.005, 0.01),
        ),
    )
    position_id = _paper_position_id(tracker)
    entry = _intent(
        position_id=position_id,
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.40,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    entry_id = tracker.record_shadow_order_intent(entry)
    engine.mirror_intent(entry_id, entry)
    exit_intent = _intent(
        position_id=position_id,
        intent_kind="exit",
        order_action="SELL",
        order_intent="SELL_LONG",
        target_price=0.55,
        reference_price=0.55,
        shares=100.0,
        simulated_fill_status="no_fill",
        reason_code="edge_near_fair",
        created_at="2026-05-07T12:10:00+00:00",
    )
    exit_id = tracker.record_shadow_order_intent(exit_intent)
    order_id = engine.mirror_intent(exit_id, exit_intent)

    fills = engine.apply_trade_event(
        clob_token_id="yes-token-shadow-exec",
        side="BUY",
        price=0.545,
        size=300.0,
        traded_at="2026-05-07T12:11:01+00:00",
    )

    order = tracker.get_shadow_exec_order(order_id)
    positions = tracker.get_shadow_exec_positions(limit=None)
    assert fills
    assert order["target_price"] == 0.545
    assert order["payload"]["shadow_execution_repricing"]["current_target_price"] == 0.545
    assert positions[0]["status"] == "closed"
    assert positions[0]["realized_pnl"] == 14.5


def test_urgent_exit_ladder_can_use_wider_concession(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    engine = _engine(
        tracker,
        config=ShadowExecutionRuntimeConfig(
            enabled=True,
            entry_ttl_seconds=1800,
            exit_ttl_seconds=300,
            queue_fill_fraction=0.5,
            rest_fallback_seconds=5,
            show_taker_exit_estimate=False,
            exit_ladder_step_seconds=60,
            exit_concession_steps=(0.005, 0.01),
            exit_urgent_concession_steps=(0.005, 0.01, 0.03),
            exit_urgent_reason_codes=("no_stop_loss",),
        ),
    )
    position_id = _paper_position_id(tracker)
    entry = _intent(
        position_id=position_id,
        simulated_fill_status="full_fill",
        simulated_fill_shares=100.0,
        simulated_avg_fill_price=0.40,
        execution_checked_at="2026-05-07T12:00:01+00:00",
    )
    entry_id = tracker.record_shadow_order_intent(entry)
    engine.mirror_intent(entry_id, entry)
    exit_intent = _intent(
        position_id=position_id,
        intent_kind="exit",
        order_action="SELL",
        order_intent="SELL_LONG",
        target_price=0.55,
        reference_price=0.55,
        shares=100.0,
        simulated_fill_status="no_fill",
        reason_code="no_stop_loss",
        created_at="2026-05-07T12:10:00+00:00",
    )
    exit_id = tracker.record_shadow_order_intent(exit_intent)
    order_id = engine.mirror_intent(exit_id, exit_intent)

    fills = engine.apply_trade_event(
        clob_token_id="yes-token-shadow-exec",
        side="BUY",
        price=0.52,
        size=300.0,
        traded_at="2026-05-07T12:13:01+00:00",
    )

    order = tracker.get_shadow_exec_order(order_id)
    assert fills
    assert order["target_price"] == 0.52
    assert order["payload"]["shadow_execution_repricing"]["events"][-1]["evidence"]["urgent_ladder"] is True
