"""Execution telemetry models used by shadow-live scaffolding."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from ..models import iso_now


@dataclass(frozen=True)
class ShadowOrderIntent:
    intent_kind: str
    execution_mode: str
    signal_key: str
    market_type: str
    market_slug: str
    event_slug: str
    city_slug: str
    event_date: str
    label: str
    direction: str
    order_action: str
    outcome_side: str
    order_intent: str
    order_type: str
    time_in_force: str
    manual_order_indicator: str
    target_price: float
    reference_price: float
    shares: float
    notional_usd: float
    estimated_fee_paid: float
    decision_final_score: float | None = None
    clob_token_id: str | None = None
    book_best_bid: float | None = None
    book_best_ask: float | None = None
    book_spread: float | None = None
    book_midpoint: float | None = None
    book_depth_at_target_shares: float | None = None
    book_depth_at_target_usd: float | None = None
    simulated_fill_status: str = "not_checked"
    simulated_fill_shares: float | None = None
    simulated_avg_fill_price: float | None = None
    simulated_notional_usd: float | None = None
    simulated_unfilled_shares: float | None = None
    simulated_slippage_bps: float | None = None
    execution_checked_at: str | None = None
    execution_error: str | None = None
    reason: str = ""
    reason_code: str | None = None
    signal_id: int | None = None
    decision_id: int | None = None
    position_id: int | None = None
    status: str = "planned"
    created_at: str = field(default_factory=iso_now)
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
