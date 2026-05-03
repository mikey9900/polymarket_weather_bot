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
