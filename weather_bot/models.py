"""Normalized weather-domain models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


@dataclass(frozen=True)
class ForecastSnapshot:
    market_type: str
    city_slug: str
    event_date: str
    unit: str
    observed_value: float | None = None
    wu_temp: float | None = None
    om_temp: float | None = None
    vc_temp: float | None = None
    noaa_temp: float | None = None
    source_probabilities: dict[str, float | None] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class WeatherSignal:
    signal_key: str
    market_type: str
    event_title: str
    market_slug: str
    event_slug: str
    city_slug: str
    event_date: str
    label: str
    direction: str
    market_prob: float
    forecast_prob: float
    edge: float
    edge_abs: float
    edge_size: str
    confidence: str
    source_count: int
    liquidity: float
    time_to_resolution_s: float | None
    source_dispersion_pct: float
    score: float
    forecast_snapshot: ForecastSnapshot
    raw_payload: dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=iso_now)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["forecast_snapshot"] = self.forecast_snapshot.to_dict()
        return payload


@dataclass(frozen=True)
class WeatherDecision:
    signal_key: str
    accepted: bool
    reason: str
    final_score: float
    policy_action: str
    created_at: str = field(default_factory=iso_now)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PaperPosition:
    id: int
    signal_key: str
    market_type: str
    market_slug: str
    event_slug: str
    city_slug: str
    event_date: str
    label: str
    direction: str
    score: float
    entry_price: float
    shares: float
    cost: float
    status: str
    resolution: str | None = None
    realized_pnl: float | None = None
    created_at: str = field(default_factory=iso_now)
    resolved_at: str | None = None
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResolutionOutcome:
    market_slug: str
    resolution: str
    resolved_positions: int
    total_realized_pnl: float
    resolved_at: str = field(default_factory=iso_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ScanBatch:
    scan_type: str
    signals: list[WeatherSignal]
    total_events: int
    processed_events: int
    flagged_events: int
    skipped_events: int
    started_at: str
    finished_at: str
    error_count: int = 0
    error_samples: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scan_type": self.scan_type,
            "signals": [signal.to_dict() for signal in self.signals],
            "total_events": self.total_events,
            "processed_events": self.processed_events,
            "flagged_events": self.flagged_events,
            "skipped_events": self.skipped_events,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "error_count": self.error_count,
            "error_samples": list(self.error_samples),
        }
