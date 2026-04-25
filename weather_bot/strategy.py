"""Paper-trading strategy evaluation and execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .config import WeatherBotConfig
from .models import PaperPosition, WeatherDecision, WeatherSignal
from .tracker import WeatherTracker


@dataclass(frozen=True)
class StrategyResult:
    signal: WeatherSignal
    decision: WeatherDecision
    position: PaperPosition | None


class WeatherStrategyEngine:
    def __init__(self, config: WeatherBotConfig, tracker: WeatherTracker, research_provider=None):
        self.config = config
        self.tracker = tracker
        self.research_provider = research_provider

    def process_signals(
        self,
        signals: list[WeatherSignal],
        *,
        auto_trade_enabled: bool,
    ) -> list[StrategyResult]:
        results: list[StrategyResult] = []
        for signal in signals:
            signal_id = self.tracker.log_signal(signal)
            decision = self.evaluate_signal(signal, auto_trade_enabled=auto_trade_enabled)
            decision_id = self.tracker.log_decision(signal_id, decision)
            position = None
            if decision.accepted:
                position = self.tracker.create_paper_position(
                    signal_id=signal_id,
                    decision_id=decision_id,
                    signal=signal,
                    stake_usd=self.config.paper.stake_usd,
                    notes="auto_paper_trade",
                )
                if position is None:
                    decision = WeatherDecision(
                        signal_key=signal.signal_key,
                        accepted=False,
                        reason="Paper capital unavailable.",
                        final_score=decision.final_score,
                        policy_action=decision.policy_action,
                        metadata={**decision.metadata, "capital_blocked": True},
                    )
                    self.tracker.log_decision(signal_id, decision)
            results.append(StrategyResult(signal=signal, decision=decision, position=position))
        return results

    def evaluate_signal(self, signal: WeatherSignal, *, auto_trade_enabled: bool) -> WeatherDecision:
        thresholds = self._thresholds(signal.market_type)
        reasons: list[str] = []
        signal_age_hours = _signal_age_hours(signal)
        component_scores = self._component_scores(signal, thresholds, signal_age_hours)
        final_score = component_scores["composite"]
        policy_action = "advisory"
        metadata = {
            "edge_abs": signal.edge_abs,
            "source_count": signal.source_count,
            "liquidity": signal.liquidity,
            "time_to_resolution_s": signal.time_to_resolution_s,
            "source_age_hours": signal_age_hours,
            "source_dispersion_pct": signal.source_dispersion_pct,
            "component_scores": component_scores,
        }

        if self.research_provider is not None and hasattr(self.research_provider, "adjust_signal"):
            research_result = self.research_provider.adjust_signal(signal)
            if isinstance(research_result, dict):
                final_score += float(research_result.get("score_adjustment", 0.0) or 0.0)
                policy_action = str(research_result.get("policy_action") or policy_action)
                metadata["research"] = research_result

        final_score = round(max(0.0, min(0.99, final_score)), 4)
        metadata["final_score"] = final_score

        if policy_action == "paper_blocked":
            reasons.append("Runtime policy blocked this signal.")
        if not auto_trade_enabled:
            reasons.append("Automatic paper trading is disabled.")
        if not self.config.paper.enabled:
            reasons.append("Paper trading is disabled in config.")
        if final_score < thresholds.min_score:
            reasons.append(f"Final score {final_score:.2f} below minimum {thresholds.min_score:.2f}.")
        if signal.edge_abs < thresholds.min_edge_abs:
            reasons.append(f"Edge {signal.edge_abs:.2%} below minimum {thresholds.min_edge_abs:.2%}.")
        if signal.source_count < thresholds.min_source_count:
            reasons.append(f"Only {signal.source_count} sources agree.")
        if signal.liquidity < thresholds.min_liquidity:
            reasons.append(f"Liquidity ${signal.liquidity:.0f} below minimum ${thresholds.min_liquidity:.0f}.")
        if signal_age_hours is not None and signal_age_hours > thresholds.max_source_age_hours:
            reasons.append(f"Signal is stale ({signal_age_hours:.1f}h old).")
        if signal.source_dispersion_pct > thresholds.max_source_dispersion_pct:
            reasons.append(
                f"Source dispersion {signal.source_dispersion_pct:.1%} exceeds {thresholds.max_source_dispersion_pct:.1%}."
            )
        if signal.time_to_resolution_s is not None:
            hours = signal.time_to_resolution_s / 3600.0
            if hours < thresholds.min_hours_to_event:
                reasons.append(f"Too close to resolution ({hours:.1f}h).")
            if hours > thresholds.max_hours_to_event:
                reasons.append(f"Too far from resolution ({hours:.1f}h).")
        if self.tracker.count_open_positions() >= self.config.paper.max_open_positions:
            reasons.append("Maximum open paper positions reached.")
        if self.tracker.count_open_positions_for_market(signal.market_slug) >= self.config.paper.max_positions_per_market:
            reasons.append("Maximum paper positions reached for this market.")
        if self.tracker.has_open_position(signal.market_slug, signal.direction):
            reasons.append("A matching open paper position already exists.")

        accepted = not reasons
        return WeatherDecision(
            signal_key=signal.signal_key,
            accepted=accepted,
            reason="Accepted for paper trade." if accepted else " ".join(reasons),
            final_score=final_score,
            policy_action="paper_trade_candidate" if accepted and policy_action == "advisory" else policy_action,
            metadata=metadata,
        )

    def _thresholds(self, market_type: str):
        if market_type == "precipitation":
            return self.config.strategy.precipitation
        return self.config.strategy.temperature

    def _component_scores(self, signal: WeatherSignal, thresholds, signal_age_hours: float | None) -> dict[str, float]:
        edge_score = min(1.0, signal.edge_abs / max(0.01, thresholds.min_edge_abs * 2.0))
        source_score = min(1.0, signal.source_count / max(1.0, float(thresholds.min_source_count + 1)))
        liquidity_score = min(1.0, signal.liquidity / max(100.0, thresholds.min_liquidity * 8.0))
        timing_score = _timing_score(signal.time_to_resolution_s, thresholds.min_hours_to_event, thresholds.max_hours_to_event)
        freshness_score = _freshness_score(signal_age_hours, thresholds.max_source_age_hours)
        dispersion_score = 1.0 - min(1.0, signal.source_dispersion_pct / max(0.01, thresholds.max_source_dispersion_pct))
        market_type_score = 1.0 if signal.market_type == "temperature" else 0.9

        composite = (
            0.36 * signal.score
            + 0.16 * edge_score
            + 0.12 * source_score
            + 0.10 * liquidity_score
            + 0.10 * timing_score
            + 0.08 * freshness_score
            + 0.05 * dispersion_score
            + 0.03 * market_type_score
        )
        if signal.edge_size == "large":
            composite += 0.04
        if signal.confidence == "confirmed":
            composite += 0.03

        return {
            "adapter_score": round(signal.score, 4),
            "edge_score": round(edge_score, 4),
            "source_score": round(source_score, 4),
            "liquidity_score": round(liquidity_score, 4),
            "timing_score": round(timing_score, 4),
            "freshness_score": round(freshness_score, 4),
            "dispersion_score": round(dispersion_score, 4),
            "market_type_score": round(market_type_score, 4),
            "composite": round(max(0.0, min(0.99, composite)), 4),
        }


def _signal_age_hours(signal: WeatherSignal) -> float | None:
    raw = str(signal.created_at or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        created_at = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    age_s = (datetime.now(timezone.utc) - created_at.astimezone(timezone.utc)).total_seconds()
    return max(0.0, age_s / 3600.0)


def _freshness_score(signal_age_hours: float | None, max_source_age_hours: float) -> float:
    if signal_age_hours is None:
        return 0.5
    if max_source_age_hours <= 0:
        return 1.0
    decay_window = max_source_age_hours * 2.0
    return max(0.0, 1.0 - min(signal_age_hours, decay_window) / max(max_source_age_hours, 1e-6))


def _timing_score(
    time_to_resolution_s: float | None,
    min_hours_to_event: float,
    max_hours_to_event: float,
) -> float:
    if time_to_resolution_s is None:
        return 0.5
    hours = max(0.0, time_to_resolution_s / 3600.0)
    if hours < min_hours_to_event:
        return 0.0
    if hours > max_hours_to_event:
        overflow = min(hours - max_hours_to_event, max_hours_to_event)
        return max(0.0, 0.45 - overflow / max(max_hours_to_event, 1.0))
    center = (min_hours_to_event + max_hours_to_event) / 2.0
    span = max((max_hours_to_event - min_hours_to_event) / 2.0, 1.0)
    return max(0.2, 1.0 - abs(hours - center) / span)
