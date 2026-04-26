"""Paper-trading strategy evaluation and execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import WeatherBotConfig
from .models import PaperPosition, WeatherDecision, WeatherSignal
from .tracker import WeatherTracker


@dataclass(frozen=True)
class StrategyResult:
    signal: WeatherSignal
    decision: WeatherDecision
    position: PaperPosition | None


@dataclass(frozen=True)
class PositionExitDecision:
    should_close: bool
    reason: str
    reason_code: str
    final_score: float | None
    signal_age_hours: float | None
    mark_price: float | None
    mark_probability: float | None
    edge_abs: float | None


class WeatherStrategyEngine:
    def __init__(self, config: WeatherBotConfig, tracker: WeatherTracker, research_provider=None):
        self.config = config
        self.tracker = tracker
        self.research_provider = research_provider
        self._paper_max_open_positions = max(1, int(self.config.paper.max_open_positions))
        self._paper_entry_min_edge_abs_override: float | None = None

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
                    decision_final_score=decision.final_score,
                    notes="auto_paper_trade",
                    fee_bps=self.config.paper.fee_bps,
                    entry_slippage_bps=self.config.paper.entry_slippage_bps,
                    exit_fee_bps=self.config.paper.fee_bps,
                    exit_slippage_bps=self.config.paper.exit_slippage_bps,
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

    @property
    def paper_max_open_positions(self) -> int:
        return int(self._paper_max_open_positions)

    def set_paper_max_open_positions(self, value: int) -> int:
        self._paper_max_open_positions = max(1, int(value))
        return int(self._paper_max_open_positions)

    @property
    def paper_entry_min_edge_abs_override(self) -> float | None:
        return None if self._paper_entry_min_edge_abs_override is None else float(self._paper_entry_min_edge_abs_override)

    @property
    def paper_entry_min_edge_abs(self) -> float:
        if self._paper_entry_min_edge_abs_override is not None:
            return float(self._paper_entry_min_edge_abs_override)
        return float(self.config.strategy.temperature.min_edge_abs)

    def set_paper_entry_min_edge_abs(self, value: float) -> float:
        normalized = max(0.05, min(0.40, float(value)))
        self._paper_entry_min_edge_abs_override = normalized
        return float(normalized)

    def evaluate_signal(self, signal: WeatherSignal, *, auto_trade_enabled: bool) -> WeatherDecision:
        thresholds = self._thresholds(signal.market_type)
        profile = self._signal_profile(signal, thresholds)
        reasons: list[str] = []
        final_score = float(profile["final_score"] or 0.0)
        policy_action = str(profile["policy_action"] or "advisory")
        metadata = dict(profile["metadata"])
        if policy_action == "paper_blocked":
            reasons.append("Runtime policy blocked this signal.")
        if not auto_trade_enabled:
            reasons.append("Automatic paper trading is disabled.")
        if not self.config.paper.enabled:
            reasons.append("Paper trading is disabled in config.")
        reasons.extend(
            self._entry_gate_reasons(
                signal,
                thresholds,
                final_score,
                profile["signal_age_hours"],
                entry_edge_floor=self._entry_edge_floor(signal.market_type, thresholds),
            )
        )
        if self.tracker.count_open_positions() >= self.paper_max_open_positions:
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

    def evaluate_position_exit(
        self,
        position: dict[str, Any],
        *,
        signal: WeatherSignal | None,
        opposite_signal: WeatherSignal | None = None,
        allow_close_on_missing_signal: bool = True,
    ) -> PositionExitDecision:
        thresholds = self._thresholds(str(position.get("market_type") or getattr(signal, "market_type", "temperature")))
        position_direction = str(position.get("direction") or "").upper()

        if signal is not None:
            profile = self._signal_profile(signal, thresholds)
            final_score = float(profile["final_score"] or 0.0)
            signal_age_hours = profile["signal_age_hours"]
            mark_price = _contract_probability(position_direction, signal.market_prob)
            mark_probability = _contract_probability(position_direction, signal.forecast_prob)
            edge_abs = float(signal.edge_abs or 0.0)
        else:
            profile = None
            final_score = None
            signal_age_hours = None
            mark_price = _as_float(position.get("mark_price"))
            mark_probability = _as_float(position.get("mark_probability"))
            edge_abs = _as_float(position.get("edge_abs")) or _as_float(position.get("mark_edge_abs"))

        if opposite_signal is not None:
            opposite_profile = self._signal_profile(opposite_signal, thresholds)
            if self._is_entry_candidate(opposite_signal, thresholds, opposite_profile):
                return PositionExitDecision(
                    should_close=True,
                    reason="Opposite side now qualifies as a fresh entry.",
                    reason_code="opposite_entry",
                    final_score=final_score,
                    signal_age_hours=signal_age_hours,
                    mark_price=mark_price,
                    mark_probability=mark_probability,
                    edge_abs=edge_abs,
                )

        if signal is None:
            reason = "No fresh qualifying signal in the latest clean review."
            return PositionExitDecision(
                should_close=allow_close_on_missing_signal,
                reason=reason if allow_close_on_missing_signal else "No matching signal; holding until a clean review confirms the edge is gone.",
                reason_code="missing_signal",
                final_score=final_score,
                signal_age_hours=signal_age_hours,
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=edge_abs,
            )

        if profile is not None and str(profile["policy_action"] or "") == "paper_blocked":
            return PositionExitDecision(
                should_close=True,
                reason="Runtime policy now blocks this trade setup.",
                reason_code="runtime_blocked",
                final_score=final_score,
                signal_age_hours=signal_age_hours,
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=edge_abs,
            )

        if final_score is not None and final_score < thresholds.exit_min_score:
            return PositionExitDecision(
                should_close=True,
                reason=f"Final score {final_score:.2f} dropped below the exit floor {thresholds.exit_min_score:.2f}.",
                reason_code="score_breakdown",
                final_score=final_score,
                signal_age_hours=signal_age_hours,
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=edge_abs,
            )

        if edge_abs is not None and edge_abs <= thresholds.exit_near_fair_edge_abs:
            return PositionExitDecision(
                should_close=True,
                reason=f"Remaining edge {edge_abs:.2%} is at or below the near-fair threshold {thresholds.exit_near_fair_edge_abs:.2%}.",
                reason_code="edge_near_fair",
                final_score=final_score,
                signal_age_hours=signal_age_hours,
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=edge_abs,
            )

        if signal_age_hours is not None and signal_age_hours > thresholds.exit_max_source_age_hours:
            return PositionExitDecision(
                should_close=True,
                reason=f"Live signal aged past the exit freshness guardrail ({signal_age_hours:.1f}h > {thresholds.exit_max_source_age_hours:.1f}h).",
                reason_code="stale_signal",
                final_score=final_score,
                signal_age_hours=signal_age_hours,
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=edge_abs,
            )

        if signal.source_dispersion_pct > thresholds.exit_max_source_dispersion_pct:
            return PositionExitDecision(
                should_close=True,
                reason=f"Source dispersion {signal.source_dispersion_pct:.1%} breached the exit ceiling {thresholds.exit_max_source_dispersion_pct:.1%}.",
                reason_code="dispersion_risk",
                final_score=final_score,
                signal_age_hours=signal_age_hours,
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=edge_abs,
            )

        if signal.time_to_resolution_s is not None:
            hours = signal.time_to_resolution_s / 3600.0
            if hours < thresholds.exit_min_hours_to_event and (
                (final_score or 0.0) < thresholds.min_score or (edge_abs or 0.0) < thresholds.min_edge_abs
            ):
                return PositionExitDecision(
                    should_close=True,
                    reason=f"Only {hours:.1f}h remain and the edge no longer clears the entry-quality bar.",
                    reason_code="time_risk",
                    final_score=final_score,
                    signal_age_hours=signal_age_hours,
                    mark_price=mark_price,
                    mark_probability=mark_probability,
                    edge_abs=edge_abs,
                )

        hold_reason = f"Holding: score {final_score:.2f} and edge {(edge_abs or 0.0):.2%} still clear the exit rails."
        return PositionExitDecision(
            should_close=False,
            reason=hold_reason,
            reason_code="hold",
            final_score=final_score,
            signal_age_hours=signal_age_hours,
            mark_price=mark_price,
            mark_probability=mark_probability,
            edge_abs=edge_abs,
        )

    def _thresholds(self, market_type: str):
        if market_type == "precipitation":
            return self.config.strategy.precipitation
        return self.config.strategy.temperature

    def _signal_profile(self, signal: WeatherSignal, thresholds) -> dict[str, Any]:
        signal_age_hours = _signal_age_hours(signal)
        component_scores = self._component_scores(signal, thresholds, signal_age_hours)
        final_score = component_scores["composite"]
        policy_action = "advisory"
        metadata: dict[str, Any] = {
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
        return {
            "final_score": final_score,
            "policy_action": policy_action,
            "metadata": metadata,
            "signal_age_hours": signal_age_hours,
        }

    def _entry_edge_floor(self, market_type: str, thresholds) -> float:
        if self._paper_entry_min_edge_abs_override is None:
            return float(thresholds.min_edge_abs)
        return float(self._paper_entry_min_edge_abs_override)

    def _entry_gate_reasons(
        self,
        signal: WeatherSignal,
        thresholds,
        final_score: float,
        signal_age_hours: float | None,
        *,
        entry_edge_floor: float | None = None,
    ) -> list[str]:
        reasons: list[str] = []
        edge_floor = float(thresholds.min_edge_abs if entry_edge_floor is None else entry_edge_floor)
        if final_score < thresholds.min_score:
            reasons.append(f"Final score {final_score:.2f} below minimum {thresholds.min_score:.2f}.")
        if signal.edge_abs < edge_floor:
            reasons.append(f"Edge {signal.edge_abs:.2%} below minimum {edge_floor:.2%}.")
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
        return reasons

    def _is_entry_candidate(self, signal: WeatherSignal, thresholds, profile: dict[str, Any]) -> bool:
        if str(profile["policy_action"] or "") == "paper_blocked":
            return False
        return not self._entry_gate_reasons(signal, thresholds, float(profile["final_score"] or 0.0), profile["signal_age_hours"])

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


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _contract_probability(direction: str, yes_probability: float | None) -> float | None:
    raw = _as_float(yes_probability)
    if raw is None:
        return None
    raw = max(0.0, min(1.0, raw))
    return raw if str(direction or "").upper() == "YES" else round(1.0 - raw, 6)
