"""Paper-trading strategy evaluation and execution."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from .config import WeatherBotConfig
from .execution import (
    PAPER_EXECUTION_MODE,
    execution_mode_creates_paper_positions,
    execution_mode_records_shadow_orders,
    normalize_execution_mode,
)
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


CONFIRMED_REVIEW_EXIT_CODES = {
    "edge_near_fair",
    "score_breakdown",
    "stale_signal",
    "dispersion_risk",
    "time_risk",
}


class WeatherStrategyEngine:
    def __init__(self, config: WeatherBotConfig, tracker: WeatherTracker, research_provider=None):
        self.config = config
        self.tracker = tracker
        self.research_provider = research_provider
        self._paper_max_open_positions = max(1, int(self.config.paper.max_open_positions))
        self._paper_entry_min_edge_abs_override: float | None = None
        self._paper_execution_mode = normalize_execution_mode(getattr(self.config.paper, "execution_mode", PAPER_EXECUTION_MODE))

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
            shadow_intent = None
            if decision.accepted and execution_mode_records_shadow_orders(self.paper_execution_mode):
                shadow_intent = self.tracker.preview_shadow_entry_intent(
                    signal_id=signal_id,
                    decision_id=decision_id,
                    signal=signal,
                    execution_mode=self.paper_execution_mode,
                    decision_final_score=decision.final_score,
                    reason=decision.reason,
                    stake_usd=self.config.paper.stake_usd,
                    fee_bps=self.config.paper.fee_bps,
                    entry_slippage_bps=self.config.paper.entry_slippage_bps,
                )
            if decision.accepted:
                if execution_mode_creates_paper_positions(self.paper_execution_mode):
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
                if position is not None and shadow_intent is not None:
                    self.tracker.record_shadow_order_intent(
                        replace(
                            shadow_intent,
                            position_id=int(position.id),
                            status="mirrored",
                        )
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

    @property
    def paper_execution_mode(self) -> str:
        return normalize_execution_mode(self._paper_execution_mode)

    def set_paper_execution_mode(self, value: str) -> str:
        self._paper_execution_mode = normalize_execution_mode(value)
        return self.paper_execution_mode

    def set_paper_entry_min_edge_abs(self, value: float) -> float:
        normalized = max(0.05, min(0.40, float(value)))
        self._paper_entry_min_edge_abs_override = normalized
        return float(normalized)

    @property
    def paper_temperature_no_stop_loss_pnl(self) -> float | None:
        return _as_float(getattr(self.config.strategy.temperature, "no_stop_loss_pnl", None))

    @property
    def paper_temperature_no_stop_loss_min_entry_price(self) -> float | None:
        return _as_float(getattr(self.config.strategy.temperature, "no_stop_loss_min_entry_price", None))

    @property
    def paper_temperature_no_stop_loss_min_probability_drop(self) -> float | None:
        return _as_float(getattr(self.config.strategy.temperature, "no_stop_loss_min_probability_drop", None))

    @property
    def paper_temperature_max_no_entry_price_override(self) -> float | None:
        return None

    @property
    def paper_temperature_max_no_entry_price(self) -> float | None:
        return None

    def set_paper_temperature_max_no_entry_price(self, value: float | None) -> float | None:
        return None

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

        stop_loss_decision = self._no_stop_loss_exit_decision(
            position,
            thresholds=thresholds,
            position_direction=position_direction,
            final_score=final_score,
            signal_age_hours=signal_age_hours,
            mark_price=mark_price,
            mark_probability=mark_probability,
            edge_abs=edge_abs,
            current_signal_available=signal is not None,
        )
        if stop_loss_decision is not None:
            return stop_loss_decision

        if signal is None:
            reason = "No fresh qualifying signal in the latest clean review."
            return PositionExitDecision(
                should_close=False,
                reason=f"{reason} Holding until a forecast-aware review confirms the edge is gone.",
                reason_code="missing_signal",
                final_score=final_score,
                signal_age_hours=signal_age_hours,
                mark_price=mark_price,
                mark_probability=mark_probability,
                edge_abs=edge_abs,
            )

        if edge_abs is not None and edge_abs <= thresholds.exit_near_fair_edge_abs:
            return self._confirm_review_exit(
                position,
                PositionExitDecision(
                    should_close=True,
                    reason=f"Remaining model edge {edge_abs:.2%} is at or below the near-fair threshold {thresholds.exit_near_fair_edge_abs:.2%}.",
                    reason_code="edge_near_fair",
                    final_score=final_score,
                    signal_age_hours=signal_age_hours,
                    mark_price=mark_price,
                    mark_probability=mark_probability,
                    edge_abs=edge_abs,
                ),
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
            return self._confirm_review_exit(
                position,
                PositionExitDecision(
                    should_close=True,
                    reason=f"Final score {final_score:.2f} dropped below the exit floor {thresholds.exit_min_score:.2f}.",
                    reason_code="score_breakdown",
                    final_score=final_score,
                    signal_age_hours=signal_age_hours,
                    mark_price=mark_price,
                    mark_probability=mark_probability,
                    edge_abs=edge_abs,
                ),
            )

        if signal_age_hours is not None and signal_age_hours > thresholds.exit_max_source_age_hours:
            return self._confirm_review_exit(
                position,
                PositionExitDecision(
                    should_close=True,
                    reason=f"Live signal aged past the exit freshness guardrail ({signal_age_hours:.1f}h > {thresholds.exit_max_source_age_hours:.1f}h).",
                    reason_code="stale_signal",
                    final_score=final_score,
                    signal_age_hours=signal_age_hours,
                    mark_price=mark_price,
                    mark_probability=mark_probability,
                    edge_abs=edge_abs,
                ),
            )

        if signal.source_dispersion_pct > thresholds.exit_max_source_dispersion_pct:
            return self._confirm_review_exit(
                position,
                PositionExitDecision(
                    should_close=True,
                    reason=f"Source dispersion {signal.source_dispersion_pct:.1%} breached the exit ceiling {thresholds.exit_max_source_dispersion_pct:.1%}.",
                    reason_code="dispersion_risk",
                    final_score=final_score,
                    signal_age_hours=signal_age_hours,
                    mark_price=mark_price,
                    mark_probability=mark_probability,
                    edge_abs=edge_abs,
                ),
            )

        if signal.time_to_resolution_s is not None:
            hours = signal.time_to_resolution_s / 3600.0
            if hours < thresholds.exit_min_hours_to_event and (
                (final_score or 0.0) < thresholds.min_score or (edge_abs or 0.0) < thresholds.min_edge_abs
            ):
                return self._confirm_review_exit(
                    position,
                    PositionExitDecision(
                        should_close=True,
                        reason=f"Only {hours:.1f}h remain and the edge no longer clears the entry-quality bar.",
                        reason_code="time_risk",
                        final_score=final_score,
                        signal_age_hours=signal_age_hours,
                        mark_price=mark_price,
                        mark_probability=mark_probability,
                        edge_abs=edge_abs,
                    ),
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

    def _confirm_review_exit(self, position: dict[str, Any], decision: PositionExitDecision) -> PositionExitDecision:
        if not decision.should_close or decision.reason_code not in CONFIRMED_REVIEW_EXIT_CODES:
            return decision
        if self._has_prior_consecutive_bad_review(position):
            return decision
        return replace(
            decision,
            should_close=False,
            reason=f"{decision.reason} First bad review; waiting for one more consecutive bad review before closing.",
        )

    def _has_prior_consecutive_bad_review(self, position: dict[str, Any]) -> bool:
        position_id = _as_int(position.get("id"))
        if position_id is None:
            return False
        try:
            history = self.tracker.get_position_review_history(limit=1, position_id=position_id)
        except Exception:
            return False
        if not history:
            return False
        reason_code = str(history[0].get("review_reason_code") or "").strip()
        return reason_code in CONFIRMED_REVIEW_EXIT_CODES

    def _no_stop_loss_exit_decision(
        self,
        position: dict[str, Any],
        *,
        thresholds,
        position_direction: str,
        final_score: float | None,
        signal_age_hours: float | None,
        mark_price: float | None,
        mark_probability: float | None,
        edge_abs: float | None,
        current_signal_available: bool,
    ) -> PositionExitDecision | None:
        if str(position.get("market_type") or "").strip().lower() != "temperature":
            return None
        if position_direction != "NO":
            return None
        stop_loss_pnl = _as_float(getattr(thresholds, "no_stop_loss_pnl", None))
        min_entry_price = _as_float(getattr(thresholds, "no_stop_loss_min_entry_price", None))
        entry_price = _as_float(position.get("entry_price"))
        if stop_loss_pnl is None or entry_price is None:
            return None
        if min_entry_price is not None and entry_price < min_entry_price:
            return None
        current_mark_price = mark_price
        if current_mark_price is None:
            current_mark_price = _as_float(position.get("mark_price"))
        if current_mark_price is None:
            current_mark_price = _as_float(position.get("market_probability"))
        if current_mark_price is None:
            return None
        mark_to_market_pnl = _position_mark_to_market_pnl(position, current_mark_price)
        if mark_to_market_pnl is None or mark_to_market_pnl > stop_loss_pnl:
            return None
        if not current_signal_available:
            return None
        current_model_probability = _as_float(mark_probability)
        entry_model_probability = _entry_model_probability(position)
        min_probability_drop = _as_float(getattr(thresholds, "no_stop_loss_min_probability_drop", None))
        if min_probability_drop is None:
            min_probability_drop = 0.15
        min_probability_drop = max(0.0, min(1.0, min_probability_drop))
        if current_model_probability is None or entry_model_probability is None:
            return None
        probability_drop = round(entry_model_probability - current_model_probability, 6)
        if probability_drop < min_probability_drop:
            return None
        return PositionExitDecision(
            should_close=True,
            reason=(
                f"NO stop loss hit: mark-to-market P/L {_signed_usd(mark_to_market_pnl)} "
                f"is at or below {_signed_usd(stop_loss_pnl)} and model probability fell "
                f"{probability_drop:.2%} from entry ({entry_model_probability:.2%} to {current_model_probability:.2%})."
            ),
            reason_code="no_stop_loss",
            final_score=final_score,
            signal_age_hours=signal_age_hours,
            mark_price=current_mark_price,
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


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _contract_probability(direction: str, yes_probability: float | None) -> float | None:
    raw = _as_float(yes_probability)
    if raw is None:
        return None
    raw = max(0.0, min(1.0, raw))
    return raw if str(direction or "").upper() == "YES" else round(1.0 - raw, 6)


def _entry_model_probability(position: dict[str, Any]) -> float | None:
    value = _as_float(position.get("entry_model_probability"))
    if value is not None:
        return max(0.0, min(1.0, value))
    if not str(position.get("mark_updated_at") or "").strip():
        value = _as_float(position.get("mark_probability"))
        if value is not None:
            return max(0.0, min(1.0, value))
    return None


def _position_mark_to_market_pnl(position: dict[str, Any], mark_price: float | None) -> float | None:
    reference_price = _as_float(mark_price)
    shares = _as_float(position.get("shares"))
    cost = _as_float(position.get("cost"))
    if reference_price is None or shares is None or cost is None:
        return None
    net_exit_value = _estimate_net_exit_value(
        shares,
        reference_price,
        fee_bps=_as_float(position.get("exit_fee_bps")) or 0.0,
        slippage_bps=_as_float(position.get("exit_slippage_bps")) or 0.0,
    )
    if net_exit_value is None:
        return None
    return round(net_exit_value - cost, 6)


def _estimate_net_exit_value(
    shares: float,
    reference_price: float,
    *,
    fee_bps: float,
    slippage_bps: float,
) -> float | None:
    fill_exit_price = _apply_exit_slippage(reference_price, slippage_bps)
    if fill_exit_price is None:
        return None
    gross_payout = round(max(0.0, shares) * fill_exit_price, 6)
    exit_fee_paid = _fee_amount(gross_payout, fee_bps)
    return round(gross_payout - exit_fee_paid, 6)


def _apply_exit_slippage(reference_price: float | None, slippage_bps: float) -> float | None:
    bounded = _as_float(reference_price)
    if bounded is None:
        return None
    bounded = max(0.0, min(1.0, bounded))
    adjustment = 1.0 - max(0.0, float(slippage_bps or 0.0)) / 10000.0
    return round(max(0.0, min(1.0, bounded * adjustment)), 6)


def _fee_amount(notional: float, fee_bps: float) -> float:
    return round(max(0.0, float(notional or 0.0)) * max(0.0, float(fee_bps or 0.0)) / 10000.0, 6)


def _signed_usd(value: float) -> str:
    amount = float(value or 0.0)
    return f"{amount:+.2f}".replace("+", "+$").replace("-", "-$")
