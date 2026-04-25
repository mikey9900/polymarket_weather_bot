"""Telegram- and UI-friendly message formatting."""

from __future__ import annotations

from .models import ResolutionOutcome, ScanBatch, WeatherSignal


def format_signal_message(signal: WeatherSignal) -> str:
    if signal.market_type == "temperature":
        from logic.discrepancy_logic import format_discrepancy_message

        return format_discrepancy_message(signal.raw_payload or signal.to_dict())
    source_parts = []
    observed = signal.forecast_snapshot.observed_value
    if observed is not None:
        source_parts.append(f"Observed: {observed:.2f}{signal.forecast_snapshot.unit}")
    if signal.forecast_snapshot.om_temp is not None:
        source_parts.append(f"OM: {signal.forecast_snapshot.om_temp:.2f}{signal.forecast_snapshot.unit}")
    if signal.forecast_snapshot.vc_temp is not None:
        source_parts.append(f"VC: {signal.forecast_snapshot.vc_temp:.2f}{signal.forecast_snapshot.unit}")
    return "\n".join(
        [
            f"*{signal.event_title}*  `{signal.label}`",
            "  ".join(source_parts),
            f"Edge: `{signal.edge:+.0%}`  Score: `{signal.score:.2f}`  Side: *{signal.direction}*",
        ]
    )


def format_scan_summary(
    batch: ScanBatch,
    *,
    accepted_count: int,
    opened_count: int,
    settled_count: int = 0,
) -> str:
    title = "Temperature" if batch.scan_type == "temperature" else "Precipitation"
    return (
        f"*{title} Scan Complete*\n\n"
        f"Events found: {batch.total_events}\n"
        f"Processed: {batch.processed_events}\n"
        f"Flagged signals: {len(batch.signals)}\n"
        f"Accepted by strategy: {accepted_count}\n"
        f"Paper trades opened: {opened_count}\n"
        f"Positions settled: {settled_count}\n"
        f"Skipped: {batch.skipped_events}"
    )


def format_resolution_message(outcome: ResolutionOutcome) -> str:
    return (
        f"*Resolved Market*\n"
        f"`{outcome.market_slug}` -> *{outcome.resolution}*\n"
        f"Resolved positions: {outcome.resolved_positions}\n"
        f"Realized PnL: `${outcome.total_realized_pnl:+.2f}`"
    )


def format_status_message(snapshot: dict) -> str:
    summary = snapshot.get("summary", {}).get("paper", {})
    controls = snapshot.get("controls", {})
    runtime = snapshot.get("runtime", {})
    codex = snapshot.get("codex", {})
    tuner = snapshot.get("tuner", {})
    return (
        f"*Weather Bot Status*\n"
        f"State: {controls.get('state', 'unknown')}\n"
        f"Temperature enabled: {controls.get('temperature_enabled')}\n"
        f"Precipitation enabled: {controls.get('precipitation_enabled')}\n"
        f"Auto paper trading: {controls.get('paper_auto_trade')}\n"
        f"Paper balance: `${summary.get('balance', 0.0):.2f}` / `${summary.get('initial', 0.0):.2f}`\n"
        f"Open positions: {summary.get('open_positions', 0)}\n"
        f"Total PnL: `${summary.get('pnl', 0.0):+.2f}`\n"
        f"Last control: {controls.get('last_action') or 'none'}\n"
        f"Last temperature scan: {runtime.get('last_temperature_scan_at') or 'never'}\n"
        f"Last precipitation scan: {runtime.get('last_precipitation_scan_at') or 'never'}\n"
        f"Codex queue: {codex.get('queue_depth', 0)} | healthy: {codex.get('healthy', False)}\n"
        f"Tuner candidate: {tuner.get('candidate_status', 'none')}"
    )
