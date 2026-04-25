"""Precipitation market scan adapter."""

from __future__ import annotations

from datetime import datetime, timezone

from logic.discrepancy_logic import find_discrepancies
from precipitation.precip_forecast import calc_precip_bucket_probs, get_om_monthly_precip, get_vc_monthly_precip
from precipitation.precip_parser import parse_precip_buckets_for_event
from precipitation.precip_scanner import fetch_precip_events

from .models import ForecastSnapshot, ScanBatch, WeatherSignal


def scan_precipitation_signals() -> ScanBatch:
    started_at = datetime.now(timezone.utc)
    bundles = fetch_precip_events()
    signals: list[WeatherSignal] = []
    processed_events = 0
    flagged_events = 0
    skipped_events = 0

    for bundle in bundles:
        event = bundle["event"]
        buckets = parse_precip_buckets_for_event(bundle["markets"])
        if not buckets:
            skipped_events += 1
            continue
        for bucket in buckets:
            bucket["event_slug"] = str(event.get("slug") or "")
        om_data = get_om_monthly_precip(bundle["city_slug"], bundle["year"], bundle["month"])
        vc_data = get_vc_monthly_precip(bundle["city_slug"], bundle["year"], bundle["month"])
        if not om_data and not vc_data:
            skipped_events += 1
            continue
        observed = (om_data or vc_data)["observed"]
        unit = (om_data or vc_data)["unit"]
        om_probs = calc_precip_bucket_probs(observed, om_data["forecast"], buckets, unit) if om_data else None
        vc_probs = calc_precip_bucket_probs(observed, vc_data["forecast"], buckets, unit) if vc_data else None
        discrepancies = find_discrepancies(
            event_title=str(event.get("title") or "Unknown"),
            city_slug=str(bundle["city_slug"]),
            event_date=f"{bundle['year']}-{bundle['month']:02d}-01",
            buckets=buckets,
            wu_probs=None,
            om_probs=om_probs,
            wu_temp=observed,
            om_temp=om_data["total_projected"] if om_data else None,
            unit_symbol=str(unit),
            vc_probs=vc_probs,
            vc_temp=vc_data["total_projected"] if vc_data else None,
            noaa_probs=None,
            noaa_temp=None,
        )
        processed_events += 1
        if discrepancies:
            flagged_events += 1
        for discrepancy in discrepancies:
            signals.append(_build_precip_signal(discrepancy, started_at))

    return ScanBatch(
        scan_type="precipitation",
        signals=signals,
        total_events=len(bundles),
        processed_events=processed_events,
        flagged_events=flagged_events,
        skipped_events=skipped_events,
        started_at=started_at.isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
    )


def _build_precip_signal(discrepancy: dict, created_at: datetime) -> WeatherSignal:
    forecast_probs = {
        "openmeteo": discrepancy.get("om_prob"),
        "visual_crossing": discrepancy.get("vc_prob"),
    }
    available = [float(value) for value in forecast_probs.values() if value is not None]
    dispersion = max(available) - min(available) if len(available) >= 2 else 0.0
    score = max(
        0.0,
        min(
            0.99,
            0.45 * min(1.0, abs(float(discrepancy.get("discrepancy") or 0.0)) / 0.25)
            + 0.25 * min(1.0, int(discrepancy.get("source_count") or 1) / 2.0)
            + 0.2 * min(1.0, float(discrepancy.get("liquidity") or 0.0) / 500.0)
            - min(0.2, dispersion),
        ),
    )
    signal_key = (
        f"precip:{discrepancy.get('market_slug') or discrepancy.get('event_slug')}:"
        f"{discrepancy.get('direction')}:{created_at.strftime('%Y%m%dT%H%M%S')}"
    )
    snapshot = ForecastSnapshot(
        market_type="precipitation",
        city_slug=str(discrepancy.get("city_slug") or ""),
        event_date=str(discrepancy.get("event_date") or ""),
        unit=str(discrepancy.get("unit") or "in"),
        observed_value=_as_float(discrepancy.get("wu_temp")),
        om_temp=_as_float(discrepancy.get("om_temp")),
        vc_temp=_as_float(discrepancy.get("vc_temp")),
        source_probabilities=forecast_probs,
    )
    return WeatherSignal(
        signal_key=signal_key,
        market_type="precipitation",
        event_title=str(discrepancy.get("event_title") or "Unknown"),
        market_slug=str(discrepancy.get("market_slug") or ""),
        event_slug=str(discrepancy.get("event_slug") or ""),
        city_slug=str(discrepancy.get("city_slug") or ""),
        event_date=str(discrepancy.get("event_date") or ""),
        label=str(discrepancy.get("label") or ""),
        direction=str(discrepancy.get("direction") or "YES"),
        market_prob=float(discrepancy.get("market_prob") or 0.0),
        forecast_prob=float(discrepancy.get("forecast_prob") or 0.0),
        edge=float(discrepancy.get("discrepancy") or 0.0),
        edge_abs=abs(float(discrepancy.get("discrepancy") or 0.0)),
        edge_size=str(discrepancy.get("edge_size") or "small"),
        confidence=str(discrepancy.get("confidence") or "unknown"),
        source_count=int(discrepancy.get("source_count") or 1),
        liquidity=float(discrepancy.get("liquidity") or 0.0),
        time_to_resolution_s=None,
        source_dispersion_pct=round(dispersion, 6),
        score=round(score, 4),
        forecast_snapshot=snapshot,
        raw_payload=dict(discrepancy),
        created_at=created_at.isoformat(),
    )


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)
