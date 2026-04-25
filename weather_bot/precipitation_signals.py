"""Precipitation market scan adapter."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
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
    error_count = 0
    error_samples: list[str] = []

    if not bundles:
        return ScanBatch(
            scan_type="precipitation",
            signals=[],
            total_events=0,
            processed_events=0,
            flagged_events=0,
            skipped_events=0,
            started_at=started_at.isoformat(),
            finished_at=datetime.now(timezone.utc).isoformat(),
        )

    worker_count = _scan_worker_count(len(bundles))
    if worker_count == 1:
        bundle_results = [_process_precipitation_bundle(bundle, started_at) for bundle in bundles]
    else:
        bundle_results = []
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="weather-precip-bundle") as executor:
            futures = [executor.submit(_process_precipitation_bundle, bundle, started_at) for bundle in bundles]
            for future in as_completed(futures):
                try:
                    bundle_results.append(future.result())
                except Exception as exc:
                    error_count += 1
                    if len(error_samples) < 5:
                        error_samples.append(str(exc))

    for item in bundle_results:
        signals.extend(item["signals"])
        processed_events += int(item["processed_events"])
        flagged_events += int(item["flagged_events"])
        skipped_events += int(item["skipped_events"])
        error_count += int(item["error_count"])
        for sample in item["error_samples"]:
            if len(error_samples) < 5:
                error_samples.append(sample)

    signals.sort(key=lambda signal: signal.score, reverse=True)
    return ScanBatch(
        scan_type="precipitation",
        signals=signals,
        total_events=len(bundles),
        processed_events=processed_events,
        flagged_events=flagged_events,
        skipped_events=skipped_events,
        started_at=started_at.isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
        error_count=error_count,
        error_samples=error_samples,
    )


def _process_precipitation_bundle(bundle: dict, created_at: datetime) -> dict[str, object]:
    try:
        event = bundle["event"]
        buckets = parse_precip_buckets_for_event(bundle["markets"])
        if not buckets:
            return _bundle_result(skipped_events=1)
        for bucket in buckets:
            bucket["event_slug"] = str(event.get("slug") or "")

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="weather-precip-provider") as executor:
            future_om = executor.submit(get_om_monthly_precip, bundle["city_slug"], bundle["year"], bundle["month"])
            future_vc = executor.submit(get_vc_monthly_precip, bundle["city_slug"], bundle["year"], bundle["month"])
            om_data = future_om.result()
            vc_data = future_vc.result()

        if not om_data and not vc_data:
            return _bundle_result(skipped_events=1)
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
        signals = [_build_precip_signal(discrepancy, created_at) for discrepancy in discrepancies]
        return _bundle_result(
            signals=signals,
            processed_events=1,
            flagged_events=1 if discrepancies else 0,
        )
    except Exception as exc:
        return _bundle_result(skipped_events=1, error_count=1, error_samples=[str(exc)])


def _bundle_result(
    *,
    signals: list[WeatherSignal] | None = None,
    processed_events: int = 0,
    flagged_events: int = 0,
    skipped_events: int = 0,
    error_count: int = 0,
    error_samples: list[str] | None = None,
) -> dict[str, object]:
    return {
        "signals": signals or [],
        "processed_events": processed_events,
        "flagged_events": flagged_events,
        "skipped_events": skipped_events,
        "error_count": error_count,
        "error_samples": list(error_samples or []),
    }


def _scan_worker_count(bundle_count: int) -> int:
    if bundle_count <= 1:
        return 1
    return min(4, max(2, bundle_count))


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
