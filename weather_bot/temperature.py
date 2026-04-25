"""Temperature market scan adapter."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

from forecast.forecast_engine import get_both_bucket_probabilities
from logic.discrepancy_logic import find_discrepancies
from parser.weather_parser import parse_temperature_buckets_for_event
from scanner.weather_event_scanner import fetch_weather_events

from .models import ForecastSnapshot, ScanBatch, WeatherSignal


def scan_temperature_signals(limit: int = 300) -> ScanBatch:
    started_at = datetime.now(timezone.utc)
    bundles = fetch_weather_events(limit=limit)
    signals: list[WeatherSignal] = []
    flagged_events = 0
    processed_events = 0
    skipped_events = 0
    error_count = 0
    error_samples: list[str] = []

    if not bundles:
        return ScanBatch(
            scan_type="temperature",
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
        bundle_results = [_process_temperature_bundle(bundle, started_at) for bundle in bundles]
    else:
        bundle_results = []
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="weather-temp-bundle") as executor:
            futures = [executor.submit(_process_temperature_bundle, bundle, started_at) for bundle in bundles]
            for future in as_completed(futures):
                try:
                    bundle_results.append(future.result())
                except Exception as exc:
                    error_count += 1
                    if len(error_samples) < 5:
                        error_samples.append(str(exc))

    for item in bundle_results:
        signals.extend(item["signals"])
        flagged_events += int(item["flagged_events"])
        processed_events += int(item["processed_events"])
        skipped_events += int(item["skipped_events"])
        error_count += int(item["error_count"])
        for sample in item["error_samples"]:
            if len(error_samples) < 5:
                error_samples.append(sample)

    signals.sort(key=lambda signal: signal.score, reverse=True)
    return ScanBatch(
        scan_type="temperature",
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


def _process_temperature_bundle(bundle: dict, created_at: datetime) -> dict[str, object]:
    try:
        event = bundle["event"]
        markets = bundle["markets"]
        end_date_str = (event.get("endDate") or "")[:10]
        city_slug = str(event.get("seriesSlug") or "").replace("-daily-weather", "")
        try:
            event_date = date.fromisoformat(end_date_str)
        except ValueError:
            return _bundle_result(skipped_events=1)
        buckets = parse_temperature_buckets_for_event(markets)
        if not buckets:
            return _bundle_result(skipped_events=1)
        event_slug = str(event.get("slug") or "")
        for bucket in buckets:
            bucket["event_slug"] = event_slug
        forecast_data = get_both_bucket_probabilities(city_slug, event_date, buckets)
        if not any(forecast_data.get(key) is not None for key in ("wu", "openmeteo", "vc", "noaa")):
            return _bundle_result(skipped_events=1)
        discrepancies = find_discrepancies(
            event_title=str(event.get("title") or "Unknown"),
            city_slug=city_slug,
            event_date=event_date,
            buckets=buckets,
            wu_probs=forecast_data.get("wu"),
            om_probs=forecast_data.get("openmeteo"),
            wu_temp=forecast_data.get("wu_temp"),
            om_temp=forecast_data.get("om_temp"),
            unit_symbol=str(forecast_data.get("unit") or "F"),
            vc_probs=forecast_data.get("vc"),
            vc_temp=forecast_data.get("vc_temp"),
            noaa_probs=forecast_data.get("noaa"),
            noaa_temp=forecast_data.get("noaa_temp"),
        )
        event_end = _parse_event_end_time(event.get("endDate"))
        signals = [
            _build_temperature_signal(
                event=event,
                discrepancy=discrepancy,
                event_end=event_end,
                created_at=created_at,
            )
            for discrepancy in discrepancies
        ]
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
    return min(6, max(2, bundle_count))


def _build_temperature_signal(*, event: dict, discrepancy: dict, event_end: datetime | None, created_at: datetime) -> WeatherSignal:
    forecast_probs = {
        "wu": discrepancy.get("wu_prob"),
        "openmeteo": discrepancy.get("om_prob"),
        "visual_crossing": discrepancy.get("vc_prob"),
        "noaa": discrepancy.get("noaa_prob"),
    }
    available = [float(value) for value in forecast_probs.values() if value is not None]
    dispersion = max(available) - min(available) if len(available) >= 2 else 0.0
    time_to_resolution_s = None
    if event_end is not None:
        time_to_resolution_s = max(0.0, (event_end - created_at).total_seconds())
    score = _score_temperature_signal(
        edge_abs=abs(float(discrepancy.get("discrepancy") or 0.0)),
        source_count=int(discrepancy.get("source_count") or 1),
        liquidity=float(discrepancy.get("liquidity") or 0.0),
        time_to_resolution_s=time_to_resolution_s,
        dispersion=dispersion,
    )
    event_date_str = str(discrepancy.get("event_date") or "")
    signal_key = (
        f"temperature:{discrepancy.get('market_slug') or discrepancy.get('event_slug')}:"
        f"{discrepancy.get('direction')}:{created_at.strftime('%Y%m%dT%H%M%S')}"
    )
    snapshot = ForecastSnapshot(
        market_type="temperature",
        city_slug=str(discrepancy.get("city_slug") or ""),
        event_date=event_date_str,
        unit=str(discrepancy.get("unit") or "F"),
        wu_temp=_as_float(discrepancy.get("wu_temp")),
        om_temp=_as_float(discrepancy.get("om_temp")),
        vc_temp=_as_float(discrepancy.get("vc_temp")),
        noaa_temp=_as_float(discrepancy.get("noaa_temp")),
        source_probabilities=forecast_probs,
    )
    return WeatherSignal(
        signal_key=signal_key,
        market_type="temperature",
        event_title=str(discrepancy.get("event_title") or event.get("title") or "Unknown"),
        market_slug=str(discrepancy.get("market_slug") or ""),
        event_slug=str(discrepancy.get("event_slug") or event.get("slug") or ""),
        city_slug=str(discrepancy.get("city_slug") or ""),
        event_date=event_date_str,
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
        time_to_resolution_s=time_to_resolution_s,
        source_dispersion_pct=round(dispersion, 6),
        score=round(score, 4),
        forecast_snapshot=snapshot,
        raw_payload=dict(discrepancy),
        created_at=created_at.isoformat(),
    )


def _score_temperature_signal(
    *,
    edge_abs: float,
    source_count: int,
    liquidity: float,
    time_to_resolution_s: float | None,
    dispersion: float,
) -> float:
    edge_score = min(1.0, edge_abs / 0.25)
    source_score = min(1.0, max(1, source_count) / 4.0)
    liquidity_score = min(1.0, liquidity / 500.0)
    if time_to_resolution_s is None:
        timing_score = 0.5
    else:
        hours = time_to_resolution_s / 3600.0
        timing_score = 0.1 if hours < 1 else 1.0 if hours <= 24 else max(0.35, 1.0 - min(hours, 240.0) / 400.0)
    dispersion_penalty = min(0.25, dispersion)
    return max(0.0, min(0.99, 0.4 * edge_score + 0.25 * source_score + 0.2 * liquidity_score + 0.15 * timing_score - dispersion_penalty))


def _parse_event_end_time(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)
