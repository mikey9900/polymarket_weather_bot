# =============================================================
# precipitation/precip_forecast.py
#
# PURPOSE:
#   Fetches monthly precipitation totals for a city/month combo
#   using two sources:
#     1. Open-Meteo  — archive (past days) + forecast (future days)
#     2. Visual Crossing — full timeline (past + future in one call)
#
# APPROACH:
#   Monthly total = observed_so_far (actual) + forecast_remaining
#
#   observed_so_far  → archive API, days 1 through yesterday
#   forecast_remaining → forecast API, today through month end
#
# UNITS:
#   US cities (fahrenheit): inches
#   International (celsius): mm
#
# PROBABILITY MODEL:
#   remaining_precip ~ N(forecast_remaining, σ²)
#   σ = max(MIN_SIGMA, CV * forecast_remaining)
#   CV = 0.40 (40% coefficient of variation — appropriate for
#        multi-week precipitation forecasts)
# =============================================================

import os
import math
import requests
import calendar
import json
import threading
import time
from datetime import date, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


def _ha_option(name: str) -> str:
    try:
        with open("/data/options.json", "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    return str(payload.get(name) or "").strip()


def _float_env(name: str, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        return default


def _bool_setting(env_name: str, ha_name: str, default: bool) -> bool:
    raw = os.getenv(env_name)
    if raw is None or not str(raw).strip():
        raw = _ha_option(ha_name)
    if raw is None or not str(raw).strip():
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


VISUAL_CROSSING_FREE_PLAN_MODE = _bool_setting(
    "VISUAL_CROSSING_FREE_PLAN_MODE",
    "visual_crossing_free_plan_mode",
    False,
)
VISUAL_CROSSING_API_KEY = os.getenv("VISUAL_CROSSING_API_KEY") or _ha_option("visual_crossing_api_key")
VISUAL_CROSSING_ENABLE_PRECIP = _bool_setting(
    "VISUAL_CROSSING_ENABLE_PRECIP",
    "visual_crossing_enable_precip",
    not VISUAL_CROSSING_FREE_PLAN_MODE,
)
VISUAL_CROSSING_PRECIP_CACHE_TTL_SECONDS = _float_env(
    "VISUAL_CROSSING_PRECIP_CACHE_TTL_SECONDS",
    86400.0 if VISUAL_CROSSING_FREE_PLAN_MODE else 43200.0,
)
_visual_crossing_precip_cache_lock = threading.Lock()
_visual_crossing_precip_cache: dict[tuple[str, int, int], dict[str, object]] = {}

# Import city coordinates and shared provider helpers from the existing forecast engine
from forecast.forecast_engine import CITY_COORDS, _normal_cdf, get_openmeteo_json, get_visual_crossing_timeline_json

# Uncertainty model parameters
PRECIP_CV       = 0.40   # coefficient of variation for remaining precip
MIN_SIGMA_INCH  = 0.25   # minimum σ in inches
MIN_SIGMA_MM    = 6.0    # minimum σ in mm


def _get_cached_vc_monthly_precip(city_slug: str, year: int, month: int) -> Optional[dict]:
    key = (city_slug, int(year), int(month))
    now = time.monotonic()
    with _visual_crossing_precip_cache_lock:
        entry = _visual_crossing_precip_cache.get(key)
        if not entry:
            return None
        expires_at = float(entry.get("expires_at", 0.0) or 0.0)
        if expires_at <= now:
            _visual_crossing_precip_cache.pop(key, None)
            return None
        payload = entry.get("payload")
        if not isinstance(payload, dict):
            return None
        return dict(payload)


def _store_vc_monthly_precip(city_slug: str, year: int, month: int, payload: dict) -> None:
    key = (city_slug, int(year), int(month))
    with _visual_crossing_precip_cache_lock:
        _visual_crossing_precip_cache[key] = {
            "payload": dict(payload),
            "expires_at": time.monotonic() + VISUAL_CROSSING_PRECIP_CACHE_TTL_SECONDS,
        }


# =============================================================
# OPEN-METEO PRECIPITATION
# =============================================================

def _get_om_precip_range(
    lat: float, lon: float,
    start: date, end: date,
    unit_group: str,   # "inch" or "mm"
    archive: bool,
) -> Optional[float]:
    """
    Fetches sum of daily precipitation from Open-Meteo for a date range.
    archive=True  → uses archive-api.open-meteo.com (historical data)
    archive=False → uses api.open-meteo.com (forecast data, up to 16 days)
    Returns total precipitation in the requested unit, or None on failure.
    """
    base = "https://archive-api.open-meteo.com/v1/archive" if archive \
           else "https://api.open-meteo.com/v1/forecast"

    try:
        params = {
            "latitude":           lat,
            "longitude":          lon,
            "daily":              "precipitation_sum",
            "precipitation_unit": unit_group,
            "timezone":           "auto",
            "start_date":         start.isoformat(),
            "end_date":           end.isoformat(),
        }
        data = get_openmeteo_json(
            url=base,
            params=params,
            timeout=15,
            rate_limit_label=f"precip {'archive' if archive else 'forecast'}",
            failure_label=f"precip failed ({'archive' if archive else 'forecast'})",
        )
        if not data:
            return None
        values = data.get("daily", {}).get("precipitation_sum", [])
        # Sum non-null values
        total = sum(v for v in values if v is not None)
        return round(total, 3)
    except Exception as e:
        print(f"    ⚠️  Open-Meteo precip failed ({'archive' if archive else 'forecast'}): {e}")
        return None


def get_om_monthly_precip(city_slug: str, year: int, month: int) -> Optional[dict]:
    """
    Returns observed + forecast monthly precipitation from Open-Meteo.

    Returns:
        {
            "observed":         float,   # actual precip for elapsed days
            "forecast":         float,   # forecast for remaining days
            "total_projected":  float,   # observed + forecast
            "unit":             "in" | "mm",
            "days_observed":    int,
            "days_forecast":    int,
        }
        None on failure.
    """
    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    lat  = coords["lat"]
    lon  = coords["lon"]
    unit = "inch" if coords["unit"] == "fahrenheit" else "mm"

    today      = date.today()
    month_start = date(year, month, 1)
    month_end   = date(year, month, calendar.monthrange(year, month)[1])

    observed      = 0.0
    days_observed = 0
    forecast      = 0.0
    days_forecast = 0

    # ── Past days: use archive API ────────────────────────────
    if today > month_start:
        archive_end   = min(today - timedelta(days=1), month_end)
        days_observed = (archive_end - month_start).days + 1
        result = _get_om_precip_range(lat, lon, month_start, archive_end, unit, archive=True)
        if result is None:
            return None
        observed = result

    # ── Future days: use forecast API ────────────────────────
    if today <= month_end:
        forecast_start = max(today, month_start)
        days_forecast  = (month_end - forecast_start).days + 1
        result = _get_om_precip_range(lat, lon, forecast_start, month_end, unit, archive=False)
        if result is None:
            return None
        forecast = result

    return {
        "observed":        observed,
        "forecast":        forecast,
        "total_projected": round(observed + forecast, 3),
        "unit":            unit,
        "days_observed":   days_observed,
        "days_forecast":   days_forecast,
    }


# =============================================================
# VISUAL CROSSING PRECIPITATION
# =============================================================

def get_vc_monthly_precip(city_slug: str, year: int, month: int) -> Optional[dict]:
    """
    Returns observed + forecast monthly precipitation from Visual Crossing.
    Uses a single timeline request for the whole month — VC handles
    past (observed) and future (forecast) in one call.

    Returns same structure as get_om_monthly_precip, or None on failure.
    """
    if not VISUAL_CROSSING_ENABLE_PRECIP:
        return None
    if not VISUAL_CROSSING_API_KEY:
        print("    ⚠️  VISUAL_CROSSING_API_KEY not set — skipping VC precip")
        return None

    cached = _get_cached_vc_monthly_precip(city_slug, year, month)
    if cached is not None:
        return cached

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    lat  = coords["lat"]
    lon  = coords["lon"]
    unit = "inch" if coords["unit"] == "fahrenheit" else "mm"
    unit_group = "us" if unit == "inch" else "metric"

    today       = date.today()
    month_start = date(year, month, 1)
    month_end   = date(year, month, calendar.monthrange(year, month)[1])

    data = get_visual_crossing_timeline_json(
        path=f"{lat},{lon}/{month_start.isoformat()}/{month_end.isoformat()}",
        params={
            "key": VISUAL_CROSSING_API_KEY,
            "unitGroup": unit_group,
            "include": "days",
            "elements": "datetime,precip",
        },
        timeout=15,
        rate_limit_label=f"precip {city_slug}",
        failure_label=f"precip failed for {city_slug}",
    )
    if not data:
        return None
    days = data.get("days", [])

    observed      = 0.0
    forecast      = 0.0
    days_observed = 0
    days_forecast = 0

    for day in days:
        day_date_str = day.get("datetime", "")
        try:
            day_date = date.fromisoformat(day_date_str)
        except Exception:
            continue

        precip = day.get("precip")
        if precip is None:
            continue

        if day_date < today:
            observed      += float(precip)
            days_observed += 1
        else:
            forecast      += float(precip)
            days_forecast += 1

    result = {
        "observed":        round(observed, 3),
        "forecast":        round(forecast, 3),
        "total_projected": round(observed + forecast, 3),
        "unit":            unit,
        "days_observed":   days_observed,
        "days_forecast":   days_forecast,
    }
    _store_vc_monthly_precip(city_slug, year, month, result)
    return result


# =============================================================
# PROBABILITY CALCULATION
# =============================================================

def calc_precip_bucket_probs(
    observed:  float,
    forecast:  float,
    buckets:   list,
    unit:      str,   # "in" or "mm"
) -> dict:
    """
    Calculates the probability that the monthly total falls in
    each bucket, given observed precipitation so far and the
    forecast for the remaining days.

    Model:
        remaining_precip ~ N(forecast, σ²)
        σ = max(MIN_SIGMA, CV * forecast)

    For each bucket [low, high]:
        needed = [low - observed, high - observed]
        P = P(needed_low ≤ remaining ≤ needed_high)

    Returns dict: label → probability (0.001 to 0.999)
    """
    min_sigma = MIN_SIGMA_INCH if unit == "in" else MIN_SIGMA_MM
    sigma     = max(min_sigma, PRECIP_CV * max(0.0, forecast))

    probs = {}
    for bucket in buckets:
        label = bucket.get("label", "")
        low   = bucket.get("low")
        high  = bucket.get("high")

        # Transform bucket bounds into "remaining needed" space
        r_low  = (low  - observed) if low  is not None else None
        r_high = (high - observed) if high is not None else None

        # Upper bound already exceeded — bucket is impossible
        if r_high is not None and r_high <= 0:
            probs[label] = 0.001
            continue

        # Lower bound already met — treat as no lower constraint on remaining
        if r_low is not None and r_low < 0:
            r_low = None

        if r_low is None and r_high is None:
            probs[label] = None
            continue
        elif r_low is None:
            # "less than high" — one-sided upper
            p = _normal_cdf(r_high, forecast, sigma)
        elif r_high is None:
            # "low or more" — one-sided lower
            p = 1.0 - _normal_cdf(r_low, forecast, sigma)
        else:
            p = _normal_cdf(r_high, forecast, sigma) - _normal_cdf(r_low, forecast, sigma)

        probs[label] = round(max(0.001, min(0.999, p)), 3)

    return probs
