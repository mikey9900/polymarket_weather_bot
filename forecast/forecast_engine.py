# =============================================================
# forecast/forecast_engine.py
#
# PURPOSE:
#   Fetches daily max temperature forecasts from TWO sources:
#     1. Weather Underground (WU) — the EXACT station Polymarket uses
#     2. Open-Meteo — free backup, good global coverage
#
#   Both forecasts are converted into probability distributions
#   that match Polymarket's bucket structure. Discrepancies are
#   flagged separately per source, and markets where BOTH sources
#   agree on a discrepancy get a "confirmed" flag — much stronger signal.
#
# WU API:
#   Uses the IBM Weather Company API (which powers WU).
#   Endpoint: /v1/geocode/{lat}/{lon}/forecast/daily/5day.json
#   We use station coordinates (not city center) so forecasts match
#   exactly what Polymarket settles against.
#
# STATION IDs:
#   Confirmed from debug_discover_stations.py by reading the
#   resolutionSource field from each Polymarket event directly.
#   4 cities (hong-kong, istanbul, tel-aviv, moscow) had UNKNOWN
#   in the Polymarket API — we've filled those in manually below.
# =============================================================

import os
import requests
import math
from datetime import date
from typing import Optional, Dict, Tuple
from dotenv import load_dotenv

load_dotenv()

# API keys from .env
WU_API_KEY             = os.getenv("WU_API_KEY")
VISUAL_CROSSING_API_KEY = os.getenv("VISUAL_CROSSING_API_KEY")

# -------------------------------------------------------------
# CITY → WU STATION + COORDINATES + UNIT
#
# station_id: ICAO code from Polymarket's resolutionSource
# lat/lon:    coordinates of that station (for WU API calls)
# unit:       "fahrenheit" or "celsius" — matches Polymarket's markets
# wu_unit:    "e" = imperial (°F), "m" = metric (°C) for WU API
# -------------------------------------------------------------
CITY_COORDS = {
    # US Cities — Fahrenheit
    "nyc":           {"station": "KLGA",  "lat": 40.7773,  "lon": -73.8740,  "unit": "fahrenheit", "wu_unit": "e"},
    "los-angeles":   {"station": "KLAX",  "lat": 33.9425,  "lon": -118.4081, "unit": "fahrenheit", "wu_unit": "e"},
    "chicago":       {"station": "KORD",  "lat": 41.9742,  "lon": -87.9073,  "unit": "fahrenheit", "wu_unit": "e"},
    "houston":       {"station": "KHOU",  "lat": 29.6454,  "lon": -95.2789,  "unit": "fahrenheit", "wu_unit": "e"},
    "dallas":        {"station": "KDAL",  "lat": 32.8481,  "lon": -96.8511,  "unit": "fahrenheit", "wu_unit": "e"},
    "austin":        {"station": "KAUS",  "lat": 30.1975,  "lon": -97.6664,  "unit": "fahrenheit", "wu_unit": "e"},
    "san-francisco": {"station": "KSFO",  "lat": 37.6190,  "lon": -122.3750, "unit": "fahrenheit", "wu_unit": "e"},
    "seattle":       {"station": "KSEA",  "lat": 47.4444,  "lon": -122.3139, "unit": "fahrenheit", "wu_unit": "e"},
    "denver":        {"station": "KBKF",  "lat": 39.7017,  "lon": -104.7517, "unit": "fahrenheit", "wu_unit": "e"},
    "atlanta":       {"station": "KATL",  "lat": 33.6407,  "lon": -84.4277,  "unit": "fahrenheit", "wu_unit": "e"},
    "miami":         {"station": "KMIA",  "lat": 25.7959,  "lon": -80.2870,  "unit": "fahrenheit", "wu_unit": "e"},

    # International — Celsius
    "london":        {"station": "EGLC",  "lat": 51.5048,  "lon": 0.0495,    "unit": "celsius",    "wu_unit": "m"},
    "paris":         {"station": "LFPG",  "lat": 49.0097,  "lon": 2.5479,    "unit": "celsius",    "wu_unit": "m"},
    "tokyo":         {"station": "RJTT",  "lat": 35.5494,  "lon": 139.7798,  "unit": "celsius",    "wu_unit": "m"},
    "toronto":       {"station": "CYYZ",  "lat": 43.6772,  "lon": -79.6306,  "unit": "celsius",    "wu_unit": "m"},
    "mexico-city":   {"station": "MMMX",  "lat": 19.4363,  "lon": -99.0721,  "unit": "celsius",    "wu_unit": "m"},
    "beijing":       {"station": "ZBAA",  "lat": 40.0799,  "lon": 116.5822,  "unit": "celsius",    "wu_unit": "m"},
    "shanghai":      {"station": "ZSPD",  "lat": 31.1443,  "lon": 121.8083,  "unit": "celsius",    "wu_unit": "m"},
    "singapore":     {"station": "WSSS",  "lat": 1.3502,   "lon": 103.9943,  "unit": "celsius",    "wu_unit": "m"},
    "hong-kong":     {"station": "VHHH",  "lat": 22.3080,  "lon": 113.9185,  "unit": "celsius",    "wu_unit": "m"},
    "seoul":         {"station": "RKSI",  "lat": 37.4602,  "lon": 126.4407,  "unit": "celsius",    "wu_unit": "m"},
    "amsterdam":     {"station": "EHAM",  "lat": 52.3086,  "lon": 4.7639,    "unit": "celsius",    "wu_unit": "m"},
    "madrid":        {"station": "LEMD",  "lat": 40.4936,  "lon": -3.5668,   "unit": "celsius",    "wu_unit": "m"},
    "helsinki":      {"station": "EFHK",  "lat": 60.3172,  "lon": 24.9633,   "unit": "celsius",    "wu_unit": "m"},
    "warsaw":        {"station": "EPWA",  "lat": 52.1657,  "lon": 20.9671,   "unit": "celsius",    "wu_unit": "m"},
    "istanbul":      {"station": "LTBA",  "lat": 40.9769,  "lon": 28.8146,   "unit": "celsius",    "wu_unit": "m"},
    "lagos":         {"station": "DNMM",  "lat": 6.5774,   "lon": 3.3214,    "unit": "celsius",    "wu_unit": "m"},
    "buenos-aires":  {"station": "SAEZ",  "lat": -34.8222, "lon": -58.5358,  "unit": "celsius",    "wu_unit": "m"},
    "sao-paulo":     {"station": "SBGR",  "lat": -23.4356, "lon": -46.4731,  "unit": "celsius",    "wu_unit": "m"},
    "jakarta":       {"station": "WIHH",  "lat": -6.1275,  "lon": 106.6537,  "unit": "celsius",    "wu_unit": "m"},
    "kuala-lumpur":  {"station": "WMKK",  "lat": 2.7456,   "lon": 101.7072,  "unit": "celsius",    "wu_unit": "m"},
    "tel-aviv":      {"station": "LLBG",  "lat": 32.0114,  "lon": 34.8867,   "unit": "celsius",    "wu_unit": "m"},
    "moscow":        {"station": "UUEE",  "lat": 55.9726,  "lon": 37.4146,   "unit": "celsius",    "wu_unit": "m"},
}

# Forecast uncertainty (standard deviation for normal distribution)
# Used to spread the point forecast across Polymarket's buckets
UNCERTAINTY_F = 2.5   # °F
UNCERTAINTY_C = 1.5   # °C


# =============================================================
# WEATHER UNDERGROUND API
# =============================================================

def get_wu_forecast_max_temp(city_slug: str, target_date: date) -> Optional[float]:
    """
    Fetches the daily max temperature from Weather Underground
    using the PWS (Personal Weather Station) API plan.

    Uses the v2/pws/dailysummary endpoint which is available on
    the PWS plan. Fetches the 7-day summary for the exact station
    Polymarket uses to resolve markets.

    For future dates (forecasts), we fall back to Open-Meteo since
    the PWS plan only provides observations, not forecasts.
    For today or past dates, we return the actual observed max temp.

    Args:
        city_slug:   e.g. "san-francisco"
        target_date: the date we want the max temp for

    Returns:
        float — max temp in city's native unit (°F or °C)
        None  — if API key missing or call fails
    """

    if not WU_API_KEY:
        print("    ⚠️  WU_API_KEY not found in .env — skipping WU")
        return None

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    station = coords["station"]
    wu_unit = coords["wu_unit"]  # "e" = imperial (°F), "m" = metric (°C)

    # PWS plan provides observations (actual readings), not forecasts.
    # - For past dates:  use v2/pws/dailysummary (end-of-day summary)
    # - For today:       use v2/pws/observations/current (live reading)
    #                    and return tempHigh from today so far
    # - For future dates: return None (no observation exists yet)
    today = date.today()

    if target_date > today:
        # No PWS observation available for future dates
        return None

    try:
        if target_date == today:
            # Use current observations for today — tempHigh so far
            r = requests.get(
                "https://api.weather.com/v2/pws/observations/current",
                params={
                    "stationId": station,
                    "format":    "json",
                    "units":     wu_unit,
                    "apiKey":    WU_API_KEY,
                },
                timeout=10,
            )
            r.raise_for_status()
            raw = r.text.strip()
            if not raw:
                print(f"    ⚠️  WU current obs: empty response for {station}")
                return None
            data = r.json()

            # Response: {"observations": [{"imperial": {"tempHigh": 62, ...}, ...}]}
            obs_list = data.get("observations", [])
            if not obs_list:
                return None

            unit_key  = "imperial" if wu_unit == "e" else "metric"
            unit_data = obs_list[0].get(unit_key, {})
            temp_high = unit_data.get("tempHigh")
            if temp_high is not None:
                return float(temp_high)
            # Fall back to current temp if tempHigh not set yet
            temp = unit_data.get("temp")
            return float(temp) if temp is not None else None

        else:
            # Use daily summary for past dates
            r = requests.get(
                "https://api.weather.com/v2/pws/dailysummary/7day",
                params={
                    "stationId": station,
                    "format":    "json",
                    "units":     wu_unit,
                    "apiKey":    WU_API_KEY,
                },
                timeout=10,
            )
            r.raise_for_status()
            raw = r.text.strip()
            if not raw:
                print(f"    ⚠️  WU daily summary: empty response for {station}")
                return None
            data = r.json()

            # Response: {"summaries": [{"obsTimeLocal": "2026-04-10 00:00:00",
            #                           "imperial": {"tempHigh": 62, ...}}]}
            summaries = data.get("summaries", [])
            target_str = target_date.isoformat()
            unit_key   = "imperial" if wu_unit == "e" else "metric"

            for summary in summaries:
                obs_time = (summary.get("obsTimeLocal") or "")[:10]
                if obs_time == target_str:
                    unit_data = summary.get(unit_key, {})
                    temp_high = unit_data.get("tempHigh")
                    if temp_high is not None:
                        return float(temp_high)

            print(f"    ⚠️  WU: no summary for {station} on {target_date}")
            return None

    except Exception as e:
        print(f"    ⚠️  WU API failed for {city_slug} ({station}): {e}")
        return None


# =============================================================
# OPEN-METEO API (free backup)
# =============================================================

def get_openmeteo_forecast_max_temp(city_slug: str, target_date: date) -> Optional[float]:
    """
    Fetches the forecasted daily max temperature from Open-Meteo.
    Free, no API key needed, good global coverage.

    Uses the same station coordinates as WU for consistency.

    Args:
        city_slug:   e.g. "san-francisco"
        target_date: the date to forecast

    Returns:
        float — max temp in city's native unit (°F or °C)
        None  — if call fails
    """

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    lat  = coords["lat"]
    lon  = coords["lon"]
    unit = coords["unit"]  # "fahrenheit" or "celsius"

    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":           lat,
                "longitude":          lon,
                "daily":              "temperature_2m_max",
                "temperature_unit":   unit,
                "timezone":           "auto",
                "start_date":         target_date.isoformat(),
                "end_date":           target_date.isoformat(),
            },
            timeout=10,
        )
        r.raise_for_status()
        data  = r.json()
        temps = data.get("daily", {}).get("temperature_2m_max", [])

        if not temps or temps[0] is None:
            return None

        return float(temps[0])

    except Exception as e:
        print(f"    ⚠️  Open-Meteo failed for {city_slug}: {e}")
        return None


# =============================================================
# VISUAL CROSSING API (free third source, 15-day global forecast)
# =============================================================

def get_visual_crossing_forecast_max_temp(city_slug: str, target_date: date) -> Optional[float]:
    """
    Fetches the daily max temperature from Visual Crossing.
    Free tier: 1000 records/day. No restrictions on forecast range.

    Args:
        city_slug:   e.g. "san-francisco"
        target_date: the date to forecast

    Returns:
        float — max temp in city's native unit (°F or °C)
        None  — if API key missing or call fails
    """

    if not VISUAL_CROSSING_API_KEY:
        return None

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    lat        = coords["lat"]
    lon        = coords["lon"]
    unit       = coords["unit"]
    unit_group = "us" if unit == "fahrenheit" else "metric"

    try:
        r = requests.get(
            f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{lat},{lon}/{target_date.isoformat()}",
            params={
                "key":       VISUAL_CROSSING_API_KEY,
                "unitGroup": unit_group,
                "include":   "days",
                "elements":  "tempmax",
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        days = data.get("days", [])
        if not days or days[0].get("tempmax") is None:
            return None
        return float(days[0]["tempmax"])

    except Exception as e:
        print(f"    ⚠️  Visual Crossing failed for {city_slug}: {e}")
        return None


# =============================================================
# COMBINED FORECAST
# =============================================================

def get_forecast_max_temp(city_slug: str, target_date: date) -> Optional[float]:
    """
    Returns the Open-Meteo forecast temp (used for single-source calls).
    Kept for backwards compatibility with run_scanner.py.
    """
    return get_openmeteo_forecast_max_temp(city_slug, target_date)


def get_both_forecast_temps(
    city_slug:   str,
    target_date: date,
) -> Dict[str, Optional[float]]:
    """
    Fetches forecast max temp from both sources for a city/date.

    Returns:
        dict with keys:
            "wu":       float or None — Weather Underground forecast
            "openmeteo": float or None — Open-Meteo forecast
            "unit":     "°F" or "°C"
            "average":  float or None — average of both (if both available)
    """

    coords      = CITY_COORDS.get(city_slug, {})
    unit_name   = coords.get("unit", "fahrenheit")
    unit_symbol = "°F" if unit_name == "fahrenheit" else "°C"

    wu_temp   = get_wu_forecast_max_temp(city_slug, target_date)
    om_temp   = get_openmeteo_forecast_max_temp(city_slug, target_date)

    # Average both if available, otherwise use whichever is available
    if wu_temp is not None and om_temp is not None:
        average = round((wu_temp + om_temp) / 2, 1)
    elif wu_temp is not None:
        average = wu_temp
    elif om_temp is not None:
        average = om_temp
    else:
        average = None

    return {
        "wu":        wu_temp,
        "openmeteo": om_temp,
        "average":   average,
        "unit":      unit_symbol,
    }


# =============================================================
# PROBABILITY DISTRIBUTION
# =============================================================

def _normal_cdf(x: float, mean: float, std: float) -> float:
    """
    Cumulative distribution function of a normal distribution.
    P(value < x) given mean and standard deviation.
    """
    return 0.5 * (1.0 + math.erf((x - mean) / (std * math.sqrt(2))))


def _probs_from_temp(
    forecast_temp: float,
    buckets:       list,
    unit:          str,
) -> Dict[str, float]:
    """
    Converts a single forecast temperature into a probability
    distribution across Polymarket's temperature buckets.

    Uses a normal distribution centered on forecast_temp with
    standard deviation UNCERTAINTY_F or UNCERTAINTY_C.

    Args:
        forecast_temp: the forecasted max temp
        buckets:       list of bucket dicts with low/high/label
        unit:          "fahrenheit" or "celsius"

    Returns:
        dict mapping label → probability (0.0 to 1.0)
    """

    std   = UNCERTAINTY_F if unit == "fahrenheit" else UNCERTAINTY_C
    probs = {}

    for bucket in buckets:
        label = bucket.get("label", "")
        low   = bucket.get("low")
        high  = bucket.get("high")

        if low is None and high is None:
            probs[label] = None
            continue
        elif low is None:
            prob = _normal_cdf(high, forecast_temp, std)
        elif high is None:
            prob = 1.0 - _normal_cdf(low, forecast_temp, std)
        else:
            prob = _normal_cdf(high, forecast_temp, std) - _normal_cdf(low, forecast_temp, std)

        probs[label] = max(0.0, min(1.0, prob))

    return probs


def get_bucket_probabilities(
    city_slug:   str,
    target_date: date,
    buckets:     list,
) -> Optional[Dict[str, float]]:
    """
    Returns bucket probabilities using Open-Meteo (backwards compatible).
    Used by run_scanner when only one source is needed.
    """
    temp = get_openmeteo_forecast_max_temp(city_slug, target_date)
    if temp is None:
        return None

    coords = CITY_COORDS.get(city_slug, {})
    unit   = coords.get("unit", "fahrenheit")

    std_str = f"±{UNCERTAINTY_F}°F" if unit == "fahrenheit" else f"±{UNCERTAINTY_C}°C"
    unit_sym = "°F" if unit == "fahrenheit" else "°C"
    print(f"    🌤️  Forecast: {temp:.1f}{unit_sym} ({std_str})")

    return _probs_from_temp(temp, buckets, unit)


def get_both_bucket_probabilities(
    city_slug:   str,
    target_date: date,
    buckets:     list,
) -> Dict[str, Optional[Dict]]:
    """
    Returns bucket probabilities from all three sources:
    Weather Underground, Open-Meteo, and Visual Crossing.

    Args:
        city_slug:   e.g. "san-francisco"
        target_date: the date of the market
        buckets:     list of bucket dicts

    Returns:
        dict with keys:
            "wu":        dict of label → prob, or None
            "openmeteo": dict of label → prob, or None
            "vc":        dict of label → prob, or None
            "wu_temp":   float or None
            "om_temp":   float or None
            "vc_temp":   float or None
            "unit":      "°F" or "°C"
    """

    coords   = CITY_COORDS.get(city_slug, {})
    unit     = coords.get("unit", "fahrenheit")
    unit_sym = "°F" if unit == "fahrenheit" else "°C"
    station  = coords.get("station", "?")

    wu_temp = get_wu_forecast_max_temp(city_slug, target_date)
    om_temp = get_openmeteo_forecast_max_temp(city_slug, target_date)
    vc_temp = get_visual_crossing_forecast_max_temp(city_slug, target_date)

    print(f"    🌡️  WU ({station}): {f'{wu_temp:.1f}{unit_sym}' if wu_temp is not None else 'N/A'}")
    print(f"    🌤️  Open-Meteo:   {f'{om_temp:.1f}{unit_sym}' if om_temp is not None else 'N/A'}")
    print(f"    🌍  Vis.Crossing: {f'{vc_temp:.1f}{unit_sym}' if vc_temp is not None else 'N/A'}")

    wu_probs = _probs_from_temp(wu_temp, buckets, unit) if wu_temp is not None else None
    om_probs = _probs_from_temp(om_temp, buckets, unit) if om_temp is not None else None
    vc_probs = _probs_from_temp(vc_temp, buckets, unit) if vc_temp is not None else None

    return {
        "wu":        wu_probs,
        "openmeteo": om_probs,
        "vc":        vc_probs,
        "wu_temp":   wu_temp,
        "om_temp":   om_temp,
        "vc_temp":   vc_temp,
        "unit":      unit_sym,
    }