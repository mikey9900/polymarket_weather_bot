# =============================================================
# forecast/forecast_engine.py
#
# PURPOSE:
#   Fetches daily max temperature forecasts from multiple sources:
#     1. Weather Company / WU daily forecast (today + next 5 days)
#     2. Open-Meteo — free backup, good global coverage
#
#   Both forecasts are converted into probability distributions
#   that match Polymarket's bucket structure. Discrepancies are
#   flagged separately per source, and markets where BOTH sources
#   agree on a discrepancy get a "confirmed" flag — much stronger signal.
#
# WU API:
#   Uses the IBM Weather Company API (which powers WU).
#   Endpoint: /v3/wx/forecast/daily/5day
#   We use Polymarket's resolution-station coordinates (not city center)
#   so forecasts stay anchored to the settlement location.
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
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Optional, Dict, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dotenv import load_dotenv
from weather_bot.persistent_weather_cache import load_cached_payload, store_cached_payload

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


# API keys from .env or HA options
WU_API_KEY = os.getenv("WU_API_KEY") or _ha_option("wu_api_key")
VISUAL_CROSSING_API_KEY = os.getenv("VISUAL_CROSSING_API_KEY") or _ha_option("visual_crossing_api_key")
WEATHERAPI_KEY = os.getenv("WEATHERAPI_KEY") or _ha_option("weatherapi_key")


def _int_env(name: str, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


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
    True,
)
VISUAL_CROSSING_ENABLE_TEMPERATURE = _bool_setting(
    "VISUAL_CROSSING_ENABLE_TEMPERATURE",
    "visual_crossing_enable_temperature",
    True,
)
VISUAL_CROSSING_ENABLE_REVIEW_TEMPERATURE = _bool_setting(
    "VISUAL_CROSSING_ENABLE_REVIEW_TEMPERATURE",
    "visual_crossing_enable_review_temperature",
    False,
)
WEATHERAPI_ENABLE_TEMPERATURE = _bool_setting(
    "WEATHERAPI_ENABLE_TEMPERATURE",
    "weatherapi_enable_temperature",
    bool(WEATHERAPI_KEY),
)
OPENMETEO_MAX_CONCURRENT = _int_env("OPENMETEO_MAX_CONCURRENT", 1)
OPENMETEO_MAX_ATTEMPTS = _int_env("OPENMETEO_MAX_ATTEMPTS", 3)
OPENMETEO_MIN_INTERVAL_SECONDS = _float_env("OPENMETEO_MIN_INTERVAL_SECONDS", 1.0)
OPENMETEO_RATE_LIMIT_COOLDOWN_SECONDS = _float_env("OPENMETEO_RATE_LIMIT_COOLDOWN_SECONDS", 300.0)
OPENMETEO_FORECAST_WINDOW_DAYS = _int_env("OPENMETEO_FORECAST_WINDOW_DAYS", 10)
OPENMETEO_CACHE_TTL_SECONDS = _float_env("OPENMETEO_CACHE_TTL_SECONDS", 900.0)
WU_MAX_CONCURRENT = _int_env("WU_MAX_CONCURRENT", 1)
WU_CACHE_TTL_SECONDS = _float_env("WU_CACHE_TTL_SECONDS", 900.0)
WEATHERAPI_CACHE_TTL_SECONDS = _float_env("WEATHERAPI_CACHE_TTL_SECONDS", 3600.0)
NOAA_CACHE_TTL_SECONDS = _float_env("NOAA_CACHE_TTL_SECONDS", 900.0)
VISUAL_CROSSING_MAX_CONCURRENT = _int_env("VISUAL_CROSSING_MAX_CONCURRENT", 1)
VISUAL_CROSSING_MAX_ATTEMPTS = _int_env("VISUAL_CROSSING_MAX_ATTEMPTS", 3)
VISUAL_CROSSING_RATE_LIMIT_COOLDOWN_SECONDS = _float_env(
    "VISUAL_CROSSING_RATE_LIMIT_COOLDOWN_SECONDS",
    3600.0 if VISUAL_CROSSING_FREE_PLAN_MODE else 900.0,
)
VISUAL_CROSSING_FORECAST_WINDOW_DAYS = _int_env("VISUAL_CROSSING_FORECAST_WINDOW_DAYS", 15)
VISUAL_CROSSING_CACHE_TTL_SECONDS = _float_env(
    "VISUAL_CROSSING_CACHE_TTL_SECONDS",
    21600.0 if VISUAL_CROSSING_FREE_PLAN_MODE else 7200.0,
)
_openmeteo_gate = threading.BoundedSemaphore(OPENMETEO_MAX_CONCURRENT)
_wu_gate = threading.BoundedSemaphore(WU_MAX_CONCURRENT)
_visual_crossing_gate = threading.BoundedSemaphore(VISUAL_CROSSING_MAX_CONCURRENT)
_openmeteo_rate_limit_lock = threading.Lock()
_openmeteo_cache_lock = threading.Lock()
_wu_cache_lock = threading.Lock()
_weatherapi_cache_lock = threading.Lock()
_noaa_cache_lock = threading.Lock()
_visual_crossing_rate_limit_lock = threading.Lock()
_visual_crossing_cache_lock = threading.Lock()
_openmeteo_disabled_until_monotonic = 0.0
_openmeteo_last_request_monotonic = 0.0
_openmeteo_cooldown_notice_sent = False
_openmeteo_daily_cache: Dict[str, Dict[str, object]] = {}
_wu_daily_cache: Dict[str, Dict[str, object]] = {}
_weatherapi_daily_cache: Dict[str, Dict[str, object]] = {}
_noaa_daily_cache: Dict[str, Dict[str, object]] = {}
_visual_crossing_disabled_until_monotonic = 0.0
_visual_crossing_cooldown_notice_sent = False
_visual_crossing_daily_cache: Dict[str, Dict[str, object]] = {}
_visual_crossing_auth_lock = threading.Lock()
_visual_crossing_auth_failed = False

_SECRET_QUERY_KEYS = frozenset({"apikey", "key"})


def _redact_url_query_secrets(url: str) -> str:
    try:
        parts = urlsplit(url)
    except Exception:
        return url
    if not parts.query:
        return url
    redacted_query = []
    changed = False
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() in _SECRET_QUERY_KEYS:
            redacted_query.append((key, "REDACTED" if value else ""))
            changed = True
        else:
            redacted_query.append((key, value))
    if not changed:
        return url
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(redacted_query, doseq=True), parts.fragment)
    )


def _format_request_error(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        base = str(exc).split(" for url:", 1)[0]
        url = _redact_url_query_secrets(str(getattr(exc.response, "url", "") or ""))
        return f"{base} for url: {url}" if url else base
    return str(exc)


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        retry_after = str(response.headers.get("Retry-After", "") or "").strip()
        if retry_after:
            try:
                return max(1.0, float(retry_after))
            except ValueError:
                pass
    return min(8.0, float(2 ** max(0, attempt - 1)))


def _openmeteo_cooldown_remaining() -> float:
    with _openmeteo_rate_limit_lock:
        return max(0.0, _openmeteo_disabled_until_monotonic - time.monotonic())


def _set_openmeteo_cooldown(seconds: float) -> None:
    global _openmeteo_disabled_until_monotonic, _openmeteo_cooldown_notice_sent
    cooldown = max(0.0, float(seconds))
    if cooldown <= 0:
        return
    with _openmeteo_rate_limit_lock:
        _openmeteo_disabled_until_monotonic = max(
            _openmeteo_disabled_until_monotonic,
            time.monotonic() + cooldown,
        )
        _openmeteo_cooldown_notice_sent = False


def _maybe_log_openmeteo_cooldown(rate_limit_label: str) -> bool:
    global _openmeteo_cooldown_notice_sent
    remaining = _openmeteo_cooldown_remaining()
    if remaining <= 0:
        return False
    with _openmeteo_rate_limit_lock:
        if _openmeteo_cooldown_notice_sent:
            return True
        _openmeteo_cooldown_notice_sent = True
    print(
        f"    ⚠️  Open-Meteo cooling down for {remaining:.0f}s after rate limits; "
        f"skipping {rate_limit_label}"
    )
    return True


def _wait_for_openmeteo_slot() -> None:
    global _openmeteo_last_request_monotonic
    delay = 0.0
    with _openmeteo_rate_limit_lock:
        if OPENMETEO_MIN_INTERVAL_SECONDS > 0:
            elapsed = time.monotonic() - _openmeteo_last_request_monotonic
            delay = max(0.0, OPENMETEO_MIN_INTERVAL_SECONDS - elapsed)
    if delay > 0:
        time.sleep(delay)
    with _openmeteo_rate_limit_lock:
        _openmeteo_last_request_monotonic = time.monotonic()


def _openmeteo_cached_temp(city_slug: str, target_date: date) -> Optional[float]:
    key = target_date.isoformat()
    now = time.monotonic()
    with _openmeteo_cache_lock:
        entry = _openmeteo_daily_cache.get(city_slug)
        if entry:
            expires_at = float(entry.get("expires_at", 0.0) or 0.0)
            if expires_at <= now:
                _openmeteo_daily_cache.pop(city_slug, None)
            else:
                temps = entry.get("temps", {})
                if isinstance(temps, dict) and key in temps:
                    value = temps.get(key)
                    return None if value is None else float(value)
    loaded = _load_persisted_daily_temps("openmeteo", city_slug)
    if loaded is None:
        return None
    temps, ttl_seconds = loaded
    _store_openmeteo_daily_cache(city_slug, temps, persist=False, ttl_seconds=ttl_seconds)
    value = temps.get(key)
    return None if value is None else float(value)


def _store_openmeteo_daily_cache(
    city_slug: str,
    temps: Dict[str, Optional[float]],
    *,
    persist: bool = True,
    ttl_seconds: Optional[float] = None,
) -> None:
    ttl = max(1.0, float(ttl_seconds if ttl_seconds is not None else OPENMETEO_CACHE_TTL_SECONDS))
    payload = dict(temps)
    with _openmeteo_cache_lock:
        _openmeteo_daily_cache[city_slug] = {
            "temps": payload,
            "expires_at": time.monotonic() + ttl,
        }
    if persist:
        _store_persisted_daily_temps("openmeteo", city_slug, payload, ttl)


def _wu_cached_temp(city_slug: str, target_date: date) -> Optional[float]:
    key = target_date.isoformat()
    now = time.monotonic()
    with _wu_cache_lock:
        entry = _wu_daily_cache.get(city_slug)
        if entry:
            expires_at = float(entry.get("expires_at", 0.0) or 0.0)
            if expires_at <= now:
                _wu_daily_cache.pop(city_slug, None)
            else:
                temps = entry.get("temps", {})
                if isinstance(temps, dict) and key in temps:
                    value = temps.get(key)
                    return None if value is None else float(value)
    loaded = _load_persisted_daily_temps("wu", city_slug)
    if loaded is None:
        return None
    temps, ttl_seconds = loaded
    _store_wu_daily_cache(city_slug, temps, persist=False, ttl_seconds=ttl_seconds)
    value = temps.get(key)
    return None if value is None else float(value)


def _store_wu_daily_cache(
    city_slug: str,
    temps: Dict[str, Optional[float]],
    *,
    persist: bool = True,
    ttl_seconds: Optional[float] = None,
) -> None:
    ttl = max(1.0, float(ttl_seconds if ttl_seconds is not None else WU_CACHE_TTL_SECONDS))
    payload = dict(temps)
    with _wu_cache_lock:
        _wu_daily_cache[city_slug] = {
            "temps": payload,
            "expires_at": time.monotonic() + ttl,
        }
    if persist:
        _store_persisted_daily_temps("wu", city_slug, payload, ttl)


def _weatherapi_cached_temp(city_slug: str, target_date: date) -> Optional[float]:
    key = target_date.isoformat()
    now = time.monotonic()
    with _weatherapi_cache_lock:
        entry = _weatherapi_daily_cache.get(city_slug)
        if entry:
            expires_at = float(entry.get("expires_at", 0.0) or 0.0)
            if expires_at <= now:
                _weatherapi_daily_cache.pop(city_slug, None)
            else:
                temps = entry.get("temps", {})
                if isinstance(temps, dict) and key in temps:
                    value = temps.get(key)
                    return None if value is None else float(value)
    loaded = _load_persisted_daily_temps("weatherapi", city_slug)
    if loaded is None:
        return None
    temps, ttl_seconds = loaded
    _store_weatherapi_daily_cache(city_slug, temps, persist=False, ttl_seconds=ttl_seconds)
    value = temps.get(key)
    return None if value is None else float(value)


def _store_weatherapi_daily_cache(
    city_slug: str,
    temps: Dict[str, Optional[float]],
    *,
    persist: bool = True,
    ttl_seconds: Optional[float] = None,
) -> None:
    ttl = max(1.0, float(ttl_seconds if ttl_seconds is not None else WEATHERAPI_CACHE_TTL_SECONDS))
    payload = dict(temps)
    with _weatherapi_cache_lock:
        _weatherapi_daily_cache[city_slug] = {
            "temps": payload,
            "expires_at": time.monotonic() + ttl,
        }
    if persist:
        _store_persisted_daily_temps("weatherapi", city_slug, payload, ttl)


def _noaa_cached_temp(city_slug: str, target_date: date) -> Optional[float]:
    key = target_date.isoformat()
    now = time.monotonic()
    with _noaa_cache_lock:
        entry = _noaa_daily_cache.get(city_slug)
        if entry:
            expires_at = float(entry.get("expires_at", 0.0) or 0.0)
            if expires_at <= now:
                _noaa_daily_cache.pop(city_slug, None)
            else:
                temps = entry.get("temps", {})
                if isinstance(temps, dict) and key in temps:
                    value = temps.get(key)
                    return None if value is None else float(value)
    loaded = _load_persisted_daily_temps("noaa", city_slug)
    if loaded is None:
        return None
    temps, ttl_seconds = loaded
    _store_noaa_daily_cache(city_slug, temps, persist=False, ttl_seconds=ttl_seconds)
    value = temps.get(key)
    return None if value is None else float(value)


def _store_noaa_daily_cache(
    city_slug: str,
    temps: Dict[str, Optional[float]],
    *,
    persist: bool = True,
    ttl_seconds: Optional[float] = None,
) -> None:
    ttl = max(1.0, float(ttl_seconds if ttl_seconds is not None else NOAA_CACHE_TTL_SECONDS))
    payload = dict(temps)
    with _noaa_cache_lock:
        _noaa_daily_cache[city_slug] = {
            "temps": payload,
            "expires_at": time.monotonic() + ttl,
        }
    if persist:
        _store_persisted_daily_temps("noaa", city_slug, payload, ttl)


def _visual_crossing_cooldown_remaining() -> float:
    with _visual_crossing_rate_limit_lock:
        return max(0.0, _visual_crossing_disabled_until_monotonic - time.monotonic())


def _set_visual_crossing_cooldown(seconds: float) -> None:
    global _visual_crossing_disabled_until_monotonic, _visual_crossing_cooldown_notice_sent
    cooldown = max(0.0, float(seconds))
    if cooldown <= 0:
        return
    with _visual_crossing_rate_limit_lock:
        _visual_crossing_disabled_until_monotonic = max(
            _visual_crossing_disabled_until_monotonic,
            time.monotonic() + cooldown,
        )
        _visual_crossing_cooldown_notice_sent = False


def _maybe_log_visual_crossing_cooldown(rate_limit_label: str) -> bool:
    global _visual_crossing_cooldown_notice_sent
    remaining = _visual_crossing_cooldown_remaining()
    if remaining <= 0:
        return False
    with _visual_crossing_rate_limit_lock:
        if _visual_crossing_cooldown_notice_sent:
            return True
        _visual_crossing_cooldown_notice_sent = True
    print(
        f"    ⚠️  Visual Crossing cooling down for {remaining:.0f}s after rate limits; "
        f"skipping {rate_limit_label}"
    )
    return True


def _visual_crossing_cached_temp(city_slug: str, target_date: date) -> Optional[float]:
    key = target_date.isoformat()
    now = time.monotonic()
    with _visual_crossing_cache_lock:
        entry = _visual_crossing_daily_cache.get(city_slug)
        if entry:
            expires_at = float(entry.get("expires_at", 0.0) or 0.0)
            if expires_at <= now:
                _visual_crossing_daily_cache.pop(city_slug, None)
            else:
                temps = entry.get("temps", {})
                if isinstance(temps, dict) and key in temps:
                    value = temps.get(key)
                    return None if value is None else float(value)
    loaded = _load_persisted_daily_temps("visual_crossing", city_slug)
    if loaded is None:
        return None
    temps, ttl_seconds = loaded
    _store_visual_crossing_daily_cache(city_slug, temps, persist=False, ttl_seconds=ttl_seconds)
    value = temps.get(key)
    return None if value is None else float(value)


def _store_visual_crossing_daily_cache(
    city_slug: str,
    temps: Dict[str, Optional[float]],
    *,
    persist: bool = True,
    ttl_seconds: Optional[float] = None,
) -> None:
    ttl = max(1.0, float(ttl_seconds if ttl_seconds is not None else VISUAL_CROSSING_CACHE_TTL_SECONDS))
    payload = dict(temps)
    with _visual_crossing_cache_lock:
        _visual_crossing_daily_cache[city_slug] = {
            "temps": payload,
            "expires_at": time.monotonic() + ttl,
        }
    if persist:
        _store_persisted_daily_temps("visual_crossing", city_slug, payload, ttl)


def _coerce_persisted_daily_temps(payload: dict) -> Dict[str, Optional[float]]:
    raw_temps = payload.get("temps")
    if not isinstance(raw_temps, dict):
        return {}
    temps: Dict[str, Optional[float]] = {}
    for raw_key, raw_value in raw_temps.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if raw_value is None:
            temps[key] = None
            continue
        try:
            temps[key] = float(raw_value)
        except (TypeError, ValueError):
            continue
    return temps


def _load_persisted_daily_temps(provider: str, city_slug: str) -> Optional[Tuple[Dict[str, Optional[float]], float]]:
    loaded = load_cached_payload("temperature", provider, city_slug)
    if loaded is None:
        return None
    payload, ttl_seconds = loaded
    temps = _coerce_persisted_daily_temps(payload)
    if not temps:
        return None
    return temps, ttl_seconds


def _store_persisted_daily_temps(
    provider: str,
    city_slug: str,
    temps: Dict[str, Optional[float]],
    ttl_seconds: float,
) -> None:
    store_cached_payload(
        "temperature",
        provider,
        city_slug,
        {"temps": dict(temps)},
        ttl_seconds,
    )


def _wu_daily_forecast_temps(data: dict) -> Dict[str, Optional[float]]:
    raw_dates = data.get("validTimeLocal") or []
    raw_temps = (
        data.get("calendarDayTemperatureMax")
        or data.get("temperatureMax")
        or []
    )
    temps: Dict[str, Optional[float]] = {}
    for idx, raw_value in enumerate(raw_dates):
        key = str(raw_value or "")[:10]
        if not key:
            continue
        value = raw_temps[idx] if idx < len(raw_temps) else None
        temps[key] = None if value is None else float(value)
    return temps


def _openmeteo_window(target_date: date) -> tuple[date, date]:
    span = max(1, int(OPENMETEO_FORECAST_WINDOW_DAYS))
    baseline_start = min(target_date, date.today() - timedelta(days=1))
    baseline_end = baseline_start + timedelta(days=span - 1)
    return baseline_start, max(target_date, baseline_end)


def _openmeteo_daily_temps(data: dict, *, fallback_date: date) -> Dict[str, Optional[float]]:
    daily = data.get("daily", {}) if isinstance(data, dict) else {}
    raw_dates = daily.get("time") or []
    raw_temps = daily.get("temperature_2m_max") or []
    temps: Dict[str, Optional[float]] = {}
    if raw_dates:
        for idx, raw_date in enumerate(raw_dates):
            key = str(raw_date or "").strip()
            if not key:
                continue
            value = raw_temps[idx] if idx < len(raw_temps) else None
            temps[key] = None if value is None else float(value)
        return temps
    if raw_temps:
        value = raw_temps[0]
        temps[fallback_date.isoformat()] = None if value is None else float(value)
    return temps


def _visual_crossing_daily_temps(data: dict, *, fallback_date: date) -> Dict[str, Optional[float]]:
    raw_days = data.get("days") or []
    temps: Dict[str, Optional[float]] = {}
    for day in raw_days:
        if not isinstance(day, dict):
            continue
        key = str(day.get("datetime") or "").strip()
        if not key:
            continue
        value = day.get("tempmax")
        temps[key] = None if value is None else float(value)
    if temps:
        return temps
    tempmax = data.get("tempmax")
    if tempmax is not None:
        temps[fallback_date.isoformat()] = float(tempmax)
    return temps


def _weatherapi_daily_temps(data: dict) -> Dict[str, Optional[float]]:
    forecast = data.get("forecast", {}) if isinstance(data, dict) else {}
    raw_days = forecast.get("forecastday") or []
    temps: Dict[str, Optional[float]] = {}
    for day in raw_days:
        if not isinstance(day, dict):
            continue
        key = str(day.get("date") or "").strip()
        if not key:
            continue
        day_payload = day.get("day", {}) if isinstance(day.get("day"), dict) else {}
        value = day_payload.get("maxtemp_f")
        if value is None:
            value = day_payload.get("maxtemp_c")
        temps[key] = None if value is None else float(value)
    return temps


def _noaa_daily_temps(periods: list[dict]) -> Dict[str, Optional[float]]:
    temps: Dict[str, Optional[float]] = {}
    for period in periods:
        if not isinstance(period, dict) or not period.get("isDaytime"):
            continue
        key = str(period.get("startTime", "") or "")[:10]
        if not key:
            continue
        value = period.get("temperature")
        temps[key] = None if value is None else float(value)
    return temps


def _visual_crossing_uses_forecast_window(target_date: date) -> bool:
    today = date.today()
    if target_date < today:
        return False
    horizon_end = today + timedelta(days=max(0, VISUAL_CROSSING_FORECAST_WINDOW_DAYS - 1))
    return target_date <= horizon_end


def _provider_context(provider_context: str) -> str:
    return "review" if str(provider_context or "").strip().lower() == "review" else "scheduled"


def _in_provider_window(target_date: date, *, past_days: int, future_days: int) -> bool:
    today = date.today()
    earliest = today - timedelta(days=max(0, int(past_days)))
    latest = today + timedelta(days=max(0, int(future_days)))
    return earliest <= target_date <= latest


def _visual_crossing_disabled() -> bool:
    with _visual_crossing_auth_lock:
        return _visual_crossing_auth_failed


def _disable_visual_crossing_for_run() -> None:
    global _visual_crossing_auth_failed
    with _visual_crossing_auth_lock:
        _visual_crossing_auth_failed = True


def get_openmeteo_json(
    *,
    url: str,
    params: dict,
    timeout: int,
    rate_limit_label: str,
    failure_label: str,
) -> Optional[dict]:
    if _maybe_log_openmeteo_cooldown(rate_limit_label):
        return None
    last_error: Exception | None = None
    with _openmeteo_gate:
        if _maybe_log_openmeteo_cooldown(rate_limit_label):
            return None
        for attempt in range(1, OPENMETEO_MAX_ATTEMPTS + 1):
            try:
                _wait_for_openmeteo_slot()
                response = requests.get(url, params=params, timeout=timeout)
                if response.status_code == 429:
                    last_error = requests.HTTPError("429 Client Error: rate limited", response=response)
                    if attempt < OPENMETEO_MAX_ATTEMPTS:
                        delay = _retry_delay(response, attempt)
                        print(
                            f"    ⚠️  Open-Meteo rate limited for {rate_limit_label}; "
                            f"retrying in {delay:.1f}s"
                        )
                        time.sleep(delay)
                        continue
                    break
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                last_error = exc
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in {429, 500, 502, 503, 504} and attempt < OPENMETEO_MAX_ATTEMPTS:
                    delay = _retry_delay(exc.response, attempt)
                    print(
                        f"    ⚠️  Open-Meteo transient error for {rate_limit_label}; "
                        f"retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue
                break
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                if attempt < OPENMETEO_MAX_ATTEMPTS:
                    delay = _retry_delay(None, attempt)
                    print(
                        f"    ⚠️  Open-Meteo transient error for {rate_limit_label}; "
                        f"retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue
                break
            except Exception as exc:
                last_error = exc
                break
    if isinstance(last_error, requests.HTTPError) and last_error.response is not None and last_error.response.status_code == 429:
        _set_openmeteo_cooldown(OPENMETEO_RATE_LIMIT_COOLDOWN_SECONDS)
    if last_error is not None:
        print(f"    ⚠️  Open-Meteo {failure_label}: {_format_request_error(last_error)}")
    return None


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

def get_wu_forecast_max_temp(
    city_slug: str,
    target_date: date,
    *,
    provider_context: str = "scheduled",
) -> Optional[float]:
    """
    Fetches the daily max temperature from Weather Company / WU.

    For today and future dates, use the working v3 daily forecast endpoint
    keyed to Polymarket's resolution-station coordinates.
    For past dates, fall back to the station daily summary endpoint so
    resolved-market lookbacks still prefer observed highs when available.

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

    if _provider_context(provider_context) == "review" and not _in_provider_window(target_date, past_days=1, future_days=5):
        return None

    today = date.today()
    station = coords["station"]
    wu_unit = coords["wu_unit"]  # "e" = imperial (°F), "m" = metric (°C)

    if target_date >= today:
        cached = _wu_cached_temp(city_slug, target_date)
        if cached is not None:
            return cached
        try:
            with _wu_gate:
                r = requests.get(
                    "https://api.weather.com/v3/wx/forecast/daily/5day",
                    params={
                        "geocode": f"{coords['lat']},{coords['lon']}",
                        "units": wu_unit,
                        "language": "en-US",
                        "format": "json",
                        "apiKey": WU_API_KEY,
                    },
                    timeout=10,
                )
                r.raise_for_status()
            raw = r.text.strip()
            if not raw:
                print(f"    ⚠️  WU daily forecast: empty response for {station}")
                return None
            temps = _wu_daily_forecast_temps(r.json())
            if not temps:
                print(f"    ⚠️  WU daily forecast: no usable days for {station}")
                return None
            _store_wu_daily_cache(city_slug, temps)
            value = temps.get(target_date.isoformat())
            return None if value is None else float(value)
        except Exception as e:
            print(f"    ⚠️  WU forecast failed for {city_slug} ({station}): {_format_request_error(e)}")
            return None

    try:
        # Use daily summary for past dates.
        r = requests.get(
            "https://api.weather.com/v2/pws/dailysummary/7day",
            params={
                "stationId": station,
                "format": "json",
                "units": wu_unit,
                "apiKey": WU_API_KEY,
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
        unit_key = "imperial" if wu_unit == "e" else "metric"

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

def get_openmeteo_forecast_max_temp(
    city_slug: str,
    target_date: date,
    *,
    provider_context: str = "scheduled",
) -> Optional[float]:
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

    if _provider_context(provider_context) == "review" and not _in_provider_window(target_date, past_days=1, future_days=7):
        return None

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    cached = _openmeteo_cached_temp(city_slug, target_date)
    if cached is not None:
        return cached

    lat  = coords["lat"]
    lon  = coords["lon"]
    unit = coords["unit"]  # "fahrenheit" or "celsius"
    start_date, end_date = _openmeteo_window(target_date)

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max",
        "temperature_unit": unit,
        "timezone": "auto",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    try:
        data = get_openmeteo_json(
            url="https://api.open-meteo.com/v1/forecast",
            params=params,
            timeout=10,
            rate_limit_label=city_slug,
            failure_label=f"failed for {city_slug}",
        )
        if not data:
            return None
        temps = _openmeteo_daily_temps(data, fallback_date=target_date)
        if not temps:
            return None
        _store_openmeteo_daily_cache(city_slug, temps)
        value = temps.get(target_date.isoformat())
        return None if value is None else float(value)

    except Exception as e:
        print(f"    ⚠️  Open-Meteo failed for {city_slug}: {_format_request_error(e)}")
        return None


# =============================================================
# VISUAL CROSSING API (free third source, 15-day global forecast)
# =============================================================

def get_visual_crossing_forecast_max_temp(
    city_slug: str,
    target_date: date,
    *,
    provider_context: str = "scheduled",
) -> Optional[float]:
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

    if _provider_context(provider_context) == "review" and not VISUAL_CROSSING_ENABLE_REVIEW_TEMPERATURE:
        return None

    if not VISUAL_CROSSING_ENABLE_TEMPERATURE or not VISUAL_CROSSING_API_KEY or _visual_crossing_disabled():
        return None

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    cached = _visual_crossing_cached_temp(city_slug, target_date)
    if cached is not None:
        return cached

    lat        = coords["lat"]
    lon        = coords["lon"]
    unit       = coords["unit"]
    unit_group = "us" if unit == "fahrenheit" else "metric"
    use_forecast_window = _visual_crossing_uses_forecast_window(target_date)

    data = get_visual_crossing_timeline_json(
        path=f"{lat},{lon}" if use_forecast_window else f"{lat},{lon}/{target_date.isoformat()}",
        params={
            "key": VISUAL_CROSSING_API_KEY,
            "unitGroup": unit_group,
            "include": "days",
            "elements": "tempmax",
        },
        timeout=10,
        rate_limit_label=city_slug,
        failure_label=f"failed for {city_slug}",
    )
    if not data:
        return None
    temps = _visual_crossing_daily_temps(data, fallback_date=target_date)
    if not temps:
        return None
    _store_visual_crossing_daily_cache(city_slug, temps)
    value = temps.get(target_date.isoformat())
    return None if value is None else float(value)


def get_weatherapi_forecast_max_temp(
    city_slug: str,
    target_date: date,
    *,
    provider_context: str = "scheduled",
) -> Optional[float]:
    """
    Fetches the daily max temperature from WeatherAPI.com's free forecast tier.
    Forecast access is limited to today through today+2.
    """

    _provider_context(provider_context)
    if not WEATHERAPI_ENABLE_TEMPERATURE or not WEATHERAPI_KEY:
        return None
    if not _in_provider_window(target_date, past_days=0, future_days=2):
        return None

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    cached = _weatherapi_cached_temp(city_slug, target_date)
    if cached is not None:
        return cached

    try:
        response = requests.get(
            "https://api.weatherapi.com/v1/forecast.json",
            params={
                "key": WEATHERAPI_KEY,
                "q": f"{coords['lat']},{coords['lon']}",
                "days": 3,
                "alerts": "no",
                "aqi": "no",
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        raw_days = ((data.get("forecast", {}) or {}).get("forecastday") or []) if isinstance(data, dict) else []
        temps: Dict[str, Optional[float]] = {}
        use_celsius = coords.get("unit") == "celsius"
        for item in raw_days:
            if not isinstance(item, dict):
                continue
            key = str(item.get("date") or "").strip()
            if not key:
                continue
            day_payload = item.get("day", {}) if isinstance(item.get("day"), dict) else {}
            raw_value = day_payload.get("maxtemp_c") if use_celsius else day_payload.get("maxtemp_f")
            if raw_value is None:
                fallback_value = day_payload.get("maxtemp_f") if use_celsius else day_payload.get("maxtemp_c")
                raw_value = fallback_value
            temps[key] = None if raw_value is None else float(raw_value)
        if not temps:
            return None
        _store_weatherapi_daily_cache(city_slug, temps)
        value = temps.get(target_date.isoformat())
        return None if value is None else float(value)
    except Exception as exc:
        print(f"    âš ï¸  WeatherAPI failed for {city_slug}: {_format_request_error(exc)}")
        return None


# =============================================================
# NOAA / NWS API (free, no API key, US cities only, 7-day forecast)
# =============================================================

# Cache grid forecast URLs per city so we only call /points/ once per session
_noaa_grid_cache: Dict[str, str] = {}

NOAA_HEADERS = {
    "User-Agent": "polymarket-weather-bot (github.com/mikey9900/polymarket_weather_bot)"
}


def get_noaa_forecast_max_temp(
    city_slug: str,
    target_date: date,
    *,
    provider_context: str = "scheduled",
) -> Optional[float]:
    """
    Fetches the daily max temperature from NOAA/NWS.
    Free, no API key required. US cities only (fahrenheit markets).

    Two-step process:
        1. /points/{lat},{lon}  → get the grid forecast URL for this location
        2. {forecast_url}       → get 7-day forecast, find daytime period for target_date

    Args:
        city_slug:   e.g. "nyc"
        target_date: the date to forecast

    Returns:
        float — max temp in °F
        None  — if city is non-US, call fails, or date out of range
    """
    if _provider_context(provider_context) == "review" and not _in_provider_window(target_date, past_days=0, future_days=6):
        return None

    coords = CITY_COORDS.get(city_slug)
    if not coords:
        return None

    # NOAA only covers US cities
    if coords.get("unit") != "fahrenheit":
        return None

    cached = _noaa_cached_temp(city_slug, target_date)
    if cached is not None:
        return cached

    lat = coords["lat"]
    lon = coords["lon"]

    try:
        # Step 1: get forecast URL (cached per session)
        if city_slug not in _noaa_grid_cache:
            r = requests.get(
                f"https://api.weather.gov/points/{lat},{lon}",
                headers=NOAA_HEADERS,
                timeout=10,
            )
            r.raise_for_status()
            forecast_url = r.json()["properties"]["forecast"]
            _noaa_grid_cache[city_slug] = forecast_url

        forecast_url = _noaa_grid_cache[city_slug]

        # Step 2: get the 7-day forecast
        r = requests.get(forecast_url, headers=NOAA_HEADERS, timeout=10)
        r.raise_for_status()
        periods = r.json()["properties"]["periods"]
        temps = _noaa_daily_temps(periods)
        if temps:
            _store_noaa_daily_cache(city_slug, temps)
            value = temps.get(target_date.isoformat())
            return None if value is None else float(value)

        # Find the daytime period for our target date — that's the daily high
        target_str = target_date.isoformat()
        for period in periods:
            if period.get("isDaytime") and period.get("startTime", "")[:10] == target_str:
                return float(period["temperature"])

        return None  # date not in 7-day window

    except Exception as e:
        print(f"    ⚠️  NOAA failed for {city_slug}: {e}")
        _noaa_grid_cache.pop(city_slug, None)  # clear cache on error so next call retries
        return None


def get_visual_crossing_timeline_json(
    *,
    path: str,
    params: dict,
    timeout: int,
    rate_limit_label: str,
    failure_label: str,
) -> Optional[dict]:
    if _visual_crossing_disabled():
        return None
    if _maybe_log_visual_crossing_cooldown(rate_limit_label):
        return None
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{path}"
    last_error: Exception | None = None
    with _visual_crossing_gate:
        if _visual_crossing_disabled():
            return None
        if _maybe_log_visual_crossing_cooldown(rate_limit_label):
            return None
        for attempt in range(1, VISUAL_CROSSING_MAX_ATTEMPTS + 1):
            try:
                response = requests.get(url, params=params, timeout=timeout)
                if response.status_code in {401, 403}:
                    _disable_visual_crossing_for_run()
                    last_error = requests.HTTPError(
                        f"{response.status_code} Client Error: invalid API credentials",
                        response=response,
                    )
                    break
                if response.status_code == 429:
                    last_error = requests.HTTPError(
                        f"429 Client Error: rate limited for url: {response.url}",
                        response=response,
                    )
                    if attempt < VISUAL_CROSSING_MAX_ATTEMPTS:
                        delay = _retry_delay(response, attempt)
                        print(
                            f"    ⚠️  Visual Crossing rate limited for {rate_limit_label}; "
                            f"retrying in {delay:.1f}s"
                        )
                        time.sleep(delay)
                        continue
                    break
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 429 and attempt < VISUAL_CROSSING_MAX_ATTEMPTS:
                    last_error = exc
                    delay = _retry_delay(exc.response, attempt)
                    print(
                        f"    ⚠️  Visual Crossing rate limited for {rate_limit_label}; "
                        f"retrying in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue
                last_error = exc
                break
            except Exception as exc:
                last_error = exc
                break
    if last_error is not None:
        if isinstance(last_error, requests.HTTPError) and last_error.response is not None and last_error.response.status_code == 429:
            _set_visual_crossing_cooldown(VISUAL_CROSSING_RATE_LIMIT_COOLDOWN_SECONDS)
        if isinstance(last_error, requests.HTTPError) and last_error.response is not None and last_error.response.status_code in {401, 403}:
            print(
                "    ⚠️  Visual Crossing auth failed; disabling VC for this run. "
                "Check VISUAL_CROSSING_API_KEY or HA visual_crossing_api_key."
            )
        else:
            print(f"    ⚠️  Visual Crossing {failure_label}: {_format_request_error(last_error)}")
    return None


def _visual_crossing_retry_delay(response: requests.Response | None, attempt: int) -> float:
    return _retry_delay(response, attempt)


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


def _provider_status_label(
    *,
    name: str,
    temp: Optional[float],
    unit_sym: str,
    unavailable_reason: Optional[str] = None,
) -> str:
    if temp is not None:
        return f"{name} {temp:.1f}{unit_sym}"
    if unavailable_reason:
        return f"{name} {unavailable_reason}"
    return f"{name} N/A"


def _wu_status_reason(target_date: date, *, provider_context: str = "scheduled") -> str:
    if not WU_API_KEY:
        return "disabled"
    today = date.today()
    if _provider_context(provider_context) == "review" and not _in_provider_window(target_date, past_days=1, future_days=5):
        return "review horizon"
    if target_date < today:
        return "obs unavailable"
    if target_date > today + timedelta(days=5):
        return "5d horizon"
    return "unavailable"


def _openmeteo_status_reason(*, provider_context: str = "scheduled", target_date: Optional[date] = None) -> str:
    if target_date is not None and _provider_context(provider_context) == "review":
        if not _in_provider_window(target_date, past_days=1, future_days=7):
            return "review horizon"
    if _openmeteo_cooldown_remaining() > 0:
        return "cooldown"
    return "unavailable"


def _visual_crossing_status_reason(*, provider_context: str = "scheduled") -> str:
    if not VISUAL_CROSSING_ENABLE_TEMPERATURE or not VISUAL_CROSSING_API_KEY:
        return "disabled"
    if _provider_context(provider_context) == "review" and not VISUAL_CROSSING_ENABLE_REVIEW_TEMPERATURE:
        return "review-disabled"
    if _visual_crossing_disabled():
        return "auth-disabled"
    if _visual_crossing_cooldown_remaining() > 0:
        return "cooldown"
    return "unavailable"


def _weatherapi_status_reason(target_date: date) -> str:
    if not WEATHERAPI_ENABLE_TEMPERATURE or not WEATHERAPI_KEY:
        return "disabled"
    if not _in_provider_window(target_date, past_days=0, future_days=2):
        return "3d horizon"
    return "unavailable"


def _noaa_status_reason(city_slug: str, target_date: date, *, provider_context: str = "scheduled") -> str:
    coords = CITY_COORDS.get(city_slug, {})
    if coords.get("unit") != "fahrenheit":
        return "us-only"
    if _provider_context(provider_context) == "review" and not _in_provider_window(target_date, past_days=0, future_days=6):
        return "review horizon"
    if target_date > date.today() + timedelta(days=6):
        return "7d horizon"
    return "unavailable"


def _call_temperature_provider(provider, city_slug: str, target_date: date, provider_context: str) -> Optional[float]:
    try:
        return provider(city_slug, target_date, provider_context=provider_context)
    except TypeError as exc:
        if "provider_context" not in str(exc):
            raise
        return provider(city_slug, target_date)


def get_both_bucket_probabilities(
    city_slug:   str,
    target_date: date,
    buckets:     list,
    *,
    provider_context: str = "scheduled",
) -> Dict[str, Optional[Dict]]:
    """
    Returns bucket probabilities from all configured temperature sources:
    Weather Company / WU, Open-Meteo, Visual Crossing, and NOAA.

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

    context = _provider_context(provider_context)

    with ThreadPoolExecutor(max_workers=5, thread_name_prefix="weather-provider") as executor:
        future_wu = executor.submit(_call_temperature_provider, get_wu_forecast_max_temp, city_slug, target_date, context)
        future_om = executor.submit(_call_temperature_provider, get_openmeteo_forecast_max_temp, city_slug, target_date, context)
        future_vc = executor.submit(_call_temperature_provider, get_visual_crossing_forecast_max_temp, city_slug, target_date, context)
        future_noaa = executor.submit(_call_temperature_provider, get_noaa_forecast_max_temp, city_slug, target_date, context)
        future_weatherapi = executor.submit(_call_temperature_provider, get_weatherapi_forecast_max_temp, city_slug, target_date, context)
        wu_temp = future_wu.result()
        om_temp = future_om.result()
        vc_temp = future_vc.result()
        noaa_temp = future_noaa.result()  # US only, None for international
        weatherapi_temp = future_weatherapi.result()

    status_parts = [
        _provider_status_label(
            name=f"WU({station})",
            temp=wu_temp,
            unit_sym=unit_sym,
            unavailable_reason=_wu_status_reason(target_date, provider_context=context),
        ),
        _provider_status_label(
            name="OM",
            temp=om_temp,
            unit_sym=unit_sym,
            unavailable_reason=_openmeteo_status_reason(provider_context=context, target_date=target_date),
        ),
        _provider_status_label(
            name="VC",
            temp=vc_temp,
            unit_sym=unit_sym,
            unavailable_reason=_visual_crossing_status_reason(provider_context=context),
        ),
        _provider_status_label(
            name="NOAA",
            temp=noaa_temp,
            unit_sym=unit_sym,
            unavailable_reason=_noaa_status_reason(city_slug, target_date, provider_context=context),
        ),
        _provider_status_label(
            name="WA",
            temp=weatherapi_temp,
            unit_sym=unit_sym,
            unavailable_reason=_weatherapi_status_reason(target_date),
        ),
    ]
    print(f"    🌦️  Sources: {' | '.join(status_parts)}")

    wu_probs   = _probs_from_temp(wu_temp,   buckets, unit) if wu_temp   is not None else None
    om_probs   = _probs_from_temp(om_temp,   buckets, unit) if om_temp   is not None else None
    vc_probs   = _probs_from_temp(vc_temp,   buckets, unit) if vc_temp   is not None else None
    noaa_probs = _probs_from_temp(noaa_temp, buckets, unit) if noaa_temp is not None else None
    weatherapi_probs = _probs_from_temp(weatherapi_temp, buckets, unit) if weatherapi_temp is not None else None

    return {
        "wu":        wu_probs,
        "openmeteo": om_probs,
        "vc":        vc_probs,
        "noaa":      noaa_probs,
        "weatherapi": weatherapi_probs,
        "wu_temp":   wu_temp,
        "om_temp":   om_temp,
        "vc_temp":   vc_temp,
        "noaa_temp": noaa_temp,
        "weatherapi_temp": weatherapi_temp,
        "unit":      unit_sym,
    }
