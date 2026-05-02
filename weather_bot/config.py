"""Typed config loader for the weather platform."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .paths import ensure_runtime_config


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class AppSettings:
    timezone: str
    log_level: str
    auto_temperature_scan_minutes: int
    auto_precipitation_scan_minutes: int
    resolution_check_minutes: int
    auto_temperature_scan_seconds: int = 0
    auto_precipitation_scan_seconds: int = 0
    open_position_review_seconds: int = 15
    open_position_weather_refresh_minutes: int = 60


@dataclass(frozen=True)
class MarketSettings:
    enabled: bool
    scan_limit: int
    auto_alerts: bool
    auto_paper_trade: bool
    market_scope: str = "both"


@dataclass(frozen=True)
class StrategyThresholds:
    min_score: float
    min_edge_abs: float
    min_source_count: int
    min_liquidity: float
    min_hours_to_event: float
    max_hours_to_event: float
    max_source_age_hours: float = 6.0
    max_source_dispersion_pct: float = 0.20
    max_forecast_temp_spread_f: float | None = None
    max_no_edge_abs: float | None = None
    max_no_entry_price: float | None = None
    no_stop_loss_pnl: float | None = None
    no_stop_loss_min_entry_price: float | None = None
    exit_min_score: float = 0.55
    exit_near_fair_edge_abs: float = 0.03
    exit_max_source_age_hours: float = 8.0
    exit_max_source_dispersion_pct: float = 0.24
    exit_min_hours_to_event: float = 0.5


@dataclass(frozen=True)
class PaperSettings:
    enabled: bool
    initial_capital: float
    stake_usd: float
    max_open_positions: int
    max_positions_per_market: int
    fee_bps: float = 50.0
    entry_slippage_bps: float = 15.0
    exit_slippage_bps: float = 15.0
    mark_stale_after_seconds: int = 75


@dataclass(frozen=True)
class AlertsSettings:
    telegram_enabled: bool
    send_scan_summary: bool
    send_paper_entries: bool
    send_resolution_updates: bool


@dataclass(frozen=True)
class DashboardSettings:
    enabled: bool
    host: str
    port: int
    refresh_seconds: float


@dataclass(frozen=True)
class ResearchSettings:
    enabled: bool
    runtime_policy_enabled: bool
    auto_refresh_enabled: bool
    auto_tuning_enabled: bool


@dataclass(frozen=True)
class StrategySettings:
    temperature: StrategyThresholds
    precipitation: StrategyThresholds


@dataclass(frozen=True)
class WeatherBotConfig:
    schema_version: int
    app: AppSettings
    temperature: MarketSettings
    precipitation: MarketSettings
    paper: PaperSettings
    strategy: StrategySettings
    alerts: AlertsSettings
    dashboard: DashboardSettings
    research: ResearchSettings
    config_path: str


def load_config(
    path_value: str | Path | None = None,
    *,
    ha_options_path: str | Path = "/data/options.json",
) -> WeatherBotConfig:
    config_path = ensure_runtime_config(path_value)
    payload = _load_yaml(config_path)
    payload = _deep_merge(payload, _load_ha_options(ha_options_path))
    payload = _deep_merge(payload, _load_env_overrides())

    return WeatherBotConfig(
        schema_version=int(payload.get("schema_version", 1)),
        app=AppSettings(**_section(payload, "app")),
        temperature=MarketSettings(**_section(payload, "temperature")),
        precipitation=MarketSettings(**_section(payload, "precipitation")),
        paper=PaperSettings(**_section(payload, "paper")),
        strategy=StrategySettings(
            temperature=StrategyThresholds(**_section(_section(payload, "strategy"), "temperature")),
            precipitation=StrategyThresholds(**_section(_section(payload, "strategy"), "precipitation")),
        ),
        alerts=AlertsSettings(**_section(payload, "alerts")),
        dashboard=DashboardSettings(**_section(payload, "dashboard")),
        research=ResearchSettings(**_section(payload, "research")),
        config_path=str(Path(config_path).resolve()),
    )


def _load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Config at {path} must be a mapping.")
    return payload


def _load_ha_options(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}

    mapped: dict[str, Any] = {}
    if "timezone" in payload:
        mapped.setdefault("app", {})["timezone"] = payload["timezone"]
    if "temperature_scan_minutes" in payload:
        mapped.setdefault("app", {})["auto_temperature_scan_minutes"] = int(payload["temperature_scan_minutes"])
    if "temperature_scan_seconds" in payload:
        mapped.setdefault("app", {})["auto_temperature_scan_seconds"] = int(payload["temperature_scan_seconds"])
    if "precipitation_scan_minutes" in payload:
        mapped.setdefault("app", {})["auto_precipitation_scan_minutes"] = int(payload["precipitation_scan_minutes"])
    if "precipitation_scan_seconds" in payload:
        mapped.setdefault("app", {})["auto_precipitation_scan_seconds"] = int(payload["precipitation_scan_seconds"])
    if "precipitation_enabled" in payload:
        mapped.setdefault("precipitation", {})["enabled"] = bool(payload["precipitation_enabled"])
    if "temperature_market_scope" in payload:
        mapped.setdefault("temperature", {})["market_scope"] = str(payload["temperature_market_scope"])
    if "temperature_max_forecast_temp_spread_f" in payload:
        mapped.setdefault("strategy", {}).setdefault("temperature", {})["max_forecast_temp_spread_f"] = float(
            payload["temperature_max_forecast_temp_spread_f"]
        )
    if "temperature_max_no_edge_abs" in payload:
        mapped.setdefault("strategy", {}).setdefault("temperature", {})["max_no_edge_abs"] = float(
            payload["temperature_max_no_edge_abs"]
        )
    if "temperature_max_no_entry_price" in payload:
        mapped.setdefault("strategy", {}).setdefault("temperature", {})["max_no_entry_price"] = float(
            payload["temperature_max_no_entry_price"]
        )
    if "temperature_no_stop_loss_pnl" in payload:
        mapped.setdefault("strategy", {}).setdefault("temperature", {})["no_stop_loss_pnl"] = float(
            payload["temperature_no_stop_loss_pnl"]
        )
    if "temperature_no_stop_loss_min_entry_price" in payload:
        mapped.setdefault("strategy", {}).setdefault("temperature", {})["no_stop_loss_min_entry_price"] = float(
            payload["temperature_no_stop_loss_min_entry_price"]
        )
    if "resolution_check_minutes" in payload:
        mapped.setdefault("app", {})["resolution_check_minutes"] = int(payload["resolution_check_minutes"])
    if "open_position_review_seconds" in payload:
        mapped.setdefault("app", {})["open_position_review_seconds"] = int(payload["open_position_review_seconds"])
    if "open_position_weather_refresh_minutes" in payload:
        mapped.setdefault("app", {})["open_position_weather_refresh_minutes"] = int(
            payload["open_position_weather_refresh_minutes"]
        )
    if "paper_stake_usd" in payload:
        mapped.setdefault("paper", {})["stake_usd"] = float(payload["paper_stake_usd"])
    if "paper_initial_capital" in payload:
        mapped.setdefault("paper", {})["initial_capital"] = float(payload["paper_initial_capital"])
    if "dashboard_port" in payload:
        mapped.setdefault("dashboard", {})["port"] = int(payload["dashboard_port"])
    return mapped


def _load_env_overrides() -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if os.getenv("WEATHER_LOG_LEVEL"):
        payload.setdefault("app", {})["log_level"] = os.getenv("WEATHER_LOG_LEVEL")
    if os.getenv("WEATHER_TEMPERATURE_SCAN_MINUTES"):
        payload.setdefault("app", {})["auto_temperature_scan_minutes"] = int(os.getenv("WEATHER_TEMPERATURE_SCAN_MINUTES", "120"))
    if os.getenv("WEATHER_TEMPERATURE_SCAN_SECONDS"):
        payload.setdefault("app", {})["auto_temperature_scan_seconds"] = int(os.getenv("WEATHER_TEMPERATURE_SCAN_SECONDS", "0"))
    if os.getenv("WEATHER_PRECIP_SCAN_MINUTES"):
        payload.setdefault("app", {})["auto_precipitation_scan_minutes"] = int(os.getenv("WEATHER_PRECIP_SCAN_MINUTES", "360"))
    if os.getenv("WEATHER_PRECIP_SCAN_SECONDS"):
        payload.setdefault("app", {})["auto_precipitation_scan_seconds"] = int(os.getenv("WEATHER_PRECIP_SCAN_SECONDS", "0"))
    if os.getenv("WEATHER_PRECIPITATION_ENABLED"):
        payload.setdefault("precipitation", {})["enabled"] = _is_truthy(os.getenv("WEATHER_PRECIPITATION_ENABLED"))
    if os.getenv("WEATHER_TEMPERATURE_MARKET_SCOPE"):
        payload.setdefault("temperature", {})["market_scope"] = os.getenv("WEATHER_TEMPERATURE_MARKET_SCOPE")
    if os.getenv("WEATHER_TEMPERATURE_MAX_FORECAST_TEMP_SPREAD_F"):
        payload.setdefault("strategy", {}).setdefault("temperature", {})["max_forecast_temp_spread_f"] = float(
            os.getenv("WEATHER_TEMPERATURE_MAX_FORECAST_TEMP_SPREAD_F", "0")
        )
    if os.getenv("WEATHER_TEMPERATURE_MAX_NO_EDGE_ABS"):
        payload.setdefault("strategy", {}).setdefault("temperature", {})["max_no_edge_abs"] = float(
            os.getenv("WEATHER_TEMPERATURE_MAX_NO_EDGE_ABS", "0")
        )
    if os.getenv("WEATHER_TEMPERATURE_MAX_NO_ENTRY_PRICE"):
        payload.setdefault("strategy", {}).setdefault("temperature", {})["max_no_entry_price"] = float(
            os.getenv("WEATHER_TEMPERATURE_MAX_NO_ENTRY_PRICE", "0")
        )
    if os.getenv("WEATHER_TEMPERATURE_NO_STOP_LOSS_PNL"):
        payload.setdefault("strategy", {}).setdefault("temperature", {})["no_stop_loss_pnl"] = float(
            os.getenv("WEATHER_TEMPERATURE_NO_STOP_LOSS_PNL", "0")
        )
    if os.getenv("WEATHER_TEMPERATURE_NO_STOP_LOSS_MIN_ENTRY_PRICE"):
        payload.setdefault("strategy", {}).setdefault("temperature", {})["no_stop_loss_min_entry_price"] = float(
            os.getenv("WEATHER_TEMPERATURE_NO_STOP_LOSS_MIN_ENTRY_PRICE", "0")
        )
    if os.getenv("WEATHER_RESOLUTION_CHECK_MINUTES"):
        payload.setdefault("app", {})["resolution_check_minutes"] = int(os.getenv("WEATHER_RESOLUTION_CHECK_MINUTES", "15"))
    if os.getenv("WEATHER_OPEN_POSITION_REVIEW_SECONDS"):
        payload.setdefault("app", {})["open_position_review_seconds"] = int(os.getenv("WEATHER_OPEN_POSITION_REVIEW_SECONDS", "15"))
    if os.getenv("WEATHER_OPEN_POSITION_WEATHER_REFRESH_MINUTES"):
        payload.setdefault("app", {})["open_position_weather_refresh_minutes"] = int(
            os.getenv("WEATHER_OPEN_POSITION_WEATHER_REFRESH_MINUTES", "60")
        )
    return payload


def _section(payload: dict[str, Any], key: str) -> dict[str, Any]:
    section = payload.get(key, {})
    if not isinstance(section, dict):
        raise ValueError(f"Config section '{key}' must be a mapping.")
    return section


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
