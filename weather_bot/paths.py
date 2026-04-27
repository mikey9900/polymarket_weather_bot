"""Shared filesystem paths for the weather platform."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data"
SHARED_DATA_ROOT = Path(os.getenv("WEATHER_SHARED_DATA_ROOT", str(DATA_ROOT)))
if not SHARED_DATA_ROOT.is_absolute():
    SHARED_DATA_ROOT = REPO_ROOT / SHARED_DATA_ROOT

CONFIG_ROOT = SHARED_DATA_ROOT / "config"
ACTIVE_CONFIG_PATH = CONFIG_ROOT / "active_config.yaml"
DEFAULT_CONFIG_TEMPLATE_PATH = REPO_ROOT / "weather_bot" / "config.default.yaml"
TRACKER_DB_PATH = SHARED_DATA_ROOT / "weatherbot.db"
WEATHER_CACHE_DB_PATH = SHARED_DATA_ROOT / "weather_cache.db"
PID_LOCK_PATH = SHARED_DATA_ROOT / "weatherbot.pid.lock"
LEGACY_TRACKING_PATH = Path(os.getenv("WEATHER_LEGACY_TRACKING_FILE", "/config/weather_bot_edges.json"))
LEGACY_TRACKING_FALLBACK_PATH = REPO_ROOT / "tracking" / "weather_bot_edges.json"

RESEARCH_ROOT = SHARED_DATA_ROOT / "research"
WAREHOUSE_PATH = RESEARCH_ROOT / "warehouse.duckdb"
RUNTIME_POLICY_PATH = RESEARCH_ROOT / "runtime_policy.json"
RESEARCH_REPORT_JSON_PATH = RESEARCH_ROOT / "research_report.json"
RESEARCH_REPORT_MD_PATH = RESEARCH_ROOT / "research_report.md"
RESEARCH_BUNDLE_PATH = RESEARCH_ROOT / "latest_bundle.json"
TUNER_STATE_PATH = RESEARCH_ROOT / "tuner_state.json"
TUNER_REPORT_JSON_PATH = RESEARCH_ROOT / "tuner_report.json"
TUNER_REPORT_MD_PATH = RESEARCH_ROOT / "tuner_report.md"
TUNER_ACTIVE_PATCH_PATH = RESEARCH_ROOT / "tuner_active_patch.diff"
CONFIG_CANDIDATES_ROOT = RESEARCH_ROOT / "candidates"
APPROVED_CONFIG_RECEIPT_PATH = CONFIG_ROOT / "last_apply_receipt.json"

CODEX_ROOT = SHARED_DATA_ROOT / "codex"
CODEX_QUEUE_ROOT = CODEX_ROOT / "queue"
CODEX_RUNS_ROOT = CODEX_ROOT / "runs"
CODEX_STATE_PATH = CODEX_ROOT / "state.json"
CODEX_LATEST_PATH = CODEX_ROOT / "latest.json"
CODEX_LOCK_PATH = CODEX_ROOT / "runner.lock"

EXPORT_ROOT = SHARED_DATA_ROOT / "exports"
STATE_EXPORT_PATH = EXPORT_ROOT / "dashboard_state.json"
SCAN_EXPORTS_ROOT = EXPORT_ROOT / "scan_runs"
ANALYSIS_BUNDLE_ROOT = EXPORT_ROOT / "analysis_bundle"
DROPBOX_SYNC_ROOT = REPO_ROOT / "dropbox_sync"


def ensure_data_dirs() -> None:
    for path in (
        SHARED_DATA_ROOT,
        CONFIG_ROOT,
        RESEARCH_ROOT,
        CONFIG_CANDIDATES_ROOT,
        CODEX_ROOT,
        CODEX_QUEUE_ROOT,
        CODEX_RUNS_ROOT,
        EXPORT_ROOT,
        SCAN_EXPORTS_ROOT,
        ANALYSIS_BUNDLE_ROOT,
    ):
        path.mkdir(parents=True, exist_ok=True)


def ensure_runtime_config(path_value: str | Path | None = None) -> Path:
    ensure_data_dirs()
    if path_value:
        path = Path(path_value)
        if not path.is_absolute():
            path = REPO_ROOT / path
    else:
        path = ACTIVE_CONFIG_PATH
    if not path.exists():
        shutil.copyfile(DEFAULT_CONFIG_TEMPLATE_PATH, path)
    return path


def candidate_legacy_tracking_paths() -> list[Path]:
    paths = []
    seen: set[Path] = set()
    for candidate in (LEGACY_TRACKING_PATH, LEGACY_TRACKING_FALLBACK_PATH):
        resolved = candidate.resolve(strict=False)
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        paths.append(resolved)
    return paths
