"""Application bootstrap and singleton access."""

from __future__ import annotations

import shutil
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import WeatherBotConfig, load_config
from .analysis_bundle import AnalysisBundleExporter
from .control_plane import ControlPlane
from .dashboard_state import DashboardStateService
from .live_api import LiveApiServer
from .paths import ANALYSIS_BUNDLE_ROOT, PID_LOCK_PATH, SCAN_EXPORTS_ROOT, STATE_EXPORT_PATH, TRACKER_DB_PATH
from .startup_health import (
    is_tracker_db_uninitialized,
    pick_best_candidate_tracker_db,
    run_startup_health_checks,
)
from .process_lock import PidLock, acquire_pid_lock
from .research.codex_automation import CodexAutomationManager
from .research.runtime import ResearchSnapshotProvider
from .runtime import WeatherRuntime
from .strategy import WeatherStrategyEngine
from .telegram_client import TelegramClient
from .tracker import WeatherTracker


@dataclass
class WeatherApplication:
    config: WeatherBotConfig
    tracker: WeatherTracker
    telegram: TelegramClient
    strategy: WeatherStrategyEngine
    runtime: WeatherRuntime
    control_plane: ControlPlane
    dashboard_state: DashboardStateService
    live_api: LiveApiServer
    pid_lock: PidLock

    def start_background_services(self) -> None:
        self.runtime.start_background_loops()
        if self.config.dashboard.enabled:
            self.dashboard_state.start()
            self.live_api.start_threaded()

    def stop_background_services(self) -> None:
        self.live_api.stop_threaded()
        self.dashboard_state.stop()
        self.runtime.stop_background_loops()
        self.pid_lock.release()

    def migrate_legacy_history(self) -> int:
        return self.tracker.migrate_legacy_edges()

    def run_forever(self) -> None:
        self.start_background_services()
        while True:
            time.sleep(1.0)


_APPLICATION: WeatherApplication | None = None


def _candidate_restore_enabled() -> bool:
    raw = str(os.getenv("WEATHER_STARTUP_ALLOW_CANDIDATE_RESTORE") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _restore_tracker_db_if_empty(candidate_db_path: Path, active_db_path: Path) -> tuple[bool, dict[str, str]]:
    if candidate_db_path == active_db_path:
        return False, {}
    if not candidate_db_path.exists() or not candidate_db_path.is_file():
        return False, {}
    active_parent = active_db_path.parent
    active_parent.mkdir(parents=True, exist_ok=True)
    backup_path = ""
    if active_db_path.exists():
        backup_path = str(
            active_db_path.with_suffix(
                active_db_path.suffix + f".preseed-backup-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
            )
        )
        try:
            shutil.copy2(active_db_path, backup_path)
        except OSError:
            return False, {}
    try:
        shutil.copy2(candidate_db_path, active_db_path)
    except OSError:
        return False, {}
    return True, {
        "tracker_db_path": str(active_db_path),
        "tracker_db_seeded_from": str(candidate_db_path),
        "tracker_db_backup_path": str(backup_path),
    }


def _resolve_tracker_db_path(active_config_path: str | None = None) -> tuple[Path, dict[str, object]]:
    active_db_path = Path(TRACKER_DB_PATH)
    startup_health = run_startup_health_checks(active_db_path, active_config_path=active_config_path)
    if is_tracker_db_uninitialized(startup_health):
        candidate_db_path = pick_best_candidate_tracker_db(active_db_path)
        if candidate_db_path is not None:
            startup_health.setdefault("checks", {}).setdefault("startup_checks", {})
            startup_health["checks"]["startup_checks"]["tracker_db_candidate_restore_available"] = str(candidate_db_path)
            startup_health["checks"]["startup_checks"]["tracker_db_candidate_restore_enabled"] = _candidate_restore_enabled()
            if _candidate_restore_enabled():
                restored, restore_info = _restore_tracker_db_if_empty(candidate_db_path, active_db_path)
                if restored:
                    startup_health = run_startup_health_checks(active_db_path, active_config_path=active_config_path)
                    startup_health.setdefault("checks", {}).setdefault("startup_checks", {})
                    startup_health["checks"]["startup_checks"]["tracker_db_restored_from_candidate"] = str(candidate_db_path)
                    startup_health["checks"]["startup_checks"]["tracker_db_restore_meta"] = dict(restore_info)
    return active_db_path, startup_health


def get_application() -> WeatherApplication:
    global _APPLICATION
    if _APPLICATION is not None:
        return _APPLICATION

    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config.app.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    tracker_db_path, startup_health = _resolve_tracker_db_path(config.config_path)
    tracker = WeatherTracker(tracker_db_path)
    tracker.ensure_paper_capital(config.paper.initial_capital)
    telegram = TelegramClient.from_env_or_options()
    research_provider = ResearchSnapshotProvider() if config.research.runtime_policy_enabled else None
    codex_manager = CodexAutomationManager() if config.research.enabled else None
    strategy = WeatherStrategyEngine(config, tracker, research_provider=research_provider)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=telegram,
        scan_export_root=SCAN_EXPORTS_ROOT,
        startup_health=startup_health,
    )
    analysis_exporter = AnalysisBundleExporter(
        tracker=tracker,
        runtime=runtime,
        bundle_root=ANALYSIS_BUNDLE_ROOT,
    )
    control_plane = ControlPlane(
        runtime,
        tracker,
        codex_manager=codex_manager,
        analysis_exporter=analysis_exporter,
    )
    dashboard_state = DashboardStateService(
        tracker=tracker,
        runtime=runtime,
        control_plane=control_plane,
        refresh_seconds=config.dashboard.refresh_seconds,
        codex_manager=codex_manager,
        state_export_path=STATE_EXPORT_PATH,
        analysis_exporter=analysis_exporter,
    )
    analysis_exporter.bind_dashboard_state(
        snapshot_refresher=dashboard_state.refresh_once,
        snapshot_getter=dashboard_state.get_state_threadsafe,
    )
    live_api = LiveApiServer(dashboard_state, host=config.dashboard.host, port=config.dashboard.port)
    pid_lock = acquire_pid_lock(PID_LOCK_PATH)
    _APPLICATION = WeatherApplication(
        config=config,
        tracker=tracker,
        telegram=telegram,
        strategy=strategy,
        runtime=runtime,
        control_plane=control_plane,
        dashboard_state=dashboard_state,
        live_api=live_api,
        pid_lock=pid_lock,
    )
    _APPLICATION.migrate_legacy_history()
    return _APPLICATION
