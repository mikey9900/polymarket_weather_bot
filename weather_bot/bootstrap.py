"""Application bootstrap and singleton access."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from .config import WeatherBotConfig, load_config
from .analysis_bundle import AnalysisBundleExporter
from .control_plane import ControlPlane
from .dashboard_state import DashboardStateService
from .live_api import LiveApiServer
from .paths import ANALYSIS_BUNDLE_ROOT, PID_LOCK_PATH, SCAN_EXPORTS_ROOT, STATE_EXPORT_PATH
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


def get_application() -> WeatherApplication:
    global _APPLICATION
    if _APPLICATION is not None:
        return _APPLICATION

    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config.app.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    tracker = WeatherTracker()
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
