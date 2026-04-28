from __future__ import annotations

import json
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from weather_bot.analysis_bundle import AnalysisBundleExporter
from weather_bot.dropbox_exports import resolve_dropbox_access_token, sync_dropbox_latest_bundle_to_local
from scanner.weather_event_scanner import cities_for_temperature_market_scope
from weather_bot.config import load_config
from weather_bot.control_plane import ControlPlane, ControlRequest
from weather_bot.dashboard_state import DashboardStateService
from weather_bot.live_api import render_dashboard_html
from weather_bot.live_api import LiveApiServer
from weather_bot.models import ForecastSnapshot, ScanBatch, WeatherSignal
from weather_bot.paths import DEFAULT_CONFIG_TEMPLATE_PATH
from weather_bot.runtime import WeatherRuntime, _scheduled_interval_seconds
from weather_bot.strategy import WeatherStrategyEngine
from weather_bot.temperature import scan_temperature_signals
from weather_bot.telegram_client import TelegramClient
from weather_bot.tracker import WeatherTracker


def _write_config(tmp_path: Path) -> Path:
    payload = yaml.safe_load(DEFAULT_CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    config_path = tmp_path / "active_config.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return config_path


def test_default_config_disables_precipitation_scans(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())

    assert config.precipitation.enabled is False
    assert runtime.get_status_snapshot()["precipitation_enabled"] is False


def test_load_config_accepts_precipitation_enabled_ha_override(tmp_path: Path):
    config_path = _write_config(tmp_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(json.dumps({"precipitation_enabled": True}), encoding="utf-8")

    config = load_config(config_path, ha_options_path=options_path)

    assert config.precipitation.enabled is True


def test_load_config_accepts_temperature_market_scope_ha_override(tmp_path: Path):
    config_path = _write_config(tmp_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(json.dumps({"temperature_market_scope": "north_america"}), encoding="utf-8")

    config = load_config(config_path, ha_options_path=options_path)

    assert config.temperature.market_scope == "north_america"


def test_temperature_market_scope_city_groups_are_split_cleanly():
    north_america = set(cities_for_temperature_market_scope("north_america"))
    international = set(cities_for_temperature_market_scope("international"))
    both = set(cities_for_temperature_market_scope("both"))

    assert {"nyc", "toronto", "mexico-city"}.issubset(north_america)
    assert "london" not in north_america
    assert "london" in international
    assert "nyc" not in international
    assert north_america | international == both


def test_runtime_startup_respects_precipitation_config_over_saved_runtime_state(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.set_runtime_state("runtime_status", {"precipitation_enabled": True})
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)

    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())

    assert runtime.get_status_snapshot()["precipitation_enabled"] is False


def test_runtime_startup_clears_stale_scan_state_and_allows_new_queue(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.set_runtime_state(
        "runtime_status",
        {
            "scan_in_progress": True,
            "scan_queue_depth": 1,
            "pending_scan_types": ["temperature"],
            "active_scan_type": "temperature",
            "active_scan_started_at": "2026-04-25T12:00:00+00:00",
            "scan_worker_healthy": True,
            "last_temperature_scan_status": "running",
            "last_temperature_scan_reason": "operator",
            "open_position_review_in_progress": True,
            "last_open_position_review_status": "running",
        },
    )
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)

    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())

    state = runtime.get_status_snapshot()
    assert state["scan_in_progress"] is False
    assert state["scan_queue_depth"] == 0
    assert state["pending_scan_types"] == []
    assert state["active_scan_type"] is None
    assert state["active_scan_started_at"] is None
    assert state["scan_worker_healthy"] is False
    assert state["last_temperature_scan_status"] == "interrupted"
    assert state["last_temperature_scan_error"] == "Previous process exited before this scan finished."
    assert state["last_open_position_review_status"] == "interrupted"
    assert state["last_open_position_review_error"] == "Previous process exited before the open-position review finished."
    assert state["last_scan_worker_error"] == "Recovered stale scan state for: Temperature."

    persisted = tracker.get_runtime_state("runtime_status")
    assert persisted["scan_in_progress"] is False
    assert persisted["active_scan_type"] is None
    assert persisted["pending_scan_types"] == []

    queued = runtime.request_scan("temperature", send_alerts=False, reason="operator")
    assert queued["queued"] is True


def test_request_scan_ignores_stray_active_type_when_scan_flag_is_clear(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    runtime._update_state(active_scan_type="temperature", scan_in_progress=False)

    first = runtime.request_scan("temperature", send_alerts=False, reason="operator")
    second = runtime.request_scan("temperature", send_alerts=False, reason="operator")

    assert first["queued"] is True
    assert second["queued"] is False


def _signal(
    key: str = "rt-1",
    *,
    direction: str = "YES",
    market_slug: str | None = None,
    market_type: str = "temperature",
    city_slug: str = "nyc",
    event_date: str = "2026-04-25",
    event_title: str = "Highest temperature in NYC on April 25",
    label: str = "70-71F",
    market_prob: float = 0.25,
    forecast_prob: float = 0.80,
    edge: float = 0.55,
    edge_abs: float = 0.55,
) -> WeatherSignal:
    return WeatherSignal(
        signal_key=key,
        market_type=market_type,
        event_title=event_title,
        market_slug=market_slug or f"market-{key}",
        event_slug=f"event-{key}",
        city_slug=city_slug,
        event_date=event_date,
        label=label,
        direction=direction,
        market_prob=market_prob,
        forecast_prob=forecast_prob,
        edge=edge,
        edge_abs=edge_abs,
        edge_size="large",
        confidence="confirmed",
        source_count=3,
        liquidity=600.0,
        time_to_resolution_s=4 * 3600.0,
        source_dispersion_pct=0.02,
        score=0.85,
        forecast_snapshot=ForecastSnapshot(
            market_type=market_type,
            city_slug=city_slug,
            event_date=event_date,
            unit="F",
            om_temp=72.0,
            vc_temp=73.0,
            source_probabilities={"openmeteo": 0.8, "visual_crossing": 0.79},
        ),
        raw_payload={"event_title": event_title, "label": label, "direction": "YES"},
    )


def _batch(signal: WeatherSignal | None = None, *, scan_type: str = "temperature", error_count: int = 0) -> ScanBatch:
    timestamp = signal.created_at if signal is not None else "2026-04-24T12:00:00+00:00"
    signals = [signal] if signal is not None else []
    return ScanBatch(
        scan_type=scan_type,
        signals=signals,
        total_events=1 if signal is not None else 0,
        processed_events=1 if signal is not None else 0,
        flagged_events=1 if signal is not None else 0,
        skipped_events=0,
        started_at=timestamp,
        finished_at=timestamp,
        error_count=error_count,
        error_samples=["provider timeout"] if error_count else [],
    )


def test_runtime_settles_market_and_updates_pnl(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal()], auto_trade_enabled=True)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        resolution_fetcher=lambda slug: "YES",
    )

    outcomes = runtime.settle_due_positions(send_alerts=False)

    assert len(outcomes) == 1
    assert outcomes[0].resolved_positions == 1
    assert tracker.get_paper_stats()["total_pnl"] > 0
    assert len(tracker.get_recent_resolutions()) == 1


def test_dashboard_control_updates_state(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"action": "stop"})

    assert response["ok"] is True
    assert response["state"]["controls"]["state"] == "paused"
    assert response["state"]["controls"]["last_action"] == "stop"
    assert response["state"]["controls"]["last_action_at"]
    assert response["state"]["recent_operator_actions"][0]["action"] == "stop"


def test_dashboard_exports_analysis_bundle(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    scan_export_root = tmp_path / "scan_runs"
    scan_export_root.mkdir(parents=True, exist_ok=True)
    (scan_export_root / "20260427T120000_temperature_completed.json").write_text(
        json.dumps({"scan_type": "temperature", "status": "completed"}),
        encoding="utf-8",
    )
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        scan_export_root=scan_export_root,
    )
    analysis_exporter = AnalysisBundleExporter(
        tracker=tracker,
        runtime=runtime,
        bundle_root=tmp_path / "analysis_bundle",
    )
    control_plane = ControlPlane(runtime, tracker, analysis_exporter=analysis_exporter)
    dashboard = DashboardStateService(
        tracker=tracker,
        runtime=runtime,
        control_plane=control_plane,
        state_export_path=tmp_path / "dashboard_state.json",
        analysis_exporter=analysis_exporter,
    )
    analysis_exporter.bind_dashboard_state(
        snapshot_refresher=dashboard.refresh_once,
        snapshot_getter=dashboard.get_state_threadsafe,
    )

    response = dashboard.apply_control_threadsafe({"action": "export_analysis_bundle"})

    assert response["ok"] is True
    bundle_path = Path(response["state"]["exports"]["last_analysis_bundle_path"])
    latest_bundle_path = Path(response["state"]["exports"]["latest_analysis_bundle_path"])
    latest_index_path = Path(response["state"]["exports"]["latest_analysis_index_path"])
    latest_report_path = Path(response["state"]["exports"]["latest_analysis_report_path"])
    report_path = Path(response["state"]["exports"]["last_analysis_report_path"])
    assert bundle_path.exists()
    assert latest_bundle_path.exists()
    assert latest_index_path.exists()
    assert latest_report_path.exists()
    assert report_path.exists()
    with zipfile.ZipFile(bundle_path) as archive:
        names = set(archive.namelist())
    assert "dashboard_state.json" in names
    assert "runtime_status.json" in names
    assert "weatherbot.db" in names
    assert "weather_cache.db" in names
    assert "analysis_report.xlsx" in names
    assert "manifest.json" in names
    assert "scan_runs/20260427T120000_temperature_completed.json" in names
    latest_index = json.loads(latest_index_path.read_text(encoding="utf-8"))
    assert latest_index["label"] == "WEATHER-BOT"
    assert latest_index["latest_bundle"]["local_path"] == str(latest_bundle_path)
    assert latest_index["archive_bundle"]["local_path"] == str(bundle_path)
    assert latest_index["latest_report"]["local_path"] == str(latest_report_path)
    assert latest_index["archive_report"]["local_path"] == str(report_path)


def test_analysis_bundle_export_updates_dropbox_latest_pointer(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    uploads: list[tuple[str, str]] = []

    def fake_upload(local_path, dropbox_path, dropbox_auth):
        uploads.append((Path(local_path).name, str(dropbox_path)))
        return {"ok": True, "status": 200, "path": str(dropbox_path)}

    def fake_link(dropbox_path, dropbox_auth):
        return f"https://dropbox.test{dropbox_path}?dl=0"

    monkeypatch.setattr("weather_bot.analysis_bundle.dropbox_upload_file", fake_upload)
    monkeypatch.setattr("weather_bot.analysis_bundle.dropbox_create_or_get_shared_link", fake_link)

    analysis_exporter = AnalysisBundleExporter(
        tracker=tracker,
        runtime=runtime,
        bundle_root=tmp_path / "analysis_bundle",
        dropbox_auth={"access_token": "token", "_cached_access_token": "token", "_cached_expires_at": 9999999999},
        dropbox_root="/weather-bot",
    )
    control_plane = ControlPlane(runtime, tracker, analysis_exporter=analysis_exporter)
    dashboard = DashboardStateService(
        tracker=tracker,
        runtime=runtime,
        control_plane=control_plane,
        state_export_path=tmp_path / "dashboard_state.json",
        analysis_exporter=analysis_exporter,
    )
    analysis_exporter.bind_dashboard_state(
        snapshot_refresher=dashboard.refresh_once,
        snapshot_getter=dashboard.get_state_threadsafe,
    )

    response = dashboard.apply_control_threadsafe({"action": "export_analysis_bundle"})

    exports = response["state"]["exports"]
    assert response["ok"] is True
    assert exports["analysis_dropbox_enabled"] is True
    assert exports["last_analysis_bundle_dropbox_path"] == "/weather-bot/latest/WEATHER-BOT_latest_bundle.zip"
    assert exports["last_analysis_index_dropbox_path"] == "/weather-bot/latest/WEATHER-BOT_latest_index.json"
    assert exports["last_analysis_report_dropbox_path"] == "/weather-bot/latest/WEATHER-BOT_latest_report.xlsx"
    assert exports["last_analysis_bundle_dropbox_url"] == "https://dropbox.test/weather-bot/latest/WEATHER-BOT_latest_bundle.zip?dl=0"
    assert exports["last_analysis_index_dropbox_url"] == "https://dropbox.test/weather-bot/latest/WEATHER-BOT_latest_index.json?dl=0"
    assert exports["last_analysis_report_dropbox_url"] == "https://dropbox.test/weather-bot/latest/WEATHER-BOT_latest_report.xlsx?dl=0"
    assert exports["last_analysis_bundle_dropbox_error"] is None
    assert "Dropbox latest bundle, report, and index are now in sync." in response["message"]
    assert "Report: /weather-bot/latest/WEATHER-BOT_latest_report.xlsx." in response["message"]
    latest_index = json.loads(Path(exports["latest_analysis_index_path"]).read_text(encoding="utf-8"))
    assert latest_index["dropbox"]["latest_bundle_url"] == exports["last_analysis_bundle_dropbox_url"]
    assert latest_index["dropbox"]["latest_index_url"] == exports["last_analysis_index_dropbox_url"]
    assert latest_index["dropbox"]["latest_report_url"] == exports["last_analysis_report_dropbox_url"]
    assert {path for _, path in uploads} >= {
        "/weather-bot/daily-archives/" + Path(exports["last_analysis_bundle_path"]).name,
        "/weather-bot/daily-archives/" + Path(exports["last_analysis_report_path"]).name,
        "/weather-bot/latest/WEATHER-BOT_latest_bundle.zip",
        "/weather-bot/latest/WEATHER-BOT_latest_index.json",
        "/weather-bot/latest/WEATHER-BOT_latest_report.xlsx",
    }


def test_sync_dropbox_latest_bundle_to_local_extracts_bundle(tmp_path: Path, monkeypatch):
    def fake_download(remote_path, dropbox_auth, local_path):
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if str(remote_path).endswith("_latest_bundle.zip"):
            with zipfile.ZipFile(local_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                archive.writestr("manifest.json", json.dumps({"label": "WEATHER-BOT"}))
        else:
            local_path.write_text(json.dumps({"label": "WEATHER-BOT"}), encoding="utf-8")
        return {"ok": True, "status": 200, "path": str(local_path)}

    monkeypatch.setattr("weather_bot.dropbox_exports.dropbox_download_file", fake_download)

    result = sync_dropbox_latest_bundle_to_local(
        dropbox_token="token",
        dropbox_root="/weather-bot",
        output_dir=tmp_path / "dropbox_sync",
        label="WEATHER-BOT",
    )

    assert result["ok"] is True
    assert Path(result["downloads"]["latest_bundle_zip"]["path"]).exists()
    assert Path(result["downloads"]["latest_index_json"]["path"]).exists()
    assert Path(result["downloads"]["latest_report_xlsx"]["path"]).exists()
    assert result["extraction_error"] is None
    assert Path(result["extracted_bundle_dir"], "manifest.json").exists()


def test_dropbox_refresh_error_surfaces_oauth_reason(monkeypatch):
    class FakeResponse:
        status_code = 400
        text = json.dumps({"error": "invalid_grant", "error_description": "refresh token is malformed"})

        def json(self):
            return json.loads(self.text)

    monkeypatch.setattr("weather_bot.dropbox_exports.requests.post", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(RuntimeError) as excinfo:
        resolve_dropbox_access_token(
            {
                "refresh_token": "bad-token",
                "app_key": "bad-key",
                "app_secret": "bad-secret",
                "_cached_access_token": None,
                "_cached_expires_at": None,
            }
        )

    message = str(excinfo.value)
    assert "Dropbox OAuth refresh failed (400)" in message
    assert "refresh token is malformed" in message


def test_dashboard_rejects_empty_control_action(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({})

    assert response["ok"] is False
    assert response["status"] == 400
    assert "empty" in response["message"].lower()


def test_dashboard_posts_controls_with_recovery_and_query_fallback():
    html = render_dashboard_html()

    assert "TRADE_COLLAPSE_KEY" in html
    assert "weather-ops-open-trade-collapse-v1" in html
    assert "toggleTradeExpanded" in html
    assert "tradeToggleKey" in html
    assert "trade-summary-pills" in html
    assert "trade-expanded-body" in html
    assert "aria-expanded=" in html
    assert 'tradeSummaryPill("Mark P/L"' in html
    assert 'tradeSummaryPill("Model P/L"' in html
    assert 'tradeSummaryPill("Edge"' in html
    assert 'tradeSummaryPill("Direction"' not in html
    assert 'tradeSummaryPill("Target"' not in html
    assert 'tradeSummaryPill("Confidence"' not in html
    assert 'tradeSummaryPill("Model Odds"' not in html
    assert "buildControlRequestSpec" in html
    assert "buildControlRoutePlans" in html
    assert "appendQuery" in html
    assert "endpointCandidates" in html
    assert 'JSON.stringify(plan.body)' in html
    assert "dashboardBaseCandidates" in html
    assert "recoverControlState" in html
    assert "renderControlDiagnostics" in html
    assert 'id="control-diagnostics"' in html
    assert "openProposalModal" in html
    assert "copyExportPath" in html
    assert "copyBundlePath" in html
    assert "copyCloudLink" in html
    assert "Showing ${shown} of ${total} open trades" in html
    assert "set_paper_entry_min_edge_abs" in html
    assert "set_temperature_market_scope" in html
    assert "export_analysis_bundle" in html
    assert "setEdgeLimit()" in html
    assert "set_temperature_scan_interval_minutes" in html
    assert "set_precipitation_scan_interval_minutes" in html
    assert "setTempCadence()" in html
    assert "setRainCadence()" in html
    assert "setTempScope()" in html
    assert "marketScopeOptions" in html
    assert "marketScopeLabel" in html
    assert "EXPORT BUNDLE" in html
    assert "COPY BUNDLE PATH" in html
    assert "COPY CLOUD LINK" in html


def test_dashboard_apply_control_returns_json_when_refresh_fails(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    dashboard.refresh_once()

    def boom() -> None:
        raise RuntimeError("refresh failed")

    monkeypatch.setattr(dashboard, "refresh_once", boom)

    response = dashboard.apply_control_threadsafe({"action": "stop"})

    assert response["ok"] is True
    assert response["status"] == 200
    assert "State refresh warning" in response["message"]
    assert response["refresh_error"] == "RuntimeError: refresh failed"
    assert response["state"]["controls"]["state"] == "running"


def test_dashboard_scan_control_returns_fast_state_without_full_refresh(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    dashboard.refresh_once()

    def boom() -> None:
        raise AssertionError("refresh_once should not run for queued scan controls")

    monkeypatch.setattr(dashboard, "refresh_once", boom)

    response = dashboard.apply_control_threadsafe({"action": "scan_temperature"})

    assert response["ok"] is True
    assert response["status"] == 202
    assert response["state"]["controls"]["scan_queue_depth"] == 1
    assert response["state"]["controls"]["pending_scan_types"] == ["temperature"]
    assert response["state"]["runtime"]["scan_queue_depth"] == 1


def test_live_api_control_returns_json_when_handler_raises(tmp_path: Path):
    class BrokenDashboardState:
        def get_state_threadsafe(self):
            return {}

        def get_history_threadsafe(self):
            return []

        def apply_control_threadsafe(self, payload):
            raise RuntimeError("boom")

    server = LiveApiServer(BrokenDashboardState(), host="127.0.0.1", port=0)
    server.start_threaded()
    try:
        port = int(server._server.server_address[1])  # type: ignore[union-attr]
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/control",
            data=json.dumps({"action": "stop"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(request, timeout=5)
            assert False, "Expected HTTP 500 response"
        except urllib.error.HTTPError as exc:
            payload = json.loads(exc.read().decode("utf-8"))
    finally:
        server.stop_threaded()

    assert payload["ok"] is False
    assert payload["status"] == 500
    assert "Control handler crashed: RuntimeError: boom" in payload["message"]


def test_live_api_control_accepts_query_params_when_body_missing(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    dashboard.refresh_once()

    server = LiveApiServer(dashboard, host="127.0.0.1", port=0)
    server.start_threaded()
    try:
        port = int(server._server.server_address[1])  # type: ignore[union-attr]
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/control/set_paper_max_open_positions?limit=80",
            data=b"",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.stop_threaded()

    assert payload["ok"] is True
    assert payload["state"]["controls"]["paper_max_open_positions"] == 80


def test_live_api_manual_close_accepts_query_params_when_body_missing(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("manual-close-query-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    dashboard.refresh_once()
    open_positions = tracker.get_dashboard_paper_positions(limit=12, status="open")

    server = LiveApiServer(dashboard, host="127.0.0.1", port=0)
    server.start_threaded()
    try:
        port = int(server._server.server_address[1])  # type: ignore[union-attr]
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/control/close_position?position_id={open_positions[0]['id']}&reason=manual_test_close_query",
            data=b"",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    finally:
        server.stop_threaded()

    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]
    assert payload["ok"] is True
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reason"] == "manual_test_close_query"


def test_control_infers_open_cap_action_from_actionless_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"value": {"limit": "80"}})

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 80


def test_control_infers_temperature_market_scope_from_actionless_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"value": {"temperature_market_scope": "north_america"}})

    assert response["ok"] is True
    assert response["state"]["controls"]["temperature_market_scope"] == "north_america"


def test_control_infers_manual_close_action_from_actionless_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("manual-close-infer-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    open_positions = tracker.get_dashboard_paper_positions(limit=12, status="open")

    response = dashboard.apply_control_threadsafe(
        {"value": {"position_id": str(open_positions[0]["id"]), "reason": "manual_test_close_inferred"}}
    )
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert response["ok"] is True
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reason"] == "manual_test_close_inferred"


def test_dashboard_exposes_recent_resolutions(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("resolved-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        resolution_fetcher=lambda slug: "YES",
    )
    runtime.settle_due_positions(send_alerts=False)
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()

    assert len(state["recent_resolutions"]) == 1
    assert state["recent_resolutions"][0]["status"] == "resolved"
    assert state["recent_resolutions"][0]["resolution"] == "YES"
    assert state["recent_resolutions"][0]["outcome_label"] == "Resolved YES"


def test_recent_operator_actions_order_by_id_when_timestamps_match(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    same_stamp = "2026-04-25T20:39:51+00:00"
    tracker.conn.execute(
        "INSERT INTO operator_events(action, payload_json, created_at) VALUES (?, ?, ?)",
        ("older_action", json.dumps({"message": "older"}), same_stamp),
    )
    tracker.conn.execute(
        "INSERT INTO operator_events(action, payload_json, created_at) VALUES (?, ?, ?)",
        ("newer_action", json.dumps({"message": "newer"}), same_stamp),
    )
    tracker.conn.commit()

    actions = tracker.get_recent_operator_actions(limit=2)

    assert actions[0]["action"] == "newer_action"
    assert actions[1]["action"] == "older_action"


def test_dashboard_exposes_recent_outcomes_with_trade_details_and_limit(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(5000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    for idx in range(12):
        strategy.process_signals([_signal(f"outcome-{idx}")], auto_trade_enabled=True)

    open_positions = tracker.get_dashboard_paper_positions(limit=20, status="open")
    assert len(open_positions) == 12

    for idx, position in enumerate(open_positions):
        result = tracker.close_paper_position(
            int(position["id"]),
            exit_price=0.56,
            reason=f"manual_outcome_{idx}",
            mark_probability=0.64,
            edge_abs=0.14,
            final_score=0.73,
            mark_reason=f"closed outcome {idx}",
        )
        assert result is not None

    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()
    outcomes = state["recent_outcomes"]

    assert len(outcomes) == 10
    assert all(item["status"] == "closed" for item in outcomes)
    latest = outcomes[0]
    assert latest["market_slug"].startswith("market-outcome-")
    assert latest["signal_key"].startswith("outcome-")
    assert latest["resolved_at"]
    assert latest["exit_reason"].startswith("manual_outcome_")
    assert latest["mark_reason"].startswith("closed outcome ")
    assert latest["exit_fee_paid"] is not None
    assert latest["net_exit_payout"] is not None
    assert latest["gross_exit_payout"] is not None
    assert latest["net_exit_payout"] <= latest["gross_exit_payout"]
    assert latest["decision_reason"]
    assert isinstance(latest["decision_metadata"], dict)


def test_dashboard_exposes_enriched_open_trade_cards(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("open-card-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()

    assert len(state["open_positions"]) == 1
    trade = state["open_positions"][0]
    assert trade["event_title"] == "Highest temperature in NYC on April 25"
    assert trade["target_label"] == "70-71F"
    assert trade["outcome_probability"] == 0.8
    assert trade["entry_fee_paid"] > 0
    assert trade["estimated_exit_fee_paid"] > 0
    assert trade["expected_value_pnl"] > 0
    assert trade["mark_to_market_pnl"] is not None
    assert trade["mark_to_market_pnl"] < (trade["mark_to_market_payout"] - trade["cost"])
    assert trade["mark_to_market_mode"] == "reviewed_contract_mark"
    assert trade["holding_seconds"] is not None


def test_control_payload_exposes_paper_metrics(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(750.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)

    payload = control_plane.build_controls_payload()

    assert payload["paper_balance"] == 750.0
    assert payload["paper_initial_capital"] == 750.0
    assert payload["paper_max_open_positions"] == 20
    assert payload["paper_entry_min_edge_abs"] == config.strategy.temperature.min_edge_abs
    assert payload["temperature_scan_interval_minutes"] == config.app.auto_temperature_scan_minutes
    assert payload["precipitation_scan_interval_minutes"] == config.app.auto_precipitation_scan_minutes
    assert payload["paper_open_positions"] == 0


def test_load_config_reads_second_level_scan_overrides(tmp_path: Path):
    config_path = _write_config(tmp_path)
    options_path = tmp_path / "options.json"
    options_path.write_text(
        json.dumps(
            {
                "temperature_scan_seconds": 5,
                "precipitation_scan_seconds": 12,
                "resolution_check_minutes": 5,
                "open_position_review_seconds": 30,
                "open_position_weather_refresh_minutes": 45,
            }
        ),
        encoding="utf-8",
    )


def _precip_signal(
    key: str = "rain-1",
    *,
    market_slug: str | None = None,
    market_prob: float = 0.30,
    forecast_prob: float = 0.78,
) -> WeatherSignal:
    return WeatherSignal(
        signal_key=key,
        market_type="precipitation",
        event_title="Rainfall in NYC for April 2026",
        market_slug=market_slug or f"precip-{key}",
        event_slug=f"precip-event-{key}",
        city_slug="nyc",
        event_date="2026-04-01",
        label="1 to 2 inches",
        direction="YES",
        market_prob=market_prob,
        forecast_prob=forecast_prob,
        edge=forecast_prob - market_prob,
        edge_abs=abs(forecast_prob - market_prob),
        edge_size="large",
        confidence="confirmed",
        source_count=2,
        liquidity=450.0,
        time_to_resolution_s=None,
        source_dispersion_pct=0.02,
        score=0.81,
        forecast_snapshot=ForecastSnapshot(
            market_type="precipitation",
            city_slug="nyc",
            event_date="2026-04-01",
            unit="in",
            observed_value=0.8,
            om_temp=1.7,
            vc_temp=1.6,
            source_probabilities={"openmeteo": forecast_prob, "visual_crossing": 0.76},
        ),
        raw_payload={"event_title": "Rainfall in NYC for April 2026", "label": "1 to 2 inches", "direction": "YES"},
    )

    config = load_config(config_path, ha_options_path=options_path)

    assert config.app.auto_temperature_scan_seconds == 5
    assert config.app.auto_precipitation_scan_seconds == 12
    assert config.app.resolution_check_minutes == 5
    assert config.app.open_position_review_seconds == 30
    assert config.app.open_position_weather_refresh_minutes == 45


def test_scheduled_interval_seconds_prefers_fast_second_overrides():
    assert _scheduled_interval_seconds(5, 120, minimum_seconds=5) == 5
    assert _scheduled_interval_seconds(0, 15, minimum_seconds=5) == 900
    assert _scheduled_interval_seconds(1, 15, minimum_seconds=5) == 5
    assert _scheduled_interval_seconds(15, 0, minimum_seconds=10) == 15
    assert _scheduled_interval_seconds(5, 0, minimum_seconds=10) == 10


def test_runtime_processes_queued_scan_and_exports_results(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    signal = _signal("queued-1")
    export_root = tmp_path / "exports"
    calls: list[int] = []

    def temperature_scanner(*, limit: int = 300) -> ScanBatch:
        calls.append(limit)
        return _batch(signal, error_count=1)

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=temperature_scanner,
        scan_export_root=export_root,
    )

    try:
        first = runtime.request_scan("temperature", send_alerts=False, reason="operator")
        second = runtime.request_scan("temperature", send_alerts=False, reason="operator")
        assert first["queued"] is True
        assert second["queued"] is False

        runtime.start_background_loops()
        assert runtime.wait_for_idle(timeout=5.0) is True
    finally:
        runtime.stop_background_loops()

    assert calls == [config.temperature.scan_limit]
    state = runtime.get_status_snapshot()
    assert state["last_temperature_scan_status"] == "completed"
    assert state["last_temperature_scan_reason"] == "operator"
    assert state["last_temperature_error_count"] == 1
    assert state["scan_queue_depth"] == 0
    assert tracker.get_paper_stats()["open_positions"] == 1

    export_files = list(export_root.glob("*.json"))
    assert len(export_files) == 1
    payload = json.loads(export_files[0].read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert payload["reason"] == "operator"
    assert payload["batch"]["error_count"] == 1
    assert payload["opened_count"] == 1


def test_control_updates_open_position_cap(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"action": "set_paper_max_open_positions", "value": 40})

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 40
    assert runtime.get_status_snapshot()["paper_max_open_positions"] == 40
    assert strategy.paper_max_open_positions == 40


def test_control_updates_temperature_scan_interval_minutes(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"action": "set_temperature_scan_interval_minutes", "value": {"temperature_scan_minutes": "30"}})

    assert response["ok"] is True
    assert response["state"]["controls"]["temperature_scan_interval_minutes"] == 30
    assert runtime.get_status_snapshot()["auto_temperature_scan_interval_seconds"] == 1800


def test_control_updates_precipitation_scan_interval_minutes(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe(
        {"action": "set_precipitation_scan_interval_minutes", "value": {"precipitation_scan_minutes": "60"}}
    )

    assert response["ok"] is True
    assert response["state"]["controls"]["precipitation_scan_interval_minutes"] == 60
    assert runtime.get_status_snapshot()["auto_precipitation_scan_interval_seconds"] == 3600


def test_control_updates_paper_entry_edge_floor(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"action": "set_paper_entry_min_edge_abs", "value": {"edge_pct": "20"}})

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_entry_min_edge_abs"] == 0.2
    assert runtime.get_status_snapshot()["paper_entry_min_edge_abs"] == 0.2
    assert runtime.get_status_snapshot()["paper_entry_min_edge_abs_override"] == 0.2
    assert strategy.paper_entry_min_edge_abs == 0.2


def test_control_updates_open_position_cap_from_string_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe(
        {"action": "set_paper_max_open_positions", "value": {"limit": "60"}}
    )

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 60
    assert runtime.get_status_snapshot()["paper_max_open_positions"] == 60
    assert strategy.paper_max_open_positions == 60


def test_control_updates_open_position_cap_from_stringified_json_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe(
        {"action": "set_paper_max_open_positions", "value": '{"limit":"70"}'}
    )

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 70
    assert runtime.get_status_snapshot()["paper_max_open_positions"] == 70
    assert strategy.paper_max_open_positions == 70


def test_control_updates_open_position_cap_from_top_level_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe(
        {"action": "set_paper_max_open_positions", "limit": "80"}
    )

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 80
    assert runtime.get_status_snapshot()["paper_max_open_positions"] == 80
    assert strategy.paper_max_open_positions == 80


def test_control_updates_open_position_cap_from_nested_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe(
        {"action": "set_paper_max_open_positions", "value": {"payload": {"limit": "90"}}}
    )

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 90
    assert runtime.get_status_snapshot()["paper_max_open_positions"] == 90
    assert strategy.paper_max_open_positions == 90


def test_control_updates_open_position_cap_from_open_position_cap_alias(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe(
        {"action": "set_paper_max_open_positions", "value": {"open_position_cap": "95"}}
    )

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 95
    assert runtime.get_status_snapshot()["paper_max_open_positions"] == 95
    assert strategy.paper_max_open_positions == 95


def test_control_infers_open_cap_action_from_open_position_cap_alias_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)

    response = dashboard.apply_control_threadsafe({"open_position_cap": "85"})

    assert response["ok"] is True
    assert response["state"]["controls"]["paper_max_open_positions"] == 85
    assert runtime.get_status_snapshot()["paper_max_open_positions"] == 85
    assert strategy.paper_max_open_positions == 85


def test_runtime_respects_live_open_position_cap_override(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    runtime.set_paper_max_open_positions(1)

    results = strategy.process_signals([_signal("limit-a"), _signal("limit-b")], auto_trade_enabled=True)

    assert results[0].position is not None
    assert results[1].position is None
    assert "Maximum open paper positions reached." in results[1].decision.reason


def test_dashboard_exports_snapshot_and_control_queue_state(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    export_path = tmp_path / "dashboard_state.json"
    dashboard = DashboardStateService(
        tracker=tracker,
        runtime=runtime,
        control_plane=control_plane,
        state_export_path=export_path,
    )

    result = control_plane.apply_sync(ControlRequest(action="scan_temperature"))
    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()
    payload = json.loads(export_path.read_text(encoding="utf-8"))

    assert result.ok is True
    assert result.status == 202
    assert state["controls"]["scan_queue_depth"] == 1
    assert state["controls"]["pending_scan_types"] == ["temperature"]
    assert payload["exports"]["dashboard_state_error"] is None
    assert payload["controls"]["scan_queue_depth"] == 1


def test_dashboard_export_failure_is_non_fatal(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(500.0)
    strategy = WeatherStrategyEngine(config, tracker)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(
        tracker=tracker,
        runtime=runtime,
        control_plane=control_plane,
        state_export_path=tmp_path,
    )

    dashboard.refresh_once()
    state = dashboard.get_state_threadsafe()

    assert state["controls"]["state"] == "running"
    assert state["exports"]["dashboard_state_error"] is not None


def test_runtime_scan_export_failure_is_non_fatal(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    export_root = tmp_path / "scan_exports"
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=lambda *, limit=300: _batch(_signal("scan-export-failure")),
        scan_export_root=export_root,
    )
    original_write_text = Path.write_text

    def flaky_write_text(path: Path, *args, **kwargs):
        if path.parent == export_root:
            raise OSError("disk full")
        return original_write_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", flaky_write_text)

    batch, results = runtime.run_temperature_scan(send_alerts=False)
    state = runtime.get_status_snapshot()

    assert len(batch.signals) == 1
    assert len(results) == 1
    assert state["last_temperature_scan_status"] == "completed"
    assert state["last_scan_export_error"] == "disk full"


def test_scan_temperature_signals_passes_market_scope_to_event_fetcher(monkeypatch):
    calls: list[tuple[int, str]] = []

    def fake_fetch_weather_events(*, limit=300, market_scope="both"):
        calls.append((int(limit), str(market_scope)))
        return []

    monkeypatch.setattr("weather_bot.temperature.fetch_weather_events", fake_fetch_weather_events)

    batch = scan_temperature_signals(limit=12, market_scope="international")

    assert batch.total_events == 0
    assert calls == [(12, "international")]


def test_runtime_passes_temperature_market_scope_to_supported_scanner(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    calls: list[tuple[int, str]] = []

    def scoped_scanner(*, limit=300, market_scope="both"):
        calls.append((int(limit), str(market_scope)))
        return _batch(_signal("scope-pass-through"))

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=scoped_scanner,
    )

    runtime.set_temperature_market_scope("north_america")
    batch, _ = runtime.run_temperature_scan(send_alerts=False)

    assert len(batch.signals) == 1
    assert calls == [(300, "north_america")]


def test_runtime_keeps_legacy_temperature_scanner_compatible(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    calls: list[int] = []

    def legacy_scanner(*, limit=300):
        calls.append(int(limit))
        return _batch(_signal("legacy-scan-compatible"))

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=legacy_scanner,
    )

    runtime.set_temperature_market_scope("international")
    batch, _ = runtime.run_temperature_scan(send_alerts=False)

    assert len(batch.signals) == 1
    assert calls == [300]


def test_runtime_review_closes_position_when_targeted_refresh_loses_edge(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("review-close-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=lambda *, limit=300: (_ for _ in ()).throw(AssertionError("temperature scanner should not run")),
        price_fetcher=lambda slug: 0.50,
    )

    monkeypatch.setattr(
        "weather_bot.runtime.get_both_bucket_probabilities",
        lambda city_slug, target_date, buckets, provider_context="scheduled": {
            "wu": None,
            "openmeteo": {buckets[0]["label"]: 0.52},
            "vc": None,
            "noaa": None,
            "weatherapi": None,
            "wu_temp": None,
            "om_temp": 72.0,
            "vc_temp": None,
            "noaa_temp": None,
            "weatherapi_temp": None,
            "unit": "F",
        },
    )

    summary = runtime.review_open_positions(reason="test_review_close")
    positions = tracker.get_dashboard_paper_positions(limit=12)

    assert summary["reviewed"] == 1
    assert summary["closed"] == 1
    assert positions[0]["status"] == "closed"
    assert "No fresh qualifying signal" in positions[0]["exit_reason"]


def test_runtime_review_reuses_cached_provider_payload_but_refreshes_market_price(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("cached-review-1")], auto_trade_enabled=True)
    provider_calls: list[tuple[str, str, tuple[str, ...]]] = []
    market_probs = iter([0.50, 0.79])

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=lambda *, limit=300: (_ for _ in ()).throw(AssertionError("temperature scanner should not run")),
        price_fetcher=lambda slug: next(market_probs),
    )

    def fake_bucket_probs(city_slug, target_date, buckets, provider_context="scheduled"):
        provider_calls.append((city_slug, str(target_date), tuple(bucket["label"] for bucket in buckets)))
        return {
            "wu": None,
            "openmeteo": {bucket["label"]: 0.80 for bucket in buckets},
            "vc": None,
            "noaa": None,
            "weatherapi": None,
            "wu_temp": None,
            "om_temp": 72.0,
            "vc_temp": None,
            "noaa_temp": None,
            "weatherapi_temp": None,
            "unit": "F",
        }

    monkeypatch.setattr("weather_bot.runtime.get_both_bucket_probabilities", fake_bucket_probs)

    first = runtime.review_open_positions(reason="test_cached_review_first")
    second = runtime.review_open_positions(reason="test_cached_review_second")
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert first["reviewed"] == 1
    assert first["closed"] == 0
    assert second["reviewed"] == 1
    assert second["closed"] == 1
    assert latest_trade["status"] == "closed"
    assert provider_calls == [("nyc", "2026-04-25", ("70-71F",))]


def test_runtime_review_refreshes_targeted_provider_payload_after_cache_interval(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("stale-review-1")], auto_trade_enabled=True)
    provider_calls: list[int] = []

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=lambda *, limit=300: (_ for _ in ()).throw(AssertionError("temperature scanner should not run")),
        price_fetcher=lambda slug: 0.5,
    )

    def fake_bucket_probs(city_slug, target_date, buckets, provider_context="scheduled"):
        provider_calls.append(1)
        return {
            "wu": None,
            "openmeteo": {bucket["label"]: 0.80 for bucket in buckets},
            "vc": None,
            "noaa": None,
            "weatherapi": None,
            "wu_temp": None,
            "om_temp": 72.0,
            "vc_temp": None,
            "noaa_temp": None,
            "weatherapi_temp": None,
            "unit": "F",
        }

    monkeypatch.setattr("weather_bot.runtime.get_both_bucket_probabilities", fake_bucket_probs)

    first = runtime.review_open_positions(reason="test_stale_review_first")
    refresh_interval_s = runtime._open_position_weather_refresh_interval_seconds()
    runtime._open_position_weather_cache["temperature"]["refreshed_at_monotonic"] -= refresh_interval_s + 1
    second = runtime.review_open_positions(reason="test_stale_review_second")

    assert first["reviewed"] == 1
    assert second["reviewed"] == 1
    assert provider_calls == [1, 1]


def test_runtime_review_batch_reuses_cached_payload_when_positions_change_and_refresh_fails(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("cache-shift-1", market_slug="market-cache-shift-1", label="70-71F")], auto_trade_enabled=True)

    requested_labels: list[tuple[str, ...]] = []
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=lambda *, limit=300: (_ for _ in ()).throw(AssertionError("temperature scanner should not run")),
    )

    def fake_bucket_probs(city_slug, target_date, buckets, provider_context="scheduled"):
        labels = tuple(sorted(bucket["label"] for bucket in buckets))
        requested_labels.append(labels)
        if len(requested_labels) == 1:
            return {
                "wu": None,
                "openmeteo": {bucket["label"]: 0.80 for bucket in buckets},
                "vc": None,
                "noaa": None,
                "weatherapi": None,
                "wu_temp": None,
                "om_temp": 72.0,
                "vc_temp": None,
                "noaa_temp": None,
                "weatherapi_temp": None,
                "unit": "F",
            }
        raise RuntimeError("provider down")

    monkeypatch.setattr("weather_bot.runtime.get_both_bucket_probabilities", fake_bucket_probs)

    initial_positions = tracker.get_dashboard_paper_positions(limit=20, status="open")
    seeded_batch, seeded_refresh = runtime._get_review_weather_batch("temperature", initial_positions)

    strategy.process_signals([_signal("cache-shift-2", market_slug="market-cache-shift-2", label="72-73F")], auto_trade_enabled=True)
    expanded_positions = tracker.get_dashboard_paper_positions(limit=20, status="open")
    fallback_batch, fallback_refresh = runtime._get_review_weather_batch("temperature", expanded_positions)

    assert seeded_refresh is False
    assert len(seeded_batch.signals) == 1
    assert fallback_refresh is False
    assert len(fallback_batch.signals) == 1
    assert requested_labels == [("70-71F",), ("70-71F", "72-73F")]
    assert runtime._open_position_weather_cache["temperature"]["scope"] == "review"


def test_runtime_review_temperature_refresh_only_requests_open_position_buckets(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals(
        [
            _signal("review-target-1", market_slug="market-review-target-1", label="70-71F"),
            _signal("review-target-2", market_slug="market-review-target-2", label="72-73F"),
        ],
        auto_trade_enabled=True,
    )
    requested: list[tuple[str, str, tuple[str, ...], str]] = []
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        temperature_scanner=lambda *, limit=300: (_ for _ in ()).throw(AssertionError("temperature scanner should not run")),
        price_fetcher=lambda slug: 0.40,
    )

    def fake_bucket_probs(city_slug, target_date, buckets, provider_context="scheduled"):
        requested.append((city_slug, str(target_date), tuple(bucket["label"] for bucket in buckets), provider_context))
        return {
            "wu": None,
            "openmeteo": {bucket["label"]: 0.82 for bucket in buckets},
            "vc": None,
            "noaa": None,
            "weatherapi": None,
            "wu_temp": None,
            "om_temp": 72.0,
            "vc_temp": None,
            "noaa_temp": None,
            "weatherapi_temp": None,
            "unit": "F",
        }

    monkeypatch.setattr("weather_bot.runtime.get_both_bucket_probabilities", fake_bucket_probs)

    summary = runtime.review_open_positions(reason="test_targeted_review_scope")

    assert summary["reviewed"] == 2
    assert len(requested) == 1
    city_slug, target_date, labels, provider_context = requested[0]
    assert city_slug == "nyc"
    assert target_date == "2026-04-25"
    assert set(labels) == {"70-71F", "72-73F"}
    assert provider_context == "review"


def test_runtime_review_precipitation_uses_targeted_refresh_without_full_scanner(tmp_path: Path, monkeypatch):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_precip_signal("review-rain-1")], auto_trade_enabled=True)
    om_calls: list[tuple[str, int, int]] = []

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        precipitation_scanner=lambda: (_ for _ in ()).throw(AssertionError("precipitation scanner should not run")),
        price_fetcher=lambda slug: 0.35,
    )

    monkeypatch.setattr(
        "weather_bot.runtime.get_om_monthly_precip",
        lambda city_slug, year, month: (
            om_calls.append((city_slug, year, month))
            or {"observed": 0.8, "forecast": 0.9, "total_projected": 1.7, "unit": "in"}
        ),
    )
    monkeypatch.setattr("weather_bot.runtime.get_vc_monthly_precip", lambda city_slug, year, month: None)
    monkeypatch.setattr(
        "weather_bot.runtime.calc_precip_bucket_probs",
        lambda observed, forecast, buckets, unit: {bucket["label"]: 0.78 for bucket in buckets},
    )

    summary = runtime.review_open_positions(reason="test_precip_targeted_review", market_types={"precipitation"})

    assert summary["reviewed"] == 1
    assert om_calls == [("nyc", 2026, 4)]


def test_dashboard_manual_close_action_closes_open_trade(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("manual-close-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    open_positions = tracker.get_dashboard_paper_positions(limit=12, status="open")

    response = dashboard.apply_control_threadsafe(
        {"action": "close_position", "value": {"position_id": open_positions[0]["id"], "reason": "manual_test_close"}}
    )
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert response["ok"] is True
    assert response["state"]["summary"]["paper"]["open_positions"] == 0
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reason"] == "manual_test_close"
    assert latest_trade["exit_fee_paid"] > 0
    assert latest_trade["net_exit_payout"] < latest_trade["gross_exit_payout"]
    assert response["state"]["recent_resolutions"][0]["status"] == "closed"
    assert response["state"]["recent_resolutions"][0]["outcome_label"] == "Sold"


def test_dashboard_manual_close_action_accepts_stringified_json_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("manual-close-json-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    open_positions = tracker.get_dashboard_paper_positions(limit=12, status="open")

    response = dashboard.apply_control_threadsafe(
        {
            "action": "close_position",
            "value": json.dumps(
                {
                    "position_id": str(open_positions[0]["id"]),
                    "reason": "manual_test_close_json",
                }
            ),
        }
    )
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert response["ok"] is True
    assert response["state"]["summary"]["paper"]["open_positions"] == 0
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reason"] == "manual_test_close_json"
    assert latest_trade["exit_fee_paid"] > 0


def test_dashboard_manual_close_action_accepts_id_alias_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("manual-close-id-alias-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    open_positions = tracker.get_dashboard_paper_positions(limit=12, status="open")

    response = dashboard.apply_control_threadsafe(
        {
            "action": "close_position",
            "value": {"id": str(open_positions[0]["id"]), "reason": "manual_test_close_id_alias"},
        }
    )
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert response["ok"] is True
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reason"] == "manual_test_close_id_alias"


def test_dashboard_manual_close_action_accepts_nested_id_alias_payload(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    strategy.process_signals([_signal("manual-close-nested-id-alias-1")], auto_trade_enabled=True)
    runtime = WeatherRuntime(config=config, tracker=tracker, strategy_engine=strategy, telegram=TelegramClient())
    control_plane = ControlPlane(runtime, tracker)
    dashboard = DashboardStateService(tracker=tracker, runtime=runtime, control_plane=control_plane)
    open_positions = tracker.get_dashboard_paper_positions(limit=12, status="open")

    response = dashboard.apply_control_threadsafe(
        {
            "action": "close_position",
            "value": {"payload": {"positionId": str(open_positions[0]["id"]), "reason": "manual_test_close_nested_alias"}},
        }
    )
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert response["ok"] is True
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reason"] == "manual_test_close_nested_alias"


def _stale_open_position(tmp_path: Path):
    config = load_config(_write_config(tmp_path))
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.ensure_paper_capital(1000.0)
    strategy = WeatherStrategyEngine(config, tracker)
    signal = _signal("stale-close-1", market_prob=0.25, forecast_prob=0.8, edge=0.55, edge_abs=0.55)
    strategy.process_signals([signal], auto_trade_enabled=True)
    open_position = tracker.get_dashboard_paper_positions(limit=12, status="open")[0]
    stale_reviewed_at = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    tracker.update_paper_position_review(
        int(open_position["id"]),
        mark_price=0.25,
        mark_probability=0.8,
        edge_abs=0.55,
        final_score=0.85,
        reviewed_at=stale_reviewed_at,
        reason="stale seed",
    )
    return config, tracker, strategy, open_position


def test_manual_close_uses_fresh_single_market_price_when_stale(tmp_path: Path):
    config, tracker, strategy, open_position = _stale_open_position(tmp_path)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        price_fetcher=lambda slug: 0.6,
        temperature_scanner=lambda *, limit=300: (_ for _ in ()).throw(AssertionError("scanner must not run on manual close")),
    )

    result = runtime.close_position(int(open_position["id"]), reason="manual_test_close")
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert result["ok"] is True
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reference_price"] == 0.6


def test_manual_close_falls_back_to_stale_price_when_fetch_returns_none(tmp_path: Path):
    config, tracker, strategy, open_position = _stale_open_position(tmp_path)
    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        price_fetcher=lambda slug: None,
    )

    result = runtime.close_position(int(open_position["id"]), reason="manual_test_close")
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert result["ok"] is True
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reference_price"] == 0.25


def test_manual_close_falls_back_when_fetcher_raises(tmp_path: Path, caplog):
    config, tracker, strategy, open_position = _stale_open_position(tmp_path)

    def _boom(slug):
        raise RuntimeError("gamma down")

    runtime = WeatherRuntime(
        config=config,
        tracker=tracker,
        strategy_engine=strategy,
        telegram=TelegramClient(),
        price_fetcher=_boom,
    )

    import logging
    with caplog.at_level(logging.WARNING, logger="weather_bot.runtime"):
        result = runtime.close_position(int(open_position["id"]), reason="manual_test_close")
    latest_trade = tracker.get_dashboard_paper_positions(limit=12)[0]

    assert result["ok"] is True
    assert latest_trade["status"] == "closed"
    assert latest_trade["exit_reference_price"] == 0.25
    assert any("manual close price refresh failed" in record.message for record in caplog.records)
