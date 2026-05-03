"""On-demand export bundles for offline and Dropbox-backed analysis."""

from __future__ import annotations

import json
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .analysis_report import build_analysis_report
from .dropbox_exports import (
    build_dropbox_auth,
    dropbox_create_or_get_shared_link,
    dropbox_settings_from_env_or_options,
    dropbox_upload_file,
    normalize_dropbox_root,
    safe_archive_label,
)
from .paths import ACTIVE_CONFIG_PATH, ANALYSIS_BUNDLE_ROOT
from .persistent_weather_cache import backup_weather_cache

DEFAULT_ANALYSIS_BUNDLE_LABEL = "WEATHER-BOT"


class AnalysisBundleExporter:
    def __init__(
        self,
        *,
        tracker,
        runtime,
        bundle_root: str | Path = ANALYSIS_BUNDLE_ROOT,
        bundle_label: str = DEFAULT_ANALYSIS_BUNDLE_LABEL,
        snapshot_refresher: Callable[[], None] | None = None,
        snapshot_getter: Callable[[], dict[str, Any]] | None = None,
        dropbox_auth: dict[str, Any] | None = None,
        dropbox_root: str | None = None,
    ) -> None:
        self.tracker = tracker
        self.runtime = runtime
        self.bundle_root = Path(bundle_root)
        self.snapshot_refresher = snapshot_refresher
        self.snapshot_getter = snapshot_getter
        self.bundle_label = safe_archive_label(bundle_label)
        self._last_bundle_path: str | None = None
        self._last_error: str | None = None
        self._last_created_at: str | None = None
        self._last_dropbox_bundle_path: str | None = None
        self._last_dropbox_bundle_url: str | None = None
        self._last_dropbox_index_path: str | None = None
        self._last_dropbox_index_url: str | None = None
        self._last_report_path: str | None = None
        self._last_dropbox_report_path: str | None = None
        self._last_dropbox_report_url: str | None = None
        self._last_dropbox_error: str | None = None
        self.bundle_root.mkdir(parents=True, exist_ok=True)

        settings = dropbox_settings_from_env_or_options()
        self.dropbox_root = normalize_dropbox_root(dropbox_root if dropbox_root is not None else settings.get("dropbox_root"))
        self.dropbox_auth: dict[str, Any] | None = dropbox_auth
        self._dropbox_configuration_error: str | None = None
        if self.dropbox_auth is None:
            try:
                self.dropbox_auth = build_dropbox_auth(
                    dropbox_token=settings.get("dropbox_token"),
                    dropbox_refresh_token=settings.get("dropbox_refresh_token"),
                    dropbox_app_key=settings.get("dropbox_app_key"),
                    dropbox_app_secret=settings.get("dropbox_app_secret"),
                )
            except Exception as exc:
                self._dropbox_configuration_error = f"{type(exc).__name__}: {exc}"
                self.dropbox_auth = None

    @property
    def latest_bundle_path(self) -> Path:
        return self.bundle_root / f"{self.bundle_label}_latest_bundle.zip"

    @property
    def latest_index_path(self) -> Path:
        return self.bundle_root / f"{self.bundle_label}_latest_index.json"

    @property
    def latest_report_path(self) -> Path:
        return self.bundle_root / f"{self.bundle_label}_latest_report.xlsx"

    def bind_dashboard_state(
        self,
        *,
        snapshot_refresher: Callable[[], None] | None,
        snapshot_getter: Callable[[], dict[str, Any]] | None,
    ) -> None:
        self.snapshot_refresher = snapshot_refresher
        self.snapshot_getter = snapshot_getter

    def status(self) -> dict[str, Any]:
        return {
            "analysis_bundle_label": self.bundle_label,
            "analysis_bundle_root": str(self.bundle_root),
            "latest_analysis_bundle_path": str(self.latest_bundle_path),
            "latest_analysis_bundle_exists": self.latest_bundle_path.exists(),
            "latest_analysis_index_path": str(self.latest_index_path),
            "latest_analysis_index_exists": self.latest_index_path.exists(),
            "latest_analysis_report_path": str(self.latest_report_path),
            "latest_analysis_report_exists": self.latest_report_path.exists(),
            "last_analysis_bundle_path": self._last_bundle_path,
            "last_analysis_report_path": self._last_report_path,
            "last_analysis_bundle_error": self._last_error,
            "last_analysis_bundle_at": self._last_created_at,
            "analysis_dropbox_enabled": self.dropbox_auth is not None,
            "analysis_dropbox_root": self.dropbox_root if self.dropbox_auth is not None or self._dropbox_configuration_error else None,
            "analysis_dropbox_configuration_error": self._dropbox_configuration_error,
            "last_analysis_bundle_dropbox_path": self._last_dropbox_bundle_path,
            "last_analysis_bundle_dropbox_url": self._last_dropbox_bundle_url,
            "last_analysis_index_dropbox_path": self._last_dropbox_index_path,
            "last_analysis_index_dropbox_url": self._last_dropbox_index_url,
            "last_analysis_report_dropbox_path": self._last_dropbox_report_path,
            "last_analysis_report_dropbox_url": self._last_dropbox_report_url,
            "last_analysis_bundle_dropbox_error": self._last_dropbox_error,
        }

    def export_bundle(self, *, reason: str = "operator") -> dict[str, Any]:
        if self.snapshot_refresher is not None:
            self.snapshot_refresher()

        created_at = datetime.now(timezone.utc)
        stamp = created_at.strftime("%Y%m%dT%H%M%S%fZ")
        bundle_path = self.bundle_root / f"{stamp}_{self.bundle_label}_analysis_bundle.zip"
        scan_export_root = Path(self.runtime.scan_export_root) if getattr(self.runtime, "scan_export_root", None) is not None else None
        scan_files = sorted(
            (scan_export_root.glob("*.json") if scan_export_root and scan_export_root.exists() else []),
            key=lambda path: path.stat().st_mtime,
        )
        included_entries: list[str] = []
        snapshot = self._current_snapshot()
        runtime_status = self.runtime.get_status_snapshot()
        latest_bundle_path = self.latest_bundle_path
        latest_index_path = self.latest_index_path
        report_path = self.bundle_root / f"{stamp}_{self.bundle_label}_analysis_report.xlsx"
        latest_report_path = self.latest_report_path
        position_review_history = self.tracker.get_position_review_history(limit=None)
        shadow_order_intents = self.tracker.get_recent_shadow_order_intents(limit=None)

        try:
            with tempfile.TemporaryDirectory(prefix="weather-analysis-bundle-") as temp_dir:
                temp_root = Path(temp_dir)
                tracker_backup_path = temp_root / "weatherbot.db"
                weather_cache_backup_path = temp_root / "weather_cache.db"
                self.tracker.backup_database(tracker_backup_path)
                backup_weather_cache(weather_cache_backup_path)
                build_analysis_report(
                    output_path=report_path,
                    label=self.bundle_label,
                    created_at=created_at,
                    snapshot=snapshot,
                    tracker=self.tracker,
                    runtime=self.runtime,
                )

                with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                    included_entries.append("dashboard_state.json")
                    archive.writestr("dashboard_state.json", json.dumps(snapshot, indent=2, sort_keys=True))

                    included_entries.append("runtime_status.json")
                    archive.writestr("runtime_status.json", json.dumps(runtime_status, indent=2, sort_keys=True))

                    config_path = Path(ACTIVE_CONFIG_PATH)
                    if config_path.exists():
                        included_entries.append("active_config.yaml")
                        archive.write(config_path, arcname="active_config.yaml")

                    included_entries.append("weatherbot.db")
                    archive.write(tracker_backup_path, arcname="weatherbot.db")

                    included_entries.append("weather_cache.db")
                    archive.write(weather_cache_backup_path, arcname="weather_cache.db")

                    included_entries.append("analysis_report.xlsx")
                    archive.write(report_path, arcname="analysis_report.xlsx")

                    included_entries.append("position_review_history.json")
                    archive.writestr("position_review_history.json", json.dumps(position_review_history, indent=2, sort_keys=True))

                    included_entries.append("shadow_order_intents.json")
                    archive.writestr("shadow_order_intents.json", json.dumps(shadow_order_intents, indent=2, sort_keys=True))

                    for path in scan_files:
                        arcname = f"scan_runs/{path.name}"
                        included_entries.append(arcname)
                        archive.write(path, arcname=arcname)

                    manifest = {
                        "label": self.bundle_label,
                        "created_at": created_at.isoformat(),
                        "reason": str(reason or "operator"),
                        "bundle_path": str(bundle_path),
                        "analysis_bundle_root": str(self.bundle_root),
                        "tracker_db_path": str(self.tracker.db_path),
                        "scan_export_root": str(scan_export_root) if scan_export_root is not None else None,
                        "scan_export_count": len(scan_files),
                        "position_review_count": len(position_review_history),
                        "shadow_order_count": len(shadow_order_intents),
                        "temperature_market_scope": runtime_status.get("temperature_market_scope"),
                        "included_entries": [*included_entries, "manifest.json"],
                    }
                    archive.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))

            shutil.copy2(bundle_path, latest_bundle_path)
            shutil.copy2(report_path, latest_report_path)
            index_payload = self._build_latest_index(
                created_at=created_at,
                reason=reason,
                bundle_path=bundle_path,
                latest_bundle_path=latest_bundle_path,
                latest_index_path=latest_index_path,
                report_path=report_path,
                latest_report_path=latest_report_path,
                scan_export_root=scan_export_root,
                scan_files=scan_files,
                position_review_count=len(position_review_history),
                shadow_order_count=len(shadow_order_intents),
                runtime_status=runtime_status,
                included_entries=[*included_entries, "manifest.json"],
            )
            self._write_latest_index(latest_index_path, index_payload)

            dropbox_result = self._sync_dropbox_artifacts(
                bundle_path=bundle_path,
                latest_bundle_path=latest_bundle_path,
                latest_index_path=latest_index_path,
                report_path=report_path,
                latest_report_path=latest_report_path,
                index_payload=index_payload,
            )

            self._last_bundle_path = str(bundle_path)
            self._last_report_path = str(report_path)
            self._last_error = None
            self._last_created_at = created_at.isoformat()
            return {
                "bundle_path": str(bundle_path),
                "latest_bundle_path": str(latest_bundle_path),
                "latest_index_path": str(latest_index_path),
                "report_path": str(report_path),
                "latest_report_path": str(latest_report_path),
                "created_at": self._last_created_at,
                "scan_export_count": len(scan_files),
                "position_review_count": len(position_review_history),
                "shadow_order_count": len(shadow_order_intents),
                "entry_count": len(included_entries) + 1,
                "dropbox_enabled": self.dropbox_auth is not None,
                "dropbox_configuration_error": self._dropbox_configuration_error,
                **dropbox_result,
            }
        except Exception as exc:
            self._last_error = str(exc)
            self._last_created_at = created_at.isoformat()
            raise

    def _current_snapshot(self) -> dict[str, Any]:
        if self.snapshot_getter is not None:
            snapshot = self.snapshot_getter()
            if isinstance(snapshot, dict) and snapshot:
                return snapshot
        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "runtime": self.runtime.get_status_snapshot(),
            "summary": {"paper": self.tracker.get_paper_stats()},
            "open_positions": self.tracker.get_dashboard_paper_positions(limit=12, status="open"),
        }

    def _build_latest_index(
        self,
        *,
        created_at: datetime,
        reason: str,
        bundle_path: Path,
        latest_bundle_path: Path,
        latest_index_path: Path,
        report_path: Path,
        latest_report_path: Path,
        scan_export_root: Path | None,
        scan_files: list[Path],
        position_review_count: int,
        shadow_order_count: int,
        runtime_status: dict[str, Any],
        included_entries: list[str],
    ) -> dict[str, Any]:
        return {
            "label": self.bundle_label,
            "created_at": created_at.isoformat(),
            "reason": str(reason or "operator"),
            "archive_bundle": {
                "filename": bundle_path.name,
                "local_path": str(bundle_path),
                "size_bytes": bundle_path.stat().st_size if bundle_path.exists() else None,
            },
            "latest_bundle": {
                "filename": latest_bundle_path.name,
                "local_path": str(latest_bundle_path),
                "size_bytes": latest_bundle_path.stat().st_size if latest_bundle_path.exists() else None,
            },
            "latest_index": {
                "filename": latest_index_path.name,
                "local_path": str(latest_index_path),
            },
            "archive_report": {
                "filename": report_path.name,
                "local_path": str(report_path),
                "size_bytes": report_path.stat().st_size if report_path.exists() else None,
            },
            "latest_report": {
                "filename": latest_report_path.name,
                "local_path": str(latest_report_path),
                "size_bytes": latest_report_path.stat().st_size if latest_report_path.exists() else None,
            },
            "analysis_bundle_root": str(self.bundle_root),
            "scan_export_root": str(scan_export_root) if scan_export_root is not None else None,
            "scan_export_count": len(scan_files),
            "position_review_count": int(position_review_count),
            "shadow_order_count": int(shadow_order_count),
            "temperature_market_scope": runtime_status.get("temperature_market_scope"),
            "included_entries": included_entries,
            "dropbox": {
                "enabled": self.dropbox_auth is not None,
                "root": self.dropbox_root if self.dropbox_auth is not None or self._dropbox_configuration_error else None,
                "configuration_error": self._dropbox_configuration_error,
                "archive_path": None,
                "latest_bundle_path": None,
                "latest_bundle_url": None,
                "latest_index_path": None,
                "latest_index_url": None,
                "archive_report_path": None,
                "latest_report_path": None,
                "latest_report_url": None,
                "error": None,
            },
        }

    def _write_latest_index(self, latest_index_path: Path, payload: dict[str, Any]) -> None:
        latest_index_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _sync_dropbox_artifacts(
        self,
        *,
        bundle_path: Path,
        latest_bundle_path: Path,
        latest_index_path: Path,
        report_path: Path,
        latest_report_path: Path,
        index_payload: dict[str, Any],
    ) -> dict[str, Any]:
        self._last_dropbox_bundle_path = None
        self._last_dropbox_bundle_url = None
        self._last_dropbox_index_path = None
        self._last_dropbox_index_url = None
        self._last_dropbox_report_path = None
        self._last_dropbox_report_url = None
        self._last_dropbox_error = self._dropbox_configuration_error
        dropbox_meta = dict(index_payload.get("dropbox") or {})

        if self.dropbox_auth is None:
            dropbox_meta["error"] = self._dropbox_configuration_error
            index_payload["dropbox"] = dropbox_meta
            self._write_latest_index(latest_index_path, index_payload)
            return {
                "dropbox_ok": False,
                "dropbox_archive_path": None,
                "dropbox_latest_bundle_path": None,
                "dropbox_latest_bundle_url": None,
                "dropbox_latest_index_path": None,
                "dropbox_latest_index_url": None,
                "dropbox_archive_report_path": None,
                "dropbox_latest_report_path": None,
                "dropbox_latest_report_url": None,
                "dropbox_error": self._dropbox_configuration_error,
            }

        archive_remote_path = self._dropbox_path("daily-archives", bundle_path.name)
        archive_report_remote_path = self._dropbox_path("daily-archives", report_path.name)
        latest_bundle_remote_path = self._dropbox_path("latest", latest_bundle_path.name)
        latest_index_remote_path = self._dropbox_path("latest", latest_index_path.name)
        latest_report_remote_path = self._dropbox_path("latest", latest_report_path.name)
        errors: list[str] = []

        for local_path, remote_path, error_label in (
            (bundle_path, archive_remote_path, "archive upload"),
            (report_path, archive_report_remote_path, "archive report upload"),
            (latest_bundle_path, latest_bundle_remote_path, "latest bundle upload"),
            (latest_report_path, latest_report_remote_path, "latest report upload"),
        ):
            response = dropbox_upload_file(local_path, remote_path, self.dropbox_auth)
            if not response.get("ok"):
                errors.append(f"{error_label}: {response.get('error') or response.get('status')}")

        latest_bundle_url = None
        latest_report_url = None
        if not errors:
            latest_bundle_url = dropbox_create_or_get_shared_link(latest_bundle_remote_path, self.dropbox_auth)
            latest_report_url = dropbox_create_or_get_shared_link(latest_report_remote_path, self.dropbox_auth)

        dropbox_meta.update(
            {
                "archive_path": archive_remote_path,
                "latest_bundle_path": latest_bundle_remote_path,
                "latest_bundle_url": latest_bundle_url,
                "latest_index_path": latest_index_remote_path,
                "archive_report_path": archive_report_remote_path,
                "latest_report_path": latest_report_remote_path,
                "latest_report_url": latest_report_url,
            }
        )
        index_payload["dropbox"] = dropbox_meta
        self._write_latest_index(latest_index_path, index_payload)

        index_upload = dropbox_upload_file(latest_index_path, latest_index_remote_path, self.dropbox_auth)
        if not index_upload.get("ok"):
            errors.append(f"latest index upload: {index_upload.get('error') or index_upload.get('status')}")

        latest_index_url = None
        if not errors:
            latest_index_url = dropbox_create_or_get_shared_link(latest_index_remote_path, self.dropbox_auth)
            dropbox_meta["latest_index_url"] = latest_index_url
            index_payload["dropbox"] = dropbox_meta
            self._write_latest_index(latest_index_path, index_payload)
            refresh_upload = dropbox_upload_file(latest_index_path, latest_index_remote_path, self.dropbox_auth)
            if not refresh_upload.get("ok"):
                errors.append(f"latest index refresh: {refresh_upload.get('error') or refresh_upload.get('status')}")

        self._last_dropbox_bundle_path = latest_bundle_remote_path if not errors else None
        self._last_dropbox_bundle_url = latest_bundle_url if not errors else None
        self._last_dropbox_index_path = latest_index_remote_path if not errors else None
        self._last_dropbox_index_url = latest_index_url if not errors else None
        self._last_dropbox_report_path = latest_report_remote_path if not errors else None
        self._last_dropbox_report_url = latest_report_url if not errors else None
        self._last_dropbox_error = " | ".join(errors) if errors else None
        dropbox_meta["error"] = self._last_dropbox_error
        index_payload["dropbox"] = dropbox_meta
        self._write_latest_index(latest_index_path, index_payload)

        return {
            "dropbox_ok": not errors,
            "dropbox_archive_path": archive_remote_path,
            "dropbox_latest_bundle_path": latest_bundle_remote_path,
            "dropbox_latest_bundle_url": latest_bundle_url,
            "dropbox_latest_index_path": latest_index_remote_path,
            "dropbox_latest_index_url": latest_index_url,
            "dropbox_archive_report_path": archive_report_remote_path,
            "dropbox_latest_report_path": latest_report_remote_path,
            "dropbox_latest_report_url": latest_report_url,
            "dropbox_error": self._last_dropbox_error,
        }

    def _dropbox_path(self, folder: str, filename: str) -> str:
        if self.dropbox_root == "/":
            return f"/{folder}/{filename}"
        return f"{self.dropbox_root}/{folder}/{filename}"
