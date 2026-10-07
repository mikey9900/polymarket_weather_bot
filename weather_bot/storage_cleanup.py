"""Bound local generated files and disposable tracker detail."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from contextlib import closing
import shutil
import sqlite3
import zipfile
import hashlib
import json

from .nas_archive import archive_before_prune

NAS_ARCHIVE_MIN_INTERVAL_HOURS = 24


def prune_matching_files(
    directory: str | Path,
    pattern: str,
    *,
    keep_latest: int,
    archived_files: dict[str, dict[str, Any]] | None = None,
) -> list[str]:
    root = Path(directory)
    if not root.exists():
        return []
    try:
        candidates = [
            path
            for path in root.glob(pattern)
            if path.is_file() and not path.is_symlink() and path.resolve().parent == root.resolve()
        ]
    except OSError:
        return []
    if not candidates:
        return []
    def modified_at(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    sorted_candidates = sorted(candidates, key=modified_at, reverse=True)
    removed: list[str] = []
    for path in sorted_candidates[max(0, int(keep_latest)) :]:
        if archived_files is not None:
            record = archived_files.get(path.name)
            if not isinstance(record, dict):
                continue
            try:
                stat = path.stat()
                if stat.st_size != int(record["size_bytes"]) or stat.st_mtime_ns != int(record["mtime_ns"]):
                    continue
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                if digest.hexdigest() != record["sha256"]:
                    continue
            except (OSError, KeyError, TypeError, ValueError):
                continue
        try:
            path.unlink()
            removed.append(str(path))
        except OSError:
            continue
    return removed


def tracker_disk_bytes(db_path: str | Path) -> int:
    """Include the WAL, which can dwarf the main SQLite file between checkpoints."""
    path = Path(db_path)
    size = 0
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        try:
            size += candidate.stat().st_size
        except OSError:
            pass
    return size


def _file_bytes(paths: list[Path]) -> int:
    size = 0
    for path in paths:
        try:
            size += path.stat().st_size
        except OSError:
            pass
    return size


def _directory_bytes(root: Path, pattern: str) -> int:
    if not root.exists():
        return 0
    return _file_bytes([path for path in root.glob(pattern) if path.is_file()])


def _file_count(root: Path, pattern: str) -> int:
    return sum(1 for path in root.glob(pattern) if path.is_file()) if root.exists() else 0


class StorageCleanup:
    def __init__(
        self,
        *,
        tracker: Any,
        settings: Any,
        scan_export_root: str | Path,
        analysis_bundle_root: str | Path,
        research_candidate_root: str | Path,
        codex_run_root: str | Path,
    ) -> None:
        self.tracker = tracker
        self.settings = settings
        self.scan_export_root = Path(scan_export_root)
        self.analysis_bundle_root = Path(analysis_bundle_root)
        self.research_candidate_root = Path(research_candidate_root)
        self.codex_run_root = Path(codex_run_root)

    def disk_usage(self) -> dict[str, int]:
        db_path = Path(self.tracker.db_path)
        return {
            "tracker_db": _file_bytes([db_path]),
            "tracker_wal": _file_bytes([Path(f"{db_path}-wal")]),
            "tracker_shm": _file_bytes([Path(f"{db_path}-shm")]),
            "scan_exports": _directory_bytes(self.scan_export_root, "*.json"),
            "analysis_bundles": _directory_bytes(self.analysis_bundle_root, "*_analysis_bundle.zip"),
            "analysis_reports": _directory_bytes(self.analysis_bundle_root, "*_analysis_report.xlsx"),
            "analysis_latest": _directory_bytes(self.analysis_bundle_root, "*_latest_*"),
            "tracker_backups": _directory_bytes(db_path.parent, f"{db_path.name}.preseed-backup-*"),
            "research_candidates": _directory_bytes(self.research_candidate_root, "*.yaml"),
            "codex_runs": _directory_bytes(self.codex_run_root, "*.json"),
            "filesystem_free": shutil.disk_usage(db_path.parent).free,
            "filesystem_total": shutil.disk_usage(db_path.parent).total,
        }

    def run(self) -> dict[str, Any]:
        if not self.settings.enabled:
            return {"enabled": False}

        db_path = Path(self.tracker.db_path)
        before_usage = self.disk_usage()
        before_bytes = tracker_disk_bytes(db_path)
        limits = {
            "signals": self.settings.signal_limit,
            "decisions": self.settings.decision_limit,
            "paper_position_reviews": self.settings.review_limit,
            "shadow_order_intents": self.settings.shadow_intent_limit,
            "shadow_exec_marks": self.settings.shadow_mark_limit,
            "shadow_exec_trade_events": self.settings.trade_event_limit,
            "operator_events": self.settings.operator_event_limit,
            "resolution_events": self.settings.resolution_event_limit,
        }
        with self.tracker._lock:
            counts = {
                table: int(self.tracker.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in limits
            }
        soft_limit_bytes = self.settings.tracker_soft_limit_mb * 1024 * 1024
        prune_needed = before_bytes > soft_limit_bytes or any(counts[table] > limit for table, limit in limits.items())
        result: dict[str, Any] = {
            "enabled": True,
            "tracker_bytes_before": before_bytes,
            "tracker_soft_limit_bytes": soft_limit_bytes,
            "tracker_counts_before": counts,
            "tracker_prune_needed": prune_needed,
            "disk_usage_before": before_usage,
        }
        artifact_pressure = any(
            _file_count(root, pattern) > keep
            for root, pattern, keep in (
                (self.scan_export_root, "*.json", self.settings.scan_export_keep),
                (self.analysis_bundle_root, "*_analysis_bundle.zip", self.settings.analysis_bundle_keep),
                (self.analysis_bundle_root, "*_analysis_report.xlsx", self.settings.analysis_report_keep),
                (self.research_candidate_root, "*.yaml", self.settings.research_candidate_keep),
                (self.codex_run_root, "*.json", self.settings.codex_run_keep),
                (db_path.parent, f"{db_path.name}.preseed-backup-*", self.settings.tracker_backup_keep),
            )
        )
        result["artifact_prune_needed"] = artifact_pressure
        try:
            with closing(sqlite3.connect(str(db_path), timeout=1.0)) as checkpoint_conn:
                checkpoint_conn.execute("PRAGMA busy_timeout=1000")
                busy, log_pages, checkpointed_pages = checkpoint_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            result["wal_checkpoint"] = {
                "status": "deferred" if busy else "ok",
                "busy": int(busy),
                "log_pages": int(log_pages),
                "checkpointed_pages": int(checkpointed_pages),
            }
        except Exception as exc:
            result["wal_checkpoint"] = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}
        checkpointed_bytes = tracker_disk_bytes(db_path)
        result["tracker_bytes_after_checkpoint"] = checkpointed_bytes
        prune_needed = checkpointed_bytes > soft_limit_bytes or any(counts[table] > limit for table, limit in limits.items())
        result["tracker_prune_needed"] = prune_needed
        if self.settings.nas_share_name and (prune_needed or artifact_pressure):
            try:
                with self.tracker._lock:
                    cursor = self.tracker.conn.execute("PRAGMA foreign_key_check")
                    violations = cursor.fetchmany(11)
                result["foreign_key_check"] = {
                    "status": "failed" if violations else "ok",
                    "sample": [list(row) for row in violations[:10]],
                    "more_than_ten": len(violations) > 10,
                }
            except sqlite3.Error as exc:
                result["foreign_key_check"] = {"status": "error", "error": str(exc)}
            if result["foreign_key_check"]["status"] != "ok":
                result["tracker_bytes_after"] = checkpointed_bytes
                result["tracker_over_soft_limit"] = checkpointed_bytes > soft_limit_bytes
                result["disk_usage_after"] = self.disk_usage()
                return result
        if not self.settings.nas_share_name:
            result["nas_archive_skipped"] = "not_configured"
            result["tracker_bytes_after"] = tracker_disk_bytes(db_path)
            result["tracker_over_soft_limit"] = result["tracker_bytes_after"] > soft_limit_bytes
            result["disk_usage_after"] = self.disk_usage()
            return result
        # A configured NAS is the durable copy of record. If it is unavailable,
        # keep every local artifact and retry next cycle.
        if self.settings.nas_share_name:
            saved = self.tracker.get_runtime_state("nas_archive_last_success", default={})
            try:
                last_at = datetime.fromisoformat(str(saved.get("created_at") or ""))
                if last_at.tzinfo is None:
                    last_at = last_at.replace(tzinfo=timezone.utc)
            except ValueError:
                last_at = None
            archive_due = last_at is None or datetime.now(timezone.utc) - last_at >= timedelta(hours=NAS_ARCHIVE_MIN_INTERVAL_HOURS)
            if not (prune_needed or artifact_pressure) or not archive_due:
                result["nas_archive_skipped"] = "no_retention_pressure" if not (prune_needed or artifact_pressure) else "minimum_interval"
                result["tracker_bytes_after"] = tracker_disk_bytes(db_path)
                result["tracker_over_soft_limit"] = result["tracker_bytes_after"] > soft_limit_bytes
                result["disk_usage_after"] = self.disk_usage()
                return result
            try:
                receipt = archive_before_prune(
                    tracker_db_path=db_path,
                    nas_share_name=self.settings.nas_share_name,
                    max_gb=self.settings.nas_archive_max_gb,
                    scan_export_root=self.scan_export_root,
                    analysis_bundle_root=self.analysis_bundle_root,
                    research_candidate_root=self.research_candidate_root,
                    codex_run_root=self.codex_run_root,
                )
                with zipfile.ZipFile(receipt.snapshot_path) as archived_zip:
                    archived_entries = set(archived_zip.namelist())
                manifest = json.loads(Path(receipt.manifest_path).read_text(encoding="utf-8"))
                archived_artifacts = {
                    str(item["entry"]): item
                    for item in manifest["artifacts"]
                    if isinstance(item, dict) and str(item.get("entry")) in archived_entries
                }
                result["nas_archive"] = asdict(receipt)
                self.tracker.set_runtime_state(
                    "nas_archive_last_success",
                    {"created_at": receipt.created_at, "snapshot_path": str(receipt.snapshot_path)},
                )
            except Exception as exc:
                result["nas_archive_error"] = f"{type(exc).__name__}: {exc}"
                result["tracker_bytes_after"] = tracker_disk_bytes(db_path)
                result["tracker_over_soft_limit"] = result["tracker_bytes_after"] > soft_limit_bytes
                result["disk_usage_after"] = self.disk_usage()
                return result
        if prune_needed:
            result["tracker_prune"] = self.tracker.compact_database(
                signal_limit=self.settings.signal_limit,
                decision_limit=self.settings.decision_limit,
                review_limit=self.settings.review_limit,
                shadow_order_limit=self.settings.shadow_intent_limit,
                mark_limit=self.settings.shadow_mark_limit,
                trade_event_limit=self.settings.trade_event_limit,
                operator_event_limit=self.settings.operator_event_limit,
                resolution_event_limit=self.settings.resolution_event_limit,
                min_history_days=self.settings.min_history_days,
            )
        if self.settings.nas_share_name and result.get("nas_archive") and tracker_disk_bytes(db_path) > soft_limit_bytes:
            result["archived_closed_ledger_prune"] = self.tracker.prune_archived_closed_positions(
                archive_receipt=receipt,
                min_history_days=max(30, self.settings.min_history_days),
            )
        result["tracker_bytes_after"] = tracker_disk_bytes(db_path)
        result["tracker_over_soft_limit"] = result["tracker_bytes_after"] > soft_limit_bytes
        result["files_pruned"] = {
            "scan_exports": prune_matching_files(self.scan_export_root, "*.json", keep_latest=self.settings.scan_export_keep, archived_files={Path(name).name: item for name, item in archived_artifacts.items() if name.startswith("scan_runs/")}),
            "analysis_bundles": prune_matching_files(self.analysis_bundle_root, "*_analysis_bundle.zip", keep_latest=self.settings.analysis_bundle_keep, archived_files={Path(name).name: item for name, item in archived_artifacts.items() if name.startswith("analysis_bundle/")}),
            "analysis_reports": prune_matching_files(self.analysis_bundle_root, "*_analysis_report.xlsx", keep_latest=self.settings.analysis_report_keep, archived_files={Path(name).name: item for name, item in archived_artifacts.items() if name.startswith("analysis_bundle/")}),
            "research_candidates": prune_matching_files(self.research_candidate_root, "*.yaml", keep_latest=self.settings.research_candidate_keep, archived_files={Path(name).name: item for name, item in archived_artifacts.items() if name.startswith("research_candidates/")}),
            "codex_runs": prune_matching_files(self.codex_run_root, "*.json", keep_latest=self.settings.codex_run_keep, archived_files={Path(name).name: item for name, item in archived_artifacts.items() if name.startswith("codex_runs/")}),
            "tracker_backups": prune_matching_files(db_path.parent, f"{db_path.name}.preseed-backup-*", keep_latest=self.settings.tracker_backup_keep, archived_files={Path(name).name: item for name, item in archived_artifacts.items() if name.startswith("tracker_backups/")}),
        }
        result["disk_usage_after"] = self.disk_usage()
        return result

