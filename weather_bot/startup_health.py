"""Startup health checks for startup/runtime/db bootstrap diagnostics."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .paths import TRACKER_DB_PATH


def pick_best_candidate_tracker_db(
    active_db_path: str | Path,
    *,
    min_signals: int = 1,
) -> Path | None:
    candidates = _discover_candidate_dbs(Path(active_db_path))
    if not candidates:
        return None

    eligible: list[tuple[Path, int, int, float]] = []
    for entry in candidates:
        path = Path(str(entry.get("path") or ""))
        if not path.is_file():
            continue
        try:
            counts = entry.get("table_counts", {})
            signal_count = int(counts.get("signals") or 0)
            if signal_count < int(min_signals):
                continue
            size_bytes = int(entry.get("size_bytes") or 0)
            try:
                mtime = path.stat().st_mtime
            except OSError:
                mtime = 0.0
        except OSError:
            continue
        eligible.append((path, signal_count, size_bytes, mtime))
    if not eligible:
        return None

    selected = max(
        eligible,
        key=lambda item: (item[1], item[2], item[3]),
    )
    return selected[0]


def is_tracker_db_uninitialized(startup_health: dict[str, Any] | None) -> bool:
    if not isinstance(startup_health, dict):
        return True
    startup_checks = startup_health.get("checks", {}).get("startup_checks", {})
    if startup_checks.get("active_db_empty") or startup_checks.get("active_db_really_small"):
        return True
    active_summary = startup_health.get("active_db", {})
    counts = active_summary.get("table_counts", {})
    if not isinstance(counts, dict):
        counts = {}
    signal_count = int(counts.get("signals") or 0)
    paper_position_count = int(counts.get("paper_positions") or 0)
    shadow_exec_order_count = int(counts.get("shadow_exec_orders") or 0)
    return signal_count == 0 and paper_position_count == 0 and shadow_exec_order_count == 0


def run_startup_health_checks(tracker_db_path: str | Path, active_config_path: str | Path | None = None) -> dict[str, Any]:
    checked_at = datetime.now(timezone.utc).isoformat()
    active_db = Path(tracker_db_path)
    active_summary = _collect_db_summary(active_db)
    result: dict[str, Any] = {
        "status": "ok",
        "checks": {
            "startup_checks": {},
            "runtime_checks": {},
            "active_db": active_summary,
            "candidate_dbs": [],
        },
        "warnings": [],
        "environment": {
            "active_db": str(active_db),
            "active_config_path": str(active_config_path) if active_config_path else None,
            "cwd": str(Path.cwd()),
            "user": os.getenv("USERNAME") or os.getenv("USER") or "unknown",
        },
        "checked_at": checked_at,
        "candidate_dbs": [],
    }

    if not active_db.exists():
        return _record_warning(result, "missing_active_db", f"Active database not found at {active_db}", severity="error", level="error")

    if not active_db.is_file():
        return _record_warning(result, "invalid_active_db", f"Active database path is not a file: {active_db}", severity="error", level="error")

    runtime_state = dict(active_summary.get("runtime_state") or {})
    checks = result["checks"]
    runtime_checks = checks["runtime_checks"]
    startup_checks = checks["startup_checks"]
    startup_checks["active_db_exists"] = True
    startup_checks["tracker_db_path"] = str(active_db)

    db_counts = active_summary.get("table_counts", {})
    execution_signal_count = int(db_counts.get("signals") or 0)
    paper_position_count = int(db_counts.get("paper_positions") or 0)
    shadow_exec_order_count = int(db_counts.get("shadow_exec_orders") or 0)
    runtime_status = runtime_state.get("runtime_status") or {}
    startup_checks["recent_lookback_hours"] = 24
    startup_checks["active_scan_count_24h"] = int(_count_rows_in_window(active_summary, "signals", "created_at", hours=24) or 0)
    startup_checks["active_paper_count_24h"] = int(_count_rows_in_window(active_summary, "paper_positions", "created_at", hours=24) or 0)
    startup_checks["active_db_recent_signals_24h"] = int(active_summary.get("recent_activity", {}).get("signals", 0) or 0)
    startup_checks["active_db_recent_paper_positions_24h"] = int(active_summary.get("recent_activity", {}).get("paper_positions", 0) or 0)
    runtime_checks["scan_worker_healthy"] = bool(runtime_status.get("scan_worker_healthy", False))
    runtime_checks["scan_in_progress"] = bool(runtime_status.get("scan_in_progress", False))
    runtime_checks["last_temperature_scan_at"] = runtime_status.get("last_temperature_scan_at")
    runtime_checks["last_precipitation_scan_at"] = runtime_status.get("last_precipitation_scan_at")

    if execution_signal_count == 0 and paper_position_count == 0 and shadow_exec_order_count == 0:
        _record_warning(
            result,
            "active_db_empty",
            "Active DB has no signals, paper positions, or shadow executions yet.",
            severity="warn",
            level="warn",
        )

    if active_summary.get("size_bytes", 0) <= 1024 * 1024 and execution_signal_count == 0:
        _record_warning(
            result,
            "active_db_really_small",
            "Active DB is very small and appears uninitialized.",
            severity="warn",
            level="warn",
        )

    latest_scan_age_hours = _most_recent_scan_age_hours(runtime_status)
    if latest_scan_age_hours is not None and latest_scan_age_hours > 12:
        _record_warning(
            result,
            "stale_scan_activity",
            f"No completed temperature/precipitation scan within the last {latest_scan_age_hours:.1f}h.",
            severity="warn",
            level="warn",
        )

    if runtime_status.get("scan_worker_healthy") is False:
        _record_warning(
            result,
            "scan_worker_unhealthy",
            "Scan worker thread is not running.",
            severity="warn",
            level="warn",
        )

    candidate_dbs = _discover_candidate_dbs(active_db)
    checks["candidate_dbs"] = candidate_dbs
    result["candidate_dbs"] = candidate_dbs
    active_has_activity = execution_signal_count > 0 or paper_position_count > 0
    if not active_has_activity and candidate_dbs:
        with_activity = [
            db
            for db in candidate_dbs
            if int((db.get("table_counts", {}) or {}).get("signals", 0) or 0) > 0
        ]
        if with_activity:
            joined = ", ".join(str(db.get("path")) for db in with_activity[:3])
            _record_warning(
                result,
                "active_db_vs_candidates_mismatch",
                (
                    "Active DB is empty but alternate DBs contain historical trade data: "
                    f"{joined}. Confirm this process points at the intended TRACKER_DB_PATH."
                ),
                severity="warn",
                level="warn",
            )

    return result


def _collect_db_summary(db_path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "path": str(db_path),
        "exists": False,
        "size_bytes": 0,
        "table_counts": {},
        "recent_activity": {},
        "runtime_state": {},
        "latest_records": {},
        "sample_errors": [],
    }
    if not db_path.exists() or not db_path.is_file():
        return summary
    summary["exists"] = True
    try:
        summary["size_bytes"] = int(db_path.stat().st_size)
    except OSError as exc:
        summary["sample_errors"].append(f"Failed to stat DB: {exc}")
    if not summary["size_bytes"]:
        return summary
    try:
        conn = sqlite3.connect(str(db_path), timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
    except Exception as exc:
        summary["sample_errors"].append(f"Failed to connect DB: {exc}")
        return summary

    try:
        with conn:
            summary["table_counts"] = _table_counts(
                conn,
                [
                    "signals",
                    "paper_positions",
                    "decisions",
                    "shadow_order_intents",
                    "shadow_exec_orders",
                    "shadow_exec_positions",
                    "shadow_exec_fills",
                    "runtime_state",
                ],
            )
            summary["recent_activity"] = {
                "signals": _count_rows_in_window(_db_state_from_conn(conn), "signals", "created_at", hours=24),
                "paper_positions": _count_rows_in_window(_db_state_from_conn(conn), "paper_positions", "created_at", hours=24),
            }
            summary["runtime_state"] = _runtime_state_dict(conn)
            summary["latest_records"] = {
                "signals_created_at": _latest_value(conn, "signals", "created_at"),
                "paper_positions_created_at": _latest_value(conn, "paper_positions", "created_at"),
                "shadow_exec_positions_opened_at": _latest_value(conn, "shadow_exec_positions", "opened_at"),
                "shadow_exec_orders_created_at": _latest_value(conn, "shadow_exec_orders", "created_at"),
            }
    except Exception as exc:
        summary["sample_errors"].append(f"Failed while reading DB: {exc}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return summary


def _discover_candidate_dbs(active_db: Path) -> list[dict[str, Any]]:
    candidates: list[Path] = []
    active_resolved = active_db.resolve()
    candidate_candidates = (
        active_resolved.parent / "tmp_bundle_weatherbot" / "weatherbot.db",
        active_resolved.parent.parent / "tmp_bundle_weatherbot" / "weatherbot.db",
        Path.cwd() / "tmp_bundle_weatherbot" / "weatherbot.db",
    )
    candidate_set: set[Path] = set()
    for candidate in candidate_candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            resolved = candidate
        if resolved == active_resolved:
            continue
        if not resolved.exists() or not resolved.is_file():
            continue
        if resolved in candidate_set:
            continue
        candidate_set.add(resolved)
        candidates.append(resolved)

    raw_env = os.getenv("WEATHER_STARTUP_CANDIDATE_DBS")
    if raw_env:
        for item in str(raw_env).split(os.pathsep):
            item = item.strip()
            if not item:
                continue
            path = Path(item)
            if path.exists() and path.is_file() and path not in candidate_set and path != active_resolved:
                candidate_set.add(path.resolve())
                candidates.append(path)

    outputs: list[dict[str, Any]] = []
    for path in candidates:
        outputs.append(_collect_db_summary(path))
    return outputs


def _table_counts(conn: sqlite3.Connection, tables: tuple[str, ...] | list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) AS value FROM {table}").fetchone()
            counts[table] = int(row["value"]) if row is not None else 0
        except Exception:
            counts[table] = 0
    return counts


def _count_rows_in_window(db_state: dict[str, Any], table: str, timestamp_column: str, *, hours: int) -> int:
    if not db_state:
        return 0
    conn = db_state.get("conn")
    if conn is None:
        return 0
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=max(1, int(hours)))).isoformat()
        row = conn.execute(
            f"SELECT COUNT(*) AS value FROM {table} WHERE {timestamp_column} >= ?",
            (cutoff,),
        ).fetchone()
        return int(row["value"]) if row is not None else 0
    except Exception:
        return 0


def _latest_value(conn: sqlite3.Connection, table: str, timestamp_column: str) -> str | None:
    try:
        row = conn.execute(
            f"SELECT {timestamp_column} FROM {table} WHERE {timestamp_column} IS NOT NULL ORDER BY {timestamp_column} DESC LIMIT 1",
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _runtime_state_dict(conn: sqlite3.Connection) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        rows = conn.execute("SELECT key, value_json FROM runtime_state").fetchall()
    except Exception:
        return payload
    for row in rows:
        key = str(row["key"] or "").strip()
        raw_value = str(row["value_json"] or "").strip()
        if not key:
            continue
        try:
            payload[key] = json.loads(raw_value) if raw_value else {}
        except json.JSONDecodeError:
            payload[key] = raw_value
    return payload


def _most_recent_scan_age_hours(runtime_status: dict[str, Any]) -> float | None:
    candidates: list[datetime] = []
    for key in ("last_temperature_scan_at", "last_precipitation_scan_at"):
        parsed = _parse_utc_timestamp(runtime_status.get(key))
        if parsed is not None:
            candidates.append(parsed)
    if not candidates:
        if bool(runtime_status.get("scan_in_progress")):
            return None
        return None
    now = datetime.now(timezone.utc)
    recent = max(candidates)
    if recent.tzinfo is None:
        recent = recent.replace(tzinfo=timezone.utc)
    if recent.tzinfo is None:
        return None
    age = now - recent.astimezone(timezone.utc)
    if age.total_seconds() < 0:
        return 0.0
    return age.total_seconds() / 3600.0


def _parse_utc_timestamp(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _record_warning(
    result: dict[str, Any],
    code: str,
    message: str,
    *,
    severity: str,
    level: str,
) -> dict[str, Any]:
    warnings = list(result.get("warnings") or [])
    warning = {"code": code, "message": message, "at": datetime.now(timezone.utc).isoformat(), "severity": severity}
    warnings.append(warning)
    result["warnings"] = warnings
    if level == "error":
        result["status"] = "error"
    elif result["status"] != "error":
        result["status"] = "warn"
    return result


def _db_state_from_conn(conn: sqlite3.Connection) -> dict[str, Any]:
    return {"conn": conn}


def _ensure_candidate_defaults() -> dict[str, Any]:
    # retained for compatibility with future extensions and to satisfy static analyzers.
    return {"paths_checked": 0, "paths_skipped": 0}


def run_startup_health_checks_from_defaults() -> dict[str, Any]:
    return run_startup_health_checks(TRACKER_DB_PATH)
