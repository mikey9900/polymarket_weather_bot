from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import sqlite3
import zipfile

from weather_bot.bootstrap import _restore_tracker_db_if_empty
from weather_bot.config import StorageSettings
from weather_bot.models import ForecastSnapshot, WeatherDecision, WeatherSignal
from weather_bot.nas_archive import ArchiveReceipt
from weather_bot.storage_cleanup import StorageCleanup, prune_matching_files
from weather_bot.tracker import WeatherTracker


OLD = "2020-01-01T00:00:00+00:00"


def _signal(key: str, created_at: str = OLD) -> WeatherSignal:
    return WeatherSignal(
        signal_key=key,
        market_type="temperature",
        event_title=f"Temperature {key}",
        market_slug=key,
        event_slug=key,
        city_slug="nyc",
        event_date="2020-01-01",
        label="70-71F",
        direction="YES",
        market_prob=0.4,
        forecast_prob=0.7,
        edge=0.3,
        edge_abs=0.3,
        edge_size="large",
        confidence="confirmed",
        source_count=2,
        liquidity=100.0,
        time_to_resolution_s=3600.0,
        source_dispersion_pct=0.02,
        score=0.8,
        forecast_snapshot=ForecastSnapshot(market_type="temperature", city_slug="nyc", event_date="2020-01-01", unit="F"),
        raw_payload={},
        created_at=created_at,
    )


def _paper_position(tracker: WeatherTracker, key: str) -> int:
    signal = _signal(key)
    signal_id = tracker.log_signal(signal)
    decision_id = tracker.log_decision(
        signal_id,
        WeatherDecision(signal_key=key, accepted=True, reason="test", final_score=0.8, policy_action="paper_trade"),
    )
    tracker.ensure_paper_capital(1000.0)
    return int(tracker.create_paper_position(
        signal_id=signal_id, decision_id=decision_id, signal=signal, stake_usd=10.0, decision_final_score=0.8,
    ).id)


def _shadow_position(tracker: WeatherTracker, paper_id: int, status: str) -> int:
    cursor = tracker.conn.execute(
        """INSERT INTO shadow_exec_positions
           (paper_position_id, market_type, market_slug, event_slug, city_slug,
            event_date, label, direction, status, opened_at, closed_at, updated_at, payload_json)
           VALUES (?, 'temperature', 'market', 'event', 'nyc', '2020-01-01',
                   '70-71F', 'YES', ?, ?, ?, ?, '{}')""",
        (paper_id, status, OLD, OLD if status == "closed" else None, OLD),
    )
    tracker.conn.commit()
    return int(cursor.lastrowid)


def _shadow_order(tracker: WeatherTracker, paper_id: int, shadow_id: int, status: str) -> int:
    cursor = tracker.conn.execute(
        """INSERT INTO shadow_exec_orders
           (paper_position_id, shadow_position_id, intent_kind, execution_mode,
            market_type, market_slug, event_slug, city_slug, event_date, label,
            direction, order_action, outcome_side, target_price, requested_shares,
            status, ttl_seconds, created_at, updated_at, payload_json)
           VALUES (?, ?, 'entry', 'paper_shadow', 'temperature', 'market', 'event',
                   'nyc', '2020-01-01', '70-71F', 'YES', 'BUY', 'YES', 0.5, 10,
                   ?, 1800, ?, ?, '{}')""",
        (paper_id, shadow_id, status, OLD, OLD),
    )
    tracker.conn.commit()
    return int(cursor.lastrowid)


def _cleanup(tracker, tmp_path, settings):
    return StorageCleanup(
        tracker=tracker,
        settings=settings,
        scan_export_root=tmp_path / "scan",
        analysis_bundle_root=tmp_path / "analysis",
        research_candidate_root=tmp_path / "candidates",
        codex_run_root=tmp_path / "runs",
    )


def test_compaction_keeps_trade_ledger_and_active_detail(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    old_closed = _paper_position(tracker, "closed")
    old_open = _paper_position(tracker, "open")
    tracker.conn.execute("UPDATE paper_positions SET status='closed', resolved_at=? WHERE id=?", (OLD, old_closed))
    closed_shadow = _shadow_position(tracker, old_closed, "closed")
    open_shadow = _shadow_position(tracker, old_open, "open")
    for position_id in (closed_shadow, open_shadow):
        tracker.conn.execute(
            "INSERT INTO shadow_exec_marks (shadow_position_id, mark_price, mark_value_usd, unrealized_pnl, total_pnl, source, evidence_json, created_at) VALUES (?, 0.5, 1, 0, 0, 'test', '{}', ?)",
            (position_id, OLD),
        )
    tracker.conn.commit()
    tracker.log_signal(_signal("unlinked"))
    tracker.log_signal(_signal("unlinked-two"))

    result = tracker.compact_database(
        signal_limit=1, decision_limit=1, review_limit=1, shadow_order_limit=1,
        mark_limit=1, trade_event_limit=1, operator_event_limit=1,
        resolution_event_limit=1, min_history_days=30,
    )

    assert result["deleted"]["signals"] == 1
    assert tracker.conn.execute("SELECT COUNT(*) FROM paper_positions").fetchone()[0] == 2
    assert tracker.conn.execute("SELECT COUNT(*) FROM shadow_exec_positions").fetchone()[0] == 2
    assert tracker.conn.execute("SELECT COUNT(*) FROM shadow_exec_marks WHERE shadow_position_id=?", (open_shadow,)).fetchone()[0] == 1
    assert tracker.conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_shadow_marks_coalesce_until_price_change_or_heartbeat(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    paper_id = _paper_position(tracker, "mark")
    shadow_id = _shadow_position(tracker, paper_id, "open")
    at = datetime.now(timezone.utc)

    tracker.mark_shadow_exec_position(shadow_id, mark_price=0.5, marked_at=at.isoformat())
    tracker.mark_shadow_exec_position(shadow_id, mark_price=0.5, marked_at=(at + timedelta(minutes=1)).isoformat())
    assert tracker.conn.execute("SELECT COUNT(*) FROM shadow_exec_marks").fetchone()[0] == 1

    tracker.mark_shadow_exec_position(shadow_id, mark_price=0.51, marked_at=(at + timedelta(minutes=2)).isoformat())
    tracker.mark_shadow_exec_position(shadow_id, mark_price=0.51, marked_at=(at + timedelta(minutes=18)).isoformat())
    assert tracker.conn.execute("SELECT COUNT(*) FROM shadow_exec_marks").fetchone()[0] == 3


def test_unchanged_paper_reviews_coalesce_without_hiding_changed_reason(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    paper_id = _paper_position(tracker, "review")
    at = datetime.now(timezone.utc)

    assert tracker.update_paper_position_review(
        paper_id, mark_price=0.5, mark_probability=0.6, edge_abs=0.1,
        final_score=0.7, reviewed_at=at.isoformat(), reason="hold", reason_code="hold",
    )
    assert tracker.update_paper_position_review(
        paper_id, mark_price=0.5, mark_probability=0.6, edge_abs=0.1,
        final_score=0.7, reviewed_at=(at + timedelta(minutes=1)).isoformat(), reason="hold", reason_code="hold",
    )
    assert tracker.conn.execute("SELECT COUNT(*) FROM paper_position_reviews").fetchone()[0] == 1
    assert tracker.update_paper_position_review(
        paper_id, mark_price=0.5, mark_probability=0.6, edge_abs=0.1,
        final_score=0.7, reviewed_at=(at + timedelta(minutes=2)).isoformat(), reason="exit risk", reason_code="risk",
    )
    assert tracker.conn.execute("SELECT COUNT(*) FROM paper_position_reviews").fetchone()[0] == 2


def test_closed_ledger_prune_requires_caller_archive_and_preserves_open(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    closed_id = _paper_position(tracker, "closed")
    open_id = _paper_position(tracker, "open")
    tracker.conn.execute("UPDATE paper_positions SET status='closed', resolved_at=? WHERE id=?", (OLD, closed_id))
    closed_shadow_id = _shadow_position(tracker, closed_id, "closed")
    _shadow_position(tracker, open_id, "open")
    closed_order_id = _shadow_order(tracker, closed_id, closed_shadow_id, "filled")
    tracker.conn.execute("UPDATE shadow_exec_positions SET entry_order_id=? WHERE id=?", (closed_order_id, closed_shadow_id))
    tracker.conn.execute(
        """INSERT INTO shadow_exec_fills
           (order_id, shadow_position_id, paper_position_id, action, price, shares,
            notional_usd, liquidity_source, evidence_json, filled_at)
           VALUES (?, ?, ?, 'BUY', 0.5, 10, 5, 'test', '{}', ?)""",
        (closed_order_id, closed_shadow_id, closed_id, OLD),
    )
    tracker.conn.commit()

    receipt = ArchiveReceipt("/nas/archive.zip", "/nas/archive.json", "abc", "def", 100, datetime.now(timezone.utc).isoformat(), 2, 100)
    result = tracker.prune_archived_closed_positions(archive_receipt=receipt, min_history_days=30)

    assert result["positions_deleted"] == 1
    assert tracker.conn.execute("SELECT id FROM paper_positions").fetchall()[0][0] == open_id
    assert tracker.conn.execute("SELECT COUNT(*) FROM shadow_exec_positions").fetchone()[0] == 1
    assert tracker.conn.execute("SELECT COUNT(*) FROM shadow_exec_orders").fetchone()[0] == 0
    assert tracker.conn.execute("SELECT COUNT(*) FROM shadow_exec_fills").fetchone()[0] == 0
    assert tracker.conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_archived_prune_does_not_remove_resting_order(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    paper_id = _paper_position(tracker, "closed-with-resting-order")
    tracker.conn.execute("UPDATE paper_positions SET status='closed', resolved_at=? WHERE id=?", (OLD, paper_id))
    shadow_id = _shadow_position(tracker, paper_id, "closed")
    order_id = _shadow_order(tracker, paper_id, shadow_id, "resting")
    receipt = ArchiveReceipt("/nas/archive.zip", "/nas/archive.json", "abc", "def", 100, datetime.now(timezone.utc).isoformat(), 2, 100)

    result = tracker.prune_archived_closed_positions(archive_receipt=receipt)

    assert result["positions_deleted"] == 0
    assert tracker.conn.execute("SELECT id FROM shadow_exec_orders").fetchone()[0] == order_id
    assert tracker.conn.execute("SELECT id FROM paper_positions").fetchone()[0] == paper_id


def test_no_nas_only_checkpoints_and_reports_usage(tmp_path):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.log_signal(_signal("one"))
    tracker.log_signal(_signal("two"))
    scan = tmp_path / "scan"
    scan.mkdir()
    (scan / "one.json").write_text("one")
    (scan / "two.json").write_text("two")
    cleanup = _cleanup(tracker, tmp_path, replace(StorageSettings(), signal_limit=1, scan_export_keep=1))

    result = cleanup.run()

    assert result["nas_archive_skipped"] == "not_configured"
    assert tracker.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 2
    assert len(list(scan.glob("*.json"))) == 2
    assert result["disk_usage_before"]["tracker_db"] > 0
    assert result["disk_usage_after"]["filesystem_free"] > 0


def test_verified_nas_receipt_precedes_local_prune(tmp_path, monkeypatch):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.log_signal(_signal("one"))
    tracker.log_signal(_signal("two"))
    scan = tmp_path / "scan"
    scan.mkdir()
    (scan / "one.json").write_text("one")
    (scan / "two.json").write_text("two")
    settings = replace(StorageSettings(), nas_share_name="WeatherArchive", signal_limit=1, scan_export_keep=1)
    cleanup = _cleanup(tracker, tmp_path, settings)
    archive_path = tmp_path / "nas-archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archived_zip:
        archived_zip.writestr("weatherbot.db", "snapshot")
        archived_zip.writestr("scan_runs/one.json", "one")
        archived_zip.writestr("scan_runs/two.json", "two")
    manifest_path = tmp_path / "nas-archive.json"
    manifest_path.write_text(json.dumps({
        "artifacts": [
            {
                "entry": f"scan_runs/{path.name}",
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size_bytes": path.stat().st_size,
                "mtime_ns": path.stat().st_mtime_ns,
            }
            for path in scan.glob("*.json")
        ],
    }))

    def archived(**_kwargs):
        assert tracker.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 2
        assert len(list(scan.glob("*.json"))) == 2
        return ArchiveReceipt(str(archive_path), str(manifest_path), "abc", "def", 100, datetime.now(timezone.utc).isoformat(), 2, 100)

    monkeypatch.setattr("weather_bot.storage_cleanup.archive_before_prune", archived)
    result = cleanup.run()

    assert "nas_archive" in result
    assert tracker.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 1
    assert len(list(scan.glob("*.json"))) == 1


def test_nas_failure_keeps_local_history(tmp_path, monkeypatch):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.log_signal(_signal("one"))
    tracker.log_signal(_signal("two"))
    settings = replace(StorageSettings(), nas_share_name="WeatherArchive", signal_limit=1)
    cleanup = _cleanup(tracker, tmp_path, settings)
    monkeypatch.setattr("weather_bot.storage_cleanup.archive_before_prune", lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")))

    result = cleanup.run()

    assert "offline" in result["nas_archive_error"]
    assert tracker.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 2


def test_nas_cleanup_stops_on_existing_foreign_key_damage(tmp_path, monkeypatch):
    tracker = WeatherTracker(tmp_path / "weatherbot.db")
    tracker.log_signal(_signal("one"))
    tracker.log_signal(_signal("two"))
    tracker.conn.execute("PRAGMA foreign_keys=OFF")
    tracker.conn.execute(
        """INSERT INTO shadow_exec_marks
           (shadow_position_id, mark_price, mark_value_usd, unrealized_pnl,
            total_pnl, source, evidence_json, created_at)
           VALUES (99999, 0.5, 1, 0, 0, 'test', '{}', ?)""",
        (OLD,),
    )
    tracker.conn.commit()
    tracker.conn.execute("PRAGMA foreign_keys=ON")
    settings = replace(StorageSettings(), nas_share_name="WeatherArchive", signal_limit=1)
    cleanup = _cleanup(tracker, tmp_path, settings)
    monkeypatch.setattr("weather_bot.storage_cleanup.archive_before_prune", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("should not archive")))

    result = cleanup.run()

    assert result["foreign_key_check"]["status"] == "failed"
    assert tracker.conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0] == 2


def test_artifact_changed_after_archive_receipt_is_not_deleted(tmp_path):
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    old.write_text("old")
    new.write_text("new")
    os.utime(old, ns=(1_000_000_000, 1_000_000_000))
    archived = {
        old.name: {
            "sha256": hashlib.sha256(old.read_bytes()).hexdigest(),
            "size_bytes": old.stat().st_size,
            "mtime_ns": old.stat().st_mtime_ns,
        }
    }
    old.write_text("BAD")
    os.utime(old, ns=(1_000_000_000, 1_000_000_000))

    assert prune_matching_files(tmp_path, "*.json", keep_latest=1, archived_files=archived) == []
    assert old.exists()


def test_candidate_restore_includes_uncheckpointed_wal(tmp_path):
    candidate = tmp_path / "candidate.db"
    active = tmp_path / "active.db"
    source = sqlite3.connect(candidate)
    source.execute("PRAGMA journal_mode=WAL")
    source.execute("CREATE TABLE marker (value TEXT)")
    source.execute("INSERT INTO marker VALUES ('candidate-wal')")
    source.commit()
    with sqlite3.connect(active) as conn:
        conn.execute("CREATE TABLE marker (value TEXT)")
        conn.execute("INSERT INTO marker VALUES ('active-before')")

    restored, info = _restore_tracker_db_if_empty(candidate, active)

    assert restored is True
    with sqlite3.connect(active) as conn:
        assert conn.execute("SELECT value FROM marker").fetchone()[0] == "candidate-wal"
    with sqlite3.connect(info["tracker_db_backup_path"]) as conn:
        assert conn.execute("SELECT value FROM marker").fetchone()[0] == "active-before"
    source.close()
