"""A NAS receipt must prove a restorable, consistent local tracker copy."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from weather_bot import nas_archive


def test_archive_contains_consistent_tracker_and_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = tmp_path / "weather_bot"
    archive_dir = tmp_path / "nas" / "weather_bot_archive"
    scans = data / "exports" / "scan_runs"
    bundles = data / "exports" / "analysis_bundle"
    candidates = data / "research" / "candidates"
    codex_runs = data / "codex" / "runs"
    for path in (data, archive_dir, scans, bundles, candidates, codex_runs):
        path.mkdir(parents=True, exist_ok=True)
    db = data / "weatherbot.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE events (message TEXT NOT NULL)")
        connection.execute("INSERT INTO events VALUES ('kept before cleanup')")
    (scans / "scan.json").write_text('{"ok": true}', encoding="utf-8")
    (bundles / "run_analysis_report.xlsx").write_bytes(b"report")
    (candidates / "candidate.yaml").write_text("min_edge: 0.2", encoding="utf-8")
    (codex_runs / "run.json").write_text("{}", encoding="utf-8")
    (data / "weatherbot.db.preseed-backup-20260101").write_bytes(b"prior backup")
    monkeypatch.setattr(nas_archive, "_mounted_archive_directory", lambda name: archive_dir)

    receipt = nas_archive.archive_before_prune(
        tracker_db_path=db,
        nas_share_name="weather_archive",
        max_gb=1,
        scan_export_root=scans,
        analysis_bundle_root=bundles,
        research_candidate_root=candidates,
        codex_run_root=codex_runs,
    )

    archived = Path(receipt.snapshot_path)
    manifest = json.loads(Path(receipt.manifest_path).read_text(encoding="utf-8"))
    assert hashlib.sha256(archived.read_bytes()).hexdigest() == receipt.snapshot_sha256
    assert manifest["tracker_sha256"] == receipt.tracker_sha256
    assert {entry["entry"] for entry in manifest["artifacts"]} == {
        "scan_runs/scan.json",
        "analysis_bundle/run_analysis_report.xlsx",
        "research_candidates/candidate.yaml",
        "codex_runs/run.json",
        "tracker_backups/weatherbot.db.preseed-backup-20260101",
    }
    assert receipt.artifact_count == 5
    with zipfile.ZipFile(archived) as package:
        assert package.testzip() is None
        assert package.read("scan_runs/scan.json") == b'{"ok": true}'
        assert package.read("analysis_bundle/run_analysis_report.xlsx") == b"report"
        assert package.read("research_candidates/candidate.yaml") == b"min_edge: 0.2"
        assert package.read("codex_runs/run.json") == b"{}"
        assert package.read("tracker_backups/weatherbot.db.preseed-backup-20260101") == b"prior backup"
        restored = tmp_path / "restored.db"
        restored.write_bytes(package.read("weatherbot.db"))
    with sqlite3.connect(restored) as connection:
        assert connection.execute("SELECT message FROM events").fetchone()[0] == "kept before cleanup"
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert not list(data.glob("weatherbot-archive-*.db"))


@pytest.mark.parametrize("name", ["weather_bot", "Weather_Bot", "../nas", "foo/bar", "C:drive", "", "name with spaces"])
def test_rejects_unsafe_or_live_share_name(name: str) -> None:
    with pytest.raises(nas_archive.ArchiveUnavailable):
        nas_archive._mounted_archive_directory(name)


def test_budget_prunes_only_old_bot_snapshots_after_new_exists(tmp_path: Path) -> None:
    older = tmp_path / "weatherbot_20260101T000000000000Z_aaaaaaaa.zip"
    newer = tmp_path / "weatherbot_20260102T000000000000Z_bbbbbbbb.zip"
    unrelated = tmp_path / "family_photos.zip"
    for path in (older, newer, unrelated):
        path.write_bytes(b"1234567890")
    older.with_suffix(".json").write_text(json.dumps({"producer": "weather_bot.nas_archive", "snapshot": older.name}), encoding="utf-8")
    newer.with_suffix(".json").write_text(json.dumps({"producer": "weather_bot.nas_archive", "snapshot": newer.name}), encoding="utf-8")

    remaining = nas_archive._archive_size_and_prune(tmp_path, newer, max_bytes=newer.stat().st_size + newer.with_suffix(".json").stat().st_size)

    assert remaining == newer.stat().st_size + newer.with_suffix(".json").stat().st_size
    assert not older.exists()
    assert not older.with_suffix(".json").exists()
    assert newer.exists()
    assert unrelated.exists()


def test_failed_verification_returns_no_receipt_or_final_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "weather_bot"
    archive_dir = tmp_path / "nas" / "weather_bot_archive"
    data.mkdir()
    archive_dir.mkdir(parents=True)
    db = data / "weatherbot.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE preserved (id INTEGER)")
        connection.execute("INSERT INTO preserved VALUES (7)")
    monkeypatch.setattr(nas_archive, "_mounted_archive_directory", lambda name: archive_dir)
    def fail_verification(path: Path, digest: str, artifact_hashes: dict[str, str]) -> None:
        raise nas_archive.ArchiveError("verification failed")

    monkeypatch.setattr(nas_archive, "_verified_zip", fail_verification)

    with pytest.raises(nas_archive.ArchiveError, match="verification failed"):
        nas_archive.archive_before_prune(
            tracker_db_path=db,
            nas_share_name="WeatherArchive",
            max_gb=1,
            scan_export_root=data / "scan_runs",
            analysis_bundle_root=data / "analysis_bundle",
        )

    assert not list(archive_dir.iterdir())
    assert not list(data.glob("weatherbot-archive-*.db"))
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT id FROM preserved").fetchone()[0] == 7


def test_oversized_new_archive_keeps_older_copy(tmp_path: Path) -> None:
    older = tmp_path / "weatherbot_20260101T000000000000Z_aaaaaaaa.zip"
    newer = tmp_path / "weatherbot_20260102T000000000000Z_bbbbbbbb.zip"
    older.write_bytes(b"old")
    newer.write_bytes(b"new archive exceeds budget")
    newer.with_suffix(".json").write_text(json.dumps({"producer": "weather_bot.nas_archive", "snapshot": newer.name}), encoding="utf-8")

    with pytest.raises(nas_archive.ArchiveError, match="alone exceeds"):
        nas_archive._archive_size_and_prune(tmp_path, newer, max_bytes=8)

    assert older.exists()
    assert not newer.exists()
    assert not newer.with_suffix(".json").exists()


def test_budget_never_deletes_unowned_matching_name(tmp_path: Path) -> None:
    unowned = tmp_path / "weatherbot_20260101T000000000000Z_aaaaaaaa.zip"
    newest = tmp_path / "weatherbot_20260102T000000000000Z_bbbbbbbb.zip"
    unowned.write_bytes(b"not an archive made by this bot")
    newest.write_bytes(b"new")
    newest.with_suffix(".json").write_text(
        json.dumps({"producer": "weather_bot.nas_archive", "snapshot": newest.name}), encoding="utf-8"
    )

    with pytest.raises(nas_archive.ArchiveError, match="could not be enforced"):
        nas_archive._archive_size_and_prune(
            tmp_path,
            newest,
            max_bytes=newest.stat().st_size + newest.with_suffix(".json").stat().st_size,
        )

    assert unowned.exists()
    assert not newest.exists()
    assert not newest.with_suffix(".json").exists()


def test_active_tracker_outside_local_weather_bot_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "other_location"
    archive_dir = tmp_path / "nas" / "weather_bot_archive"
    data.mkdir()
    archive_dir.mkdir(parents=True)
    db = data / "weatherbot.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE sample (id INTEGER)")
    monkeypatch.setattr(nas_archive, "_mounted_archive_directory", lambda name: archive_dir)

    with pytest.raises(nas_archive.ArchiveUnavailable, match="local /share/weather_bot"):
        nas_archive.archive_before_prune(
            tracker_db_path=db,
            nas_share_name="WeatherArchive",
            max_gb=1,
            scan_export_root=data / "scan",
            analysis_bundle_root=data / "analysis",
        )

    assert not list(archive_dir.iterdir())


def test_changed_artifact_aborts_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data = tmp_path / "weather_bot"
    archive_dir = tmp_path / "nas" / "weather_bot_archive"
    scans = data / "scan_runs"
    scans.mkdir(parents=True)
    archive_dir.mkdir(parents=True)
    db = data / "weatherbot.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE sample (id INTEGER)")
    scan = scans / "scan.json"
    scan.write_text('{"state":"partial"}', encoding="utf-8")
    monkeypatch.setattr(nas_archive, "_mounted_archive_directory", lambda name: archive_dir)
    original_sha256 = nas_archive._sha256

    def mutate_before_postcopy_hash(path: Path) -> str:
        if path == scan:
            scan.write_text('{"state":"finished"}', encoding="utf-8")
        return original_sha256(path)

    monkeypatch.setattr(nas_archive, "_sha256", mutate_before_postcopy_hash)
    with pytest.raises(nas_archive.ArchiveError, match="changed while archiving"):
        nas_archive.archive_before_prune(
            tracker_db_path=db,
            nas_share_name="WeatherArchive",
            max_gb=1,
            scan_export_root=scans,
            analysis_bundle_root=data / "analysis",
        )
    assert not list(archive_dir.iterdir())


def test_disappearing_mount_aborts_receipt_before_local_prune(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "weather_bot"
    archive_dir = tmp_path / "nas" / "weather_bot_archive"
    data.mkdir()
    archive_dir.mkdir(parents=True)
    db = data / "weatherbot.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE preserved (id INTEGER)")
        connection.execute("INSERT INTO preserved VALUES (7)")
    calls = 0

    def mount_then_disappear(_name: str) -> Path:
        nonlocal calls
        calls += 1
        if calls > 1:
            raise nas_archive.ArchiveUnavailable("NAS disconnected")
        return archive_dir

    monkeypatch.setattr(nas_archive, "_mounted_archive_directory", mount_then_disappear)
    with pytest.raises(nas_archive.ArchiveUnavailable, match="disconnected"):
        nas_archive.archive_before_prune(
            tracker_db_path=db,
            nas_share_name="WeatherArchive",
            max_gb=1,
            scan_export_root=data / "scan_runs",
            analysis_bundle_root=data / "analysis_bundle",
        )

    assert calls == 2
    assert not list(archive_dir.iterdir())
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT id FROM preserved").fetchone()[0] == 7


def test_insufficient_nas_space_keeps_local_tracker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data = tmp_path / "weather_bot"
    archive_dir = tmp_path / "nas" / "weather_bot_archive"
    data.mkdir()
    archive_dir.mkdir(parents=True)
    db = data / "weatherbot.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE preserved (id INTEGER)")
        connection.execute("INSERT INTO preserved VALUES (7)")
    monkeypatch.setattr(nas_archive, "_mounted_archive_directory", lambda _name: archive_dir)
    actual_disk_usage = nas_archive.shutil.disk_usage

    def disk_usage(path: Path) -> object:
        if Path(path) == archive_dir:
            return SimpleNamespace(free=1)
        return actual_disk_usage(path)

    monkeypatch.setattr(nas_archive.shutil, "disk_usage", disk_usage)
    with pytest.raises(nas_archive.ArchiveError, match="Not enough free space on the mounted NAS"):
        nas_archive.archive_before_prune(
            tracker_db_path=db,
            nas_share_name="WeatherArchive",
            max_gb=1,
            scan_export_root=data / "scan_runs",
            analysis_bundle_root=data / "analysis_bundle",
        )

    assert not list(archive_dir.iterdir())
    assert not list(data.glob("weatherbot-archive-*.db"))
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT id FROM preserved").fetchone()[0] == 7
