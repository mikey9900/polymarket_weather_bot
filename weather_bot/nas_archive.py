"""Verified, bounded archives on a Home Assistant network Share mount."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import time
import uuid
import zipfile
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ARCHIVE_DIRECTORY = "weather_bot_archive"
ARCHIVE_NAME = re.compile(r"^weatherbot_\d{8}T\d{12}Z_[0-9a-f]{8}\.zip$")
SHARE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
PART_NAME = re.compile(r"^weatherbot_\d{8}T\d{12}Z_[0-9a-f]{8}\.zip\.part$")


class ArchiveUnavailable(RuntimeError):
    """The configured NAS mount is absent or unsafe to write."""


class ArchiveError(RuntimeError):
    """A snapshot could not be verified or fitted in the archive budget."""


@dataclass(frozen=True)
class ArchiveReceipt:
    snapshot_path: str
    manifest_path: str
    snapshot_sha256: str
    tracker_sha256: str
    size_bytes: int
    created_at: str
    artifact_count: int
    archive_bytes_after: int


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(path.name + ".part")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _remove_abandoned_parts(directory: Path, pattern: str, *, minimum_age_seconds: int = 86400) -> None:
    cutoff = time.time() - minimum_age_seconds
    for path in directory.iterdir():
        if not path.is_file() or path.is_symlink() or not re.fullmatch(pattern, path.name):
            continue
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
        except OSError:
            continue


def _mounted_archive_directory(name: str) -> Path:
    if not SHARE_NAME.fullmatch(name) or name.lower() == "weather_bot":
        raise ArchiveUnavailable("NAS share name must be one safe directory name distinct from weather_bot.")
    try:
        share_root = Path("/share").resolve(strict=True)
    except OSError as exc:
        raise ArchiveUnavailable("Home Assistant /share is unavailable.") from exc
    mount = share_root / name
    if mount.is_symlink() or not mount.is_dir() or not os.path.ismount(mount):
        raise ArchiveUnavailable(f"NAS Share /share/{name} is not mounted; local cleanup must keep its history.")
    resolved_mount = mount.resolve(strict=True)
    if resolved_mount.parent != share_root:
        raise ArchiveUnavailable("NAS Share resolves outside /share.")
    archive_dir = mount / ARCHIVE_DIRECTORY
    if archive_dir.is_symlink():
        raise ArchiveUnavailable("NAS archive directory must not be a symlink.")
    archive_dir.mkdir(exist_ok=True)
    if archive_dir.resolve(strict=True).parent != resolved_mount:
        raise ArchiveUnavailable("NAS archive directory resolves outside the mounted Share.")
    return archive_dir


def _confirm_archive_still_mounted(name: str, archive_dir: Path, mount_device: int, *files: Path) -> None:
    """Reject a receipt if a vanished mount exposed a local fallback directory."""
    current_dir = _mounted_archive_directory(name)
    if current_dir != archive_dir or current_dir.stat().st_dev != mount_device:
        raise ArchiveUnavailable("NAS Share changed while the archive was being written.")
    for path in files:
        if not path.is_file() or path.stat().st_dev != mount_device:
            raise ArchiveUnavailable("NAS archive file is no longer on the mounted Share.")


def _snapshot_sqlite(source_path: Path) -> Path:
    # Build the SQLite backup on HA's local filesystem. SQLite connections on
    # network filesystems are intentionally avoided, including for the backup.
    _remove_abandoned_parts(source_path.parent, r"weatherbot-archive-[A-Za-z0-9_\-]+\.db")
    with closing(sqlite3.connect(str(source_path))) as source:
        page_count = int(source.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(source.execute("PRAGMA page_size").fetchone()[0])
    estimated_bytes = page_count * page_size
    reserve = max(64 * 1024 * 1024, estimated_bytes // 10)
    available_bytes = shutil.disk_usage(source_path.parent).free
    if available_bytes < estimated_bytes + reserve:
        raise ArchiveError(
            f"Not enough local HA free space for a consistent SQLite backup "
            f"(need about {estimated_bytes + reserve} bytes, have {available_bytes})."
        )
    temporary = tempfile.NamedTemporaryFile(prefix="weatherbot-archive-", suffix=".db", dir=source_path.parent, delete=False)
    snapshot_path = Path(temporary.name)
    temporary.close()
    try:
        with closing(sqlite3.connect(str(source_path))) as source, closing(sqlite3.connect(str(snapshot_path))) as target:
            source.backup(target, pages=1000, sleep=0.1)
            check = target.execute("PRAGMA quick_check").fetchone()
            if check is None or check[0] != "ok":
                raise ArchiveError(f"SQLite backup failed quick_check: {check}")
        return snapshot_path
    except Exception:
        snapshot_path.unlink(missing_ok=True)
        raise


def _artifact_files(
    scan_export_root: Path,
    analysis_bundle_root: Path,
    research_candidate_root: Path | None,
    codex_run_root: Path | None,
    tracker_db_path: Path,
) -> list[tuple[Path, str]]:
    artifacts: list[tuple[Path, str]] = []
    sources: list[tuple[Path, str, str]] = [
        (scan_export_root, "*.json", "scan_runs"),
        (analysis_bundle_root, "*_analysis_bundle.zip", "analysis_bundle"),
        (analysis_bundle_root, "*_analysis_report.xlsx", "analysis_bundle"),
        (tracker_db_path.parent, f"{tracker_db_path.name}.preseed-backup-*", "tracker_backups"),
    ]
    if research_candidate_root is not None:
        sources.append((research_candidate_root, "*.yaml", "research_candidates"))
    if codex_run_root is not None:
        sources.append((codex_run_root, "*.json", "codex_runs"))
    for root, pattern, prefix in sources:
        if root.is_dir():
            resolved_root = root.resolve()
            artifacts.extend(
                (path, f"{prefix}/{path.name}")
                for path in sorted(root.glob(pattern))
                if path.is_file() and not path.is_symlink() and path.resolve().parent == resolved_root
            )
    return artifacts


def _copy_stable_artifact(archive: zipfile.ZipFile, source: Path, entry_name: str) -> dict[str, object]:
    before = source.stat()
    digest = hashlib.sha256()
    with source.open("rb") as input_stream, archive.open(entry_name, "w") as output_stream:
        for block in iter(lambda: input_stream.read(1024 * 1024), b""):
            output_stream.write(block)
            digest.update(block)
    after = source.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or _sha256(source) != digest.hexdigest():
        raise ArchiveError(f"Artifact changed while archiving: {source}")
    return {
        "entry": entry_name,
        "sha256": digest.hexdigest(),
        "size_bytes": before.st_size,
        "mtime_ns": before.st_mtime_ns,
    }


def _verified_zip(path: Path, tracker_digest: str, artifact_hashes: dict[str, str]) -> None:
    with zipfile.ZipFile(path, "r") as archive:
        expected = {"weatherbot.db": tracker_digest, **artifact_hashes}
        if set(archive.namelist()) != set(expected):
            raise ArchiveError("NAS archive entries differ from the expected tracker and artifacts.")
        for entry_name, expected_digest in expected.items():
            digest = hashlib.sha256()
            with archive.open(entry_name) as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(block)
            if digest.hexdigest() != expected_digest:
                raise ArchiveError(f"NAS archive hash differs from the source for {entry_name}.")


def _archive_size_and_prune(archive_dir: Path, newest: Path, max_bytes: int) -> int:
    if newest.parent.resolve() != archive_dir.resolve() or not ARCHIVE_NAME.fullmatch(newest.name):
        raise ArchiveError("New archive is outside the bot archive directory.")
    snapshots = sorted(
        (path for path in archive_dir.iterdir() if path.is_file() and not path.is_symlink() and ARCHIVE_NAME.fullmatch(path.name)),
        key=lambda path: path.name,
    )
    def owned(path: Path) -> bool:
        manifest = path.with_suffix(".json")
        if not manifest.is_file() or manifest.is_symlink():
            return False
        try:
            payload = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return False
        return isinstance(payload, dict) and payload.get("producer") == "weather_bot.nas_archive" and payload.get("snapshot") == path.name

    def size_of(path: Path) -> int:
        manifest = path.with_suffix(".json")
        return path.stat().st_size + (manifest.stat().st_size if manifest.is_file() else 0)

    if not owned(newest):
        raise ArchiveError("New archive is missing its bot-produced receipt.")
    total = sum(size_of(path) for path in snapshots)
    if size_of(newest) > max_bytes:
        newest.unlink()
        newest.with_suffix(".json").unlink(missing_ok=True)
        raise ArchiveError("New archive alone exceeds the configured NAS archive budget.")
    projected = total
    to_remove: list[Path] = []
    for old in snapshots:
        if projected <= max_bytes:
            break
        if old == newest or not owned(old):
            continue
        to_remove.append(old)
        projected -= size_of(old)
    if projected > max_bytes:
        newest.unlink()
        newest.with_suffix(".json").unlink(missing_ok=True)
        raise ArchiveError("NAS archive budget could not be enforced; inspect archive directory permissions.")
    for old in to_remove:
        old_size = size_of(old)
        old.unlink()
        old.with_suffix(".json").unlink(missing_ok=True)
        total -= old_size
    return total


def archive_before_prune(
    *,
    tracker_db_path: str | Path,
    nas_share_name: str,
    max_gb: int,
    scan_export_root: str | Path,
    analysis_bundle_root: str | Path,
    research_candidate_root: str | Path | None = None,
    codex_run_root: str | Path | None = None,
) -> ArchiveReceipt:
    """Copy a consistent tracker snapshot and current artifacts before local pruning.

    A return value is a durable, verified receipt. On any failure, callers must
    skip destructive pruning of archival ledger rows and retry on a later run.
    """
    source_argument = Path(tracker_db_path).absolute()
    source = source_argument.resolve(strict=True)
    if not source.is_file():
        raise ArchiveError("Local tracker database is missing.")
    if not 1 <= int(max_gb) <= 10000:
        raise ValueError("max_gb must be between 1 and 10000.")
    archive_dir = _mounted_archive_directory(nas_share_name)
    mount_device = archive_dir.stat().st_dev
    share_root = archive_dir.parent.parent
    live_dir = share_root / "weather_bot"
    if (
        source_argument.parent != live_dir
        or source.parent != live_dir
        or live_dir.is_symlink()
        or os.path.ismount(live_dir)
    ):
        raise ArchiveUnavailable("The active SQLite tracker must stay in local /share/weather_bot.")
    _remove_abandoned_parts(archive_dir, PART_NAME.pattern)
    max_bytes = int(max_gb) * 1024**3
    created_at = datetime.now(timezone.utc).isoformat()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    final = archive_dir / f"weatherbot_{stamp}_{uuid.uuid4().hex[:8]}.zip"
    part = final.with_name(final.name + ".part")
    manifest = final.with_suffix(".json")
    snapshot = _snapshot_sqlite(source)
    try:
        tracker_digest = _sha256(snapshot)
        artifacts = _artifact_files(
            Path(scan_export_root),
            Path(analysis_bundle_root),
            Path(research_candidate_root) if research_candidate_root is not None else None,
            Path(codex_run_root) if codex_run_root is not None else None,
            source,
        )
        uncompressed_bytes = snapshot.stat().st_size + sum(path.stat().st_size for path, _ in artifacts)
        reserve_bytes = max(64 * 1024 * 1024, uncompressed_bytes // 20)
        free_bytes = shutil.disk_usage(archive_dir).free
        if free_bytes < uncompressed_bytes + reserve_bytes:
            raise ArchiveError(
                "Not enough free space on the mounted NAS Share for the next archive "
                f"(need about {uncompressed_bytes + reserve_bytes} bytes, have {free_bytes})."
            )
        pruning_started = False
        try:
            artifact_entries: list[dict[str, object]] = []
            with part.open("wb") as stream:
                with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=3) as archive:
                    archive.write(snapshot, arcname="weatherbot.db")
                    for source_file, entry_name in artifacts:
                        artifact_entries.append(_copy_stable_artifact(archive, source_file, entry_name))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(part, final)
            artifact_hashes = {str(entry["entry"]): str(entry["sha256"]) for entry in artifact_entries}
            _verified_zip(final, tracker_digest, artifact_hashes)
            archive_digest = _sha256(final)
            payload: dict[str, object] = {
                "producer": "weather_bot.nas_archive",
                "created_at": created_at,
                "snapshot": final.name,
                "snapshot_sha256": archive_digest,
                "tracker_sha256": tracker_digest,
                "size_bytes": final.stat().st_size,
                "artifact_count": len(artifacts),
                "artifacts": artifact_entries,
            }
            _write_json_atomic(manifest, payload)
            _confirm_archive_still_mounted(nas_share_name, archive_dir, mount_device, final, manifest)
            pruning_started = True
            after = _archive_size_and_prune(archive_dir, final, max_bytes)
            _confirm_archive_still_mounted(nas_share_name, archive_dir, mount_device, final, manifest)
            return ArchiveReceipt(
                snapshot_path=str(final),
                manifest_path=str(manifest),
                snapshot_sha256=archive_digest,
                tracker_sha256=tracker_digest,
                size_bytes=final.stat().st_size,
                created_at=created_at,
                artifact_count=len(artifacts),
                archive_bytes_after=after,
            )
        except Exception:
            part.unlink(missing_ok=True)
            if not pruning_started:
                manifest.unlink(missing_ok=True)
                final.unlink(missing_ok=True)
            raise
    finally:
        snapshot.unlink(missing_ok=True)
