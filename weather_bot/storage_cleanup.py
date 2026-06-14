"""Filesystem retention helpers for generated bot artifacts."""

from __future__ import annotations

from pathlib import Path


def prune_matching_files(directory: str | Path, pattern: str, *, keep_latest: int) -> list[str]:
    root = Path(directory)
    if not root.exists():
        return []
    try:
        candidates = [
            path
            for path in root.glob(pattern)
            if path.is_file()
        ]
    except OSError:
        return []
    if not candidates:
        return []
    sorted_candidates = sorted(
        candidates,
        key=lambda path: path.stat().st_mtime if path.exists() else 0.0,
        reverse=True,
    )
    removed: list[str] = []
    for path in sorted_candidates[max(0, int(keep_latest)) :]:
        try:
            path.unlink()
            removed.append(str(path))
        except OSError:
            continue
    return removed

