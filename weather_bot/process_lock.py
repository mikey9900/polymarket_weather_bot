"""Single-process PID lock."""

from __future__ import annotations

import atexit
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PidLock:
    path: Path
    fd: int

    def release(self) -> None:
        try:
            os.close(self.fd)
        except OSError:
            pass
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def acquire_pid_lock(path: str | Path) -> PidLock:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(file_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
    except FileExistsError as exc:
        raise RuntimeError(f"Another weather bot instance is already running: {file_path}") from exc
    os.write(fd, str(os.getpid()).encode("utf-8"))
    lock = PidLock(file_path, fd)
    atexit.register(lock.release)
    return lock
