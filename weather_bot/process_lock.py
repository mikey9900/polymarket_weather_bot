"""Single-process PID lock."""

from __future__ import annotations

import atexit
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class PidLock:
    path: Path
    fd: int
    pid: int
    process_start_token: str | None = None
    _released: bool = field(default=False, init=False, repr=False)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            os.close(self.fd)
        except OSError:
            pass
        try:
            payload = _read_lock_payload(self.path)
            if _payload_matches_process(payload, self.pid, self.process_start_token):
                self.path.unlink()
        except FileNotFoundError:
            pass


def acquire_pid_lock(path: str | Path) -> PidLock:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    pid = os.getpid()
    process_start_token = _process_start_token(pid)

    while True:
        try:
            fd = os.open(str(file_path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            payload = _read_lock_payload(file_path)
            if _lock_is_stale(payload, current_pid=pid):
                try:
                    file_path.unlink()
                except FileNotFoundError:
                    pass
                continue
            raise RuntimeError(f"Another weather bot instance is already running: {file_path}")

        record = {
            "pid": pid,
            "process_start_token": process_start_token,
            "argv0": os.path.basename(os.getenv("_", "")) or "weather-bot",
        }
        os.write(fd, json.dumps(record, sort_keys=True).encode("utf-8"))
        lock = PidLock(file_path, fd, pid=pid, process_start_token=process_start_token)
        atexit.register(lock.release)
        return lock


def _lock_is_stale(payload: dict[str, Any], *, current_pid: int) -> bool:
    pid = _payload_pid(payload)
    if pid is None or pid <= 0:
        return True
    if pid == current_pid and not payload.get("process_start_token"):
        return True
    if not _pid_exists(pid):
        return True
    recorded_token = payload.get("process_start_token")
    current_token = _process_start_token(pid)
    if recorded_token and current_token:
        return recorded_token != current_token
    return False


def _payload_pid(payload: dict[str, Any]) -> int | None:
    value = payload.get("pid")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _payload_matches_process(payload: dict[str, Any], pid: int, process_start_token: str | None) -> bool:
    payload_pid = _payload_pid(payload)
    if payload_pid != pid:
        return False
    payload_token = payload.get("process_start_token")
    if payload_token and process_start_token:
        return str(payload_token) == str(process_start_token)
    return payload_token is None and process_start_token is None


def _read_lock_payload(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return {}
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        return payload
    try:
        return {"pid": int(raw)}
    except ValueError:
        return {}


def _pid_exists(pid: int) -> bool:
    if os.name == "nt":
        return _windows_pid_exists(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _windows_pid_exists(pid: int) -> bool:
    import ctypes

    process_query_limited_information = 0x1000
    still_active = 259
    access_denied = 5

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
    if not handle:
        return ctypes.get_last_error() == access_denied
    try:
        exit_code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return True
        return int(exit_code.value) == still_active
    finally:
        kernel32.CloseHandle(handle)


def _process_start_token(pid: int) -> str | None:
    stat_path = Path("/proc") / str(pid) / "stat"
    try:
        raw = stat_path.read_text(encoding="utf-8")
    except OSError:
        return None
    close_paren = raw.rfind(")")
    if close_paren == -1:
        return None
    fields = raw[close_paren + 1 :].strip().split()
    if len(fields) <= 19:
        return None
    return fields[19]
