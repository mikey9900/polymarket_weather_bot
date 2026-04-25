from __future__ import annotations

import json
import os

import pytest

from weather_bot.process_lock import acquire_pid_lock


def test_acquire_pid_lock_reclaims_dead_pid_file(tmp_path):
    lock_path = tmp_path / "weatherbot.pid.lock"
    lock_path.write_text("999999", encoding="utf-8")

    lock = acquire_pid_lock(lock_path)
    payload = json.loads(lock_path.read_text(encoding="utf-8"))

    assert payload["pid"] == os.getpid()
    lock.release()
    assert not lock_path.exists()


def test_acquire_pid_lock_reclaims_legacy_current_pid_file(tmp_path):
    lock_path = tmp_path / "weatherbot.pid.lock"
    lock_path.write_text(str(os.getpid()), encoding="utf-8")

    lock = acquire_pid_lock(lock_path)
    payload = json.loads(lock_path.read_text(encoding="utf-8"))

    assert payload["pid"] == os.getpid()
    lock.release()
    assert not lock_path.exists()


def test_acquire_pid_lock_blocks_active_instance(tmp_path, monkeypatch):
    lock_path = tmp_path / "weatherbot.pid.lock"
    current_pid = os.getpid()
    monkeypatch.setattr("weather_bot.process_lock._process_start_token", lambda pid: "active-token" if pid == current_pid else None)
    lock_path.write_text(
        json.dumps({"pid": current_pid, "process_start_token": "active-token"}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Another weather bot instance is already running"):
        acquire_pid_lock(lock_path)
