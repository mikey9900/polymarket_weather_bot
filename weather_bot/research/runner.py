"""Background runner for queue-backed research jobs."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from .codex_automation import CodexAutomationManager


def main() -> None:
    manager = CodexAutomationManager()
    poll_seconds = _resolve_poll_seconds()
    while True:
        manager.run_heartbeat()
        time.sleep(poll_seconds)


def _resolve_poll_seconds() -> float:
    env_value = os.getenv("WEATHER_CODEX_POLL_SECONDS", "").strip()
    if env_value:
        try:
            return max(2.0, float(env_value))
        except ValueError:
            pass
    options_path = Path("/data/options.json")
    if options_path.exists():
        try:
            payload = json.loads(options_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {}
        if isinstance(payload, dict):
            try:
                return max(2.0, float(payload.get("poll_seconds", 15)))
            except (TypeError, ValueError):
                pass
    return 15.0


if __name__ == "__main__":
    main()
