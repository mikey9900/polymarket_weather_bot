"""Queue-backed research and tuning automation manager."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifacts import build_artifacts
from .tuner import promote_candidate, propose_tuning, reject_candidate
from ..paths import CODEX_LATEST_PATH, CODEX_LOCK_PATH, CODEX_QUEUE_ROOT, CODEX_RUNS_ROOT, CODEX_STATE_PATH, TUNER_STATE_PATH


class CodexAutomationManager:
    def __init__(
        self,
        *,
        state_path: str | Path = CODEX_STATE_PATH,
        latest_path: str | Path = CODEX_LATEST_PATH,
        queue_root: str | Path = CODEX_QUEUE_ROOT,
        runs_root: str | Path = CODEX_RUNS_ROOT,
        lock_path: str | Path = CODEX_LOCK_PATH,
    ) -> None:
        self.state_path = Path(state_path)
        self.latest_path = Path(latest_path)
        self.queue_root = Path(queue_root)
        self.runs_root = Path(runs_root)
        self.lock_path = Path(lock_path)
        for path in (self.state_path.parent, self.latest_path.parent, self.queue_root, self.runs_root, self.lock_path.parent):
            path.mkdir(parents=True, exist_ok=True)

    def read_state(self) -> dict[str, Any]:
        return _load_json(
            self.state_path,
            default={
                "runner": {"healthy": False, "last_heartbeat_at": None},
                "active_run": None,
                "last_run": None,
            },
        )

    def save_state(self, state: dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def queue_depth(self) -> int:
        return len(list(self.queue_root.glob("*.json")))

    def snapshot(self) -> dict[str, Any]:
        state = self.read_state()
        tuner_state = _load_json(Path(TUNER_STATE_PATH), default={"status": "none", "latest_candidate": {}})
        return {
            "codex": {
                "healthy": bool((state.get("runner") or {}).get("healthy", False)),
                "last_heartbeat_at": (state.get("runner") or {}).get("last_heartbeat_at"),
                "queue_depth": self.queue_depth(),
                "active_run": state.get("active_run"),
                "last_run": state.get("last_run"),
            },
            "tuner": {
                "candidate_status": tuner_state.get("status", "none"),
                "latest_candidate": tuner_state.get("latest_candidate", {}),
            },
        }

    def enqueue_daily_refresh(self, *, requested_by: str = "operator", args: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.enqueue_job("daily_research_refresh", requested_by=requested_by, args=args)

    def enqueue_tuning(self, *, requested_by: str = "operator", args: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.enqueue_job("tuning_proposal", requested_by=requested_by, args=args)

    def promote_latest_candidate(self, *, requested_by: str = "operator") -> dict[str, Any]:
        result = promote_candidate()
        self._record_latest({"requested_by": requested_by, "result": result})
        return result

    def reject_latest_candidate(self, *, requested_by: str = "operator", reason: str = "Rejected by operator.") -> dict[str, Any]:
        result = reject_candidate(reason=reason)
        self._record_latest({"requested_by": requested_by, "result": result})
        return result

    def enqueue_job(self, job_type: str, *, requested_by: str = "operator", args: dict[str, Any] | None = None) -> dict[str, Any]:
        job_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        payload = {
            "job_id": job_id,
            "job_type": job_type,
            "requested_by": requested_by,
            "args": args or {},
            "requested_at": datetime.now(timezone.utc).isoformat(),
        }
        path = self.queue_root / f"{job_id}_{job_type}.json"
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return {"ok": True, "status": 200, "message": f"Queued {job_type}.", "job_path": str(path)}

    def run_heartbeat(self) -> dict[str, Any]:
        state = self.read_state()
        state.setdefault("runner", {})
        state["runner"]["healthy"] = True
        state["runner"]["last_heartbeat_at"] = datetime.now(timezone.utc).isoformat()
        self.save_state(state)

        job_paths = sorted(self.queue_root.glob("*.json"))
        if not job_paths:
            return {"ok": True, "message": "No queued jobs."}
        return self._process_job(job_paths[0])

    def _process_job(self, path: Path) -> dict[str, Any]:
        payload = _load_json(path, default={})
        state = self.read_state()
        state["active_run"] = payload
        self.save_state(state)
        result: dict[str, Any]
        job_type = str(payload.get("job_type") or "")
        if job_type == "daily_research_refresh":
            result = build_artifacts()
            result = {"ok": True, "status": 200, "message": "Research artifacts refreshed.", **result}
        elif job_type == "tuning_proposal":
            tuning = propose_tuning()
            result = {"ok": True, "status": 200, "message": "Tuning proposal refreshed.", **tuning}
        else:
            result = {"ok": False, "status": 400, "message": f"Unknown job type: {job_type}"}

        run_record = {
            "job_type": job_type,
            "job_id": payload.get("job_id"),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "result": result,
        }
        (self.runs_root / f"{payload.get('job_id') or 'run'}.json").write_text(json.dumps(run_record, indent=2, sort_keys=True), encoding="utf-8")
        self._record_latest(run_record)
        state = self.read_state()
        state["active_run"] = None
        state["last_run"] = run_record
        self.save_state(state)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        return result

    def _record_latest(self, payload: dict[str, Any]) -> None:
        self.latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _load_json(path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default
    return payload if isinstance(payload, dict) else default
