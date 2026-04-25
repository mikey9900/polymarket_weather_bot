"""Runtime-facing research policy loader."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .buckets import cluster_payload
from ..paths import RUNTIME_POLICY_PATH


class ResearchSnapshotProvider:
    def __init__(self, policy_path: str | Path = RUNTIME_POLICY_PATH):
        self.path = Path(policy_path)
        self._snapshot: dict = {"clusters": {}, "city_features": {}}
        self._mtime_ns: int | None = None
        self._last_loaded_at: str | None = None
        self._last_error: str | None = None

    def adjust_signal(self, signal) -> dict[str, object]:
        self._reload_if_needed()
        payload = cluster_payload(
            market_type=signal.market_type,
            city_slug=signal.city_slug,
            source_count=signal.source_count,
            edge_abs=signal.edge_abs,
            liquidity=signal.liquidity,
            time_to_resolution_s=signal.time_to_resolution_s,
        )
        cluster = (self._snapshot.get("clusters") or {}).get(payload["cluster_id"]) or {}
        city_features = (self._snapshot.get("city_features") or {}).get(signal.city_slug) or {}
        policy_action = str(cluster.get("policy_action") or "advisory")
        score_adjustment = float(cluster.get("score_adjustment") or 0.0) + float(city_features.get("score_adjustment") or 0.0)
        return {
            "cluster_id": payload["cluster_id"],
            "policy_action": policy_action,
            "cluster_sample_size": int(cluster.get("sample_size") or 0),
            "cluster_win_pct": float(cluster.get("win_pct") or 0.0),
            "score_adjustment": round(score_adjustment, 4),
        }

    def status(self) -> dict[str, object]:
        self._reload_if_needed()
        return {
            "artifact_path": str(self.path),
            "artifact_exists": self.path.exists(),
            "cluster_count": len(self._snapshot.get("clusters") or {}),
            "city_feature_count": len(self._snapshot.get("city_features") or {}),
            "last_loaded_at": self._last_loaded_at,
            "last_error": self._last_error,
        }

    def _reload_if_needed(self) -> None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            self._snapshot = {"clusters": {}, "city_features": {}}
            self._mtime_ns = None
            self._last_error = None
            return
        if self._mtime_ns == stat.st_mtime_ns:
            return
        try:
            self._snapshot = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            self._last_error = f"Invalid research policy JSON: {exc}"
            return
        self._mtime_ns = stat.st_mtime_ns
        self._last_loaded_at = datetime.now(timezone.utc).isoformat()
        self._last_error = None
