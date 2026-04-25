"""Runtime-facing research policy loader."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .buckets import (
    agreement_bucket,
    cluster_payload,
    dispersion_bucket,
    edge_bucket,
    liquidity_bucket,
    staleness_bucket,
    time_bucket,
)
from ..paths import RUNTIME_POLICY_PATH


class ResearchSnapshotProvider:
    def __init__(self, policy_path: str | Path = RUNTIME_POLICY_PATH):
        self.path = Path(policy_path)
        self._snapshot: dict = {
            "clusters": {},
            "city_features": {},
            "market_features": {},
            "agreement_features": {},
            "edge_features": {},
            "liquidity_features": {},
            "time_features": {},
            "dispersion_features": {},
            "staleness_features": {},
        }
        self._mtime_ns: int | None = None
        self._last_loaded_at: str | None = None
        self._last_error: str | None = None

    def adjust_signal(self, signal) -> dict[str, object]:
        self._reload_if_needed()
        source_age_hours = _signal_age_hours(signal)
        payload = cluster_payload(
            market_type=signal.market_type,
            city_slug=signal.city_slug,
            source_count=signal.source_count,
            edge_abs=signal.edge_abs,
            liquidity=signal.liquidity,
            time_to_resolution_s=signal.time_to_resolution_s,
            source_dispersion_pct=signal.source_dispersion_pct,
            source_age_hours=source_age_hours,
            confidence=signal.confidence,
        )
        feature_hits = {
            "city": ((self._snapshot.get("city_features") or {}).get(signal.city_slug) or {}),
            "market": ((self._snapshot.get("market_features") or {}).get(signal.market_type) or {}),
            "agreement": ((self._snapshot.get("agreement_features") or {}).get(agreement_bucket(signal.source_count, signal.confidence)) or {}),
            "edge": ((self._snapshot.get("edge_features") or {}).get(edge_bucket(signal.edge_abs)) or {}),
            "liquidity": ((self._snapshot.get("liquidity_features") or {}).get(liquidity_bucket(signal.liquidity)) or {}),
            "time": ((self._snapshot.get("time_features") or {}).get(time_bucket(signal.time_to_resolution_s)) or {}),
            "dispersion": ((self._snapshot.get("dispersion_features") or {}).get(dispersion_bucket(signal.source_dispersion_pct)) or {}),
            "staleness": ((self._snapshot.get("staleness_features") or {}).get(staleness_bucket(source_age_hours)) or {}),
        }
        cluster = (self._snapshot.get("clusters") or {}).get(payload["cluster_id"]) or {}
        cluster_action = str(cluster.get("policy_action") or "advisory")
        cluster_adjustment = float(cluster.get("score_adjustment") or 0.0)
        feature_adjustments = {
            name: round(float(item.get("score_adjustment") or 0.0), 4)
            for name, item in feature_hits.items()
        }
        score_adjustment = round(
            max(-0.16, min(0.12, cluster_adjustment + sum(feature_adjustments.values()))),
            4,
        )
        return {
            "cluster_id": payload["cluster_id"],
            "policy_action": cluster_action,
            "cluster_sample_size": int(cluster.get("sample_size") or 0),
            "cluster_win_pct": float(cluster.get("win_pct") or 0.0),
            "score_adjustment": score_adjustment,
            "feature_adjustments": feature_adjustments,
            "feature_keys": {
                "agreement": payload["agreement_bucket"],
                "edge": payload["edge_bucket"],
                "liquidity": payload["liquidity_bucket"],
                "time": payload["time_bucket"],
                "dispersion": payload["dispersion_bucket"],
                "staleness": payload["staleness_bucket"],
            },
        }

    def status(self) -> dict[str, object]:
        self._reload_if_needed()
        return {
            "artifact_path": str(self.path),
            "artifact_exists": self.path.exists(),
            "cluster_count": len(self._snapshot.get("clusters") or {}),
            "city_feature_count": len(self._snapshot.get("city_features") or {}),
            "dimension_feature_counts": {
                "market": len(self._snapshot.get("market_features") or {}),
                "agreement": len(self._snapshot.get("agreement_features") or {}),
                "edge": len(self._snapshot.get("edge_features") or {}),
                "liquidity": len(self._snapshot.get("liquidity_features") or {}),
                "time": len(self._snapshot.get("time_features") or {}),
                "dispersion": len(self._snapshot.get("dispersion_features") or {}),
                "staleness": len(self._snapshot.get("staleness_features") or {}),
            },
            "last_loaded_at": self._last_loaded_at,
            "last_error": self._last_error,
        }

    def _reload_if_needed(self) -> None:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            self._snapshot = {
                "clusters": {},
                "city_features": {},
                "market_features": {},
                "agreement_features": {},
                "edge_features": {},
                "liquidity_features": {},
                "time_features": {},
                "dispersion_features": {},
                "staleness_features": {},
            }
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


def _signal_age_hours(signal) -> float | None:
    raw = str(getattr(signal, "created_at", "") or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        created_at = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    age_s = (datetime.now(timezone.utc) - created_at.astimezone(timezone.utc)).total_seconds()
    return max(0.0, age_s / 3600.0)
