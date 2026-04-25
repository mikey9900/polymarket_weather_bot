"""Build runtime policy and research reports from tracker history."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .buckets import cluster_payload
from .warehouse import ResearchWarehouse
from ..paths import RESEARCH_REPORT_JSON_PATH, RESEARCH_REPORT_MD_PATH, RUNTIME_POLICY_PATH, TRACKER_DB_PATH, WAREHOUSE_PATH


def build_artifacts(
    *,
    tracker_db: str | Path = TRACKER_DB_PATH,
    policy_path: str | Path = RUNTIME_POLICY_PATH,
    report_json_path: str | Path = RESEARCH_REPORT_JSON_PATH,
    report_md_path: str | Path = RESEARCH_REPORT_MD_PATH,
    warehouse_path: str | Path = WAREHOUSE_PATH,
    lookback_days: int = 90,
) -> dict[str, object]:
    tracker_path = Path(tracker_db)
    tracker_path.parent.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(lookback_days))
    rows = _load_resolved_positions(tracker_path, cutoff)
    clusters: dict[str, dict[str, Any]] = {}
    city_rollups: dict[str, dict[str, Any]] = defaultdict(lambda: {"sample_size": 0, "wins": 0, "losses": 0, "total_pnl": 0.0})

    for row in rows:
        payload = cluster_payload(
            market_type=str(row["market_type"] or "unknown"),
            city_slug=str(row["city_slug"] or "unknown"),
            source_count=int(row["source_count"] or 1),
            edge_abs=float(row["edge_abs"] or 0.0),
            liquidity=float(row["liquidity"] or 0.0),
            time_to_resolution_s=float(row["time_to_resolution_s"] or 0.0),
        )
        cluster = clusters.setdefault(
            payload["cluster_id"],
            {
                **payload,
                "sample_size": 0,
                "wins": 0,
                "losses": 0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "win_pct": 0.0,
                "policy_action": "advisory",
                "score_adjustment": 0.0,
            },
        )
        pnl = float(row["realized_pnl"] or 0.0)
        cluster["sample_size"] += 1
        cluster["wins"] += int(pnl > 0)
        cluster["losses"] += int(pnl < 0)
        cluster["total_pnl"] += pnl

        city = city_rollups[str(row["city_slug"] or "unknown")]
        city["sample_size"] += 1
        city["wins"] += int(pnl > 0)
        city["losses"] += int(pnl < 0)
        city["total_pnl"] += pnl

    for cluster in clusters.values():
        sample_size = int(cluster["sample_size"] or 0)
        wins = int(cluster["wins"] or 0)
        total_pnl = float(cluster["total_pnl"] or 0.0)
        cluster["avg_pnl"] = round(total_pnl / sample_size, 6) if sample_size else 0.0
        cluster["win_pct"] = round((wins / sample_size * 100.0), 2) if sample_size else 0.0
        if sample_size >= 8 and cluster["avg_pnl"] < 0 and cluster["win_pct"] < 45.0:
            cluster["policy_action"] = "paper_blocked"
            cluster["score_adjustment"] = -0.08
        elif sample_size >= 8 and cluster["avg_pnl"] > 0 and cluster["win_pct"] >= 60.0:
            cluster["policy_action"] = "boosted"
            cluster["score_adjustment"] = 0.04

    city_features = {}
    for city_slug, data in city_rollups.items():
        sample_size = int(data["sample_size"] or 0)
        wins = int(data["wins"] or 0)
        win_pct = (wins / sample_size * 100.0) if sample_size else 0.0
        adjustment = 0.0
        if sample_size >= 5 and win_pct < 45.0:
            adjustment = -0.03
        elif sample_size >= 5 and win_pct > 60.0:
            adjustment = 0.02
        city_features[city_slug] = {
            "city_slug": city_slug,
            "sample_size": sample_size,
            "win_pct": round(win_pct, 2),
            "avg_pnl": round(float(data["total_pnl"] or 0.0) / sample_size, 6) if sample_size else 0.0,
            "score_adjustment": adjustment,
        }

    policy = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": int(lookback_days),
        "outcome_count": len(rows),
        "cluster_count": len(clusters),
        "clusters": dict(sorted(clusters.items())),
        "city_features": city_features,
    }
    report = {
        "generated_at": policy["generated_at"],
        "lookback_days": int(lookback_days),
        "outcome_count": len(rows),
        "cluster_count": len(clusters),
        "top_clusters": sorted(clusters.values(), key=lambda item: (float(item["avg_pnl"]), int(item["sample_size"])), reverse=True)[:10],
        "weak_clusters": sorted(clusters.values(), key=lambda item: (float(item["avg_pnl"]), -int(item["sample_size"])))[:10],
        "city_features": sorted(city_features.values(), key=lambda item: (float(item["score_adjustment"]), float(item["avg_pnl"])), reverse=True),
    }

    policy_output = Path(policy_path)
    report_json_output = Path(report_json_path)
    report_md_output = Path(report_md_path)
    policy_output.parent.mkdir(parents=True, exist_ok=True)
    report_json_output.parent.mkdir(parents=True, exist_ok=True)
    report_md_output.parent.mkdir(parents=True, exist_ok=True)
    policy_output.write_text(json.dumps(policy, indent=2, sort_keys=True), encoding="utf-8")
    report_json_output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    report_md_output.write_text(_render_markdown(report), encoding="utf-8")

    warehouse_status = {"ok": False, "skipped": True, "message": "duckdb unavailable"}
    try:
        warehouse = ResearchWarehouse(warehouse_path)
    except RuntimeError:
        warehouse = None
    if warehouse is not None:
        try:
            warehouse_status = warehouse.sync_from_tracker(tracker_path)
        finally:
            warehouse.close()

    return {
        "policy_path": str(policy_output),
        "report_json_path": str(report_json_output),
        "report_md_path": str(report_md_output),
        "cluster_count": len(clusters),
        "outcome_count": len(rows),
        "warehouse": warehouse_status,
    }


def _load_resolved_positions(db_path: Path, cutoff: datetime) -> list[sqlite3.Row]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                pp.market_type,
                pp.city_slug,
                pp.realized_pnl,
                pp.resolved_at,
                COALESCE(s.source_count, 1) AS source_count,
                COALESCE(s.edge_abs, 0.0) AS edge_abs,
                COALESCE(s.liquidity, 0.0) AS liquidity,
                COALESCE(s.time_to_resolution_s, 0.0) AS time_to_resolution_s
            FROM paper_positions pp
            LEFT JOIN signals s ON s.id = pp.signal_id
            WHERE pp.status = 'resolved'
              AND COALESCE(pp.resolved_at, pp.created_at) >= ?
            """,
            (cutoff.isoformat(),),
        ).fetchall()
    finally:
        conn.close()
    return list(rows)


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Weather Research Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Outcomes: `{report['outcome_count']}`",
        f"- Clusters: `{report['cluster_count']}`",
        "",
        "## Top Clusters",
    ]
    for cluster in report.get("top_clusters", []):
        lines.append(
            f"- `{cluster['cluster_id']}` | avg pnl `{cluster['avg_pnl']:+.4f}` | "
            f"win `{cluster['win_pct']:.1f}%` | n `{cluster['sample_size']}`"
        )
    lines.append("")
    lines.append("## City Features")
    for item in report.get("city_features", []):
        lines.append(
            f"- `{item['city_slug']}` | win `{item['win_pct']:.1f}%` | "
            f"avg pnl `{item['avg_pnl']:+.4f}` | score adj `{item['score_adjustment']:+.2f}`"
        )
    return "\n".join(lines)
