"""Build runtime policy and research reports from tracker history."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .buckets import (
    agreement_bucket,
    cluster_payload,
    dispersion_bucket,
    edge_bucket,
    liquidity_bucket,
    staleness_bucket,
    time_bucket,
)
from .warehouse import ResearchWarehouse
from ..paths import (
    APPROVED_CONFIG_RECEIPT_PATH,
    CONFIG_CANDIDATES_ROOT,
    RESEARCH_BUNDLE_PATH,
    RESEARCH_REPORT_JSON_PATH,
    RESEARCH_REPORT_MD_PATH,
    RUNTIME_POLICY_PATH,
    TRACKER_DB_PATH,
    TUNER_STATE_PATH,
    WAREHOUSE_PATH,
)


POLICY_RULES = {
    "cluster": {"min_samples": 8, "negative_adjustment": -0.08, "positive_adjustment": 0.04},
    "city": {"min_samples": 6, "negative_adjustment": -0.03, "positive_adjustment": 0.02},
    "market": {"min_samples": 6, "negative_adjustment": -0.02, "positive_adjustment": 0.02},
    "agreement": {"min_samples": 6, "negative_adjustment": -0.02, "positive_adjustment": 0.015},
    "edge": {"min_samples": 6, "negative_adjustment": -0.02, "positive_adjustment": 0.015},
    "liquidity": {"min_samples": 6, "negative_adjustment": -0.02, "positive_adjustment": 0.015},
    "time": {"min_samples": 6, "negative_adjustment": -0.02, "positive_adjustment": 0.015},
    "dispersion": {"min_samples": 5, "negative_adjustment": -0.03, "positive_adjustment": 0.02},
    "staleness": {"min_samples": 5, "negative_adjustment": -0.03, "positive_adjustment": 0.02},
}


def build_artifacts(
    *,
    tracker_db: str | Path = TRACKER_DB_PATH,
    policy_path: str | Path = RUNTIME_POLICY_PATH,
    report_json_path: str | Path = RESEARCH_REPORT_JSON_PATH,
    report_md_path: str | Path = RESEARCH_REPORT_MD_PATH,
    bundle_path: str | Path = RESEARCH_BUNDLE_PATH,
    warehouse_path: str | Path = WAREHOUSE_PATH,
    lookback_days: int = 90,
) -> dict[str, object]:
    tracker_path = Path(tracker_db)
    tracker_path.parent.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(days=int(lookback_days))
    rows = _load_resolved_positions(tracker_path, cutoff)

    clusters = _rollup_clusters(rows)
    city_features = _rollup_dimension(rows, "city", lambda row: str(row.get("city_slug") or "unknown"), "city_slug")
    market_features = _rollup_dimension(rows, "market", lambda row: str(row.get("market_type") or "unknown"), "market_type")
    agreement_features = _rollup_dimension(rows, "agreement", lambda row: str(row.get("agreement_bucket") or "unknown"), "agreement_bucket")
    edge_features = _rollup_dimension(rows, "edge", lambda row: str(row.get("edge_bucket") or "unknown"), "edge_bucket")
    liquidity_features = _rollup_dimension(rows, "liquidity", lambda row: str(row.get("liquidity_bucket") or "unknown"), "liquidity_bucket")
    time_features = _rollup_dimension(rows, "time", lambda row: str(row.get("time_bucket") or "unknown"), "time_bucket")
    dispersion_features = _rollup_dimension(rows, "dispersion", lambda row: str(row.get("dispersion_bucket") or "unknown"), "dispersion_bucket")
    staleness_features = _rollup_dimension(rows, "staleness", lambda row: str(row.get("staleness_bucket") or "unknown"), "staleness_bucket")

    policy = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lookback_days": int(lookback_days),
        "outcome_count": len(rows),
        "cluster_count": len(clusters),
        "clusters": clusters,
        "city_features": city_features,
        "market_features": market_features,
        "agreement_features": agreement_features,
        "edge_features": edge_features,
        "liquidity_features": liquidity_features,
        "time_features": time_features,
        "dispersion_features": dispersion_features,
        "staleness_features": staleness_features,
    }
    feature_sections = {
        "cities": city_features,
        "markets": market_features,
        "agreement": agreement_features,
        "edge": edge_features,
        "liquidity": liquidity_features,
        "time": time_features,
        "dispersion": dispersion_features,
        "staleness": staleness_features,
    }
    report = {
        "generated_at": policy["generated_at"],
        "lookback_days": int(lookback_days),
        "outcome_count": len(rows),
        "cluster_count": len(clusters),
        "summary": _summary(rows),
        "top_clusters": _top_entries(clusters, count=10, reverse=True),
        "weak_clusters": _top_entries(clusters, count=10, reverse=False),
        "feature_sections": {name: _top_entries(items, count=6, reverse=True) for name, items in feature_sections.items()},
        "feature_risks": {name: _top_entries(items, count=6, reverse=False) for name, items in feature_sections.items()},
    }

    policy_output = Path(policy_path)
    report_json_output = Path(report_json_path)
    report_md_output = Path(report_md_path)
    bundle_output = Path(bundle_path)
    for path in (policy_output, report_json_output, report_md_output, bundle_output):
        path.parent.mkdir(parents=True, exist_ok=True)
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

    bundle = _build_bundle(
        report=report,
        policy_output=policy_output,
        report_json_output=report_json_output,
        report_md_output=report_md_output,
        warehouse_status=warehouse_status,
    )
    bundle_output.write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "policy_path": str(policy_output),
        "report_json_path": str(report_json_output),
        "report_md_path": str(report_md_output),
        "bundle_path": str(bundle_output),
        "cluster_count": len(clusters),
        "outcome_count": len(rows),
        "warehouse": warehouse_status,
    }


def _load_resolved_positions(db_path: Path, cutoff: datetime) -> list[dict[str, Any]]:
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
                pp.market_slug,
                pp.event_date,
                pp.label,
                pp.direction,
                pp.realized_pnl,
                pp.resolved_at,
                pp.created_at AS position_created_at,
                COALESCE(s.confidence, 'unknown') AS confidence,
                COALESCE(s.source_count, 1) AS source_count,
                COALESCE(s.edge_abs, 0.0) AS edge_abs,
                COALESCE(s.liquidity, 0.0) AS liquidity,
                COALESCE(s.time_to_resolution_s, 0.0) AS time_to_resolution_s,
                COALESCE(s.source_dispersion_pct, 0.0) AS source_dispersion_pct,
                COALESCE(s.score, 0.0) AS adapter_score,
                COALESCE(d.final_score, 0.0) AS final_score,
                COALESCE(d.policy_action, 'advisory') AS decision_policy_action,
                d.metadata_json
            FROM paper_positions pp
            LEFT JOIN signals s ON s.id = pp.signal_id
            LEFT JOIN (
                SELECT d1.*
                FROM decisions d1
                JOIN (
                    SELECT signal_id, MAX(id) AS latest_id
                    FROM decisions
                    GROUP BY signal_id
                ) latest ON latest.latest_id = d1.id
            ) d ON d.signal_id = pp.signal_id
            WHERE pp.status = 'resolved'
              AND COALESCE(pp.resolved_at, pp.created_at) >= ?
            """,
            (cutoff.isoformat(),),
        ).fetchall()
    finally:
        conn.close()

    payloads: list[dict[str, Any]] = []
    for row in rows:
        metadata = _load_json_blob(row["metadata_json"])
        source_age_hours = _float_or_none(metadata.get("source_age_hours"))
        cluster = cluster_payload(
            market_type=str(row["market_type"] or "unknown"),
            city_slug=str(row["city_slug"] or "unknown"),
            source_count=int(row["source_count"] or 1),
            edge_abs=float(row["edge_abs"] or 0.0),
            liquidity=float(row["liquidity"] or 0.0),
            time_to_resolution_s=_float_or_none(row["time_to_resolution_s"]),
            source_dispersion_pct=float(row["source_dispersion_pct"] or 0.0),
            source_age_hours=source_age_hours,
            confidence=str(row["confidence"] or "unknown"),
        )
        pnl = float(row["realized_pnl"] or 0.0)
        payloads.append(
            {
                "market_type": str(row["market_type"] or "unknown"),
                "city_slug": str(row["city_slug"] or "unknown"),
                "market_slug": str(row["market_slug"] or ""),
                "event_date": str(row["event_date"] or ""),
                "label": str(row["label"] or ""),
                "direction": str(row["direction"] or ""),
                "realized_pnl": pnl,
                "resolved_at": str(row["resolved_at"] or ""),
                "confidence": str(row["confidence"] or "unknown"),
                "source_count": int(row["source_count"] or 1),
                "edge_abs": float(row["edge_abs"] or 0.0),
                "liquidity": float(row["liquidity"] or 0.0),
                "time_to_resolution_s": _float_or_none(row["time_to_resolution_s"]),
                "source_dispersion_pct": float(row["source_dispersion_pct"] or 0.0),
                "source_age_hours": source_age_hours,
                "adapter_score": float(row["adapter_score"] or 0.0),
                "final_score": float(row["final_score"] or 0.0),
                "decision_policy_action": str(row["decision_policy_action"] or "advisory"),
                "realized_outcome": "win" if pnl > 0 else "loss" if pnl < 0 else "flat",
                **cluster,
            }
        )
    return payloads


def _rollup_clusters(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    items: dict[str, dict[str, Any]] = {}
    for row in rows:
        cluster_id = str(row.get("cluster_id") or "unknown")
        item = items.setdefault(
            cluster_id,
            {
                "cluster_id": cluster_id,
                "market_type": row.get("market_type"),
                "city_slug": row.get("city_slug"),
                "agreement_bucket": row.get("agreement_bucket"),
                "edge_bucket": row.get("edge_bucket"),
                "liquidity_bucket": row.get("liquidity_bucket"),
                "time_bucket": row.get("time_bucket"),
                "dispersion_bucket": row.get("dispersion_bucket"),
                "staleness_bucket": row.get("staleness_bucket"),
                "sample_size": 0,
                "wins": 0,
                "losses": 0,
                "flats": 0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "win_pct": 0.0,
                "policy_action": "advisory",
                "score_adjustment": 0.0,
            },
        )
        _accumulate(item, row)
    return {key: _finalize_rollup(item, "cluster") for key, item in sorted(items.items())}


def _rollup_dimension(
    rows: list[dict[str, Any]],
    scope: str,
    key_fn: Callable[[dict[str, Any]], str],
    field_name: str,
) -> dict[str, dict[str, Any]]:
    items: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = key_fn(row)
        item = items.setdefault(
            key,
            {
                field_name: key,
                "sample_size": 0,
                "wins": 0,
                "losses": 0,
                "flats": 0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "win_pct": 0.0,
                "policy_action": "advisory",
                "score_adjustment": 0.0,
            },
        )
        _accumulate(item, row)
    return {key: _finalize_rollup(item, scope) for key, item in sorted(items.items())}


def _accumulate(item: dict[str, Any], row: dict[str, Any]) -> None:
    pnl = float(row.get("realized_pnl") or 0.0)
    item["sample_size"] += 1
    item["wins"] += int(pnl > 0)
    item["losses"] += int(pnl < 0)
    item["flats"] += int(pnl == 0)
    item["total_pnl"] += pnl


def _finalize_rollup(item: dict[str, Any], scope: str) -> dict[str, Any]:
    sample_size = int(item.get("sample_size") or 0)
    wins = int(item.get("wins") or 0)
    total_pnl = float(item.get("total_pnl") or 0.0)
    item["avg_pnl"] = round(total_pnl / sample_size, 6) if sample_size else 0.0
    item["win_pct"] = round((wins / sample_size * 100.0), 2) if sample_size else 0.0
    action, adjustment = _policy_for_rollup(scope, sample_size, item["avg_pnl"], item["win_pct"])
    item["policy_action"] = action
    item["score_adjustment"] = adjustment
    item["total_pnl"] = round(total_pnl, 6)
    return item


def _policy_for_rollup(scope: str, sample_size: int, avg_pnl: float, win_pct: float) -> tuple[str, float]:
    rule = POLICY_RULES[scope]
    if sample_size < int(rule["min_samples"]):
        return "advisory", 0.0
    if avg_pnl < 0 and win_pct < 45.0:
        action = "paper_blocked" if scope == "cluster" else "risk_off"
        return action, float(rule["negative_adjustment"])
    if avg_pnl > 0 and win_pct >= 60.0:
        action = "boosted" if scope == "cluster" else "favored"
        return action, float(rule["positive_adjustment"])
    return "advisory", 0.0


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for row in rows if float(row.get("realized_pnl") or 0.0) > 0)
    losses = sum(1 for row in rows if float(row.get("realized_pnl") or 0.0) < 0)
    total_pnl = round(sum(float(row.get("realized_pnl") or 0.0) for row in rows), 6)
    return {
        "wins": wins,
        "losses": losses,
        "flats": len(rows) - wins - losses,
        "total_pnl": total_pnl,
        "win_rate": round((wins / len(rows) * 100.0), 2) if rows else 0.0,
    }


def _top_entries(items: dict[str, dict[str, Any]], *, count: int, reverse: bool) -> list[dict[str, Any]]:
    values = list(items.values())
    values.sort(
        key=lambda item: (float(item.get("avg_pnl") or 0.0), float(item.get("win_pct") or 0.0), int(item.get("sample_size") or 0)),
        reverse=reverse,
    )
    return values[:count]


def _build_bundle(
    *,
    report: dict[str, Any],
    policy_output: Path,
    report_json_output: Path,
    report_md_output: Path,
    warehouse_status: dict[str, Any],
) -> dict[str, Any]:
    tuner_state = _load_optional_json(Path(TUNER_STATE_PATH))
    receipt = _load_optional_json(Path(APPROVED_CONFIG_RECEIPT_PATH))
    candidates = [
        {
            "path": str(path),
            "modified_at": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(),
        }
        for path in sorted(CONFIG_CANDIDATES_ROOT.glob("*.yaml"), key=lambda item: item.stat().st_mtime, reverse=True)[:5]
    ]
    return {
        "generated_at": report["generated_at"],
        "summary": {
            "outcome_count": report["outcome_count"],
            "cluster_count": report["cluster_count"],
            "wins": (report.get("summary") or {}).get("wins", 0),
            "losses": (report.get("summary") or {}).get("losses", 0),
            "total_pnl": (report.get("summary") or {}).get("total_pnl", 0.0),
        },
        "artifacts": {
            "policy_path": str(policy_output),
            "report_json_path": str(report_json_output),
            "report_md_path": str(report_md_output),
            "warehouse": warehouse_status,
        },
        "review": {
            "top_clusters": report.get("top_clusters", [])[:5],
            "weak_clusters": report.get("weak_clusters", [])[:5],
            "top_cities": (report.get("feature_sections") or {}).get("cities", [])[:5],
            "risky_staleness": (report.get("feature_risks") or {}).get("staleness", [])[:5],
            "risky_dispersion": (report.get("feature_risks") or {}).get("dispersion", [])[:5],
        },
        "tuner": tuner_state,
        "approved_config_receipt": receipt,
        "candidates": candidates,
    }


def _load_optional_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_json_blob(value: object) -> dict[str, Any]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Weather Research Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Outcomes: `{report['outcome_count']}`",
        f"- Clusters: `{report['cluster_count']}`",
        f"- Win rate: `{(report.get('summary') or {}).get('win_rate', 0.0):.1f}%`",
        f"- Total PnL: `{(report.get('summary') or {}).get('total_pnl', 0.0):+.4f}`",
        "",
        "## Top Clusters",
    ]
    for cluster in report.get("top_clusters", []):
        lines.append(
            f"- `{cluster['cluster_id']}` | avg pnl `{cluster['avg_pnl']:+.4f}` | "
            f"win `{cluster['win_pct']:.1f}%` | n `{cluster['sample_size']}`"
        )
    lines.append("")
    lines.append("## Risk Clusters")
    for cluster in report.get("weak_clusters", []):
        lines.append(
            f"- `{cluster['cluster_id']}` | avg pnl `{cluster['avg_pnl']:+.4f}` | "
            f"win `{cluster['win_pct']:.1f}%` | n `{cluster['sample_size']}`"
        )
    for section_name in ("cities", "markets", "agreement", "dispersion", "staleness"):
        lines.append("")
        lines.append(f"## {section_name.title()} Features")
        for item in (report.get("feature_sections") or {}).get(section_name, []):
            key = next((value for key_name, value in item.items() if key_name.endswith("_bucket") or key_name.endswith("_slug") or key_name == "market_type"), "unknown")
            lines.append(
                f"- `{key}` | avg pnl `{item['avg_pnl']:+.4f}` | "
                f"win `{item['win_pct']:.1f}%` | n `{item['sample_size']}` | adj `{item['score_adjustment']:+.3f}`"
            )
    return "\n".join(lines)
