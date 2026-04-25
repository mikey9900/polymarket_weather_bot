"""Deterministic config tuning helpers for the weather platform."""

from __future__ import annotations

import difflib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .artifacts import build_artifacts
from ..paths import (
    ACTIVE_CONFIG_PATH,
    APPROVED_CONFIG_RECEIPT_PATH,
    CONFIG_CANDIDATES_ROOT,
    TRACKER_DB_PATH,
    TUNER_ACTIVE_PATCH_PATH,
    TUNER_REPORT_JSON_PATH,
    TUNER_REPORT_MD_PATH,
    TUNER_STATE_PATH,
)


SAFE_TUNER_PATHS = {
    "strategy.temperature.min_score",
    "strategy.temperature.min_edge_abs",
    "strategy.temperature.min_liquidity",
    "strategy.temperature.min_source_count",
    "strategy.temperature.max_source_dispersion_pct",
    "strategy.temperature.max_source_age_hours",
}


def propose_tuning(
    *,
    config_path: str | Path = ACTIVE_CONFIG_PATH,
    tracker_db: str | Path = TRACKER_DB_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    report_json_path: str | Path = TUNER_REPORT_JSON_PATH,
    report_md_path: str | Path = TUNER_REPORT_MD_PATH,
    patch_path: str | Path = TUNER_ACTIVE_PATCH_PATH,
    artifact_overrides: dict[str, str | Path] | None = None,
) -> dict[str, Any]:
    config_file = Path(config_path)
    current_config = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
    tracker_path = Path(tracker_db)
    metrics = _load_metrics(tracker_path)
    changes = _recommend_changes(current_config, metrics)

    generated_at = datetime.now(timezone.utc)
    candidate_id = generated_at.strftime("%Y%m%dT%H%M%SZ")
    candidate_path: Path | None = None
    candidate_config = json.loads(json.dumps(current_config))
    if changes:
        for change in changes:
            _set_path(candidate_config, change["path"], change["recommended"])
        CONFIG_CANDIDATES_ROOT.mkdir(parents=True, exist_ok=True)
        candidate_path = CONFIG_CANDIDATES_ROOT / f"{candidate_id}_weather_tuned.yaml"
        candidate_path.write_text(yaml.safe_dump(candidate_config, sort_keys=False), encoding="utf-8")

    current_text = yaml.safe_dump(current_config, sort_keys=False)
    candidate_text = yaml.safe_dump(candidate_config, sort_keys=False)
    patch_file = Path(patch_path)
    patch_file.parent.mkdir(parents=True, exist_ok=True)
    patch_file.write_text(
        "".join(
            difflib.unified_diff(
                current_text.splitlines(keepends=True),
                candidate_text.splitlines(keepends=True),
                fromfile=str(config_file),
                tofile=str(candidate_path or config_file),
            )
        ),
        encoding="utf-8",
    )

    artifact_kwargs = dict(artifact_overrides or {})
    artifact_result = build_artifacts(tracker_db=tracker_path, **artifact_kwargs)
    report = {
        "generated_at": generated_at.isoformat(),
        "candidate_id": candidate_id if candidate_path else None,
        "candidate_status": "ready" if candidate_path else "none",
        "candidate_path": str(candidate_path) if candidate_path else "",
        "metrics": metrics,
        "changes": changes,
        "artifact_result": artifact_result,
    }
    report_json_file = Path(report_json_path)
    report_md_file = Path(report_md_path)
    report_json_file.parent.mkdir(parents=True, exist_ok=True)
    report_md_file.parent.mkdir(parents=True, exist_ok=True)
    report_json_file.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    report_md_file.write_text(_render_markdown(report), encoding="utf-8")

    state = {
        "status": "ready" if candidate_path else "none",
        "latest_candidate": {
            "candidate_id": candidate_id if candidate_path else "",
            "candidate_path": str(candidate_path) if candidate_path else "",
            "generated_at": generated_at.isoformat(),
            "change_count": len(changes),
            "changed_paths": [change["path"] for change in changes],
        },
    }
    Path(tuner_state_path).write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    return report


def promote_candidate(
    *,
    candidate_path: str | Path | None = None,
    config_path: str | Path = ACTIVE_CONFIG_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    receipt_path: str | Path = APPROVED_CONFIG_RECEIPT_PATH,
) -> dict[str, Any]:
    state = _load_json(Path(tuner_state_path), default={})
    latest_candidate = dict(state.get("latest_candidate") or {})
    candidate_file = Path(candidate_path or latest_candidate.get("candidate_path") or "")
    if not candidate_file.exists():
        return {"ok": False, "status": 404, "message": "No candidate config is available to promote."}
    config_file = Path(config_path)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    history_root = config_file.parent / "history"
    history_root.mkdir(parents=True, exist_ok=True)
    backup_path = history_root / f"active_config_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.yaml"
    if config_file.exists():
        backup_path.write_text(config_file.read_text(encoding="utf-8"), encoding="utf-8")
    config_file.write_text(candidate_file.read_text(encoding="utf-8"), encoding="utf-8")
    receipt = {
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "candidate_id": latest_candidate.get("candidate_id", ""),
        "candidate_path": str(candidate_file),
        "backup_path": str(backup_path),
        "changed_paths": list(latest_candidate.get("changed_paths") or []),
    }
    receipt_file = Path(receipt_path)
    receipt_file.parent.mkdir(parents=True, exist_ok=True)
    receipt_file.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
    Path(tuner_state_path).write_text(
        json.dumps(
            {
                "status": "promoted",
                "latest_candidate": {
                    **latest_candidate,
                    "candidate_path": str(candidate_file),
                    "promoted_at": receipt["applied_at"],
                },
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return {"ok": True, "status": 200, "message": "Candidate promoted to active config.", "receipt": receipt}


def reject_candidate(
    *,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    reason: str = "Rejected by operator.",
) -> dict[str, Any]:
    state = _load_json(Path(tuner_state_path), default={})
    latest_candidate = dict(state.get("latest_candidate") or {})
    latest_candidate["rejected_at"] = datetime.now(timezone.utc).isoformat()
    latest_candidate["rejection_reason"] = reason
    Path(tuner_state_path).write_text(
        json.dumps({"status": "rejected", "latest_candidate": latest_candidate}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return {"ok": True, "status": 200, "message": "Candidate rejected."}


def _load_metrics(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"sample_size": 0}
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                pp.realized_pnl,
                COALESCE(s.score, 0.0) AS score,
                COALESCE(s.edge_abs, 0.0) AS edge_abs,
                COALESCE(s.liquidity, 0.0) AS liquidity,
                COALESCE(s.source_count, 1) AS source_count,
                COALESCE(s.source_dispersion_pct, 0.0) AS source_dispersion_pct,
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
              AND pp.market_type = 'temperature'
            """
        ).fetchall()
    finally:
        conn.close()
    sample_size = len(rows)
    if not rows:
        return {"sample_size": 0}

    enriched = []
    for row in rows:
        metadata = _load_json_blob(row["metadata_json"])
        enriched.append(
            {
                "realized_pnl": float(row["realized_pnl"] or 0.0),
                "score": float(row["score"] or 0.0),
                "edge_abs": float(row["edge_abs"] or 0.0),
                "liquidity": float(row["liquidity"] or 0.0),
                "source_count": int(row["source_count"] or 1),
                "source_dispersion_pct": float(row["source_dispersion_pct"] or 0.0),
                "source_age_hours": _float_or_zero(metadata.get("source_age_hours")),
            }
        )
    wins = [row for row in enriched if row["realized_pnl"] > 0]
    losses = [row for row in enriched if row["realized_pnl"] < 0]
    return {
        "sample_size": sample_size,
        "win_rate": round(len(wins) / sample_size * 100.0, 2),
        "avg_score_winner": _avg([row["score"] for row in wins]),
        "avg_score_loser": _avg([row["score"] for row in losses]),
        "avg_edge_winner": _avg([row["edge_abs"] for row in wins]),
        "avg_edge_loser": _avg([row["edge_abs"] for row in losses]),
        "avg_liquidity_winner": _avg([row["liquidity"] for row in wins]),
        "avg_liquidity_loser": _avg([row["liquidity"] for row in losses]),
        "avg_source_count_winner": _avg([float(row["source_count"]) for row in wins]),
        "avg_source_count_loser": _avg([float(row["source_count"]) for row in losses]),
        "avg_dispersion_winner": _avg([row["source_dispersion_pct"] for row in wins]),
        "avg_dispersion_loser": _avg([row["source_dispersion_pct"] for row in losses]),
        "avg_source_age_winner": _avg([row["source_age_hours"] for row in wins]),
        "avg_source_age_loser": _avg([row["source_age_hours"] for row in losses]),
    }


def _recommend_changes(config_payload: dict[str, Any], metrics: dict[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    if int(metrics.get("sample_size") or 0) < 8:
        return changes

    current_score = float(_get_path(config_payload, "strategy.temperature.min_score") or 0.62)
    current_edge = float(_get_path(config_payload, "strategy.temperature.min_edge_abs") or 0.12)
    current_liquidity = float(_get_path(config_payload, "strategy.temperature.min_liquidity") or 25.0)
    current_source_count = int(_get_path(config_payload, "strategy.temperature.min_source_count") or 2)
    current_dispersion = float(_get_path(config_payload, "strategy.temperature.max_source_dispersion_pct") or 0.18)
    current_source_age = float(_get_path(config_payload, "strategy.temperature.max_source_age_hours") or 6.0)

    win_rate = float(metrics.get("win_rate") or 0.0)
    avg_score_winner = float(metrics.get("avg_score_winner") or current_score)
    avg_score_loser = float(metrics.get("avg_score_loser") or current_score)
    avg_edge_loser = float(metrics.get("avg_edge_loser") or current_edge)
    avg_edge_winner = float(metrics.get("avg_edge_winner") or current_edge)
    avg_liq_loser = float(metrics.get("avg_liquidity_loser") or current_liquidity)
    avg_liq_winner = float(metrics.get("avg_liquidity_winner") or current_liquidity)
    avg_source_count_winner = float(metrics.get("avg_source_count_winner") or current_source_count)
    avg_source_count_loser = float(metrics.get("avg_source_count_loser") or current_source_count)
    avg_dispersion_winner = float(metrics.get("avg_dispersion_winner") or current_dispersion)
    avg_dispersion_loser = float(metrics.get("avg_dispersion_loser") or current_dispersion)
    avg_source_age_winner = float(metrics.get("avg_source_age_winner") or current_source_age)
    avg_source_age_loser = float(metrics.get("avg_source_age_loser") or current_source_age)

    recommended_score = current_score
    if win_rate < 50.0 and avg_score_loser >= current_score:
        recommended_score = round(min(0.9, current_score + 0.03), 2)
    elif win_rate > 65.0 and current_score > 0.55:
        recommended_score = round(max(0.5, current_score - 0.02), 2)
    if recommended_score != current_score:
        changes.append(
            _change(
                "strategy.temperature.min_score",
                current_score,
                recommended_score,
                f"Win rate {win_rate:.1f}% with loser score avg {avg_score_loser:.2f} and winner score avg {avg_score_winner:.2f}.",
            )
        )

    recommended_edge = current_edge
    if avg_edge_loser >= current_edge and avg_edge_loser <= avg_edge_winner and win_rate < 55.0:
        recommended_edge = round(min(0.3, current_edge + 0.02), 2)
    if recommended_edge != current_edge:
        changes.append(
            _change(
                "strategy.temperature.min_edge_abs",
                current_edge,
                recommended_edge,
                f"Losing trades still average {avg_edge_loser:.2%} edge while winners average {avg_edge_winner:.2%}.",
            )
        )

    recommended_liquidity = current_liquidity
    if avg_liq_loser and avg_liq_loser < avg_liq_winner and win_rate < 55.0:
        recommended_liquidity = round(min(250.0, current_liquidity + 10.0), 2)
    if recommended_liquidity != current_liquidity:
        changes.append(
            _change(
                "strategy.temperature.min_liquidity",
                current_liquidity,
                recommended_liquidity,
                f"Losing trades average ${avg_liq_loser:.0f} liquidity versus ${avg_liq_winner:.0f} for winners.",
            )
        )

    recommended_source_count = current_source_count
    if avg_source_count_winner > avg_source_count_loser and avg_source_count_winner >= current_source_count + 0.5 and win_rate < 55.0:
        recommended_source_count = min(4, current_source_count + 1)
    if recommended_source_count != current_source_count:
        changes.append(
            _change(
                "strategy.temperature.min_source_count",
                current_source_count,
                recommended_source_count,
                f"Winners average {avg_source_count_winner:.2f} agreeing sources versus {avg_source_count_loser:.2f} for losers.",
            )
        )

    recommended_dispersion = current_dispersion
    if avg_dispersion_loser > avg_dispersion_winner + 0.01 and win_rate < 55.0:
        target = round(max(0.05, min(current_dispersion, (avg_dispersion_winner + avg_dispersion_loser) / 2.0)), 2)
        if target < current_dispersion:
            recommended_dispersion = target
    if recommended_dispersion != current_dispersion:
        changes.append(
            _change(
                "strategy.temperature.max_source_dispersion_pct",
                current_dispersion,
                recommended_dispersion,
                f"Losing trades average {avg_dispersion_loser:.2%} dispersion versus {avg_dispersion_winner:.2%} for winners.",
            )
        )

    recommended_source_age = current_source_age
    if avg_source_age_loser > avg_source_age_winner + 0.5 and win_rate < 55.0:
        target_age = round(max(1.0, min(current_source_age, avg_source_age_winner + 1.0)), 1)
        if target_age < current_source_age:
            recommended_source_age = target_age
    if recommended_source_age != current_source_age:
        changes.append(
            _change(
                "strategy.temperature.max_source_age_hours",
                current_source_age,
                recommended_source_age,
                f"Losing trades average {avg_source_age_loser:.1f}h source age versus {avg_source_age_winner:.1f}h for winners.",
            )
        )

    return [change for change in changes if change["path"] in SAFE_TUNER_PATHS]


def _change(path: str, current: Any, recommended: Any, evidence: str) -> dict[str, Any]:
    return {"path": path, "current": current, "recommended": recommended, "evidence": evidence}


def _avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 4) if values else 0.0


def _get_path(payload: dict[str, Any], dotted_path: str) -> Any:
    current = payload
    for part in dotted_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _set_path(payload: dict[str, Any], dotted_path: str, value: Any) -> None:
    current = payload
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = value


def _load_json(path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default
    return payload if isinstance(payload, dict) else default


def _load_json_blob(value: object) -> dict[str, Any]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _float_or_zero(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Weather Tuning Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Candidate status: `{report['candidate_status']}`",
        f"- Sample size: `{report['metrics'].get('sample_size', 0)}`",
        f"- Win rate: `{report['metrics'].get('win_rate', 0.0):.1f}%`",
        "",
        "## Proposed Changes",
    ]
    changes = report.get("changes", [])
    if not changes:
        lines.append("- No changes proposed.")
    else:
        for change in changes:
            lines.append(
                f"- `{change['path']}`: `{change['current']}` -> `{change['recommended']}`"
                f" | {change['evidence']}"
            )
    return "\n".join(lines)
