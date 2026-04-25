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
}


def propose_tuning(
    *,
    config_path: str | Path = ACTIVE_CONFIG_PATH,
    tracker_db: str | Path = TRACKER_DB_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
    report_json_path: str | Path = TUNER_REPORT_JSON_PATH,
    report_md_path: str | Path = TUNER_REPORT_MD_PATH,
    patch_path: str | Path = TUNER_ACTIVE_PATCH_PATH,
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

    artifact_result = build_artifacts(tracker_db=tracker_path)
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
        },
    }
    Path(tuner_state_path).write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    return report


def promote_candidate(
    *,
    candidate_path: str | Path | None = None,
    config_path: str | Path = ACTIVE_CONFIG_PATH,
    tuner_state_path: str | Path = TUNER_STATE_PATH,
) -> dict[str, Any]:
    state = _load_json(Path(tuner_state_path), default={})
    candidate_file = Path(candidate_path or (state.get("latest_candidate") or {}).get("candidate_path") or "")
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
        "candidate_path": str(candidate_file),
        "backup_path": str(backup_path),
    }
    APPROVED_CONFIG_RECEIPT_PATH.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
    Path(tuner_state_path).write_text(
        json.dumps(
            {
                "status": "promoted",
                "latest_candidate": {
                    **(state.get("latest_candidate") or {}),
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
                COALESCE(s.liquidity, 0.0) AS liquidity
            FROM paper_positions pp
            LEFT JOIN signals s ON s.id = pp.signal_id
            WHERE pp.status = 'resolved'
              AND pp.market_type = 'temperature'
            """
        ).fetchall()
    finally:
        conn.close()
    sample_size = len(rows)
    if not rows:
        return {"sample_size": 0}
    wins = [row for row in rows if float(row["realized_pnl"] or 0.0) > 0]
    losses = [row for row in rows if float(row["realized_pnl"] or 0.0) < 0]
    return {
        "sample_size": sample_size,
        "win_rate": round(len(wins) / sample_size * 100.0, 2),
        "avg_score_winner": _avg([float(row["score"] or 0.0) for row in wins]),
        "avg_score_loser": _avg([float(row["score"] or 0.0) for row in losses]),
        "avg_edge_winner": _avg([float(row["edge_abs"] or 0.0) for row in wins]),
        "avg_edge_loser": _avg([float(row["edge_abs"] or 0.0) for row in losses]),
        "avg_liquidity_winner": _avg([float(row["liquidity"] or 0.0) for row in wins]),
        "avg_liquidity_loser": _avg([float(row["liquidity"] or 0.0) for row in losses]),
    }


def _recommend_changes(config_payload: dict[str, Any], metrics: dict[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    if int(metrics.get("sample_size") or 0) < 8:
        return changes

    current_score = float(_get_path(config_payload, "strategy.temperature.min_score") or 0.62)
    current_edge = float(_get_path(config_payload, "strategy.temperature.min_edge_abs") or 0.12)
    current_liquidity = float(_get_path(config_payload, "strategy.temperature.min_liquidity") or 25.0)

    win_rate = float(metrics.get("win_rate") or 0.0)
    avg_score_winner = float(metrics.get("avg_score_winner") or current_score)
    avg_score_loser = float(metrics.get("avg_score_loser") or current_score)
    avg_edge_loser = float(metrics.get("avg_edge_loser") or current_edge)
    avg_edge_winner = float(metrics.get("avg_edge_winner") or current_edge)
    avg_liq_loser = float(metrics.get("avg_liquidity_loser") or current_liquidity)
    avg_liq_winner = float(metrics.get("avg_liquidity_winner") or current_liquidity)

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
