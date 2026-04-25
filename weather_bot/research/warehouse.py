"""DuckDB sync for the research warehouse."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ..paths import TRACKER_DB_PATH, WAREHOUSE_PATH


def require_duckdb():
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("duckdb is not installed") from exc
    return duckdb


class ResearchWarehouse:
    def __init__(self, db_path: str | Path = WAREHOUSE_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._duckdb = require_duckdb()
        self.conn = self._duckdb.connect(str(self.db_path))
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                signal_id BIGINT,
                signal_key VARCHAR,
                market_type VARCHAR,
                event_title VARCHAR,
                market_slug VARCHAR,
                event_slug VARCHAR,
                city_slug VARCHAR,
                event_date VARCHAR,
                label VARCHAR,
                direction VARCHAR,
                confidence VARCHAR,
                market_prob DOUBLE,
                forecast_prob DOUBLE,
                edge DOUBLE,
                edge_abs DOUBLE,
                liquidity DOUBLE,
                source_count INTEGER,
                time_to_resolution_s DOUBLE,
                source_dispersion_pct DOUBLE,
                score DOUBLE,
                created_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS decisions (
                decision_id BIGINT,
                signal_id BIGINT,
                signal_key VARCHAR,
                accepted BOOLEAN,
                reason VARCHAR,
                final_score DOUBLE,
                policy_action VARCHAR,
                source_age_hours DOUBLE,
                created_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS paper_positions (
                position_id BIGINT,
                signal_id BIGINT,
                decision_id BIGINT,
                signal_key VARCHAR,
                market_type VARCHAR,
                market_slug VARCHAR,
                event_slug VARCHAR,
                city_slug VARCHAR,
                event_date VARCHAR,
                label VARCHAR,
                direction VARCHAR,
                score DOUBLE,
                entry_price DOUBLE,
                shares DOUBLE,
                cost DOUBLE,
                status VARCHAR,
                resolution VARCHAR,
                realized_pnl DOUBLE,
                notes VARCHAR,
                created_at TIMESTAMP,
                resolved_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS resolved_outcomes (
                signal_key VARCHAR,
                market_type VARCHAR,
                city_slug VARCHAR,
                event_date VARCHAR,
                label VARCHAR,
                direction VARCHAR,
                confidence VARCHAR,
                source_count INTEGER,
                edge_abs DOUBLE,
                liquidity DOUBLE,
                time_to_resolution_s DOUBLE,
                source_dispersion_pct DOUBLE,
                source_age_hours DOUBLE,
                adapter_score DOUBLE,
                final_score DOUBLE,
                policy_action VARCHAR,
                market_slug VARCHAR,
                cost DOUBLE,
                shares DOUBLE,
                resolution VARCHAR,
                realized_pnl DOUBLE,
                resolved_at TIMESTAMP,
                realized_outcome VARCHAR
            );
            """
        )

    def close(self) -> None:
        self.conn.close()

    def sync_from_tracker(self, tracker_db_path: str | Path = TRACKER_DB_PATH) -> dict[str, object]:
        tracker_path = Path(tracker_db_path)
        if not tracker_path.exists():
            return {"ok": False, "message": f"Tracker DB not found: {tracker_path}"}
        sqlite_conn = sqlite3.connect(str(tracker_path))
        sqlite_conn.row_factory = sqlite3.Row
        try:
            signals = sqlite_conn.execute(
                """
                SELECT
                    id,
                    signal_key,
                    market_type,
                    event_title,
                    market_slug,
                    event_slug,
                    city_slug,
                    event_date,
                    label,
                    direction,
                    confidence,
                    market_prob,
                    forecast_prob,
                    edge,
                    edge_abs,
                    liquidity,
                    source_count,
                    time_to_resolution_s,
                    source_dispersion_pct,
                    score,
                    created_at
                FROM signals
                """
            ).fetchall()
            decisions = sqlite_conn.execute(
                """
                SELECT
                    id,
                    signal_id,
                    signal_key,
                    accepted,
                    reason,
                    final_score,
                    policy_action,
                    metadata_json,
                    created_at
                FROM decisions
                """
            ).fetchall()
            positions = sqlite_conn.execute(
                """
                SELECT
                    id,
                    signal_id,
                    decision_id,
                    signal_key,
                    market_type,
                    market_slug,
                    event_slug,
                    city_slug,
                    event_date,
                    label,
                    direction,
                    score,
                    entry_price,
                    shares,
                    cost,
                    status,
                    resolution,
                    realized_pnl,
                    notes,
                    created_at,
                    resolved_at
                FROM paper_positions
                """
            ).fetchall()
        finally:
            sqlite_conn.close()

        self.conn.execute("DELETE FROM signals")
        self.conn.execute("DELETE FROM decisions")
        self.conn.execute("DELETE FROM paper_positions")
        self.conn.execute("DELETE FROM resolved_outcomes")
        self.conn.executemany(
            """
            INSERT INTO signals(
                signal_id, signal_key, market_type, event_title, market_slug, event_slug, city_slug,
                event_date, label, direction, confidence, market_prob, forecast_prob, edge, edge_abs,
                liquidity, source_count, time_to_resolution_s, source_dispersion_pct, score, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row) for row in signals],
        )
        self.conn.executemany(
            """
            INSERT INTO decisions(
                decision_id, signal_id, signal_key, accepted, reason, final_score, policy_action, source_age_hours, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    int(row["id"]),
                    row["signal_id"],
                    row["signal_key"],
                    bool(row["accepted"]),
                    row["reason"],
                    row["final_score"],
                    row["policy_action"],
                    _metadata_source_age_hours(row["metadata_json"]),
                    row["created_at"],
                )
                for row in decisions
            ],
        )
        self.conn.executemany(
            """
            INSERT INTO paper_positions(
                position_id, signal_id, decision_id, signal_key, market_type, market_slug, event_slug,
                city_slug, event_date, label, direction, score, entry_price, shares, cost, status,
                resolution, realized_pnl, notes, created_at, resolved_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row) for row in positions],
        )
        self.conn.execute(
            """
            INSERT INTO resolved_outcomes
            SELECT
                p.signal_key,
                COALESCE(s.market_type, p.market_type) AS market_type,
                COALESCE(s.city_slug, p.city_slug) AS city_slug,
                COALESCE(s.event_date, p.event_date) AS event_date,
                COALESCE(s.label, p.label) AS label,
                p.direction,
                COALESCE(s.confidence, 'unknown') AS confidence,
                COALESCE(s.source_count, 1) AS source_count,
                COALESCE(s.edge_abs, 0.0) AS edge_abs,
                COALESCE(s.liquidity, 0.0) AS liquidity,
                COALESCE(s.time_to_resolution_s, 0.0) AS time_to_resolution_s,
                COALESCE(s.source_dispersion_pct, 0.0) AS source_dispersion_pct,
                d.source_age_hours,
                COALESCE(s.score, p.score) AS adapter_score,
                COALESCE(d.final_score, 0.0) AS final_score,
                COALESCE(d.policy_action, 'advisory') AS policy_action,
                p.market_slug,
                p.cost,
                p.shares,
                p.resolution,
                COALESCE(p.realized_pnl, 0.0) AS realized_pnl,
                p.resolved_at,
                CASE
                    WHEN COALESCE(p.realized_pnl, 0.0) > 0 THEN 'win'
                    WHEN COALESCE(p.realized_pnl, 0.0) < 0 THEN 'loss'
                    ELSE 'flat'
                END AS realized_outcome
            FROM paper_positions p
            LEFT JOIN signals s ON s.signal_id = p.signal_id
            LEFT JOIN (
                SELECT *
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY signal_id ORDER BY decision_id DESC) AS row_num
                    FROM decisions
                )
                WHERE row_num = 1
            ) d ON d.signal_id = p.signal_id
            WHERE p.status = 'resolved'
            """
        )
        resolved_count = self.conn.execute("SELECT COUNT(*) FROM resolved_outcomes").fetchone()[0]
        return {
            "ok": True,
            "signals": len(signals),
            "decisions": len(decisions),
            "paper_positions": len(positions),
            "resolved_outcomes": int(resolved_count),
            "warehouse_path": str(self.db_path),
        }


def _metadata_source_age_hours(metadata_json: object) -> float | None:
    raw = str(metadata_json or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    value = payload.get("source_age_hours")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
