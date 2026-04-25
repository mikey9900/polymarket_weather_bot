"""Optional DuckDB sync for the research warehouse."""

from __future__ import annotations

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
                signal_key VARCHAR,
                market_type VARCHAR,
                city_slug VARCHAR,
                direction VARCHAR,
                edge_abs DOUBLE,
                liquidity DOUBLE,
                source_count INTEGER,
                score DOUBLE,
                created_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS paper_positions (
                signal_key VARCHAR,
                market_slug VARCHAR,
                direction VARCHAR,
                status VARCHAR,
                realized_pnl DOUBLE,
                resolved_at TIMESTAMP
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
                SELECT signal_key, market_type, city_slug, direction, edge_abs,
                       liquidity, source_count, score, created_at
                FROM signals
                """
            ).fetchall()
            positions = sqlite_conn.execute(
                """
                SELECT signal_key, market_slug, direction, status, realized_pnl, resolved_at
                FROM paper_positions
                """
            ).fetchall()
        finally:
            sqlite_conn.close()

        self.conn.execute("DELETE FROM signals")
        self.conn.execute("DELETE FROM paper_positions")
        self.conn.executemany(
            """
            INSERT INTO signals(signal_key, market_type, city_slug, direction, edge_abs, liquidity, source_count, score, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [tuple(row) for row in signals],
        )
        self.conn.executemany(
            """
            INSERT INTO paper_positions(signal_key, market_slug, direction, status, realized_pnl, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [tuple(row) for row in positions],
        )
        return {"ok": True, "signals": len(signals), "paper_positions": len(positions), "warehouse_path": str(self.db_path)}
