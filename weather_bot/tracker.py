"""SQLite-backed persistence for signals, decisions, and paper positions."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import ForecastSnapshot, PaperPosition, ResolutionOutcome, WeatherDecision, WeatherSignal, iso_now
from .paths import TRACKER_DB_PATH, candidate_legacy_tracking_paths


class WeatherTracker:
    def __init__(self, db_path: str | Path = TRACKER_DB_PATH):
        self.db_path = str(Path(db_path))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT NOT NULL,
                market_type TEXT NOT NULL,
                event_title TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                city_slug TEXT NOT NULL,
                event_date TEXT NOT NULL,
                label TEXT NOT NULL,
                direction TEXT NOT NULL,
                market_prob REAL NOT NULL,
                forecast_prob REAL NOT NULL,
                edge REAL NOT NULL,
                edge_abs REAL NOT NULL,
                edge_size TEXT NOT NULL,
                confidence TEXT NOT NULL,
                source_count INTEGER NOT NULL,
                liquidity REAL NOT NULL,
                time_to_resolution_s REAL,
                source_dispersion_pct REAL NOT NULL,
                score REAL NOT NULL,
                raw_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_signals_market_slug ON signals(market_slug);
            CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at);
            CREATE INDEX IF NOT EXISTS idx_signals_market_type ON signals(market_type);

            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                signal_key TEXT NOT NULL,
                accepted INTEGER NOT NULL,
                reason TEXT NOT NULL,
                final_score REAL NOT NULL,
                policy_action TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(signal_id) REFERENCES signals(id)
            );

            CREATE TABLE IF NOT EXISTS paper_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                decision_id INTEGER,
                signal_key TEXT NOT NULL,
                market_type TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                city_slug TEXT NOT NULL,
                event_date TEXT NOT NULL,
                label TEXT NOT NULL,
                direction TEXT NOT NULL,
                score REAL NOT NULL,
                entry_price REAL NOT NULL,
                entry_reference_price REAL,
                entry_fee_paid REAL,
                entry_fee_bps REAL,
                entry_slippage_bps REAL,
                shares REAL NOT NULL,
                cost REAL NOT NULL,
                status TEXT NOT NULL,
                resolution TEXT,
                realized_pnl REAL,
                mark_price REAL,
                mark_probability REAL,
                mark_edge_abs REAL,
                mark_final_score REAL,
                mark_updated_at TEXT,
                mark_reason TEXT,
                exit_price REAL,
                exit_reference_price REAL,
                exit_fee_paid REAL,
                exit_fee_bps REAL,
                exit_slippage_bps REAL,
                gross_exit_payout REAL,
                net_exit_payout REAL,
                exit_reason TEXT,
                notes TEXT NOT NULL,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                FOREIGN KEY(signal_id) REFERENCES signals(id),
                FOREIGN KEY(decision_id) REFERENCES decisions(id)
            );

            CREATE INDEX IF NOT EXISTS idx_positions_status ON paper_positions(status);
            CREATE INDEX IF NOT EXISTS idx_positions_market_slug ON paper_positions(market_slug);

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runtime_state (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS operator_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS resolution_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_slug TEXT NOT NULL,
                resolution TEXT NOT NULL,
                resolved_positions INTEGER NOT NULL,
                total_realized_pnl REAL NOT NULL,
                total_payout REAL NOT NULL,
                resolved_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_resolution_events_resolved_at ON resolution_events(resolved_at);
            """
        )
        self._ensure_paper_position_columns()
        self.conn.commit()

    def backup_database(self, destination: str | Path) -> str:
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            backup_conn = sqlite3.connect(str(target))
            try:
                self.conn.backup(backup_conn)
                backup_conn.commit()
            finally:
                backup_conn.close()
        return str(target)

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def log_signal(self, signal: WeatherSignal) -> int:
        payload = signal.to_dict()
        with self._lock:
            cursor = self.conn.execute(
                """
                INSERT INTO signals (
                    signal_key, market_type, event_title, market_slug, event_slug, city_slug,
                    event_date, label, direction, market_prob, forecast_prob, edge, edge_abs,
                    edge_size, confidence, source_count, liquidity, time_to_resolution_s,
                    source_dispersion_pct, score, raw_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.signal_key,
                    signal.market_type,
                    signal.event_title,
                    signal.market_slug,
                    signal.event_slug,
                    signal.city_slug,
                    signal.event_date,
                    signal.label,
                    signal.direction,
                    signal.market_prob,
                    signal.forecast_prob,
                    signal.edge,
                    signal.edge_abs,
                    signal.edge_size,
                    signal.confidence,
                    signal.source_count,
                    signal.liquidity,
                    signal.time_to_resolution_s,
                    signal.source_dispersion_pct,
                    signal.score,
                    json.dumps(payload, sort_keys=True),
                    signal.created_at,
                ),
            )
            self.conn.commit()
            return int(cursor.lastrowid)

    def log_decision(self, signal_id: int, decision: WeatherDecision) -> int:
        with self._lock:
            cursor = self.conn.execute(
                """
                INSERT INTO decisions (
                    signal_id, signal_key, accepted, reason, final_score,
                    policy_action, metadata_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    decision.signal_key,
                    1 if decision.accepted else 0,
                    decision.reason,
                    decision.final_score,
                    decision.policy_action,
                    json.dumps(decision.metadata, sort_keys=True),
                    decision.created_at,
                ),
            )
            self.conn.commit()
            return int(cursor.lastrowid)

    def set_paper_capital(self, amount: float) -> None:
        self.set_setting("paper_capital", {"initial": float(amount), "available": float(amount)})

    def ensure_paper_capital(self, amount: float) -> None:
        if self.get_setting("paper_capital") is None:
            self.set_paper_capital(amount)

    def get_paper_capital(self) -> tuple[float, float]:
        payload = self.get_setting("paper_capital") or {"initial": 0.0, "available": 0.0}
        return float(payload.get("initial", 0.0) or 0.0), float(payload.get("available", 0.0) or 0.0)

    def count_open_positions(self) -> int:
        with self._lock:
            row = self.conn.execute("SELECT COUNT(*) AS count FROM paper_positions WHERE status = 'open'").fetchone()
            return int(row["count"] or 0)

    def has_open_position(self, market_slug: str, direction: str) -> bool:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT 1
                FROM paper_positions
                WHERE market_slug = ? AND direction = ? AND status = 'open'
                LIMIT 1
                """,
                (market_slug, direction),
            ).fetchone()
            return row is not None

    def count_open_positions_for_market(self, market_slug: str) -> int:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM paper_positions
                WHERE market_slug = ? AND status = 'open'
                """,
                (market_slug,),
            ).fetchone()
            return int(row["count"] or 0)

    def create_paper_position(
        self,
        *,
        signal_id: int,
        decision_id: int,
        signal: WeatherSignal,
        stake_usd: float,
        decision_final_score: float | None = None,
        notes: str = "auto",
        fee_bps: float = 0.0,
        entry_slippage_bps: float = 0.0,
        exit_fee_bps: float = 0.0,
        exit_slippage_bps: float = 0.0,
    ) -> PaperPosition | None:
        entry_context = _paper_entry_context(signal, entry_slippage_bps)
        if entry_context is None:
            return None
        with self._lock:
            initial, available = self.get_paper_capital()
            position_size = _paper_entry_position_size(stake_usd, available, entry_context["entry_price"], fee_bps)
            if position_size is None:
                return None
            self.set_setting(
                "paper_capital",
                {"initial": initial, "available": round(available - position_size["cost"], 6)},
            )
            values = _paper_position_insert_values(
                signal_id=signal_id,
                decision_id=decision_id,
                signal=signal,
                notes=notes,
                decision_final_score=decision_final_score,
                entry_reference_price=entry_context["entry_reference_price"],
                entry_price=entry_context["entry_price"],
                mark_probability=entry_context["mark_probability"],
                entry_fee_paid=position_size["entry_fee_paid"],
                shares=position_size["shares"],
                cost=position_size["cost"],
                fee_bps=fee_bps,
                entry_slippage_bps=entry_slippage_bps,
                exit_fee_bps=exit_fee_bps,
                exit_slippage_bps=exit_slippage_bps,
            )
            placeholders = ", ".join(["?"] * len(values))
            cursor = self.conn.execute(
                f"""
                INSERT INTO paper_positions (
                    signal_id, decision_id, signal_key, market_type, market_slug, event_slug, city_slug,
                    event_date, label, direction, score, entry_price, entry_reference_price, entry_fee_paid,
                    entry_fee_bps, entry_slippage_bps, shares, cost, status, resolution, realized_pnl,
                    mark_price, mark_probability, mark_edge_abs, mark_final_score, mark_updated_at, mark_reason,
                    exit_price, exit_reference_price, exit_fee_paid, exit_fee_bps, exit_slippage_bps,
                    gross_exit_payout, net_exit_payout, exit_reason, notes, created_at, resolved_at
                )
                VALUES ({placeholders})
                """,
                values,
            )
            self.conn.commit()
            return _paper_position_record(
                position_id=int(cursor.lastrowid),
                signal=signal,
                entry_price=entry_context["entry_price"],
                shares=position_size["shares"],
                cost=position_size["cost"],
                notes=notes,
            )

    def update_paper_position_review(
        self,
        position_id: int,
        *,
        mark_price: float | None,
        mark_probability: float | None,
        edge_abs: float | None,
        final_score: float | None,
        reviewed_at: str | None = None,
        reason: str = "",
        exit_fee_bps: float | None = None,
        exit_slippage_bps: float | None = None,
    ) -> bool:
        reviewed_at = str(reviewed_at or iso_now())
        with self._lock:
            cursor = self.conn.execute(
                """
                UPDATE paper_positions
                SET mark_price = ?,
                    mark_probability = ?,
                    mark_edge_abs = ?,
                    mark_final_score = ?,
                    mark_updated_at = ?,
                    mark_reason = ?,
                    exit_fee_bps = COALESCE(?, exit_fee_bps),
                    exit_slippage_bps = COALESCE(?, exit_slippage_bps)
                WHERE id = ? AND status = 'open'
                """,
                (
                    _bounded_probability(mark_price),
                    _bounded_probability(mark_probability),
                    _as_float(edge_abs),
                    _as_float(final_score),
                    reviewed_at,
                    str(reason or ""),
                    _as_float(_bps(exit_fee_bps)) if exit_fee_bps is not None else None,
                    _as_float(_bps(exit_slippage_bps)) if exit_slippage_bps is not None else None,
                    int(position_id),
                ),
            )
            self.conn.commit()
            return bool(cursor.rowcount)

    def close_paper_position(
        self,
        position_id: int,
        *,
        exit_price: float | None = None,
        reason: str = "manual",
        closed_at: str | None = None,
        mark_probability: float | None = None,
        edge_abs: float | None = None,
        final_score: float | None = None,
        mark_reason: str | None = None,
        exit_fee_bps: float | None = None,
        exit_slippage_bps: float | None = None,
    ) -> dict[str, Any] | None:
        closed_at = str(closed_at or iso_now())
        with self._lock:
            row = self.conn.execute("SELECT * FROM paper_positions WHERE id = ? AND status = 'open'", (int(position_id),)).fetchone()
            if row is None:
                return None

            initial, available = self.get_paper_capital()
            exit_context = _paper_position_exit_context(
                row,
                exit_price=exit_price,
                exit_fee_bps=exit_fee_bps,
                exit_slippage_bps=exit_slippage_bps,
            )
            pnl = round(exit_context["net_payout"] - float(row["cost"] or 0.0), 6)

            self.conn.execute(
                """
                UPDATE paper_positions
                SET status = 'closed',
                    realized_pnl = ?,
                    mark_price = ?,
                    mark_probability = COALESCE(?, mark_probability),
                    mark_edge_abs = COALESCE(?, mark_edge_abs),
                    mark_final_score = COALESCE(?, mark_final_score),
                    mark_updated_at = ?,
                    mark_reason = ?,
                    exit_price = ?,
                    exit_reference_price = ?,
                    exit_fee_paid = ?,
                    exit_fee_bps = ?,
                    exit_slippage_bps = ?,
                    gross_exit_payout = ?,
                    net_exit_payout = ?,
                    exit_reason = ?,
                    resolved_at = ?
                WHERE id = ?
                """,
                (
                    pnl,
                    exit_context["reference_price"],
                    _bounded_probability(mark_probability),
                    _as_float(edge_abs),
                    _as_float(final_score),
                    closed_at,
                    str(mark_reason or reason or ""),
                    exit_context["fill_exit_price"],
                    exit_context["reference_price"],
                    exit_context["exit_fee_paid"],
                    exit_context["applied_exit_fee_bps"],
                    exit_context["applied_exit_slippage_bps"],
                    exit_context["gross_payout"],
                    exit_context["net_payout"],
                    str(reason or "manual"),
                    closed_at,
                    int(position_id),
                ),
            )
            self.set_setting(
                "paper_capital",
                {"initial": initial, "available": round(available + exit_context["net_payout"], 6)},
            )
            self.conn.commit()
            return _closed_position_payload(
                row,
                exit_context=exit_context,
                pnl=pnl,
                closed_at=closed_at,
                reason=reason,
            )

    def settle_market(self, market_slug: str, resolution: str) -> ResolutionOutcome:
        resolution = str(resolution or "").upper()
        if resolution not in {"YES", "NO"}:
            raise ValueError(f"Unsupported resolution: {resolution}")
        resolved_at = iso_now()
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM paper_positions WHERE market_slug = ? AND status = 'open'",
                (market_slug,),
            ).fetchall()
            if not rows:
                return ResolutionOutcome(market_slug=market_slug, resolution=resolution, resolved_positions=0, total_realized_pnl=0.0)

            initial, available = self.get_paper_capital()
            total_payout = 0.0
            total_pnl = 0.0
            for row in rows:
                gross_payout = float(row["shares"]) if str(row["direction"]).upper() == resolution else 0.0
                applied_exit_fee_bps = _bps(row["exit_fee_bps"])
                exit_fee_paid = _fee_amount(gross_payout, applied_exit_fee_bps) if gross_payout > 0 else 0.0
                net_payout = round(gross_payout - exit_fee_paid, 6)
                pnl = round(net_payout - float(row["cost"]), 6)
                total_payout += net_payout
                total_pnl += pnl
                self.conn.execute(
                    """
                    UPDATE paper_positions
                    SET status = 'resolved',
                        resolution = ?,
                        realized_pnl = ?,
                        mark_price = ?,
                        exit_price = ?,
                        exit_reference_price = ?,
                        exit_fee_paid = ?,
                        gross_exit_payout = ?,
                        net_exit_payout = ?,
                        exit_reason = ?,
                        resolved_at = ?
                    WHERE id = ?
                    """,
                    (
                        resolution,
                        pnl,
                        1.0 if str(row["direction"]).upper() == resolution else 0.0,
                        1.0 if str(row["direction"]).upper() == resolution else 0.0,
                        1.0 if str(row["direction"]).upper() == resolution else 0.0,
                        exit_fee_paid,
                        gross_payout,
                        net_payout,
                        f"resolved:{resolution}",
                        resolved_at,
                        int(row["id"]),
                    ),
                )
            self.conn.execute(
                """
                INSERT INTO resolution_events(
                    market_slug, resolution, resolved_positions, total_realized_pnl, total_payout, resolved_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    market_slug,
                    resolution,
                    len(rows),
                    round(total_pnl, 6),
                    round(total_payout, 6),
                    resolved_at,
                ),
            )
            self.set_setting("paper_capital", {"initial": initial, "available": round(available + total_payout, 6)})
            self.conn.commit()
            return ResolutionOutcome(
                market_slug=market_slug,
                resolution=resolution,
                resolved_positions=len(rows),
                total_realized_pnl=round(total_pnl, 6),
                resolved_at=resolved_at,
            )

    def get_open_positions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM paper_positions WHERE status = 'open' ORDER BY created_at DESC").fetchall()
            return [dict(row) for row in rows]

    def get_recent_paper_positions(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM paper_positions ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
            return [dict(row) for row in rows]

    def get_dashboard_paper_positions(
        self,
        limit: int = 20,
        *,
        status: str | None = None,
        statuses: list[str] | tuple[str, ...] | set[str] | None = None,
        mark_stale_after_seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        status_values: list[str] = []
        if statuses:
            status_values = [str(item) for item in statuses if str(item).strip()]
        elif status:
            status_values = [str(status)]
        if status_values:
            placeholders = ", ".join("?" for _ in status_values)
            where = f"WHERE p.status IN ({placeholders})"
            params.extend(status_values)
        params.append(int(limit))
        with self._lock:
            rows = self.conn.execute(
                f"""
                SELECT
                    p.*,
                    s.event_title AS signal_event_title,
                    s.market_prob AS signal_market_prob,
                    s.forecast_prob AS signal_forecast_prob,
                    s.edge AS signal_edge,
                    s.edge_abs AS signal_edge_abs,
                    s.edge_size AS signal_edge_size,
                    s.confidence AS signal_confidence,
                    s.source_count AS signal_source_count,
                    s.liquidity AS signal_liquidity,
                    s.time_to_resolution_s AS signal_time_to_resolution_s,
                    s.source_dispersion_pct AS signal_source_dispersion_pct,
                    s.score AS signal_adapter_score,
                    d.final_score AS decision_final_score,
                    d.reason AS decision_reason,
                    d.policy_action AS decision_policy_action,
                    d.metadata_json AS decision_metadata_json
                FROM paper_positions p
                LEFT JOIN signals s ON s.id = p.signal_id
                LEFT JOIN decisions d ON d.id = p.decision_id
                {where}
                ORDER BY COALESCE(p.resolved_at, p.created_at) DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            items = [_serialize_dashboard_position(row) for row in rows]
        if mark_stale_after_seconds is None:
            return items
        threshold = max(0, int(mark_stale_after_seconds))
        for item in items:
            mark_age = _as_float(item.get("mark_age_seconds"))
            item["mark_is_stale"] = mark_age is not None and mark_age > threshold
        return items

    def get_recent_resolutions(self, limit: int = 20) -> list[dict[str, Any]]:
        limit = max(0, int(limit))
        with self._lock:
            resolved_rows = self.conn.execute(
                "SELECT * FROM resolution_events ORDER BY resolved_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            closed_rows = self.conn.execute(
                """
                SELECT
                    market_slug,
                    realized_pnl,
                    net_exit_payout,
                    exit_reason,
                    resolved_at
                FROM paper_positions
                WHERE status = 'closed' AND resolved_at IS NOT NULL
                ORDER BY resolved_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in resolved_rows:
            items.append(
                {
                    "market_slug": str(row["market_slug"] or ""),
                    "resolution": str(row["resolution"] or ""),
                    "resolved_positions": int(row["resolved_positions"] or 0),
                    "total_realized_pnl": float(row["total_realized_pnl"] or 0.0),
                    "total_payout": float(row["total_payout"] or 0.0),
                    "resolved_at": str(row["resolved_at"] or ""),
                    "status": "resolved",
                    "event_kind": "settlement",
                    "outcome_label": f"Resolved {str(row['resolution'] or '').upper()}".strip(),
                }
            )
        for row in closed_rows:
            items.append(
                {
                    "market_slug": str(row["market_slug"] or ""),
                    "resolution": "",
                    "resolved_positions": 1,
                    "total_realized_pnl": float(row["realized_pnl"] or 0.0),
                    "total_payout": float(row["net_exit_payout"] or 0.0),
                    "resolved_at": str(row["resolved_at"] or ""),
                    "status": "closed",
                    "event_kind": "exit",
                    "outcome_label": _close_outcome_label(row["exit_reason"]),
                }
            )
        items.sort(key=lambda item: str(item.get("resolved_at") or ""), reverse=True)
        return items[:limit]

    def get_recent_operator_actions(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM operator_events ORDER BY created_at DESC, id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                payload = {}
            items.append(
                {
                    "id": int(row["id"]),
                    "action": str(row["action"]),
                    "created_at": str(row["created_at"]),
                    "payload": payload,
                }
            )
        return items

    def get_recent_signals(self, limit: int = 20, *, market_type: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if market_type:
                rows = self.conn.execute(
                    "SELECT * FROM signals WHERE market_type = ? ORDER BY created_at DESC LIMIT ?",
                    (market_type, int(limit)),
                ).fetchall()
            else:
                rows = self.conn.execute("SELECT * FROM signals ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
            return [dict(row) for row in rows]

    def get_signal_summary(self) -> dict[str, Any]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT market_type, COUNT(*) AS count
                FROM signals
                WHERE created_at >= datetime('now', '-1 day')
                GROUP BY market_type
                """
            ).fetchall()
            return {str(row["market_type"]): int(row["count"]) for row in rows}

    def get_paper_stats(self) -> dict[str, Any]:
        with self._lock:
            initial, available = self.get_paper_capital()
            summary = self.conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status IN ('resolved', 'closed') AND realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN status IN ('resolved', 'closed') AND realized_pnl < 0 THEN 1 ELSE 0 END) AS losses,
                    SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_positions,
                    COALESCE(SUM(CASE WHEN status IN ('resolved', 'closed') THEN realized_pnl ELSE 0 END), 0.0) AS realized_pnl
                FROM paper_positions
                """
            ).fetchone()
            wins = int(summary["wins"] or 0)
            losses = int(summary["losses"] or 0)
            open_positions = int(summary["open_positions"] or 0)
            realized_pnl = float(summary["realized_pnl"] or 0.0)
            open_mark_value = sum(
                float(item.get("net_liquidation_value") or 0.0)
                for item in self.get_dashboard_paper_positions(limit=500, status="open")
            )
            current_equity = float(available) + open_mark_value
            total_pnl = current_equity - float(initial)
            total_closed = wins + losses
            return {
                "initial_capital": float(initial),
                "current_balance": float(available),
                "current_equity": float(current_equity),
                "wins": wins,
                "losses": losses,
                "open_positions": open_positions,
                "realized_pnl": realized_pnl,
                "total_pnl": float(total_pnl),
                "win_rate": (wins / total_closed * 100.0) if total_closed else 0.0,
            }

    def _ensure_paper_position_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(paper_positions)").fetchall()
        }
        required_columns = {
            "entry_reference_price": "REAL",
            "entry_fee_paid": "REAL",
            "entry_fee_bps": "REAL",
            "entry_slippage_bps": "REAL",
            "mark_price": "REAL",
            "mark_probability": "REAL",
            "mark_edge_abs": "REAL",
            "mark_final_score": "REAL",
            "mark_updated_at": "TEXT",
            "mark_reason": "TEXT",
            "exit_price": "REAL",
            "exit_reference_price": "REAL",
            "exit_fee_paid": "REAL",
            "exit_fee_bps": "REAL",
            "exit_slippage_bps": "REAL",
            "gross_exit_payout": "REAL",
            "net_exit_payout": "REAL",
            "exit_reason": "TEXT",
        }
        for column, column_type in required_columns.items():
            if column in existing:
                continue
            self.conn.execute(f"ALTER TABLE paper_positions ADD COLUMN {column} {column_type}")

    def set_setting(self, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, sort_keys=True)
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO settings(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
                """,
                (key, payload, iso_now()),
            )
            self.conn.commit()

    def get_setting(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self.conn.execute("SELECT value_json FROM settings WHERE key = ?", (key,)).fetchone()
            if row is None:
                return None
            return json.loads(row["value_json"])

    def set_runtime_state(self, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, sort_keys=True)
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO runtime_state(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
                """,
                (key, payload, iso_now()),
            )
            self.conn.commit()

    def get_runtime_state(self, key: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT value_json FROM runtime_state WHERE key = ?", (key,)).fetchone()
            if row is None:
                return dict(default or {})
            return json.loads(row["value_json"])

    def record_operator_action(self, action: str, payload: dict[str, Any]) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT INTO operator_events(action, payload_json, created_at) VALUES (?, ?, ?)",
                (action, json.dumps(payload, sort_keys=True), iso_now()),
            )
            self.conn.commit()

    def migrate_legacy_edges(self, paths: list[Path] | None = None) -> int:
        if self.get_setting("legacy_json_migrated"):
            return 0
        imported = 0
        for path in paths or candidate_legacy_tracking_paths():
            try:
                raw = json.loads(Path(path).read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError):
                continue
            if not isinstance(raw, list):
                continue
            for edge in raw:
                if isinstance(edge, dict):
                    imported += self._import_legacy_edge(edge)
        self.set_setting("legacy_json_migrated", {"imported": imported, "completed_at": iso_now()})
        return imported

    def _import_legacy_edge(self, edge: dict[str, Any]) -> int:
        signal_key = f"legacy:{edge.get('market_slug') or edge.get('event_slug') or edge.get('id')}"
        snapshot = ForecastSnapshot(
            market_type="temperature",
            city_slug=str(edge.get("city_slug") or ""),
            event_date=str(edge.get("event_date") or ""),
            unit="F",
        )
        signal = WeatherSignal(
            signal_key=signal_key,
            market_type="temperature",
            event_title=str(edge.get("event_title") or ""),
            market_slug=str(edge.get("market_slug") or ""),
            event_slug=str(edge.get("event_slug") or ""),
            city_slug=str(edge.get("city_slug") or ""),
            event_date=str(edge.get("event_date") or ""),
            label=str(edge.get("label") or ""),
            direction=str(edge.get("direction") or "YES"),
            market_prob=float(edge.get("market_price") or 0.0),
            forecast_prob=float(edge.get("wu_prob") or edge.get("om_prob") or edge.get("vc_prob") or edge.get("market_price") or 0.0),
            edge=float(edge.get("edge") or 0.0),
            edge_abs=abs(float(edge.get("edge") or 0.0)),
            edge_size=str(edge.get("edge_size") or "small"),
            confidence=str(edge.get("confidence") or "legacy"),
            source_count=2 if str(edge.get("confidence") or "") == "confirmed" else 1,
            liquidity=float(edge.get("liquidity") or 0.0),
            time_to_resolution_s=None,
            source_dispersion_pct=0.0,
            score=min(0.95, 0.45 + abs(float(edge.get("edge") or 0.0))),
            forecast_snapshot=snapshot,
            raw_payload=dict(edge),
            created_at=str(edge.get("scan_time") or iso_now()),
        )
        signal_id = self.log_signal(signal)
        decision_id = self.log_decision(
            signal_id,
            WeatherDecision(
                signal_key=signal.signal_key,
                accepted=bool(edge.get("bought")),
                reason="Imported from legacy JSON history.",
                final_score=signal.score,
                policy_action="legacy_import",
            ),
        )
        if not edge.get("bought") or not edge.get("resolved"):
            return 1
        cost = float(edge.get("buy_price") or edge.get("market_price") or 0.0)
        payout = 1.0 if str(edge.get("resolution") or "").upper() == str(edge.get("direction") or "").upper() else 0.0
        pnl = round(payout - cost, 6)
        self.conn.execute(
            """
            INSERT INTO paper_positions (
                signal_id, decision_id, signal_key, market_type, market_slug, event_slug, city_slug,
                event_date, label, direction, score, entry_price, shares, cost, status,
                resolution, realized_pnl, notes, created_at, resolved_at
            )
            VALUES (?, ?, ?, 'temperature', ?, ?, ?, ?, ?, ?, ?, ?, 1.0, ?, 'resolved', ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                decision_id,
                signal.signal_key,
                signal.market_slug,
                signal.event_slug,
                signal.city_slug,
                signal.event_date,
                signal.label,
                signal.direction,
                signal.score,
                signal.market_prob,
                cost,
                str(edge.get("resolution") or ""),
                pnl,
                "legacy_import",
                signal.created_at,
                iso_now(),
            ),
        )
        self.conn.commit()
        return 1


def _serialize_dashboard_position(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    now = datetime.now(timezone.utc)
    holding_metrics = _dashboard_holding_metrics(payload, now)
    pricing_metrics = _dashboard_pricing_metrics(payload)
    decision_metadata = _json_object(payload.get("decision_metadata_json"))

    return {
        "id": int(payload["id"]),
        "signal_key": str(payload.get("signal_key") or ""),
        "market_type": str(payload.get("market_type") or ""),
        "market_slug": str(payload.get("market_slug") or ""),
        "event_slug": str(payload.get("event_slug") or ""),
        "city_slug": str(payload.get("city_slug") or ""),
        "event_date": str(payload.get("event_date") or ""),
        "event_title": str(payload.get("signal_event_title") or payload.get("market_slug") or "Unknown market"),
        "target_label": str(payload.get("label") or ""),
        "direction": str(payload.get("direction") or ""),
        "status": str(payload.get("status") or ""),
        "notes": str(payload.get("notes") or ""),
        "score": _as_float(payload.get("score")),
        "adapter_score": _as_float(payload.get("signal_adapter_score")),
        "decision_final_score": _as_float(payload.get("decision_final_score")),
        "decision_reason": str(payload.get("decision_reason") or ""),
        "decision_policy_action": str(payload.get("decision_policy_action") or ""),
        "decision_metadata": decision_metadata,
        "entry_price": pricing_metrics["entry_price"],
        "entry_reference_price": pricing_metrics["entry_reference_price"],
        "entry_fee_paid": pricing_metrics["entry_fee_paid"],
        "entry_fee_bps": pricing_metrics["entry_fee_bps"],
        "entry_slippage_bps": pricing_metrics["entry_slippage_bps"],
        "market_probability": pricing_metrics["mark_price"],
        "outcome_probability": pricing_metrics["outcome_probability"],
        "shares": pricing_metrics["shares"],
        "cost": pricing_metrics["cost"],
        "realized_pnl": _as_float(payload.get("realized_pnl")),
        "expected_payout": pricing_metrics["expected_payout"],
        "expected_value_pnl": pricing_metrics["expected_value_pnl"],
        "mark_price": pricing_metrics["mark_price"],
        "mark_probability": pricing_metrics["outcome_probability"],
        "mark_to_market_payout": pricing_metrics["mark_to_market_payout"],
        "mark_to_market_pnl": pricing_metrics["mark_to_market_pnl"],
        "estimated_exit_price": pricing_metrics["estimated_exit_price"],
        "estimated_exit_fee_paid": pricing_metrics["estimated_exit_fee_paid"],
        "gross_liquidation_value": pricing_metrics["gross_liquidation_value"],
        "net_liquidation_value": pricing_metrics["net_liquidation_value"],
        "model_exit_price": pricing_metrics["model_exit_price"],
        "model_exit_fee_paid": pricing_metrics["model_exit_fee_paid"],
        "gross_model_value": pricing_metrics["gross_model_value"],
        "net_model_value": pricing_metrics["net_model_value"],
        "mark_edge_abs": _as_float(payload.get("mark_edge_abs")),
        "mark_final_score": _as_float(payload.get("mark_final_score")),
        "mark_updated_at": str(payload.get("mark_updated_at") or ""),
        "mark_reason": str(payload.get("mark_reason") or ""),
        "mark_to_market_mode": "reviewed_contract_mark" if payload.get("mark_updated_at") else "entry_contract_mark",
        "expected_value_mode": "reviewed_model_prob" if payload.get("mark_probability") is not None else "entry_model_prob",
        "exit_price": _bounded_probability(payload.get("exit_price")),
        "exit_reference_price": _bounded_probability(payload.get("exit_reference_price")),
        "exit_fee_paid": _as_float(payload.get("exit_fee_paid")),
        "exit_fee_bps": pricing_metrics["exit_fee_bps"],
        "exit_slippage_bps": pricing_metrics["exit_slippage_bps"],
        "gross_exit_payout": _as_float(payload.get("gross_exit_payout")),
        "net_exit_payout": _as_float(payload.get("net_exit_payout")),
        "exit_reason": str(payload.get("exit_reason") or ""),
        "mark_age_seconds": holding_metrics["mark_age_seconds"],
        "mark_is_stale": False,
        "edge": _as_float(payload.get("signal_edge")),
        "edge_abs": _as_float(payload.get("signal_edge_abs")),
        "edge_size": str(payload.get("signal_edge_size") or ""),
        "confidence": str(payload.get("signal_confidence") or ""),
        "source_count": int(payload.get("signal_source_count") or 0),
        "liquidity": _as_float(payload.get("signal_liquidity")),
        "source_dispersion_pct": _as_float(payload.get("signal_source_dispersion_pct")),
        "time_to_resolution_s": _as_float(payload.get("signal_time_to_resolution_s")),
        "remaining_to_resolution_s": holding_metrics["remaining_to_resolution_s"],
        "holding_seconds": holding_metrics["holding_seconds"],
        "created_at": str(payload.get("created_at") or ""),
        "resolved_at": str(payload.get("resolved_at") or ""),
        "resolution": str(payload.get("resolution") or ""),
    }


def _paper_entry_context(signal: WeatherSignal, entry_slippage_bps: Any) -> dict[str, float | None] | None:
    entry_reference_price = _contract_probability(signal.direction, signal.market_prob)
    entry_price = _apply_entry_slippage(entry_reference_price, entry_slippage_bps)
    if entry_reference_price is None or entry_price is None or entry_price <= 0:
        return None
    return {
        "entry_reference_price": entry_reference_price,
        "mark_probability": _contract_probability(signal.direction, signal.forecast_prob),
        "entry_price": entry_price,
    }


def _paper_entry_position_size(
    stake_usd: Any,
    available: float,
    entry_price: float | None,
    fee_bps: Any,
) -> dict[str, float] | None:
    if entry_price is None or entry_price <= 0:
        return None
    cost = round(min(float(stake_usd), available), 6)
    if cost <= 0:
        return None
    entry_fee_paid = _fee_amount(cost, fee_bps)
    net_entry_notional = round(cost - entry_fee_paid, 6)
    if net_entry_notional <= 0:
        return None
    return {
        "cost": cost,
        "entry_fee_paid": entry_fee_paid,
        "shares": round(net_entry_notional / float(entry_price), 6),
    }


def _paper_position_insert_values(
    *,
    signal_id: int,
    decision_id: int,
    signal: WeatherSignal,
    notes: str,
    decision_final_score: float | None,
    entry_reference_price: float | None,
    entry_price: float | None,
    mark_probability: float | None,
    entry_fee_paid: float,
    shares: float,
    cost: float,
    fee_bps: Any,
    entry_slippage_bps: Any,
    exit_fee_bps: Any,
    exit_slippage_bps: Any,
) -> tuple[Any, ...]:
    return (
        signal_id,
        decision_id,
        signal.signal_key,
        signal.market_type,
        signal.market_slug,
        signal.event_slug,
        signal.city_slug,
        signal.event_date,
        signal.label,
        signal.direction,
        signal.score,
        entry_price,
        entry_reference_price,
        entry_fee_paid,
        _bps(fee_bps),
        _bps(entry_slippage_bps),
        shares,
        cost,
        "open",
        None,
        None,
        entry_reference_price,
        mark_probability,
        signal.edge_abs,
        decision_final_score,
        signal.created_at,
        "Entry mark captured from signal.",
        None,
        None,
        None,
        _bps(exit_fee_bps),
        _bps(exit_slippage_bps),
        None,
        None,
        None,
        notes,
        signal.created_at,
        None,
    )


def _paper_position_record(
    *,
    position_id: int,
    signal: WeatherSignal,
    entry_price: float | None,
    shares: float,
    cost: float,
    notes: str,
) -> PaperPosition:
    return PaperPosition(
        id=position_id,
        signal_key=signal.signal_key,
        market_type=signal.market_type,
        market_slug=signal.market_slug,
        event_slug=signal.event_slug,
        city_slug=signal.city_slug,
        event_date=signal.event_date,
        label=signal.label,
        direction=signal.direction,
        score=signal.score,
        entry_price=entry_price or 0.0,
        shares=shares,
        cost=cost,
        status="open",
        notes=notes,
        created_at=signal.created_at,
    )


def _paper_position_exit_context(
    row: sqlite3.Row,
    *,
    exit_price: Any,
    exit_fee_bps: Any,
    exit_slippage_bps: Any,
) -> dict[str, float]:
    reference_price = _first_bounded_probability(
        exit_price,
        row["mark_price"],
        row["entry_reference_price"],
        row["entry_price"],
        default=0.0,
    )
    applied_exit_fee_bps = _bps(exit_fee_bps if exit_fee_bps is not None else row["exit_fee_bps"])
    applied_exit_slippage_bps = _bps(exit_slippage_bps if exit_slippage_bps is not None else row["exit_slippage_bps"])
    fill_exit_price, gross_payout, exit_fee_paid, net_payout = _estimate_net_exit_value(
        row["shares"],
        reference_price,
        fee_bps=applied_exit_fee_bps,
        slippage_bps=applied_exit_slippage_bps,
    )
    return {
        "reference_price": reference_price,
        "applied_exit_fee_bps": applied_exit_fee_bps,
        "applied_exit_slippage_bps": applied_exit_slippage_bps,
        "fill_exit_price": fill_exit_price or 0.0,
        "gross_payout": gross_payout or 0.0,
        "exit_fee_paid": exit_fee_paid or 0.0,
        "net_payout": net_payout or 0.0,
    }


def _closed_position_payload(
    row: sqlite3.Row,
    *,
    exit_context: dict[str, float],
    pnl: float,
    closed_at: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "ok": True,
        "position_id": int(row["id"]),
        "market_slug": str(row["market_slug"] or ""),
        "direction": str(row["direction"] or ""),
        "status": "closed",
        "exit_price": exit_context["fill_exit_price"],
        "exit_reference_price": exit_context["reference_price"],
        "exit_reason": str(reason or "manual"),
        "exit_fee_paid": exit_context["exit_fee_paid"],
        "gross_exit_payout": exit_context["gross_payout"],
        "net_exit_payout": exit_context["net_payout"],
        "realized_pnl": pnl,
        "closed_at": closed_at,
    }


def _dashboard_holding_metrics(payload: dict[str, Any], now: datetime) -> dict[str, float | None]:
    created_at = _parse_iso_datetime(payload.get("created_at"))
    resolved_at = _parse_iso_datetime(payload.get("resolved_at"))
    anchor = resolved_at or now
    holding_seconds = None
    if created_at is not None:
        holding_seconds = max(0.0, (anchor - created_at).total_seconds())
    remaining_to_resolution_s = _remaining_resolution_seconds(payload.get("signal_time_to_resolution_s"), holding_seconds)
    mark_updated_at = _parse_iso_datetime(payload.get("mark_updated_at"))
    mark_age_seconds = None
    if mark_updated_at is not None:
        mark_age_seconds = max(0.0, (now - mark_updated_at).total_seconds())
    return {
        "holding_seconds": holding_seconds,
        "remaining_to_resolution_s": remaining_to_resolution_s,
        "mark_age_seconds": mark_age_seconds,
    }


def _remaining_resolution_seconds(time_to_resolution_s: Any, holding_seconds: float | None) -> float | None:
    total_window = _as_float(time_to_resolution_s)
    if total_window is None:
        return None
    if holding_seconds is None:
        return max(0.0, total_window)
    return max(0.0, total_window - holding_seconds)


def _dashboard_pricing_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    direction = str(payload.get("direction") or "").upper()
    default_market_probability = _contract_probability(direction, payload.get("signal_market_prob"))
    default_outcome_probability = _contract_probability(direction, payload.get("signal_forecast_prob"))
    entry_reference_price = _first_bounded_probability(
        payload.get("entry_reference_price"),
        default=default_market_probability,
    )
    entry_price = _first_bounded_probability(
        payload.get("entry_price"),
        entry_reference_price,
        default_market_probability,
        default=0.0,
    )
    mark_price = _first_bounded_probability(payload.get("mark_price"), default=default_market_probability)
    outcome_probability = _first_bounded_probability(
        payload.get("mark_probability"),
        default=default_outcome_probability,
    )
    shares = _as_float(payload.get("shares")) or 0.0
    cost = _as_float(payload.get("cost")) or 0.0
    entry_fee_paid = _as_float(payload.get("entry_fee_paid")) or 0.0
    entry_fee_bps = _bps(payload.get("entry_fee_bps"))
    entry_slippage_bps = _bps(payload.get("entry_slippage_bps"))
    exit_fee_bps = _bps(payload.get("exit_fee_bps"))
    exit_slippage_bps = _bps(payload.get("exit_slippage_bps"))
    estimated_exit_price, gross_liquidation_value, estimated_exit_fee_paid, net_liquidation_value = _estimate_net_exit_value(
        shares,
        mark_price,
        fee_bps=exit_fee_bps,
        slippage_bps=exit_slippage_bps,
    )
    model_exit_price, gross_model_value, model_exit_fee_paid, net_model_value = _estimate_net_exit_value(
        shares,
        outcome_probability,
        fee_bps=exit_fee_bps,
        slippage_bps=exit_slippage_bps,
    )
    expected_payout = round(shares * outcome_probability, 6) if outcome_probability is not None else None
    expected_value_pnl = round((net_model_value or 0.0) - cost, 6) if outcome_probability is not None else None
    mark_to_market_payout = round(shares * mark_price, 6) if mark_price is not None else None
    mark_to_market_pnl = round((net_liquidation_value or 0.0) - cost, 6) if mark_price is not None else None
    return {
        "entry_reference_price": entry_reference_price,
        "entry_price": entry_price,
        "entry_fee_paid": entry_fee_paid,
        "entry_fee_bps": entry_fee_bps,
        "entry_slippage_bps": entry_slippage_bps,
        "exit_fee_bps": exit_fee_bps,
        "exit_slippage_bps": exit_slippage_bps,
        "mark_price": mark_price,
        "outcome_probability": outcome_probability,
        "shares": shares,
        "cost": cost,
        "expected_payout": expected_payout,
        "expected_value_pnl": expected_value_pnl,
        "mark_to_market_payout": mark_to_market_payout,
        "mark_to_market_pnl": mark_to_market_pnl,
        "estimated_exit_price": estimated_exit_price,
        "estimated_exit_fee_paid": estimated_exit_fee_paid,
        "gross_liquidation_value": gross_liquidation_value,
        "net_liquidation_value": net_liquidation_value,
        "model_exit_price": model_exit_price,
        "model_exit_fee_paid": model_exit_fee_paid,
        "gross_model_value": gross_model_value,
        "net_model_value": net_model_value,
    }


def _json_object(value: Any) -> dict[str, Any]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bounded_probability(value: Any) -> float | None:
    raw = _as_float(value)
    if raw is None:
        return None
    return round(max(0.0, min(1.0, raw)), 6)


def _first_bounded_probability(*values: Any, default: float | None = None) -> float | None:
    for value in values:
        bounded = _bounded_probability(value)
        if bounded is not None:
            return bounded
    return default


def _contract_probability(direction: Any, yes_probability: Any) -> float | None:
    raw = _bounded_probability(yes_probability)
    if raw is None:
        return None
    return raw if str(direction or "").upper() == "YES" else round(1.0 - raw, 6)


def _bps(value: Any) -> float:
    raw = _as_float(value)
    if raw is None:
        return 0.0
    return round(max(0.0, raw), 6)


def _apply_entry_slippage(probability: Any, slippage_bps: Any) -> float | None:
    raw = _bounded_probability(probability)
    if raw is None:
        return None
    return _bounded_probability(raw * (1.0 + (_bps(slippage_bps) / 10000.0)))


def _apply_exit_slippage(probability: Any, slippage_bps: Any) -> float | None:
    raw = _bounded_probability(probability)
    if raw is None:
        return None
    return _bounded_probability(raw * (1.0 - (_bps(slippage_bps) / 10000.0)))


def _fee_amount(notional: Any, fee_bps: Any) -> float:
    raw = max(0.0, _as_float(notional) or 0.0)
    return round(raw * (_bps(fee_bps) / 10000.0), 6)


def _estimate_net_exit_value(
    shares: Any,
    reference_price: Any,
    *,
    fee_bps: Any,
    slippage_bps: Any,
) -> tuple[float | None, float | None, float | None, float | None]:
    raw_reference = _bounded_probability(reference_price)
    if raw_reference is None:
        return None, None, None, None
    shares_value = max(0.0, _as_float(shares) or 0.0)
    fill_exit_price = _apply_exit_slippage(raw_reference, slippage_bps)
    if fill_exit_price is None:
        return None, None, None, None
    gross_payout = round(shares_value * fill_exit_price, 6)
    exit_fee_paid = _fee_amount(gross_payout, fee_bps)
    net_payout = round(gross_payout - exit_fee_paid, 6)
    return fill_exit_price, gross_payout, exit_fee_paid, net_payout


def _close_outcome_label(reason: Any) -> str:
    normalized = str(reason or "").strip().lower()
    if not normalized:
        return "Closed"
    if normalized.startswith("manual"):
        return "Sold"
    if "review" in normalized:
        return "Auto Exit"
    return "Closed"
