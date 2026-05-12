"""SQLite-backed persistence for signals, decisions, and paper positions."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .execution.models import ShadowOrderIntent
from .execution.shadow_fill import extract_clob_token_ids, token_id_for_outcome
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
                reason_code TEXT,
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

            CREATE TABLE IF NOT EXISTS paper_position_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER NOT NULL,
                reviewed_at TEXT NOT NULL,
                event_kind TEXT NOT NULL,
                status TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                city_slug TEXT NOT NULL,
                direction TEXT NOT NULL,
                reason TEXT NOT NULL,
                reason_code TEXT,
                mark_price REAL,
                mark_probability REAL,
                mark_edge_abs REAL,
                mark_final_score REAL,
                mark_to_market_pnl REAL,
                net_liquidation_value REAL,
                estimated_exit_price REAL,
                estimated_exit_fee_paid REAL,
                payload_json TEXT NOT NULL,
                FOREIGN KEY(position_id) REFERENCES paper_positions(id)
            );

            CREATE INDEX IF NOT EXISTS idx_position_reviews_position_id ON paper_position_reviews(position_id);
            CREATE INDEX IF NOT EXISTS idx_position_reviews_reviewed_at ON paper_position_reviews(reviewed_at);

            CREATE TABLE IF NOT EXISTS shadow_order_intents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                decision_id INTEGER,
                position_id INTEGER,
                intent_kind TEXT NOT NULL,
                execution_mode TEXT NOT NULL,
                signal_key TEXT NOT NULL,
                market_type TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                city_slug TEXT NOT NULL,
                event_date TEXT NOT NULL,
                label TEXT NOT NULL,
                direction TEXT NOT NULL,
                order_action TEXT NOT NULL,
                outcome_side TEXT NOT NULL,
                order_intent TEXT NOT NULL,
                order_type TEXT NOT NULL,
                time_in_force TEXT NOT NULL,
                manual_order_indicator TEXT NOT NULL,
                target_price REAL NOT NULL,
                reference_price REAL NOT NULL,
                shares REAL NOT NULL,
                notional_usd REAL NOT NULL,
                estimated_fee_paid REAL NOT NULL,
                decision_final_score REAL,
                clob_token_id TEXT,
                book_best_bid REAL,
                book_best_ask REAL,
                book_spread REAL,
                book_midpoint REAL,
                book_depth_at_target_shares REAL,
                book_depth_at_target_usd REAL,
                simulated_fill_status TEXT NOT NULL DEFAULT 'not_checked',
                simulated_fill_shares REAL,
                simulated_avg_fill_price REAL,
                simulated_notional_usd REAL,
                simulated_unfilled_shares REAL,
                simulated_slippage_bps REAL,
                execution_checked_at TEXT,
                execution_error TEXT,
                reason TEXT NOT NULL,
                reason_code TEXT,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(signal_id) REFERENCES signals(id),
                FOREIGN KEY(decision_id) REFERENCES decisions(id),
                FOREIGN KEY(position_id) REFERENCES paper_positions(id)
            );

            CREATE INDEX IF NOT EXISTS idx_shadow_order_intents_created_at ON shadow_order_intents(created_at);
            CREATE INDEX IF NOT EXISTS idx_shadow_order_intents_market_slug ON shadow_order_intents(market_slug);
            CREATE INDEX IF NOT EXISTS idx_shadow_order_intents_kind ON shadow_order_intents(intent_kind);

            CREATE TABLE IF NOT EXISTS shadow_exec_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shadow_intent_id INTEGER,
                paper_position_id INTEGER,
                shadow_position_id INTEGER,
                parent_order_id INTEGER,
                intent_kind TEXT NOT NULL,
                execution_mode TEXT NOT NULL,
                market_type TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                city_slug TEXT NOT NULL,
                event_date TEXT NOT NULL,
                label TEXT NOT NULL,
                direction TEXT NOT NULL,
                order_action TEXT NOT NULL,
                outcome_side TEXT NOT NULL,
                target_price REAL NOT NULL,
                reference_price REAL,
                requested_shares REAL NOT NULL,
                filled_shares REAL NOT NULL DEFAULT 0,
                avg_fill_price REAL,
                filled_notional_usd REAL NOT NULL DEFAULT 0,
                unfilled_shares REAL NOT NULL DEFAULT 0,
                estimated_fee_paid REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                status_reason TEXT,
                ttl_seconds INTEGER NOT NULL,
                expires_at TEXT,
                clob_token_id TEXT,
                taker_estimate_avg_price REAL,
                taker_estimate_notional_usd REAL,
                taker_estimate_fill_shares REAL,
                taker_estimate_pnl REAL,
                queue_fill_fraction REAL NOT NULL DEFAULT 0.5,
                last_checked_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                FOREIGN KEY(shadow_intent_id) REFERENCES shadow_order_intents(id),
                FOREIGN KEY(paper_position_id) REFERENCES paper_positions(id),
                FOREIGN KEY(shadow_position_id) REFERENCES shadow_exec_positions(id),
                FOREIGN KEY(parent_order_id) REFERENCES shadow_exec_orders(id)
            );

            CREATE INDEX IF NOT EXISTS idx_shadow_exec_orders_status ON shadow_exec_orders(status);
            CREATE INDEX IF NOT EXISTS idx_shadow_exec_orders_created_at ON shadow_exec_orders(created_at);
            CREATE INDEX IF NOT EXISTS idx_shadow_exec_orders_token ON shadow_exec_orders(clob_token_id);
            CREATE INDEX IF NOT EXISTS idx_shadow_exec_orders_paper_position ON shadow_exec_orders(paper_position_id);

            CREATE TABLE IF NOT EXISTS shadow_exec_fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                shadow_position_id INTEGER,
                shadow_intent_id INTEGER,
                paper_position_id INTEGER,
                clob_token_id TEXT,
                action TEXT NOT NULL,
                price REAL NOT NULL,
                shares REAL NOT NULL,
                notional_usd REAL NOT NULL,
                liquidity_source TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                filled_at TEXT NOT NULL,
                FOREIGN KEY(order_id) REFERENCES shadow_exec_orders(id),
                FOREIGN KEY(shadow_position_id) REFERENCES shadow_exec_positions(id),
                FOREIGN KEY(shadow_intent_id) REFERENCES shadow_order_intents(id),
                FOREIGN KEY(paper_position_id) REFERENCES paper_positions(id)
            );

            CREATE INDEX IF NOT EXISTS idx_shadow_exec_fills_order ON shadow_exec_fills(order_id);
            CREATE INDEX IF NOT EXISTS idx_shadow_exec_fills_filled_at ON shadow_exec_fills(filled_at);

            CREATE TABLE IF NOT EXISTS shadow_exec_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                paper_position_id INTEGER,
                market_type TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                city_slug TEXT NOT NULL,
                event_date TEXT NOT NULL,
                label TEXT NOT NULL,
                direction TEXT NOT NULL,
                clob_token_id TEXT,
                entry_order_id INTEGER,
                exit_order_id INTEGER,
                status TEXT NOT NULL,
                total_entry_shares REAL NOT NULL DEFAULT 0,
                total_entry_notional_usd REAL NOT NULL DEFAULT 0,
                avg_entry_price REAL,
                open_shares REAL NOT NULL DEFAULT 0,
                closed_shares REAL NOT NULL DEFAULT 0,
                remaining_cost_basis_usd REAL NOT NULL DEFAULT 0,
                exit_notional_usd REAL NOT NULL DEFAULT 0,
                avg_exit_price REAL,
                realized_pnl REAL NOT NULL DEFAULT 0,
                mark_price REAL,
                mark_value_usd REAL,
                unrealized_pnl REAL,
                total_pnl REAL,
                taker_exit_estimated_pnl REAL,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                last_marked_at TEXT,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                FOREIGN KEY(paper_position_id) REFERENCES paper_positions(id),
                FOREIGN KEY(entry_order_id) REFERENCES shadow_exec_orders(id),
                FOREIGN KEY(exit_order_id) REFERENCES shadow_exec_orders(id)
            );

            CREATE INDEX IF NOT EXISTS idx_shadow_exec_positions_status ON shadow_exec_positions(status);
            CREATE INDEX IF NOT EXISTS idx_shadow_exec_positions_paper_position ON shadow_exec_positions(paper_position_id);
            CREATE INDEX IF NOT EXISTS idx_shadow_exec_positions_market ON shadow_exec_positions(market_slug);

            CREATE TABLE IF NOT EXISTS shadow_exec_marks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shadow_position_id INTEGER NOT NULL,
                mark_price REAL NOT NULL,
                mark_value_usd REAL NOT NULL,
                unrealized_pnl REAL NOT NULL,
                total_pnl REAL NOT NULL,
                source TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(shadow_position_id) REFERENCES shadow_exec_positions(id)
            );

            CREATE INDEX IF NOT EXISTS idx_shadow_exec_marks_position ON shadow_exec_marks(shadow_position_id);
            CREATE INDEX IF NOT EXISTS idx_shadow_exec_marks_created_at ON shadow_exec_marks(created_at);
            """
        )
        self._ensure_paper_position_columns()
        self._ensure_decision_columns()
        self._ensure_shadow_order_columns()
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

    def backup_compact_database(
        self,
        destination: str | Path,
        *,
        signal_limit: int = 20000,
        decision_limit: int = 20000,
        review_limit: int = 5000,
        shadow_order_limit: int = 5000,
        operator_event_limit: int = 1000,
        resolution_event_limit: int = 1000,
    ) -> str:
        target = Path(self.backup_database(destination))
        conn = sqlite3.connect(str(target))
        try:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute(
                """
                DELETE FROM paper_position_reviews
                WHERE id NOT IN (
                    SELECT id FROM paper_position_reviews
                    ORDER BY reviewed_at DESC, id DESC
                    LIMIT ?
                )
                """,
                (max(0, int(review_limit)),),
            )
            conn.execute(
                """
                DELETE FROM shadow_order_intents
                WHERE id NOT IN (
                    SELECT id FROM shadow_order_intents
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                )
                """,
                (max(0, int(shadow_order_limit)),),
            )
            for table_name, order_column in (
                ("shadow_exec_marks", "created_at"),
                ("shadow_exec_fills", "filled_at"),
                ("shadow_exec_orders", "created_at"),
                ("shadow_exec_positions", "updated_at"),
            ):
                conn.execute(
                    f"""
                    DELETE FROM {table_name}
                    WHERE id NOT IN (
                        SELECT id FROM {table_name}
                        ORDER BY {order_column} DESC, id DESC
                        LIMIT ?
                    )
                    """,
                    (max(0, int(shadow_order_limit)),),
                )
            conn.execute(
                """
                DELETE FROM operator_events
                WHERE id NOT IN (
                    SELECT id FROM operator_events
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                )
                """,
                (max(0, int(operator_event_limit)),),
            )
            conn.execute(
                """
                DELETE FROM resolution_events
                WHERE id NOT IN (
                    SELECT id FROM resolution_events
                    ORDER BY resolved_at DESC, id DESC
                    LIMIT ?
                )
                """,
                (max(0, int(resolution_event_limit)),),
            )
            conn.execute(
                """
                DELETE FROM decisions
                WHERE id NOT IN (
                    SELECT id FROM decisions
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                )
                AND id NOT IN (
                    SELECT decision_id FROM paper_positions WHERE decision_id IS NOT NULL
                )
                AND id NOT IN (
                    SELECT decision_id FROM shadow_order_intents WHERE decision_id IS NOT NULL
                )
                """,
                (max(0, int(decision_limit)),),
            )
            conn.execute(
                """
                DELETE FROM signals
                WHERE id NOT IN (
                    SELECT id FROM signals
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?
                )
                AND id NOT IN (
                    SELECT signal_id FROM decisions WHERE signal_id IS NOT NULL
                )
                AND id NOT IN (
                    SELECT signal_id FROM paper_positions WHERE signal_id IS NOT NULL
                )
                AND id NOT IN (
                    SELECT signal_id FROM shadow_order_intents WHERE signal_id IS NOT NULL
                )
                """,
                (max(0, int(signal_limit)),),
            )
            conn.commit()
            conn.execute("VACUUM")
        finally:
            conn.close()
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
                    reason_code, policy_action, metadata_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    decision.signal_key,
                    1 if decision.accepted else 0,
                    decision.reason,
                    decision.final_score,
                    None if decision.reason_code is None else str(decision.reason_code),
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

    def preview_shadow_entry_intent(
        self,
        *,
        signal_id: int,
        decision_id: int,
        signal: WeatherSignal,
        execution_mode: str,
        decision_final_score: float | None = None,
        reason: str = "",
        reason_code: str | None = None,
        stake_usd: float,
        fee_bps: float = 0.0,
        entry_slippage_bps: float = 0.0,
    ) -> ShadowOrderIntent | None:
        entry_context = _paper_entry_context(signal, entry_slippage_bps)
        if entry_context is None:
            return None
        with self._lock:
            _, available = self.get_paper_capital()
            position_size = _paper_entry_position_size(stake_usd, available, entry_context["entry_price"], fee_bps)
        if position_size is None:
            return None
        outcome_side = str(signal.direction or "").upper()
        signal_payload = dict(signal.raw_payload or {})
        clob_token_id = token_id_for_outcome(signal_payload, outcome_side)
        return ShadowOrderIntent(
            signal_id=int(signal_id),
            decision_id=int(decision_id),
            intent_kind="entry",
            execution_mode=str(execution_mode or "paper"),
            signal_key=signal.signal_key,
            market_type=signal.market_type,
            market_slug=signal.market_slug,
            event_slug=signal.event_slug,
            city_slug=signal.city_slug,
            event_date=signal.event_date,
            label=signal.label,
            direction=signal.direction,
            order_action="BUY",
            outcome_side=outcome_side,
            order_intent="BUY_SHORT" if outcome_side == "NO" else "BUY_LONG",
            order_type="LIMIT",
            time_in_force="GTC",
            manual_order_indicator="AUTOMATIC",
            target_price=float(entry_context["entry_price"] or 0.0),
            reference_price=float(entry_context["entry_reference_price"] or 0.0),
            shares=float(position_size["shares"] or 0.0),
            notional_usd=float(position_size["cost"] or 0.0),
            estimated_fee_paid=float(position_size["entry_fee_paid"] or 0.0),
            decision_final_score=_as_float(decision_final_score),
            clob_token_id=clob_token_id,
            reason=str(reason or ""),
            reason_code=reason_code,
            payload={
                "signal_score": _as_float(signal.score),
                "market_prob": _as_float(signal.market_prob),
                "forecast_prob": _as_float(signal.forecast_prob),
                "edge_abs": _as_float(signal.edge_abs),
                "source_count": int(signal.source_count or 0),
                "liquidity": _as_float(signal.liquidity),
                "requested_stake_usd": float(stake_usd or 0.0),
                "available_capital": float(available or 0.0),
                "entry_fee_bps": _bps(fee_bps),
                "entry_slippage_bps": _bps(entry_slippage_bps),
                "clob_token_ids": extract_clob_token_ids(signal_payload),
                "yes_token_id": signal_payload.get("yes_token_id") or signal_payload.get("yesTokenId"),
                "no_token_id": signal_payload.get("no_token_id") or signal_payload.get("noTokenId"),
            },
        )

    def preview_shadow_exit_intent(
        self,
        position_id: int,
        *,
        execution_mode: str,
        exit_price: float | None = None,
        reason: str = "",
        reason_code: str | None = None,
        decision_final_score: float | None = None,
        exit_fee_bps: float | None = None,
        exit_slippage_bps: float | None = None,
    ) -> ShadowOrderIntent | None:
        with self._lock:
            row = self.conn.execute("SELECT * FROM paper_positions WHERE id = ? AND status = 'open'", (int(position_id),)).fetchone()
            if row is None:
                return None
            exit_context = _paper_position_exit_context(
                row,
                exit_price=exit_price,
                exit_fee_bps=exit_fee_bps,
                exit_slippage_bps=exit_slippage_bps,
            )
            entry_shadow = self._shadow_entry_payload_for_position_locked(int(position_id))
        direction = str(row["direction"] or "").upper()
        clob_token_id = token_id_for_outcome(entry_shadow, direction)
        return ShadowOrderIntent(
            signal_id=int(row["signal_id"]) if row["signal_id"] is not None else None,
            decision_id=int(row["decision_id"]) if row["decision_id"] is not None else None,
            position_id=int(row["id"]),
            intent_kind="exit",
            execution_mode=str(execution_mode or "paper"),
            signal_key=str(row["signal_key"] or ""),
            market_type=str(row["market_type"] or ""),
            market_slug=str(row["market_slug"] or ""),
            event_slug=str(row["event_slug"] or ""),
            city_slug=str(row["city_slug"] or ""),
            event_date=str(row["event_date"] or ""),
            label=str(row["label"] or ""),
            direction=direction,
            order_action="SELL",
            outcome_side=direction,
            order_intent="SELL_SHORT" if direction == "NO" else "SELL_LONG",
            order_type="LIMIT",
            time_in_force="IOC",
            manual_order_indicator="AUTOMATIC",
            target_price=float(exit_context["fill_exit_price"] or 0.0),
            reference_price=float(exit_context["reference_price"] or 0.0),
            shares=float(row["shares"] or 0.0),
            notional_usd=float(exit_context["net_payout"] or 0.0),
            estimated_fee_paid=float(exit_context["exit_fee_paid"] or 0.0),
            decision_final_score=_as_float(decision_final_score),
            clob_token_id=clob_token_id,
            reason=str(reason or ""),
            reason_code=reason_code,
            payload={
                "entry_shadow_order": entry_shadow,
                "entry_price": _as_float(row["entry_price"]),
                "cost": _as_float(row["cost"]),
                "mark_price": _as_float(row["mark_price"]),
                "mark_probability": _as_float(row["mark_probability"]),
                "mark_reason": str(row["mark_reason"] or ""),
                "exit_fee_bps": exit_context["applied_exit_fee_bps"],
                "exit_slippage_bps": exit_context["applied_exit_slippage_bps"],
                "gross_payout": float(exit_context["gross_payout"] or 0.0),
            },
        )

    def _shadow_entry_payload_for_position_locked(self, position_id: int) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT *
            FROM shadow_order_intents
            WHERE position_id = ? AND intent_kind = 'entry'
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            (int(position_id),),
        ).fetchone()
        if row is None:
            return {}
        payload = _json_object(row["payload_json"])
        return {
            **payload,
            "clob_token_id": str(row["clob_token_id"] or "") if "clob_token_id" in row.keys() else "",
            "simulated_fill_status": str(row["simulated_fill_status"] or "") if "simulated_fill_status" in row.keys() else "",
        }

    def record_shadow_order_intent(self, intent: ShadowOrderIntent) -> int:
        payload_json = json.dumps(intent.payload, sort_keys=True)
        with self._lock:
            cursor = self.conn.execute(
                """
                INSERT INTO shadow_order_intents (
                    signal_id, decision_id, position_id, intent_kind, execution_mode, signal_key,
                    market_type, market_slug, event_slug, city_slug, event_date, label, direction,
                    order_action, outcome_side, order_intent, order_type, time_in_force,
                    manual_order_indicator, target_price, reference_price, shares, notional_usd,
                    estimated_fee_paid, decision_final_score, clob_token_id, book_best_bid,
                    book_best_ask, book_spread, book_midpoint, book_depth_at_target_shares,
                    book_depth_at_target_usd, simulated_fill_status, simulated_fill_shares,
                    simulated_avg_fill_price, simulated_notional_usd, simulated_unfilled_shares,
                    simulated_slippage_bps, execution_checked_at, execution_error, reason,
                    reason_code, status, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.signal_id,
                    intent.decision_id,
                    intent.position_id,
                    intent.intent_kind,
                    intent.execution_mode,
                    intent.signal_key,
                    intent.market_type,
                    intent.market_slug,
                    intent.event_slug,
                    intent.city_slug,
                    intent.event_date,
                    intent.label,
                    intent.direction,
                    intent.order_action,
                    intent.outcome_side,
                    intent.order_intent,
                    intent.order_type,
                    intent.time_in_force,
                    intent.manual_order_indicator,
                    intent.target_price,
                    intent.reference_price,
                    intent.shares,
                    intent.notional_usd,
                    intent.estimated_fee_paid,
                    intent.decision_final_score,
                    intent.clob_token_id,
                    intent.book_best_bid,
                    intent.book_best_ask,
                    intent.book_spread,
                    intent.book_midpoint,
                    intent.book_depth_at_target_shares,
                    intent.book_depth_at_target_usd,
                    intent.simulated_fill_status,
                    intent.simulated_fill_shares,
                    intent.simulated_avg_fill_price,
                    intent.simulated_notional_usd,
                    intent.simulated_unfilled_shares,
                    intent.simulated_slippage_bps,
                    intent.execution_checked_at,
                    intent.execution_error,
                    intent.reason,
                    intent.reason_code,
                    intent.status,
                    payload_json,
                    intent.created_at,
                ),
            )
            self.conn.commit()
            return int(cursor.lastrowid)

    def get_recent_shadow_order_intents(self, *, limit: int | None = 20, intent_kind: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if intent_kind:
            where = "WHERE intent_kind = ?"
            params.append(str(intent_kind))
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(max(1, int(limit)))
        with self._lock:
            rows = self.conn.execute(
                f"""
                SELECT *
                FROM shadow_order_intents
                {where}
                ORDER BY created_at DESC, id DESC
                {limit_clause}
                """,
                tuple(params),
            ).fetchall()
        return [_serialize_shadow_order_intent(row) for row in rows]

    def create_shadow_exec_order_from_intent(
        self,
        *,
        shadow_intent_id: int,
        intent: ShadowOrderIntent,
        entry_ttl_seconds: int = 1800,
        exit_ttl_seconds: int = 300,
        queue_fill_fraction: float = 0.5,
        taker_estimate: dict[str, Any] | None = None,
    ) -> int:
        ttl_seconds = int(entry_ttl_seconds if str(intent.intent_kind or "").lower() == "entry" else exit_ttl_seconds)
        ttl_seconds = max(1, ttl_seconds)
        created_at = str(intent.created_at or iso_now())
        created_dt = _parse_iso_datetime(created_at) or datetime.now(timezone.utc)
        expires_at = (created_dt + timedelta(seconds=ttl_seconds)).isoformat()
        requested_shares = max(0.0, float(intent.shares or 0.0))
        status = "resting"
        status_reason = "Waiting for executable public market evidence."
        shadow_position_id = None
        shadow_position_open_shares = None
        if not str(intent.clob_token_id or "").strip():
            status = "error"
            status_reason = "missing_token_id"
        elif requested_shares <= 0:
            status = "error"
            status_reason = "zero_requested_shares"
        elif str(intent.intent_kind or "").lower() == "exit":
            with self._lock:
                row = self.conn.execute(
                    """
                    SELECT id, open_shares
                    FROM shadow_exec_positions
                    WHERE paper_position_id = ? AND status = 'open'
                    ORDER BY opened_at ASC, id ASC
                    LIMIT 1
                    """,
                    (intent.position_id,),
                ).fetchone()
            if row is None:
                status = "no_position"
                status_reason = "No realistic shadow position exists for this paper exit."
            else:
                shadow_position_id = int(row["id"])
                shadow_position_open_shares = max(0.0, float(row["open_shares"] or 0.0))
                requested_shares = min(requested_shares, shadow_position_open_shares)
                if requested_shares <= 0:
                    status = "no_position"
                    status_reason = "Realistic shadow position has no open shares."

        payload = {
            "shadow_intent": intent.to_dict(),
            "shadow_execution": {
                "entry_ttl_seconds": int(entry_ttl_seconds),
                "exit_ttl_seconds": int(exit_ttl_seconds),
                "queue_fill_fraction": float(queue_fill_fraction),
                "shadow_position_open_shares": shadow_position_open_shares,
            },
        }
        if taker_estimate:
            payload["taker_estimate"] = taker_estimate
        with self._lock:
            existing = self.conn.execute(
                "SELECT id FROM shadow_exec_orders WHERE shadow_intent_id = ? LIMIT 1",
                (int(shadow_intent_id),),
            ).fetchone()
            if existing is not None:
                return int(existing["id"])
            cursor = self.conn.execute(
                """
                INSERT INTO shadow_exec_orders (
                    shadow_intent_id, paper_position_id, shadow_position_id, parent_order_id,
                    intent_kind, execution_mode, market_type, market_slug, event_slug, city_slug,
                    event_date, label, direction, order_action, outcome_side, target_price,
                    reference_price, requested_shares, filled_shares, avg_fill_price,
                    filled_notional_usd, unfilled_shares, estimated_fee_paid, status,
                    status_reason, ttl_seconds, expires_at, clob_token_id,
                    taker_estimate_avg_price, taker_estimate_notional_usd,
                    taker_estimate_fill_shares, taker_estimate_pnl, queue_fill_fraction,
                    last_checked_at, created_at, updated_at, payload_json
                )
                VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(shadow_intent_id),
                    intent.position_id,
                    shadow_position_id,
                    str(intent.intent_kind or ""),
                    str(intent.execution_mode or ""),
                    str(intent.market_type or ""),
                    str(intent.market_slug or ""),
                    str(intent.event_slug or ""),
                    str(intent.city_slug or ""),
                    str(intent.event_date or ""),
                    str(intent.label or ""),
                    str(intent.direction or ""),
                    str(intent.order_action or ""),
                    str(intent.outcome_side or ""),
                    float(intent.target_price or 0.0),
                    _as_float(intent.reference_price),
                    requested_shares,
                    requested_shares,
                    float(intent.estimated_fee_paid or 0.0),
                    status,
                    status_reason,
                    ttl_seconds,
                    expires_at,
                    str(intent.clob_token_id or ""),
                    _as_float((taker_estimate or {}).get("avg_fill_price")),
                    _as_float((taker_estimate or {}).get("notional_usd")),
                    _as_float((taker_estimate or {}).get("fill_shares")),
                    _as_float((taker_estimate or {}).get("estimated_pnl")),
                    max(0.0, min(1.0, float(queue_fill_fraction))),
                    None,
                    created_at,
                    created_at,
                    json.dumps(payload, sort_keys=True),
                ),
            )
            order_id = int(cursor.lastrowid)
            if shadow_position_id is not None:
                self.conn.execute(
                    """
                    UPDATE shadow_exec_positions
                    SET exit_order_id = ?,
                        taker_exit_estimated_pnl = COALESCE(?, taker_exit_estimated_pnl),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        order_id,
                        _as_float((taker_estimate or {}).get("estimated_pnl")),
                        created_at,
                        shadow_position_id,
                    ),
                )
            if intent.position_id is not None:
                self._sync_paper_position_with_shadow_locked(int(intent.position_id), synced_at=created_at)
            self.conn.commit()
            return order_id

    def update_shadow_exec_order_target(
        self,
        order_id: int,
        *,
        target_price: float,
        reason: str,
        evidence: dict[str, Any] | None = None,
        updated_at: str | None = None,
    ) -> dict[str, Any] | None:
        updated_at = str(updated_at or iso_now())
        target_price = _bounded_probability(target_price)
        if target_price is None:
            return self.get_shadow_exec_order(order_id)
        with self._lock:
            row = self.conn.execute("SELECT * FROM shadow_exec_orders WHERE id = ?", (int(order_id),)).fetchone()
            if row is None:
                return None
            payload = _json_object(row["payload_json"])
            repricing = payload.get("shadow_execution_repricing")
            if not isinstance(repricing, dict):
                repricing = {
                    "original_target_price": _as_float(row["target_price"]),
                    "events": [],
                }
            events = repricing.get("events")
            if not isinstance(events, list):
                events = []
            events.append(
                {
                    "target_price": target_price,
                    "reason": str(reason or "repriced"),
                    "updated_at": updated_at,
                    "evidence": evidence or {},
                }
            )
            repricing["events"] = events[-20:]
            repricing["current_target_price"] = target_price
            repricing["last_reason"] = str(reason or "repriced")
            repricing["last_updated_at"] = updated_at
            payload["shadow_execution_repricing"] = repricing
            self.conn.execute(
                """
                UPDATE shadow_exec_orders
                SET target_price = ?, status_reason = ?, last_checked_at = ?, updated_at = ?, payload_json = ?
                WHERE id = ?
                """,
                (
                    target_price,
                    str(reason or "repriced"),
                    updated_at,
                    updated_at,
                    json.dumps(payload, sort_keys=True),
                    int(order_id),
                ),
            )
            self.conn.commit()
        return self.get_shadow_exec_order(order_id)

    def get_shadow_exec_order(self, order_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self.conn.execute("SELECT * FROM shadow_exec_orders WHERE id = ?", (int(order_id),)).fetchone()
        return _serialize_shadow_exec_order(row) if row is not None else None

    def get_active_shadow_exec_orders(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(max(1, int(limit)))
        with self._lock:
            rows = self.conn.execute(
                f"""
                SELECT *
                FROM shadow_exec_orders
                WHERE status IN ('resting', 'partial_fill')
                ORDER BY created_at ASC, id ASC
                {limit_clause}
                """,
                tuple(params),
            ).fetchall()
        return [_serialize_shadow_exec_order(row) for row in rows]

    def record_shadow_exec_fill(
        self,
        order_id: int,
        *,
        price: float,
        shares: float,
        liquidity_source: str,
        evidence: dict[str, Any] | None = None,
        filled_at: str | None = None,
    ) -> dict[str, Any] | None:
        filled_at = str(filled_at or iso_now())
        price = _bounded_probability(price) or 0.0
        requested_fill_shares = max(0.0, float(shares or 0.0))
        if price <= 0 or requested_fill_shares <= 0:
            return None
        with self._lock:
            order = self.conn.execute("SELECT * FROM shadow_exec_orders WHERE id = ?", (int(order_id),)).fetchone()
            if order is None:
                return None
            if str(order["status"] or "") not in {"resting", "partial_fill"}:
                return None
            remaining = max(0.0, float(order["requested_shares"] or 0.0) - float(order["filled_shares"] or 0.0))
            fill_shares = min(remaining, requested_fill_shares)
            shadow_position_id = int(order["shadow_position_id"]) if order["shadow_position_id"] is not None else None
            if str(order["intent_kind"] or "").lower() == "exit":
                if shadow_position_id is None:
                    self.conn.execute(
                        """
                        UPDATE shadow_exec_orders
                        SET status = 'no_position', status_reason = ?, last_checked_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        ("No realistic shadow position exists for this exit order.", filled_at, filled_at, int(order_id)),
                    )
                    if order["paper_position_id"] is not None:
                        self._sync_paper_position_with_shadow_locked(int(order["paper_position_id"]), synced_at=filled_at)
                    self.conn.commit()
                    return None
                position = self.conn.execute("SELECT * FROM shadow_exec_positions WHERE id = ?", (shadow_position_id,)).fetchone()
                if position is None or str(position["status"] or "") != "open":
                    self.conn.execute(
                        """
                        UPDATE shadow_exec_orders
                        SET status = 'no_position', status_reason = ?, last_checked_at = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        ("Realistic shadow position was already closed.", filled_at, filled_at, int(order_id)),
                    )
                    if order["paper_position_id"] is not None:
                        self._sync_paper_position_with_shadow_locked(int(order["paper_position_id"]), synced_at=filled_at)
                    self.conn.commit()
                    return None
                fill_shares = min(fill_shares, max(0.0, float(position["open_shares"] or 0.0)))
            if fill_shares <= 0:
                return None
            notional = round(fill_shares * price, 6)
            cursor = self.conn.execute(
                """
                INSERT INTO shadow_exec_fills (
                    order_id, shadow_position_id, shadow_intent_id, paper_position_id,
                    clob_token_id, action, price, shares, notional_usd,
                    liquidity_source, evidence_json, filled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(order_id),
                    shadow_position_id,
                    order["shadow_intent_id"],
                    order["paper_position_id"],
                    str(order["clob_token_id"] or ""),
                    str(order["order_action"] or ""),
                    price,
                    round(fill_shares, 6),
                    notional,
                    str(liquidity_source or "unknown"),
                    json.dumps(evidence or {}, sort_keys=True),
                    filled_at,
                ),
            )
            fill_id = int(cursor.lastrowid)
            filled_before = float(order["filled_shares"] or 0.0)
            notional_before = float(order["filled_notional_usd"] or 0.0)
            filled_total = round(filled_before + fill_shares, 6)
            notional_total = round(notional_before + notional, 6)
            unfilled = round(max(0.0, float(order["requested_shares"] or 0.0) - filled_total), 6)
            avg_fill_price = round(notional_total / filled_total, 6) if filled_total > 0 else None
            order_status = "filled" if unfilled <= 0.000001 else "partial_fill"
            self.conn.execute(
                """
                UPDATE shadow_exec_orders
                SET filled_shares = ?, avg_fill_price = ?, filled_notional_usd = ?,
                    unfilled_shares = ?, status = ?, status_reason = ?, last_checked_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    filled_total,
                    avg_fill_price,
                    notional_total,
                    unfilled,
                    order_status,
                    str(liquidity_source or "fill"),
                    filled_at,
                    filled_at,
                    int(order_id),
                ),
            )
            if str(order["intent_kind"] or "").lower() == "entry":
                shadow_position_id = self._apply_shadow_entry_fill_locked(order, int(order_id), fill_shares, notional, filled_at)
                self.conn.execute(
                    "UPDATE shadow_exec_fills SET shadow_position_id = ? WHERE id = ?",
                    (shadow_position_id, fill_id),
                )
                self.conn.execute(
                    "UPDATE shadow_exec_orders SET shadow_position_id = ? WHERE id = ?",
                    (shadow_position_id, int(order_id)),
                )
            else:
                self._apply_shadow_exit_fill_locked(shadow_position_id, int(order_id), fill_shares, notional, filled_at)
            self.conn.commit()
        return self.get_recent_shadow_exec_fills(limit=1)[0]

    def expire_shadow_exec_orders(self, *, now: str | None = None) -> int:
        now = str(now or iso_now())
        now_dt = _parse_iso_datetime(now) or datetime.now(timezone.utc)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM shadow_exec_orders WHERE status IN ('resting', 'partial_fill')"
            ).fetchall()
            expired = 0
            for row in rows:
                expires_at = _parse_iso_datetime(row["expires_at"])
                if expires_at is None or expires_at > now_dt:
                    continue
                self.conn.execute(
                    """
                    UPDATE shadow_exec_orders
                    SET status = 'expired', status_reason = ?, last_checked_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    ("Order TTL elapsed before remaining shares filled.", now, now, int(row["id"])),
                )
                if row["paper_position_id"] is not None:
                    self._sync_paper_position_with_shadow_locked(int(row["paper_position_id"]), synced_at=now)
                expired += 1
            self.conn.commit()
            return expired

    def mark_shadow_exec_position(
        self,
        position_id: int,
        *,
        mark_price: float,
        source: str = "mark",
        evidence: dict[str, Any] | None = None,
        marked_at: str | None = None,
    ) -> dict[str, Any] | None:
        marked_at = str(marked_at or iso_now())
        mark_price = _bounded_probability(mark_price) or 0.0
        with self._lock:
            payload = self._refresh_shadow_exec_position_metrics_locked(
                int(position_id),
                mark_price=mark_price,
                marked_at=marked_at,
                source=source,
                evidence=evidence,
            )
            self.conn.commit()
        return payload

    def get_recent_shadow_exec_orders(self, *, limit: int | None = 5000) -> list[dict[str, Any]]:
        params: list[Any] = []
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(max(1, int(limit)))
        with self._lock:
            rows = self.conn.execute(
                f"SELECT * FROM shadow_exec_orders ORDER BY created_at DESC, id DESC {limit_clause}",
                tuple(params),
            ).fetchall()
        return [_serialize_shadow_exec_order(row) for row in rows]

    def get_recent_shadow_exec_fills(self, *, limit: int | None = 5000) -> list[dict[str, Any]]:
        params: list[Any] = []
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(max(1, int(limit)))
        with self._lock:
            rows = self.conn.execute(
                f"SELECT * FROM shadow_exec_fills ORDER BY filled_at DESC, id DESC {limit_clause}",
                tuple(params),
            ).fetchall()
        return [_serialize_shadow_exec_fill(row) for row in rows]

    def get_shadow_exec_positions(self, *, limit: int | None = 5000, status: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if status:
            where = "WHERE status = ?"
            params.append(str(status))
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(max(1, int(limit)))
        with self._lock:
            rows = self.conn.execute(
                f"SELECT * FROM shadow_exec_positions {where} ORDER BY updated_at DESC, id DESC {limit_clause}",
                tuple(params),
            ).fetchall()
        return [_serialize_shadow_exec_position(row) for row in rows]

    def get_recent_shadow_exec_marks(self, *, limit: int | None = 5000) -> list[dict[str, Any]]:
        params: list[Any] = []
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(max(1, int(limit)))
        with self._lock:
            rows = self.conn.execute(
                f"SELECT * FROM shadow_exec_marks ORDER BY created_at DESC, id DESC {limit_clause}",
                tuple(params),
            ).fetchall()
        return [_serialize_shadow_exec_mark(row) for row in rows]

    def get_shadow_execution_missed_paper_trades(self, *, limit: int | None = 50) -> list[dict[str, Any]]:
        with self._lock:
            entry_rows = self.conn.execute(
                "SELECT DISTINCT paper_position_id FROM shadow_exec_orders WHERE intent_kind = 'entry' AND paper_position_id IS NOT NULL"
            ).fetchall()
            shadow_rows = self.conn.execute(
                "SELECT DISTINCT paper_position_id FROM shadow_exec_positions WHERE paper_position_id IS NOT NULL"
            ).fetchall()
            positions = self._query_dashboard_paper_positions(limit=None)
        entry_ids = {int(row["paper_position_id"]) for row in entry_rows if row["paper_position_id"] is not None}
        shadow_ids = {int(row["paper_position_id"]) for row in shadow_rows if row["paper_position_id"] is not None}
        missed: list[dict[str, Any]] = []
        for position in positions:
            position_id = int(position.get("id") or 0)
            if position_id not in entry_ids or position_id in shadow_ids:
                continue
            paper_pnl = _paper_position_current_pnl(position)
            missed.append(
                {
                    "paper_position_id": position_id,
                    "market_slug": position.get("market_slug"),
                    "event_title": position.get("event_title"),
                    "city_slug": position.get("city_slug"),
                    "event_date": position.get("event_date"),
                    "label": position.get("target_label"),
                    "direction": position.get("direction"),
                    "status": position.get("status"),
                    "paper_pnl": paper_pnl,
                    "paper_cost": position.get("cost"),
                    "created_at": position.get("created_at"),
                    "resolved_at": position.get("resolved_at"),
                }
            )
        missed.sort(key=lambda item: abs(float(item.get("paper_pnl") or 0.0)), reverse=True)
        if limit is not None:
            return missed[: max(0, int(limit))]
        return missed

    def get_shadow_execution_summary(self) -> dict[str, Any]:
        with self._lock:
            order_rows = self.conn.execute("SELECT * FROM shadow_exec_orders").fetchall()
            position_rows = self.conn.execute("SELECT * FROM shadow_exec_positions").fetchall()
            fill_rows = self.conn.execute("SELECT * FROM shadow_exec_fills").fetchall()
            last_order = self.conn.execute("SELECT MAX(created_at) AS value FROM shadow_exec_orders").fetchone()
            last_fill = self.conn.execute("SELECT MAX(filled_at) AS value FROM shadow_exec_fills").fetchone()
        orders = [_serialize_shadow_exec_order(row) for row in order_rows]
        positions = [_serialize_shadow_exec_position(row) for row in position_rows]
        fills = [_serialize_shadow_exec_fill(row) for row in fill_rows]
        paper_stats = self.get_paper_stats()
        missed = self.get_shadow_execution_missed_paper_trades(limit=None)
        total_pnl = round(sum(float(item.get("total_pnl") or 0.0) for item in positions), 6)
        realized_pnl = round(sum(float(item.get("realized_pnl") or 0.0) for item in positions), 6)
        unrealized_pnl = round(sum(float(item.get("unrealized_pnl") or 0.0) for item in positions), 6)
        open_exposure = round(sum(float(item.get("mark_value_usd") or 0.0) for item in positions if item.get("status") == "open"), 6)
        entry_orders = [item for item in orders if item.get("intent_kind") == "entry"]
        exit_orders = [item for item in orders if item.get("intent_kind") == "exit"]
        filled_entries = [item for item in entry_orders if float(item.get("filled_shares") or 0.0) > 0]
        unfilled_entries = [item for item in entry_orders if float(item.get("filled_shares") or 0.0) <= 0]
        filled_orders = [item for item in orders if float(item.get("filled_shares") or 0.0) > 0]
        paper_pnl = float(paper_stats.get("total_pnl") or 0.0)
        missed_paper_pnl = round(sum(float(item.get("paper_pnl") or 0.0) for item in missed), 6)
        taker_exit_estimated_pnl = round(
            sum(float(item.get("taker_exit_estimated_pnl") or 0.0) for item in positions),
            6,
        )
        return {
            "generated_at": iso_now(),
            "order_count": len(orders),
            "entry_order_count": len(entry_orders),
            "exit_order_count": len(exit_orders),
            "filled_order_count": len(filled_orders),
            "open_order_count": sum(1 for item in orders if item.get("status") in {"resting", "partial_fill"}),
            "expired_order_count": sum(1 for item in orders if item.get("status") == "expired"),
            "no_position_order_count": sum(1 for item in orders if item.get("status") == "no_position"),
            "position_count": len(positions),
            "open_position_count": sum(1 for item in positions if item.get("status") == "open"),
            "closed_position_count": sum(1 for item in positions if item.get("status") == "closed"),
            "fill_count": len(fills),
            "entry_fill_rate": round((len(filled_entries) / len(entry_orders) * 100.0), 2) if entry_orders else 0.0,
            "realistic_total_pnl": total_pnl,
            "realistic_realized_pnl": realized_pnl,
            "realistic_unrealized_pnl": unrealized_pnl,
            "taker_exit_estimated_pnl": taker_exit_estimated_pnl,
            "paper_total_pnl": round(paper_pnl, 6),
            "paper_signal_total_pnl": round(paper_pnl, 6),
            "scoreboard_pnl": total_pnl,
            "scoreboard_label": "Executable Shadow P/L",
            "paper_vs_realistic_gap": round(total_pnl - paper_pnl, 6),
            "signal_vs_realistic_gap": round(paper_pnl - total_pnl, 6),
            "missed_paper_pnl": missed_paper_pnl,
            "open_exposure": open_exposure,
            "realistic_entry_fill_count": len(filled_entries),
            "unfilled_entry_order_count": len(unfilled_entries),
            "last_order_at": last_order["value"] if last_order is not None else None,
            "last_fill_at": last_fill["value"] if last_fill is not None else None,
            "by_status": _count_by_key(orders, "status"),
        }

    def _apply_shadow_entry_fill_locked(
        self,
        order: sqlite3.Row,
        order_id: int,
        fill_shares: float,
        notional: float,
        filled_at: str,
    ) -> int:
        position = None
        if order["paper_position_id"] is not None:
            position = self.conn.execute(
                """
                SELECT *
                FROM shadow_exec_positions
                WHERE paper_position_id = ? AND status = 'open'
                ORDER BY opened_at ASC, id ASC
                LIMIT 1
                """,
                (order["paper_position_id"],),
            ).fetchone()
        if position is None:
            avg_entry_price = round(notional / fill_shares, 6) if fill_shares > 0 else None
            cursor = self.conn.execute(
                """
                INSERT INTO shadow_exec_positions (
                    paper_position_id, market_type, market_slug, event_slug, city_slug,
                    event_date, label, direction, clob_token_id, entry_order_id,
                    exit_order_id, status, total_entry_shares, total_entry_notional_usd,
                    avg_entry_price, open_shares, closed_shares, remaining_cost_basis_usd,
                    exit_notional_usd, avg_exit_price, realized_pnl, mark_price,
                    mark_value_usd, unrealized_pnl, total_pnl, taker_exit_estimated_pnl,
                    opened_at, closed_at, last_marked_at, updated_at, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'open', ?, ?, ?, ?, 0, ?, 0, NULL, 0, ?, ?, ?, ?, NULL, ?, NULL, ?, ?, ?)
                """,
                (
                    order["paper_position_id"],
                    str(order["market_type"] or ""),
                    str(order["market_slug"] or ""),
                    str(order["event_slug"] or ""),
                    str(order["city_slug"] or ""),
                    str(order["event_date"] or ""),
                    str(order["label"] or ""),
                    str(order["direction"] or ""),
                    str(order["clob_token_id"] or ""),
                    int(order_id),
                    round(fill_shares, 6),
                    round(notional, 6),
                    avg_entry_price,
                    round(fill_shares, 6),
                    round(notional, 6),
                    avg_entry_price,
                    round(fill_shares * (avg_entry_price or 0.0), 6),
                    0.0,
                    0.0,
                    filled_at,
                    filled_at,
                    filled_at,
                    json.dumps({"entry_order_id": int(order_id)}, sort_keys=True),
                ),
            )
            position_id = int(cursor.lastrowid)
        else:
            position_id = int(position["id"])
            total_shares = round(float(position["total_entry_shares"] or 0.0) + fill_shares, 6)
            total_notional = round(float(position["total_entry_notional_usd"] or 0.0) + notional, 6)
            open_shares = round(float(position["open_shares"] or 0.0) + fill_shares, 6)
            remaining_cost = round(float(position["remaining_cost_basis_usd"] or 0.0) + notional, 6)
            avg_entry_price = round(total_notional / total_shares, 6) if total_shares > 0 else None
            self.conn.execute(
                """
                UPDATE shadow_exec_positions
                SET total_entry_shares = ?, total_entry_notional_usd = ?, avg_entry_price = ?,
                    open_shares = ?, remaining_cost_basis_usd = ?, updated_at = ?
                WHERE id = ?
                """,
                (total_shares, total_notional, avg_entry_price, open_shares, remaining_cost, filled_at, position_id),
            )
        self._refresh_shadow_exec_position_metrics_locked(position_id, marked_at=filled_at, source="entry_fill")
        return position_id

    def _apply_shadow_exit_fill_locked(
        self,
        position_id: int,
        order_id: int,
        fill_shares: float,
        notional: float,
        filled_at: str,
    ) -> None:
        position = self.conn.execute("SELECT * FROM shadow_exec_positions WHERE id = ?", (int(position_id),)).fetchone()
        if position is None:
            return
        open_before = max(0.0, float(position["open_shares"] or 0.0))
        if open_before <= 0:
            return
        close_shares = min(open_before, fill_shares)
        cost_before = max(0.0, float(position["remaining_cost_basis_usd"] or 0.0))
        cost_reduction = round(cost_before * (close_shares / open_before), 6) if open_before > 0 else 0.0
        realized_delta = round(notional - cost_reduction, 6)
        open_after = round(max(0.0, open_before - close_shares), 6)
        closed_shares = round(float(position["closed_shares"] or 0.0) + close_shares, 6)
        remaining_cost = round(max(0.0, cost_before - cost_reduction), 6)
        exit_notional = round(float(position["exit_notional_usd"] or 0.0) + notional, 6)
        realized_pnl = round(float(position["realized_pnl"] or 0.0) + realized_delta, 6)
        avg_exit = round(exit_notional / closed_shares, 6) if closed_shares > 0 else None
        status = "closed" if open_after <= 0.000001 else "open"
        closed_at = filled_at if status == "closed" else position["closed_at"]
        self.conn.execute(
            """
            UPDATE shadow_exec_positions
            SET exit_order_id = ?, status = ?, open_shares = ?, closed_shares = ?,
                remaining_cost_basis_usd = ?, exit_notional_usd = ?, avg_exit_price = ?,
                realized_pnl = ?, closed_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                int(order_id),
                status,
                open_after,
                closed_shares,
                remaining_cost,
                exit_notional,
                avg_exit,
                realized_pnl,
                closed_at,
                filled_at,
                int(position_id),
            ),
        )
        self._refresh_shadow_exec_position_metrics_locked(position_id, marked_at=filled_at, source="exit_fill")

    def _refresh_shadow_exec_position_metrics_locked(
        self,
        position_id: int,
        *,
        mark_price: float | None = None,
        marked_at: str | None = None,
        source: str = "mark",
        evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        marked_at = str(marked_at or iso_now())
        row = self.conn.execute("SELECT * FROM shadow_exec_positions WHERE id = ?", (int(position_id),)).fetchone()
        if row is None:
            return None
        effective_mark = _bounded_probability(mark_price)
        if effective_mark is None:
            effective_mark = _bounded_probability(row["mark_price"]) or _bounded_probability(row["avg_entry_price"]) or 0.0
        open_shares = max(0.0, float(row["open_shares"] or 0.0))
        remaining_cost = max(0.0, float(row["remaining_cost_basis_usd"] or 0.0))
        realized_pnl = float(row["realized_pnl"] or 0.0)
        mark_value = round(open_shares * effective_mark, 6)
        unrealized_pnl = round(mark_value - remaining_cost, 6)
        if str(row["status"] or "") == "closed" or open_shares <= 0.000001:
            mark_value = 0.0
            unrealized_pnl = 0.0
        total_pnl = round(realized_pnl + unrealized_pnl, 6)
        self.conn.execute(
            """
            UPDATE shadow_exec_positions
            SET mark_price = ?, mark_value_usd = ?, unrealized_pnl = ?, total_pnl = ?,
                last_marked_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (effective_mark, mark_value, unrealized_pnl, total_pnl, marked_at, marked_at, int(position_id)),
        )
        if source not in {"entry_fill", "exit_fill"}:
            self.conn.execute(
                """
                INSERT INTO shadow_exec_marks (
                    shadow_position_id, mark_price, mark_value_usd, unrealized_pnl,
                    total_pnl, source, evidence_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(position_id),
                    effective_mark,
                    mark_value,
                    unrealized_pnl,
                    total_pnl,
                    str(source or "mark"),
                    json.dumps(evidence or {}, sort_keys=True),
                    marked_at,
                ),
            )
        if row["paper_position_id"] is not None:
            self._sync_paper_position_with_shadow_locked(int(row["paper_position_id"]), synced_at=marked_at)
        return {
            "id": int(position_id),
            "mark_price": effective_mark,
            "mark_value_usd": mark_value,
            "unrealized_pnl": unrealized_pnl,
            "total_pnl": total_pnl,
        }

    def get_same_day_risk_tracking(self, *, limit: int | None = 5000) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT
                    d.id AS decision_id,
                    d.signal_id AS signal_id,
                    d.signal_key AS signal_key,
                    d.accepted AS accepted,
                    d.reason AS reason,
                    d.reason_code AS reason_code,
                    d.final_score AS final_score,
                    d.policy_action AS policy_action,
                    d.metadata_json AS metadata_json,
                    d.created_at AS decision_created_at,
                    s.market_type AS market_type,
                    s.event_title AS event_title,
                    s.market_slug AS market_slug,
                    s.event_slug AS event_slug,
                    s.city_slug AS city_slug,
                    s.event_date AS event_date,
                    s.label AS label,
                    s.direction AS direction,
                    s.market_prob AS market_prob,
                    s.forecast_prob AS forecast_prob,
                    s.edge AS edge,
                    s.edge_abs AS edge_abs,
                    s.edge_size AS edge_size,
                    s.confidence AS confidence,
                    s.source_count AS source_count,
                    s.liquidity AS liquidity,
                    s.time_to_resolution_s AS time_to_resolution_s,
                    s.source_dispersion_pct AS source_dispersion_pct,
                    s.score AS signal_score,
                    s.created_at AS signal_created_at
                FROM decisions d
                LEFT JOIN signals s ON s.id = d.signal_id
                ORDER BY d.created_at DESC, d.id DESC
                """
            ).fetchall()
        items: list[dict[str, Any]] = []
        max_items = None if limit is None else max(0, int(limit))
        for row in rows:
            item = _serialize_same_day_risk_tracking_row(row)
            if not (item["same_day_temperature_entry"] or item["same_day_low_edge_blocked"]):
                continue
            items.append(item)
            if max_items is not None and len(items) >= max_items:
                break
        return items

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
        entry_context_override: dict[str, float | None] | None = None,
    ) -> PaperPosition | None:
        entry_context = _paper_entry_context(signal, entry_slippage_bps)
        if entry_context_override is not None:
            entry_context = _paper_entry_context_with_override(signal, entry_context_override)
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
        reason_code: str | None = None,
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
            updated = bool(cursor.rowcount)
            if updated:
                self._record_position_review_snapshot(
                    int(position_id),
                    reviewed_at=reviewed_at,
                    event_kind="review",
                    reason=reason,
                    reason_code=reason_code,
                )
            self.conn.commit()
            return updated

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
        reason_code: str | None = None,
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
            self._record_position_review_snapshot(
                int(position_id),
                reviewed_at=closed_at,
                event_kind="close",
                reason=str(mark_reason or reason or ""),
                reason_code=reason_code,
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

    def sync_paper_position_with_shadow(
        self,
        position_id: int,
        *,
        synced_at: str | None = None,
    ) -> dict[str, Any] | None:
        synced_at = str(synced_at or iso_now())
        with self._lock:
            payload = self._sync_paper_position_with_shadow_locked(int(position_id), synced_at=synced_at)
            self.conn.commit()
        return payload

    def _sync_paper_position_with_shadow_locked(
        self,
        position_id: int,
        *,
        synced_at: str,
    ) -> dict[str, Any] | None:
        paper = self.conn.execute("SELECT * FROM paper_positions WHERE id = ?", (int(position_id),)).fetchone()
        if paper is None:
            return None
        shadow_position = self.conn.execute(
            """
            SELECT *
            FROM shadow_exec_positions
            WHERE paper_position_id = ?
            ORDER BY CASE WHEN status = 'open' THEN 0 ELSE 1 END,
                     COALESCE(updated_at, last_marked_at, closed_at, opened_at) DESC,
                     id DESC
            LIMIT 1
            """,
            (int(position_id),),
        ).fetchone()
        entry_order = self.conn.execute(
            """
            SELECT *
            FROM shadow_exec_orders
            WHERE paper_position_id = ? AND intent_kind = 'entry'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (int(position_id),),
        ).fetchone()
        current_status = str(paper["status"] or "").lower()
        old_locked_cost = max(0.0, float(paper["cost"] or 0.0)) if current_status in {"open", "pending"} else 0.0
        old_realized = float(paper["realized_pnl"] or 0.0)
        initial, available = self.get_paper_capital()

        next_status = current_status or "open"
        shares = float(paper["shares"] or 0.0)
        cost = max(0.0, float(paper["cost"] or 0.0))
        entry_price = _bounded_probability(paper["entry_price"])
        mark_price = _bounded_probability(paper["mark_price"])
        mark_updated_at = str(paper["mark_updated_at"] or "") or None
        realized_pnl = old_realized
        exit_price = _bounded_probability(paper["exit_price"])
        exit_reference_price = _bounded_probability(paper["exit_reference_price"])
        gross_exit_payout = _as_float(paper["gross_exit_payout"])
        net_exit_payout = _as_float(paper["net_exit_payout"])
        exit_reason = str(paper["exit_reason"] or "") or None
        resolved_at = str(paper["resolved_at"] or "") or None
        new_locked_cost = old_locked_cost

        if shadow_position is not None:
            shadow_status = str(shadow_position["status"] or "").lower()
            open_shares = max(0.0, float(shadow_position["open_shares"] or 0.0))
            total_entry_shares = max(0.0, float(shadow_position["total_entry_shares"] or 0.0))
            total_entry_notional = max(0.0, float(shadow_position["total_entry_notional_usd"] or 0.0))
            remaining_cost = max(0.0, float(shadow_position["remaining_cost_basis_usd"] or 0.0))
            entry_price = _bounded_probability(shadow_position["avg_entry_price"]) or entry_price
            mark_price = _bounded_probability(shadow_position["mark_price"]) or mark_price
            mark_updated_at = str(shadow_position["last_marked_at"] or mark_updated_at or synced_at)
            realized_pnl = round(float(shadow_position["realized_pnl"] or 0.0), 6)
            avg_exit_price = _bounded_probability(shadow_position["avg_exit_price"])
            exit_notional = round(float(shadow_position["exit_notional_usd"] or 0.0), 6)
            if avg_exit_price is not None:
                exit_price = avg_exit_price
                exit_reference_price = avg_exit_price
            if exit_notional > 0:
                gross_exit_payout = exit_notional
                net_exit_payout = exit_notional
            if shadow_status == "closed" or open_shares <= 0.000001:
                next_status = "closed"
                shares = total_entry_shares
                cost = total_entry_notional
                new_locked_cost = 0.0
                resolved_at = str(shadow_position["closed_at"] or synced_at)
                exit_reason = exit_reason or "shadow_exec_exit_fill"
            else:
                next_status = "open"
                shares = open_shares
                cost = remaining_cost
                new_locked_cost = remaining_cost
                resolved_at = None
        else:
            entry_status = str((entry_order["status"] if entry_order is not None else "") or "").lower()
            entry_filled_shares = max(0.0, float((entry_order["filled_shares"] if entry_order is not None else 0.0) or 0.0))
            if entry_order is not None and entry_filled_shares <= 0.000001:
                if entry_status in {"resting", "partial_fill"}:
                    next_status = "pending"
                    shares = 0.0
                    cost = 0.0
                    realized_pnl = 0.0
                    new_locked_cost = 0.0
                    exit_price = None
                    exit_reference_price = None
                    gross_exit_payout = None
                    net_exit_payout = None
                    exit_reason = None
                    resolved_at = None
                    mark_updated_at = synced_at
                elif entry_status:
                    next_status = "closed"
                    shares = 0.0
                    cost = 0.0
                    realized_pnl = 0.0
                    new_locked_cost = 0.0
                    exit_price = None
                    exit_reference_price = None
                    gross_exit_payout = 0.0
                    net_exit_payout = 0.0
                    exit_reason = "shadow_entry_no_fill"
                    resolved_at = synced_at
                    mark_updated_at = synced_at

        self.conn.execute(
            """
            UPDATE paper_positions
            SET status = ?,
                entry_price = COALESCE(?, entry_price),
                shares = ?,
                cost = ?,
                realized_pnl = ?,
                mark_price = ?,
                mark_updated_at = ?,
                exit_price = ?,
                exit_reference_price = ?,
                gross_exit_payout = ?,
                net_exit_payout = ?,
                exit_reason = ?,
                resolved_at = ?
            WHERE id = ?
            """,
            (
                next_status,
                entry_price,
                round(max(0.0, shares), 6),
                round(max(0.0, cost), 6),
                round(realized_pnl, 6),
                mark_price,
                mark_updated_at,
                exit_price,
                exit_reference_price,
                gross_exit_payout,
                net_exit_payout,
                exit_reason,
                resolved_at,
                int(position_id),
            ),
        )
        capital_delta = round((old_locked_cost - new_locked_cost) + (realized_pnl - old_realized), 6)
        if abs(capital_delta) > 0.0000005:
            self.set_setting(
                "paper_capital",
                {"initial": initial, "available": round(available + capital_delta, 6)},
            )
        return {
            "id": int(position_id),
            "status": next_status,
            "shares": round(max(0.0, shares), 6),
            "cost": round(max(0.0, cost), 6),
            "realized_pnl": round(realized_pnl, 6),
            "exit_reason": exit_reason,
            "resolved_at": resolved_at,
        }

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

    def get_position_review_history(
        self,
        limit: int | None = 1000,
        *,
        position_id: int | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if position_id is not None:
            where = "WHERE position_id = ?"
            params.append(int(position_id))
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(int(limit))
        with self._lock:
            rows = self.conn.execute(
                f"""
                SELECT *
                FROM paper_position_reviews
                {where}
                ORDER BY reviewed_at DESC, id DESC
                {limit_clause}
                """,
                tuple(params),
            ).fetchall()
        return [_deserialize_position_review_row(row) for row in rows]

    def get_position_review_count(self, *, position_id: int | None = None) -> int:
        params: list[Any] = []
        where = ""
        if position_id is not None:
            where = "WHERE position_id = ?"
            params.append(int(position_id))
        with self._lock:
            row = self.conn.execute(
                f"SELECT COUNT(*) AS count FROM paper_position_reviews {where}",
                tuple(params),
            ).fetchone()
        return int(row["count"] or 0) if row is not None else 0

    def get_dashboard_paper_positions(
        self,
        limit: int = 20,
        *,
        status: str | None = None,
        statuses: list[str] | tuple[str, ...] | set[str] | None = None,
        mark_stale_after_seconds: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            items = self._query_dashboard_paper_positions(limit=limit, status=status, statuses=statuses)
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

    def get_pnl_analytics(self, *, timezone_name: str | None = None) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        analytics_tz = timezone.utc
        if timezone_name:
            try:
                analytics_tz = ZoneInfo(str(timezone_name))
            except ZoneInfoNotFoundError:
                analytics_tz = timezone.utc
        today_start_local = now.astimezone(analytics_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        today_cutoff = today_start_local.astimezone(timezone.utc)
        windows = (
            ("24h", "Last 24 Hours", timedelta(hours=24)),
            ("7d", "Last 7 Days", timedelta(days=7)),
            ("30d", "Last 30 Days", timedelta(days=30)),
        )
        with self._lock:
            closed_positions = self._query_dashboard_paper_positions(limit=None, statuses=("closed", "resolved"))
            open_positions = self._query_dashboard_paper_positions(limit=500, status="open")
        payload = {
            "generated_at": now.isoformat(),
            "open_book": _summarize_open_book(open_positions),
            "windows": {
                "today": _build_pnl_window_payload(
                    "Today",
                    [
                        item
                        for item in closed_positions
                        if (_closed_trade_timestamp(item) or datetime.min.replace(tzinfo=timezone.utc)) >= today_cutoff
                    ],
                )
            },
        }
        for key, label, span in windows:
            cutoff = now - span
            window_positions = [
                item for item in closed_positions if (_closed_trade_timestamp(item) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
            ]
            payload["windows"][key] = _build_pnl_window_payload(label, window_positions)
        return payload

    def get_paper_vs_shadow_daily_summary(
        self,
        *,
        start: str | None = None,
        end: str | None = None,
        timezone_name: str | None = None,
        weekend_only: bool = False,
        limit_days: int = 14,
    ) -> dict[str, Any]:
        analytics_tz = _resolve_analytics_timezone(timezone_name)
        local_now = datetime.now(analytics_tz)
        start_date, end_date = _resolve_summary_window(
            start=start,
            end=end,
            now_date=local_now.date(),
            weekend_only=weekend_only,
            limit_days=limit_days,
        )
        window_start_local = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=analytics_tz)
        window_end_local = datetime.combine(end_date + timedelta(days=1), datetime.min.time()).replace(tzinfo=analytics_tz)
        window_start_utc = window_start_local.astimezone(timezone.utc)
        window_end_utc = window_end_local.astimezone(timezone.utc)
        window_start = window_start_utc.isoformat()
        window_end = window_end_utc.isoformat()
        with self._lock:
            paper_rows = self.conn.execute(
                """
                SELECT
                    id,
                    market_type,
                    city_slug,
                    label,
                    direction,
                    realized_pnl,
                    cost,
                    exit_reason,
                    COALESCE(resolved_at, created_at) AS event_at
                FROM paper_positions
                WHERE status IN ('resolved', 'closed')
                  AND COALESCE(resolved_at, created_at) >= ?
                  AND COALESCE(resolved_at, created_at) < ?
                ORDER BY event_at
                """,
                (window_start, window_end),
            ).fetchall()
            signal_rows = self.conn.execute(
                """
                SELECT market_type, created_at
                FROM signals
                WHERE created_at >= ? AND created_at < ?
                ORDER BY created_at
                """,
                (window_start, window_end),
            ).fetchall()
            decision_rows = self.conn.execute(
                """
                SELECT accepted, reason_code, policy_action, created_at
                FROM decisions
                WHERE created_at >= ? AND created_at < ?
                ORDER BY created_at
                """,
                (window_start, window_end),
            ).fetchall()
            shadow_position_rows = self.conn.execute(
                """
                SELECT
                    id,
                    market_type,
                    city_slug,
                    label,
                    direction,
                    realized_pnl,
                    COALESCE(closed_at, opened_at) AS event_at,
                    open_shares,
                    mark_value_usd,
                    status
                FROM shadow_exec_positions
                WHERE status IN ('closed', 'resolved')
                  AND COALESCE(closed_at, opened_at) >= ?
                  AND COALESCE(closed_at, opened_at) < ?
                ORDER BY event_at
                """,
                (window_start, window_end),
            ).fetchall()
            shadow_intent_rows = self.conn.execute(
                """
                SELECT
                    intent_kind,
                    simulated_fill_status,
                    created_at
                FROM shadow_order_intents
                WHERE created_at >= ? AND created_at < ?
                ORDER BY created_at
                """,
                (window_start, window_end),
            ).fetchall()
            open_shadow_positions = self.conn.execute(
                "SELECT COUNT(*) AS value, COALESCE(SUM(mark_value_usd), 0.0) AS open_exposure FROM shadow_exec_positions WHERE status = 'open'"
            ).fetchone()
            paper_open_positions = self._query_dashboard_paper_positions(limit=500, status="open")
        paper_rows_by_day = _group_rows_by_local_date(paper_rows, timestamp_key="event_at", timezone=analytics_tz)
        shadow_rows_by_day = _group_rows_by_local_date(shadow_position_rows, timestamp_key="event_at", timezone=analytics_tz)
        signal_rows_by_day = _group_rows_by_local_date(signal_rows, timestamp_key="created_at", timezone=analytics_tz)
        decision_rows_by_day = _group_rows_by_local_date(decision_rows, timestamp_key="created_at", timezone=analytics_tz)
        shadow_intent_rows_by_day = _group_rows_by_local_date(shadow_intent_rows, timestamp_key="created_at", timezone=analytics_tz)
        overall_decision_summary = _summarize_decision_rows([dict(item) for item in decision_rows])
        day_count = max(0, (end_date - start_date).days) + 1
        days = [str(start_date + timedelta(days=index)) for index in range(day_count)]
        per_day: list[dict[str, Any]] = []
        totals = {
            "paper_realized_pnl": 0.0,
            "paper_closed_trades": 0,
            "paper_signals": len(signal_rows),
            "paper_decisions": int(overall_decision_summary["decision_count"]),
            "paper_accepted_decisions": int(overall_decision_summary["accepted_count"]),
            "paper_rejected_decisions": int(overall_decision_summary["rejected_count"]),
            "paper_accept_rate": float(overall_decision_summary["accept_rate"]),
            "paper_accepted_by_policy_action": dict(overall_decision_summary["accepted_by_policy_action"]),
            "paper_rejected_by_policy_action": dict(overall_decision_summary["rejected_by_policy_action"]),
            "paper_rejected_reason_codes": dict(overall_decision_summary["rejected_reason_codes"]),
            "paper_top_rejected_reason_codes": _ranked_count_items(
                overall_decision_summary["rejected_reason_codes"],
                limit=8,
            ),
            "shadow_realized_pnl": 0.0,
            "shadow_closed_positions": 0,
            "shadow_entry_intents": 0,
            "shadow_filled_intents": 0,
        }
        for day in days:
            papers = paper_rows_by_day.get(day, [])
            shadows = shadow_rows_by_day.get(day, [])
            signals = signal_rows_by_day.get(day, [])
            decisions = decision_rows_by_day.get(day, [])
            intents = shadow_intent_rows_by_day.get(day, [])
            decision_summary = _summarize_decision_rows(decisions)
            paper_realized_rows = [float(item.get("realized_pnl") or 0.0) for item in papers]
            paper_realized_pnl = round(sum(paper_realized_rows), 6)
            paper_wins = sum(1 for value in paper_realized_rows if value > 0)
            paper_losses = sum(1 for value in paper_realized_rows if value < 0)
            paper_closed = len(paper_realized_rows)
            paper_best = round(max(paper_realized_rows), 6) if paper_realized_rows else 0.0
            paper_worst = round(min(paper_realized_rows), 6) if paper_realized_rows else 0.0
            shadow_realized_rows = [float(item.get("realized_pnl") or 0.0) for item in shadows]
            shadow_realized_pnl = round(sum(shadow_realized_rows), 6)
            shadow_closed = len(shadow_realized_rows)
            shadow_closed_exposure = round(sum(float(item.get("mark_value_usd") or 0.0) for item in shadows if str(item.get("status") or "") == "closed"), 6)
            totals["paper_realized_pnl"] += paper_realized_pnl
            totals["paper_closed_trades"] += paper_closed
            totals["shadow_realized_pnl"] += shadow_realized_pnl
            totals["shadow_closed_positions"] += shadow_closed
            totals["shadow_entry_intents"] += sum(1 for item in intents if str(item.get("intent_kind") or "") == "entry")
            totals["shadow_filled_intents"] += sum(
                1 for item in intents if str(item.get("simulated_fill_status") or "") in {"partial_fill", "full_fill"}
            )
            day_summary = {
                "date": day,
                "weekday": datetime.fromisoformat(f"{day}T00:00:00").strftime("%A"),
                "paper": {
                    "closed_trades": paper_closed,
                    "realized_pnl": paper_realized_pnl,
                    "wins": paper_wins,
                    "losses": paper_losses,
                    "win_rate": round((paper_wins / paper_closed * 100.0), 2) if paper_closed else 0.0,
                    "best_trade_pnl": paper_best,
                    "worst_trade_pnl": paper_worst,
                    "signals": len(signals),
                    "decisions": int(decision_summary["decision_count"]),
                    "accepted_decisions": int(decision_summary["accepted_count"]),
                    "rejected_decisions": int(decision_summary["rejected_count"]),
                    "decision_accept_rate": float(decision_summary["accept_rate"]),
                    "accepted_by_policy_action": dict(decision_summary["accepted_by_policy_action"]),
                    "rejected_by_policy_action": dict(decision_summary["rejected_by_policy_action"]),
                    "rejected_reason_codes": dict(decision_summary["rejected_reason_codes"]),
                    "top_rejected_reason_codes": _ranked_count_items(
                        decision_summary["rejected_reason_codes"],
                        limit=5,
                    ),
                    "by_market": _count_by_key([dict(item) for item in papers], "market_type"),
                    "by_direction": _count_by_key([dict(item) for item in papers], "direction"),
                    "by_city": _count_by_key([dict(item) for item in papers], "city_slug"),
                    "by_exit_reason": _count_by_key([dict(item) for item in papers], "exit_reason"),
                },
                "shadow": {
                    "closed_positions": shadow_closed,
                    "realized_pnl": shadow_realized_pnl,
                    "closed_exposure": shadow_closed_exposure,
                    "entry_intents": sum(
                        1 for item in intents if str(item.get("intent_kind") or "") == "entry"
                    ),
                    "exit_intents": sum(
                        1 for item in intents if str(item.get("intent_kind") or "") == "exit"
                    ),
                    "intents_full_fill": sum(
                        1 for item in intents if str(item.get("simulated_fill_status") or "") == "full_fill"
                    ),
                    "intents_partial_fill": sum(
                        1 for item in intents if str(item.get("simulated_fill_status") or "") == "partial_fill"
                    ),
                    "intents_no_fill": sum(
                        1 for item in intents if str(item.get("simulated_fill_status") or "") == "no_fill"
                    ),
                    "by_market": _count_by_key([dict(item) for item in shadows], "market_type"),
                    "by_direction": _count_by_key([dict(item) for item in shadows], "direction"),
                    "by_city": _count_by_key([dict(item) for item in shadows], "city_slug"),
                    "active_open_exposure_usd": round(float(open_shadow_positions["open_exposure"] or 0.0), 6) if open_shadow_positions else 0.0,
                    "active_open_positions": int(open_shadow_positions["value"] or 0) if open_shadow_positions else 0,
                },
                "gap_real": {
                    "paper_vs_shadow_realized": round(paper_realized_pnl - shadow_realized_pnl, 6),
                    "paper_signals_minus_shadows": len(signals) - len(shadows),
                },
            }
            per_day.append(day_summary)
        totals["paper_open_positions"] = len(paper_open_positions)
        totals["paper_open_exposure"] = round(
            sum(float(item.get("net_liquidation_value") or 0.0) for item in paper_open_positions),
            6,
        )
        totals["shadow_open_positions"] = int(open_shadow_positions["value"] or 0) if open_shadow_positions else 0
        totals["shadow_open_exposure"] = round(float(open_shadow_positions["open_exposure"] or 0.0), 6) if open_shadow_positions else 0.0
        totals["paper_realized_pnl"] = round(float(totals["paper_realized_pnl"]), 6)
        totals["shadow_realized_pnl"] = round(float(totals["shadow_realized_pnl"]), 6)
        totals["paper_vs_shadow_realized_total"] = round(
            float(totals["paper_realized_pnl"]) - float(totals["shadow_realized_pnl"]),
            6,
        )
        return {
            "generated_at": iso_now(),
            "timezone": str(analytics_tz),
            "weekend_only": bool(weekend_only),
            "window": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "start_utc": window_start_utc.isoformat(),
                "end_utc": window_end_utc.isoformat(),
            },
            "days": per_day,
            "totals": totals,
        }

    def get_decision_activity_summary(
        self,
        *,
        hours: int = 12,
        timezone_name: str | None = None,
    ) -> dict[str, Any]:
        analytics_tz = _resolve_analytics_timezone(timezone_name)
        lookback_hours = max(1, int(hours or 12))
        window_end_utc = datetime.now(timezone.utc)
        window_start_utc = window_end_utc - timedelta(hours=lookback_hours)
        window_start = window_start_utc.isoformat()
        with self._lock:
            signal_rows = self.conn.execute(
                """
                SELECT market_type, city_slug, direction, created_at
                FROM signals
                WHERE created_at >= ?
                ORDER BY created_at
                """,
                (window_start,),
            ).fetchall()
            decision_rows = self.conn.execute(
                """
                SELECT accepted, reason_code, policy_action, created_at
                FROM decisions
                WHERE created_at >= ?
                ORDER BY created_at
                """,
                (window_start,),
            ).fetchall()
            paper_opened_rows = self.conn.execute(
                """
                SELECT market_type, city_slug, direction, created_at
                FROM paper_positions
                WHERE created_at >= ?
                ORDER BY created_at
                """,
                (window_start,),
            ).fetchall()
            paper_closed_rows = self.conn.execute(
                """
                SELECT market_type, city_slug, direction, status, exit_reason, realized_pnl,
                       COALESCE(resolved_at, created_at) AS event_at
                FROM paper_positions
                WHERE status IN ('closed', 'resolved')
                  AND COALESCE(resolved_at, created_at) >= ?
                ORDER BY event_at
                """,
                (window_start,),
            ).fetchall()
            shadow_intent_rows = self.conn.execute(
                """
                SELECT intent_kind, execution_mode, simulated_fill_status, reason_code, created_at
                FROM shadow_order_intents
                WHERE created_at >= ?
                ORDER BY created_at
                """,
                (window_start,),
            ).fetchall()
            shadow_order_rows = self.conn.execute(
                """
                SELECT intent_kind, status, created_at
                FROM shadow_exec_orders
                WHERE created_at >= ?
                ORDER BY created_at
                """,
                (window_start,),
            ).fetchall()
            shadow_fill_rows = self.conn.execute(
                """
                SELECT action, filled_at
                FROM shadow_exec_fills
                WHERE filled_at >= ?
                ORDER BY filled_at
                """,
                (window_start,),
            ).fetchall()
            shadow_opened_rows = self.conn.execute(
                """
                SELECT market_type, direction, opened_at
                FROM shadow_exec_positions
                WHERE opened_at >= ?
                ORDER BY opened_at
                """,
                (window_start,),
            ).fetchall()
            shadow_closed_rows = self.conn.execute(
                """
                SELECT market_type, direction, status, realized_pnl,
                       COALESCE(closed_at, updated_at, opened_at) AS event_at
                FROM shadow_exec_positions
                WHERE status IN ('closed', 'resolved')
                  AND COALESCE(closed_at, updated_at, opened_at) >= ?
                ORDER BY event_at
                """,
                (window_start,),
            ).fetchall()
        decision_summary = _summarize_decision_rows([dict(item) for item in decision_rows])
        paper_closed_realized_pnl = round(sum(float(item["realized_pnl"] or 0.0) for item in paper_closed_rows), 6)
        shadow_closed_realized_pnl = round(sum(float(item["realized_pnl"] or 0.0) for item in shadow_closed_rows), 6)
        entry_intents = [dict(item) for item in shadow_intent_rows if str(item["intent_kind"] or "") == "entry"]
        return {
            "generated_at": iso_now(),
            "timezone": str(analytics_tz),
            "lookback_hours": lookback_hours,
            "window": {
                "start_utc": window_start_utc.isoformat(),
                "end_utc": window_end_utc.isoformat(),
                "start_local": window_start_utc.astimezone(analytics_tz).isoformat(),
                "end_local": window_end_utc.astimezone(analytics_tz).isoformat(),
            },
            "signals": {
                "count": len(signal_rows),
                "by_market": _count_by_key([dict(item) for item in signal_rows], "market_type"),
                "by_direction": _count_by_key([dict(item) for item in signal_rows], "direction"),
                "by_city": _count_by_key([dict(item) for item in signal_rows], "city_slug"),
                "last_at": str(signal_rows[-1]["created_at"]) if signal_rows else None,
            },
            "decisions": {
                "count": int(decision_summary["decision_count"]),
                "accepted_count": int(decision_summary["accepted_count"]),
                "rejected_count": int(decision_summary["rejected_count"]),
                "accept_rate": float(decision_summary["accept_rate"]),
                "accepted_by_policy_action": dict(decision_summary["accepted_by_policy_action"]),
                "rejected_by_policy_action": dict(decision_summary["rejected_by_policy_action"]),
                "rejected_reason_codes": dict(decision_summary["rejected_reason_codes"]),
                "top_rejected_reason_codes": _ranked_count_items(
                    decision_summary["rejected_reason_codes"],
                    limit=8,
                ),
                "last_at": str(decision_rows[-1]["created_at"]) if decision_rows else None,
            },
            "paper": {
                "opened_positions": len(paper_opened_rows),
                "opened_by_market": _count_by_key([dict(item) for item in paper_opened_rows], "market_type"),
                "closed_positions": len(paper_closed_rows),
                "closed_by_exit_reason": _count_by_key([dict(item) for item in paper_closed_rows], "exit_reason"),
                "closed_realized_pnl": paper_closed_realized_pnl,
                "last_opened_at": str(paper_opened_rows[-1]["created_at"]) if paper_opened_rows else None,
                "last_closed_at": str(paper_closed_rows[-1]["event_at"]) if paper_closed_rows else None,
            },
            "shadow": {
                "entry_intents": len(entry_intents),
                "exit_intents": sum(1 for item in shadow_intent_rows if str(item["intent_kind"] or "") == "exit"),
                "entry_fillable_intents": sum(
                    1
                    for item in entry_intents
                    if str(item.get("simulated_fill_status") or "") in {"partial_fill", "full_fill"}
                ),
                "entry_no_fill_intents": sum(
                    1 for item in entry_intents if str(item.get("simulated_fill_status") or "") == "no_fill"
                ),
                "entry_reason_codes": _count_reason_codes(entry_intents, key="reason_code"),
                "orders_created": len(shadow_order_rows),
                "fills": len(shadow_fill_rows),
                "opened_positions": len(shadow_opened_rows),
                "closed_positions": len(shadow_closed_rows),
                "closed_realized_pnl": shadow_closed_realized_pnl,
                "last_intent_at": str(shadow_intent_rows[-1]["created_at"]) if shadow_intent_rows else None,
                "last_order_at": str(shadow_order_rows[-1]["created_at"]) if shadow_order_rows else None,
                "last_fill_at": str(shadow_fill_rows[-1]["filled_at"]) if shadow_fill_rows else None,
            },
            "idle_flags": {
                "no_signals": len(signal_rows) == 0,
                "no_accepted_decisions": int(decision_summary["accepted_count"]) == 0,
                "no_paper_entries": len(paper_opened_rows) == 0,
                "no_shadow_entries": len(entry_intents) == 0,
            },
        }

    def get_shadow_order_summary(self) -> dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN intent_kind = 'entry' THEN 1 ELSE 0 END) AS entry_count,
                    SUM(CASE WHEN intent_kind = 'exit' THEN 1 ELSE 0 END) AS exit_count,
                    SUM(CASE WHEN simulated_fill_status = 'full_fill' THEN 1 ELSE 0 END) AS full_fill_count,
                    SUM(CASE WHEN simulated_fill_status = 'partial_fill' THEN 1 ELSE 0 END) AS partial_fill_count,
                    SUM(CASE WHEN simulated_fill_status = 'no_fill' THEN 1 ELSE 0 END) AS no_fill_count,
                    SUM(CASE WHEN simulated_fill_status IN ('missing_token_id', 'book_error', 'book_empty') THEN 1 ELSE 0 END) AS unknown_fill_count,
                    MAX(created_at) AS last_created_at
                FROM shadow_order_intents
                """
            ).fetchone()
        return {
            "total_count": int(row["total_count"] or 0),
            "entry_count": int(row["entry_count"] or 0),
            "exit_count": int(row["exit_count"] or 0),
            "full_fill_count": int(row["full_fill_count"] or 0),
            "partial_fill_count": int(row["partial_fill_count"] or 0),
            "no_fill_count": int(row["no_fill_count"] or 0),
            "unknown_fill_count": int(row["unknown_fill_count"] or 0),
            "last_created_at": row["last_created_at"],
        }

    def _query_dashboard_paper_positions(
        self,
        *,
        limit: int | None,
        status: str | None = None,
        statuses: list[str] | tuple[str, ...] | set[str] | None = None,
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
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            params.append(int(limit))
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
                d.reason_code AS decision_reason_code,
                d.metadata_json AS decision_metadata_json
            FROM paper_positions p
            LEFT JOIN signals s ON s.id = p.signal_id
            LEFT JOIN decisions d ON d.id = p.decision_id
            {where}
            ORDER BY COALESCE(p.resolved_at, p.created_at) DESC
            {limit_clause}
            """,
            tuple(params),
        ).fetchall()
        return [_serialize_dashboard_position(row) for row in rows]

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

    def _ensure_decision_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(decisions)").fetchall()
        }
        if "reason_code" not in existing:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN reason_code TEXT")

    def _ensure_shadow_order_columns(self) -> None:
        existing = {
            str(row["name"])
            for row in self.conn.execute("PRAGMA table_info(shadow_order_intents)").fetchall()
        }
        required_columns = {
            "clob_token_id": "TEXT",
            "book_best_bid": "REAL",
            "book_best_ask": "REAL",
            "book_spread": "REAL",
            "book_midpoint": "REAL",
            "book_depth_at_target_shares": "REAL",
            "book_depth_at_target_usd": "REAL",
            "simulated_fill_status": "TEXT NOT NULL DEFAULT 'not_checked'",
            "simulated_fill_shares": "REAL",
            "simulated_avg_fill_price": "REAL",
            "simulated_notional_usd": "REAL",
            "simulated_unfilled_shares": "REAL",
            "simulated_slippage_bps": "REAL",
            "execution_checked_at": "TEXT",
            "execution_error": "TEXT",
        }
        for column, column_type in required_columns.items():
            if column in existing:
                continue
            self.conn.execute(f"ALTER TABLE shadow_order_intents ADD COLUMN {column} {column_type}")

    def _record_position_review_snapshot(
        self,
        position_id: int,
        *,
        reviewed_at: str,
        event_kind: str,
        reason: str,
        reason_code: str | None = None,
    ) -> None:
        snapshot = self._fetch_dashboard_position_snapshot(position_id)
        if not snapshot:
            return
        payload_json = json.dumps(snapshot, sort_keys=True)
        self.conn.execute(
            """
            INSERT INTO paper_position_reviews (
                position_id,
                reviewed_at,
                event_kind,
                status,
                market_slug,
                city_slug,
                direction,
                reason,
                reason_code,
                mark_price,
                mark_probability,
                mark_edge_abs,
                mark_final_score,
                mark_to_market_pnl,
                net_liquidation_value,
                estimated_exit_price,
                estimated_exit_fee_paid,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(position_id),
                str(reviewed_at or ""),
                str(event_kind or "review"),
                str(snapshot.get("status") or ""),
                str(snapshot.get("market_slug") or ""),
                str(snapshot.get("city_slug") or ""),
                str(snapshot.get("direction") or ""),
                str(reason or ""),
                None if reason_code is None else str(reason_code),
                _as_float(snapshot.get("mark_price")),
                _as_float(snapshot.get("mark_probability")),
                _as_float(snapshot.get("mark_edge_abs")),
                _as_float(snapshot.get("mark_final_score")),
                _as_float(snapshot.get("mark_to_market_pnl")),
                _as_float(snapshot.get("net_liquidation_value")),
                _as_float(snapshot.get("estimated_exit_price")),
                _as_float(snapshot.get("estimated_exit_fee_paid")),
                payload_json,
            ),
        )

    def _fetch_dashboard_position_snapshot(self, position_id: int) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
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
                d.reason_code AS decision_reason_code,
                d.metadata_json AS decision_metadata_json
            FROM paper_positions p
            LEFT JOIN signals s ON s.id = p.signal_id
            LEFT JOIN decisions d ON d.id = p.decision_id
            WHERE p.id = ?
            LIMIT 1
            """,
            (int(position_id),),
        ).fetchone()
        if row is None:
            return None
        return _serialize_dashboard_position(row)

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
        "decision_reason_code": str(payload.get("decision_reason_code") or ""),
        "decision_policy_action": str(payload.get("decision_policy_action") or ""),
        "decision_metadata": decision_metadata,
        "entry_price": pricing_metrics["entry_price"],
        "entry_reference_price": pricing_metrics["entry_reference_price"],
        "entry_market_probability": pricing_metrics["entry_reference_price"],
        "entry_model_probability": pricing_metrics["entry_model_probability"],
        "entry_yes_forecast_probability": _bounded_probability(payload.get("signal_forecast_prob")),
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


def _serialize_shadow_order_intent(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    created_at = _parse_iso_datetime(payload.get("created_at"))
    age_seconds = None
    if created_at is not None:
        age_seconds = max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds())
    return {
        "id": int(payload["id"]),
        "signal_id": int(payload["signal_id"]) if payload.get("signal_id") is not None else None,
        "decision_id": int(payload["decision_id"]) if payload.get("decision_id") is not None else None,
        "position_id": int(payload["position_id"]) if payload.get("position_id") is not None else None,
        "intent_kind": str(payload.get("intent_kind") or ""),
        "execution_mode": str(payload.get("execution_mode") or ""),
        "signal_key": str(payload.get("signal_key") or ""),
        "market_type": str(payload.get("market_type") or ""),
        "market_slug": str(payload.get("market_slug") or ""),
        "event_slug": str(payload.get("event_slug") or ""),
        "city_slug": str(payload.get("city_slug") or ""),
        "event_date": str(payload.get("event_date") or ""),
        "label": str(payload.get("label") or ""),
        "direction": str(payload.get("direction") or ""),
        "order_action": str(payload.get("order_action") or ""),
        "outcome_side": str(payload.get("outcome_side") or ""),
        "order_intent": str(payload.get("order_intent") or ""),
        "order_type": str(payload.get("order_type") or ""),
        "time_in_force": str(payload.get("time_in_force") or ""),
        "manual_order_indicator": str(payload.get("manual_order_indicator") or ""),
        "target_price": _as_float(payload.get("target_price")),
        "reference_price": _as_float(payload.get("reference_price")),
        "shares": _as_float(payload.get("shares")),
        "notional_usd": _as_float(payload.get("notional_usd")),
        "estimated_fee_paid": _as_float(payload.get("estimated_fee_paid")),
        "decision_final_score": _as_float(payload.get("decision_final_score")),
        "clob_token_id": str(payload.get("clob_token_id") or ""),
        "book_best_bid": _as_float(payload.get("book_best_bid")),
        "book_best_ask": _as_float(payload.get("book_best_ask")),
        "book_spread": _as_float(payload.get("book_spread")),
        "book_midpoint": _as_float(payload.get("book_midpoint")),
        "book_depth_at_target_shares": _as_float(payload.get("book_depth_at_target_shares")),
        "book_depth_at_target_usd": _as_float(payload.get("book_depth_at_target_usd")),
        "simulated_fill_status": str(payload.get("simulated_fill_status") or "not_checked"),
        "simulated_fill_shares": _as_float(payload.get("simulated_fill_shares")),
        "simulated_avg_fill_price": _as_float(payload.get("simulated_avg_fill_price")),
        "simulated_notional_usd": _as_float(payload.get("simulated_notional_usd")),
        "simulated_unfilled_shares": _as_float(payload.get("simulated_unfilled_shares")),
        "simulated_slippage_bps": _as_float(payload.get("simulated_slippage_bps")),
        "execution_checked_at": str(payload.get("execution_checked_at") or ""),
        "execution_error": str(payload.get("execution_error") or ""),
        "reason": str(payload.get("reason") or ""),
        "reason_code": str(payload.get("reason_code") or ""),
        "status": str(payload.get("status") or ""),
        "created_at": payload.get("created_at"),
        "age_seconds": age_seconds,
        "payload": _json_object(payload.get("payload_json")),
    }


def _serialize_shadow_exec_order(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    payload_json = _json_object(payload.get("payload_json"))
    created_at = _parse_iso_datetime(payload.get("created_at"))
    age_seconds = None
    if created_at is not None:
        age_seconds = max(0.0, (datetime.now(timezone.utc) - created_at).total_seconds())
    requested = _as_float(payload.get("requested_shares")) or 0.0
    filled = _as_float(payload.get("filled_shares")) or 0.0
    target = _as_float(payload.get("target_price"))
    pricing = _shadow_exec_pricing_summary(payload_json, target_price=target)
    return {
        "id": int(payload["id"]),
        "shadow_intent_id": int(payload["shadow_intent_id"]) if payload.get("shadow_intent_id") is not None else None,
        "paper_position_id": int(payload["paper_position_id"]) if payload.get("paper_position_id") is not None else None,
        "shadow_position_id": int(payload["shadow_position_id"]) if payload.get("shadow_position_id") is not None else None,
        "parent_order_id": int(payload["parent_order_id"]) if payload.get("parent_order_id") is not None else None,
        "intent_kind": str(payload.get("intent_kind") or ""),
        "execution_mode": str(payload.get("execution_mode") or ""),
        "market_type": str(payload.get("market_type") or ""),
        "market_slug": str(payload.get("market_slug") or ""),
        "event_slug": str(payload.get("event_slug") or ""),
        "city_slug": str(payload.get("city_slug") or ""),
        "event_date": str(payload.get("event_date") or ""),
        "label": str(payload.get("label") or ""),
        "direction": str(payload.get("direction") or ""),
        "order_action": str(payload.get("order_action") or ""),
        "outcome_side": str(payload.get("outcome_side") or ""),
        "target_price": target,
        "original_target_price": pricing["original_target_price"],
        "target_price_adjustment": pricing["target_price_adjustment"],
        "execution_pricing_kind": pricing["execution_pricing_kind"],
        "last_reprice_concession": pricing["last_reprice_concession"],
        "reprice_event_count": pricing["reprice_event_count"],
        "reference_price": _as_float(payload.get("reference_price")),
        "requested_shares": requested,
        "filled_shares": filled,
        "fill_ratio": round(filled / requested, 6) if requested > 0 else 0.0,
        "avg_fill_price": _as_float(payload.get("avg_fill_price")),
        "filled_notional_usd": _as_float(payload.get("filled_notional_usd")) or 0.0,
        "unfilled_shares": _as_float(payload.get("unfilled_shares")) or 0.0,
        "estimated_fee_paid": _as_float(payload.get("estimated_fee_paid")) or 0.0,
        "status": str(payload.get("status") or ""),
        "status_reason": str(payload.get("status_reason") or ""),
        "ttl_seconds": int(payload.get("ttl_seconds") or 0),
        "expires_at": str(payload.get("expires_at") or ""),
        "clob_token_id": str(payload.get("clob_token_id") or ""),
        "taker_estimate_avg_price": _as_float(payload.get("taker_estimate_avg_price")),
        "taker_estimate_notional_usd": _as_float(payload.get("taker_estimate_notional_usd")),
        "taker_estimate_fill_shares": _as_float(payload.get("taker_estimate_fill_shares")),
        "taker_estimate_pnl": _as_float(payload.get("taker_estimate_pnl")),
        "queue_fill_fraction": _as_float(payload.get("queue_fill_fraction")),
        "last_checked_at": str(payload.get("last_checked_at") or ""),
        "created_at": str(payload.get("created_at") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
        "age_seconds": age_seconds,
        "payload": payload_json,
    }


def _serialize_shadow_exec_fill(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    return {
        "id": int(payload["id"]),
        "order_id": int(payload["order_id"]),
        "shadow_position_id": int(payload["shadow_position_id"]) if payload.get("shadow_position_id") is not None else None,
        "shadow_intent_id": int(payload["shadow_intent_id"]) if payload.get("shadow_intent_id") is not None else None,
        "paper_position_id": int(payload["paper_position_id"]) if payload.get("paper_position_id") is not None else None,
        "clob_token_id": str(payload.get("clob_token_id") or ""),
        "action": str(payload.get("action") or ""),
        "price": _as_float(payload.get("price")),
        "shares": _as_float(payload.get("shares")) or 0.0,
        "notional_usd": _as_float(payload.get("notional_usd")) or 0.0,
        "liquidity_source": str(payload.get("liquidity_source") or ""),
        "evidence": _json_object(payload.get("evidence_json")),
        "filled_at": str(payload.get("filled_at") or ""),
    }


def _shadow_exec_pricing_summary(payload: dict[str, Any], *, target_price: float | None) -> dict[str, Any]:
    shadow_intent = payload.get("shadow_intent") if isinstance(payload.get("shadow_intent"), dict) else {}
    intent_payload = shadow_intent.get("payload") if isinstance(shadow_intent.get("payload"), dict) else {}
    entry_pricing = (
        intent_payload.get("shadow_execution_pricing")
        if isinstance(intent_payload.get("shadow_execution_pricing"), dict)
        else {}
    )
    repricing = payload.get("shadow_execution_repricing") if isinstance(payload.get("shadow_execution_repricing"), dict) else {}
    reprice_events = repricing.get("events") if isinstance(repricing.get("events"), list) else []
    original_target = _as_float(entry_pricing.get("original_target_price"))
    if original_target is None:
        original_target = _as_float(repricing.get("original_target_price"))
    if original_target is None:
        original_target = _as_float(shadow_intent.get("target_price"))
    adjustment = None
    if target_price is not None and original_target is not None:
        adjustment = round(float(target_price) - float(original_target), 6)
    last_concession = None
    if reprice_events:
        last = reprice_events[-1]
        if isinstance(last, dict):
            evidence = last.get("evidence") if isinstance(last.get("evidence"), dict) else {}
            last_concession = _as_float(evidence.get("concession"))
    pricing_kind = str(entry_pricing.get("kind") or repricing.get("last_reason") or "")
    return {
        "original_target_price": original_target,
        "target_price_adjustment": adjustment,
        "execution_pricing_kind": pricing_kind,
        "last_reprice_concession": last_concession,
        "reprice_event_count": len(reprice_events),
    }


def _serialize_shadow_exec_position(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    return {
        "id": int(payload["id"]),
        "paper_position_id": int(payload["paper_position_id"]) if payload.get("paper_position_id") is not None else None,
        "market_type": str(payload.get("market_type") or ""),
        "market_slug": str(payload.get("market_slug") or ""),
        "event_slug": str(payload.get("event_slug") or ""),
        "city_slug": str(payload.get("city_slug") or ""),
        "event_date": str(payload.get("event_date") or ""),
        "label": str(payload.get("label") or ""),
        "direction": str(payload.get("direction") or ""),
        "clob_token_id": str(payload.get("clob_token_id") or ""),
        "entry_order_id": int(payload["entry_order_id"]) if payload.get("entry_order_id") is not None else None,
        "exit_order_id": int(payload["exit_order_id"]) if payload.get("exit_order_id") is not None else None,
        "status": str(payload.get("status") or ""),
        "total_entry_shares": _as_float(payload.get("total_entry_shares")) or 0.0,
        "total_entry_notional_usd": _as_float(payload.get("total_entry_notional_usd")) or 0.0,
        "avg_entry_price": _as_float(payload.get("avg_entry_price")),
        "open_shares": _as_float(payload.get("open_shares")) or 0.0,
        "closed_shares": _as_float(payload.get("closed_shares")) or 0.0,
        "remaining_cost_basis_usd": _as_float(payload.get("remaining_cost_basis_usd")) or 0.0,
        "exit_notional_usd": _as_float(payload.get("exit_notional_usd")) or 0.0,
        "avg_exit_price": _as_float(payload.get("avg_exit_price")),
        "realized_pnl": _as_float(payload.get("realized_pnl")) or 0.0,
        "mark_price": _as_float(payload.get("mark_price")),
        "mark_value_usd": _as_float(payload.get("mark_value_usd")) or 0.0,
        "unrealized_pnl": _as_float(payload.get("unrealized_pnl")) or 0.0,
        "total_pnl": _as_float(payload.get("total_pnl")) or 0.0,
        "taker_exit_estimated_pnl": _as_float(payload.get("taker_exit_estimated_pnl")),
        "opened_at": str(payload.get("opened_at") or ""),
        "closed_at": str(payload.get("closed_at") or ""),
        "last_marked_at": str(payload.get("last_marked_at") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
        "payload": _json_object(payload.get("payload_json")),
    }


def _serialize_shadow_exec_mark(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    return {
        "id": int(payload["id"]),
        "shadow_position_id": int(payload["shadow_position_id"]),
        "mark_price": _as_float(payload.get("mark_price")),
        "mark_value_usd": _as_float(payload.get("mark_value_usd")) or 0.0,
        "unrealized_pnl": _as_float(payload.get("unrealized_pnl")) or 0.0,
        "total_pnl": _as_float(payload.get("total_pnl")) or 0.0,
        "source": str(payload.get("source") or ""),
        "evidence": _json_object(payload.get("evidence_json")),
        "created_at": str(payload.get("created_at") or ""),
    }


def _serialize_same_day_risk_tracking_row(row: sqlite3.Row) -> dict[str, Any]:
    payload = dict(row)
    metadata = _json_object(payload.get("metadata_json"))
    return {
        "decision_id": int(payload["decision_id"]),
        "signal_id": int(payload["signal_id"]) if payload.get("signal_id") is not None else None,
        "signal_key": str(payload.get("signal_key") or ""),
        "accepted": bool(payload.get("accepted")),
        "decision_created_at": str(payload.get("decision_created_at") or ""),
        "reason": str(payload.get("reason") or ""),
        "reason_code": str(payload.get("reason_code") or ""),
        "policy_action": str(payload.get("policy_action") or ""),
        "final_score": _as_float(payload.get("final_score")),
        "market_type": str(payload.get("market_type") or ""),
        "event_title": str(payload.get("event_title") or ""),
        "market_slug": str(payload.get("market_slug") or ""),
        "event_slug": str(payload.get("event_slug") or ""),
        "city_slug": str(payload.get("city_slug") or ""),
        "event_date": str(payload.get("event_date") or ""),
        "label": str(payload.get("label") or ""),
        "direction": str(payload.get("direction") or ""),
        "market_prob": _as_float(payload.get("market_prob")),
        "forecast_prob": _as_float(payload.get("forecast_prob")),
        "edge": _as_float(payload.get("edge")),
        "edge_abs": _as_float(payload.get("edge_abs")),
        "edge_size": str(payload.get("edge_size") or ""),
        "confidence": str(payload.get("confidence") or ""),
        "source_count": int(payload.get("source_count") or 0),
        "liquidity": _as_float(payload.get("liquidity")),
        "time_to_resolution_s": _as_float(payload.get("time_to_resolution_s")),
        "source_dispersion_pct": _as_float(payload.get("source_dispersion_pct")),
        "signal_score": _as_float(payload.get("signal_score")),
        "signal_created_at": str(payload.get("signal_created_at") or ""),
        "same_day_temperature_entry": bool(metadata.get("same_day_temperature_entry")),
        "same_day_entry_floor_applied": bool(metadata.get("same_day_entry_floor_applied")),
        "same_day_low_edge_blocked": bool(metadata.get("same_day_low_edge_blocked")),
        "same_day_entry_floor_source": str(metadata.get("same_day_entry_floor_source") or ""),
        "entry_block_reason_code": str(metadata.get("entry_block_reason_code") or ""),
        "entry_edge_floor": _as_float(metadata.get("entry_edge_floor")),
        "base_entry_edge_floor": _as_float(metadata.get("base_entry_edge_floor")),
        "same_day_min_edge_abs": _as_float(metadata.get("same_day_min_edge_abs")),
        "manual_entry_floor_override_active": bool(metadata.get("manual_entry_floor_override_active")),
        "metadata": metadata,
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


def _paper_entry_context_with_override(
    signal: WeatherSignal,
    entry_context_override: dict[str, float | None] | None,
) -> dict[str, float | None] | None:
    if not isinstance(entry_context_override, dict):
        return None
    entry_reference_price = _bounded_probability(entry_context_override.get("entry_reference_price"))
    entry_price = _bounded_probability(entry_context_override.get("entry_price"))
    mark_probability = _bounded_probability(entry_context_override.get("mark_probability"))
    if entry_reference_price is None or entry_price is None or entry_price <= 0:
        return None
    if mark_probability is None:
        mark_probability = _contract_probability(signal.direction, signal.forecast_prob)
    return {
        "entry_reference_price": entry_reference_price,
        "mark_probability": mark_probability,
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
    realized_pnl = _as_float(payload.get("realized_pnl")) or 0.0
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
    expected_value_pnl = round(realized_pnl + ((net_model_value or 0.0) - cost), 6) if outcome_probability is not None else None
    mark_to_market_payout = round(shares * mark_price, 6) if mark_price is not None else None
    mark_to_market_pnl = round(realized_pnl + ((net_liquidation_value or 0.0) - cost), 6) if mark_price is not None else None
    return {
        "entry_reference_price": entry_reference_price,
        "entry_model_probability": default_outcome_probability,
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


def _deserialize_position_review_row(row: sqlite3.Row) -> dict[str, Any]:
    payload = _json_object(row["payload_json"])
    payload.update(
        {
            "review_id": int(row["id"]),
            "position_id": int(row["position_id"]),
            "reviewed_at": str(row["reviewed_at"] or ""),
            "event_kind": str(row["event_kind"] or ""),
            "status": str(row["status"] or payload.get("status") or ""),
            "market_slug": str(row["market_slug"] or payload.get("market_slug") or ""),
            "city_slug": str(row["city_slug"] or payload.get("city_slug") or ""),
            "direction": str(row["direction"] or payload.get("direction") or ""),
            "review_reason": str(row["reason"] or ""),
            "review_reason_code": str(row["reason_code"] or ""),
            "mark_price": _as_float(row["mark_price"]) if row["mark_price"] is not None else _as_float(payload.get("mark_price")),
            "mark_probability": _as_float(row["mark_probability"])
            if row["mark_probability"] is not None
            else _as_float(payload.get("mark_probability")),
            "mark_edge_abs": _as_float(row["mark_edge_abs"]) if row["mark_edge_abs"] is not None else _as_float(payload.get("mark_edge_abs")),
            "mark_final_score": _as_float(row["mark_final_score"])
            if row["mark_final_score"] is not None
            else _as_float(payload.get("mark_final_score")),
            "mark_to_market_pnl": _as_float(row["mark_to_market_pnl"])
            if row["mark_to_market_pnl"] is not None
            else _as_float(payload.get("mark_to_market_pnl")),
            "net_liquidation_value": _as_float(row["net_liquidation_value"])
            if row["net_liquidation_value"] is not None
            else _as_float(payload.get("net_liquidation_value")),
            "estimated_exit_price": _as_float(row["estimated_exit_price"])
            if row["estimated_exit_price"] is not None
            else _as_float(payload.get("estimated_exit_price")),
            "estimated_exit_fee_paid": _as_float(row["estimated_exit_fee_paid"])
            if row["estimated_exit_fee_paid"] is not None
            else _as_float(payload.get("estimated_exit_fee_paid")),
        }
    )
    return payload


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


def _closed_trade_timestamp(item: dict[str, Any]) -> datetime | None:
    return _parse_iso_datetime(item.get("resolved_at")) or _parse_iso_datetime(item.get("created_at"))


def _build_pnl_window_payload(label: str, trades: list[dict[str, Any]]) -> dict[str, Any]:
    overall = _summarize_trade_cohort(trades)
    by_direction = {direction: _summarize_trade_cohort([trade for trade in trades if str(trade.get("direction") or "").upper() == direction]) for direction in ("YES", "NO")}
    return {
        "label": label,
        "closed_count": overall["count"],
        "wins": overall["wins"],
        "losses": overall["losses"],
        "win_rate": overall["win_rate"],
        "realized_pnl": overall["realized_pnl"],
        "gross_win_pnl": overall["gross_win_pnl"],
        "gross_loss_pnl": overall["gross_loss_pnl"],
        "gross_loss_pnl_abs": overall["gross_loss_pnl_abs"],
        "avg_win_pnl": overall["avg_win_pnl"],
        "avg_loss_pnl": overall["avg_loss_pnl"],
        "best_trade_pnl": overall["best_trade_pnl"],
        "worst_trade_pnl": overall["worst_trade_pnl"],
        "by_direction": by_direction,
        "by_city": _summarize_grouped_trades(trades, key="city_slug", label_key="city_slug", limit=8),
        "by_exit_reason": _summarize_grouped_trades(trades, key="exit_reason", label_key="exit_reason", limit=6),
        "top_winners": _top_trade_rows(trades, reverse=True),
        "top_losses": _top_trade_rows(trades, reverse=False),
    }


def _summarize_trade_cohort(trades: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(trade.get("realized_pnl") or 0.0) for trade in trades]
    winners = [pnl for pnl in pnls if pnl > 0]
    losers = [pnl for pnl in pnls if pnl < 0]
    realized_pnl = round(sum(pnls), 6)
    gross_win_pnl = round(sum(winners), 6)
    gross_loss_pnl = round(sum(losers), 6)
    gross_loss_pnl_abs = round(abs(gross_loss_pnl), 6)
    wins = len(winners)
    losses = len(losers)
    return {
        "count": len(trades),
        "wins": wins,
        "losses": losses,
        "win_rate": round((wins / len(trades) * 100.0), 2) if trades else 0.0,
        "realized_pnl": realized_pnl,
        "gross_win_pnl": gross_win_pnl,
        "gross_loss_pnl": gross_loss_pnl,
        "gross_loss_pnl_abs": gross_loss_pnl_abs,
        "avg_win_pnl": round(gross_win_pnl / wins, 6) if wins else 0.0,
        "avg_loss_pnl": round(gross_loss_pnl / losses, 6) if losses else 0.0,
        "best_trade_pnl": round(max(pnls), 6) if pnls else 0.0,
        "worst_trade_pnl": round(min(pnls), 6) if pnls else 0.0,
    }


def _summarize_grouped_trades(
    trades: list[dict[str, Any]],
    *,
    key: str,
    label_key: str,
    limit: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for trade in trades:
        raw_label = str(trade.get(label_key) or trade.get(key) or "--").strip() or "--"
        label = raw_label.upper() if key == "city_slug" else raw_label.replace("_", " ")
        row = grouped.setdefault(
            label,
            {"label": label, "count": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0},
        )
        pnl = float(trade.get("realized_pnl") or 0.0)
        row["count"] += 1
        row["realized_pnl"] = round(float(row["realized_pnl"]) + pnl, 6)
        if pnl > 0:
            row["wins"] += 1
        elif pnl < 0:
            row["losses"] += 1
    ranked = sorted(
        grouped.values(),
        key=lambda item: (-abs(float(item["realized_pnl"])), -int(item["count"]), str(item["label"])),
    )
    return ranked[: max(0, int(limit))]


def _top_trade_rows(trades: list[dict[str, Any]], *, reverse: bool) -> list[dict[str, Any]]:
    filtered = [trade for trade in trades if (float(trade.get("realized_pnl") or 0.0) > 0 if reverse else float(trade.get("realized_pnl") or 0.0) < 0)]
    ranked = sorted(
        filtered,
        key=lambda trade: float(trade.get("realized_pnl") or 0.0),
        reverse=reverse,
    )[:5]
    rows: list[dict[str, Any]] = []
    for trade in ranked:
        rows.append(
            {
                "id": int(trade.get("id") or 0),
                "event_title": str(trade.get("event_title") or trade.get("market_slug") or "Unknown market"),
                "city_slug": str(trade.get("city_slug") or "").upper(),
                "direction": str(trade.get("direction") or "").upper(),
                "target_label": str(trade.get("target_label") or ""),
                "realized_pnl": round(float(trade.get("realized_pnl") or 0.0), 6),
                "entry_price": _bounded_probability(trade.get("entry_price")),
                "resolved_at": str(trade.get("resolved_at") or ""),
                "exit_reason": str(trade.get("exit_reason") or ""),
            }
        )
    return rows


def _summarize_open_book(open_positions: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "count": len(open_positions),
        "stake": round(sum(float(item.get("cost") or 0.0) for item in open_positions), 6),
        "mark_to_market_pnl": round(sum(float(item.get("mark_to_market_pnl") or 0.0) for item in open_positions), 6),
        "model_pnl": round(sum(float(item.get("expected_value_pnl") or 0.0) for item in open_positions), 6),
        "by_direction": {},
    }
    for direction in ("YES", "NO"):
        items = [item for item in open_positions if str(item.get("direction") or "").upper() == direction]
        summary["by_direction"][direction] = {
            "count": len(items),
            "stake": round(sum(float(item.get("cost") or 0.0) for item in items), 6),
            "mark_to_market_pnl": round(sum(float(item.get("mark_to_market_pnl") or 0.0) for item in items), 6),
            "model_pnl": round(sum(float(item.get("expected_value_pnl") or 0.0) for item in items), 6),
        }
    return summary


def _paper_position_current_pnl(position: dict[str, Any]) -> float:
    status = str(position.get("status") or "").lower()
    if status in {"closed", "resolved"}:
        return round(float(position.get("realized_pnl") or 0.0), 6)
    return round(float(position.get("mark_to_market_pnl") or position.get("expected_value_pnl") or 0.0), 6)


def _count_by_key(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _split_reason_codes(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    return [part.strip() for part in raw.split("|") if part.strip()]


def _count_reason_codes(rows: list[dict[str, Any]], *, key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for reason_code in _split_reason_codes(row.get(key)):
            counts[reason_code] = counts.get(reason_code, 0) + 1
    return counts


def _summarize_decision_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    accepted_by_policy_action: dict[str, int] = {}
    rejected_by_policy_action: dict[str, int] = {}
    rejected_reason_codes: dict[str, int] = {}
    accepted_count = 0
    rejected_count = 0
    for row in rows:
        accepted = int(row.get("accepted") or 0) > 0
        policy_action = str(row.get("policy_action") or "unknown")
        if accepted:
            accepted_count += 1
            accepted_by_policy_action[policy_action] = accepted_by_policy_action.get(policy_action, 0) + 1
            continue
        rejected_count += 1
        rejected_by_policy_action[policy_action] = rejected_by_policy_action.get(policy_action, 0) + 1
        for reason_code in _split_reason_codes(row.get("reason_code")):
            rejected_reason_codes[reason_code] = rejected_reason_codes.get(reason_code, 0) + 1
    decision_count = len(rows)
    return {
        "decision_count": decision_count,
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "accept_rate": round((accepted_count / decision_count * 100.0), 2) if decision_count else 0.0,
        "accepted_by_policy_action": accepted_by_policy_action,
        "rejected_by_policy_action": rejected_by_policy_action,
        "rejected_reason_codes": rejected_reason_codes,
    }


def _ranked_count_items(counts: dict[str, int], *, limit: int | None = None) -> list[dict[str, Any]]:
    items = [
        {"label": str(label or "unknown"), "count": int(count or 0)}
        for label, count in counts.items()
    ]
    items.sort(key=lambda item: (-int(item["count"]), str(item["label"])))
    if limit is None:
        return items
    return items[: max(0, int(limit))]


def _resolve_analytics_timezone(timezone_name: str | None) -> ZoneInfo:
    raw_name = str(timezone_name or "UTC").strip() or "UTC"
    try:
        return ZoneInfo(raw_name)
    except ZoneInfoNotFoundError:
        return timezone.utc


def _resolve_summary_window(
    *,
    start: str | None,
    end: str | None,
    now_date: datetime.date,
    weekend_only: bool,
    limit_days: int,
) -> tuple[datetime.date, datetime.date]:
    parsed_start = _parse_summary_date(start)
    parsed_end = _parse_summary_date(end)
    max_span = max(1, int(limit_days or 1))
    if weekend_only:
        if parsed_start is None or parsed_end is None:
            weekend_start, weekend_end = _most_recent_weekend(now_date)
            parsed_start = weekend_start
            parsed_end = weekend_end
    if parsed_start is None and parsed_end is None:
        parsed_end = now_date
        parsed_start = parsed_end - timedelta(days=max_span - 1)
    elif parsed_start is None:
        parsed_end = parsed_end or now_date
        parsed_start = parsed_end - timedelta(days=max_span - 1)
    elif parsed_end is None:
        parsed_end = parsed_start + timedelta(days=max_span - 1)
    if parsed_end < parsed_start:
        parsed_start, parsed_end = parsed_end, parsed_start
    window_span = (parsed_end - parsed_start).days + 1
    if not weekend_only and window_span > max_span:
        parsed_start = parsed_end - timedelta(days=max_span - 1)
    return parsed_start, parsed_end


def _most_recent_weekend(now_date: datetime.date) -> tuple[datetime.date, datetime.date]:
    weekday = now_date.weekday()  # Monday=0 ... Sunday=6
    if weekday >= 5:
        saturday = now_date - timedelta(days=weekday - 5)
    else:
        saturday = now_date - timedelta(days=weekday + 2)
    return saturday, saturday + timedelta(days=1)


def _parse_summary_date(value: str | None) -> datetime.date | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if isinstance(parsed, datetime):
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed.date()
    return parsed


def _group_rows_by_local_date(
    rows: Any,
    *,
    timestamp_key: str,
    timezone: ZoneInfo,
) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        row_data = dict(row)
        timestamp = row_data.get(timestamp_key)
        parsed = _parse_iso_datetime(timestamp) if timestamp else None
        if parsed is None:
            continue
        local_date = parsed.astimezone(timezone).date().isoformat()
        buckets[local_date].append(row_data)
    return buckets
