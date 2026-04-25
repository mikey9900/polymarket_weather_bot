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
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
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
                shares REAL NOT NULL,
                cost REAL NOT NULL,
                status TEXT NOT NULL,
                resolution TEXT,
                realized_pnl REAL,
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
        self.conn.commit()

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
        row = self.conn.execute("SELECT COUNT(*) AS count FROM paper_positions WHERE status = 'open'").fetchone()
        return int(row["count"] or 0)

    def has_open_position(self, market_slug: str, direction: str) -> bool:
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
        notes: str = "auto",
    ) -> PaperPosition | None:
        if signal.market_prob <= 0:
            return None
        with self._lock:
            initial, available = self.get_paper_capital()
            cost = round(min(float(stake_usd), available), 6)
            if cost <= 0:
                return None
            shares = round(cost / float(signal.market_prob), 6)
            self.set_setting("paper_capital", {"initial": initial, "available": round(available - cost, 6)})
            cursor = self.conn.execute(
                """
                INSERT INTO paper_positions (
                    signal_id, decision_id, signal_key, market_type, market_slug, event_slug, city_slug,
                    event_date, label, direction, score, entry_price, shares, cost, status,
                    resolution, realized_pnl, notes, created_at, resolved_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', NULL, NULL, ?, ?, NULL)
                """,
                (
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
                    signal.market_prob,
                    shares,
                    cost,
                    notes,
                    signal.created_at,
                ),
            )
            self.conn.commit()
            return PaperPosition(
                id=int(cursor.lastrowid),
                signal_key=signal.signal_key,
                market_type=signal.market_type,
                market_slug=signal.market_slug,
                event_slug=signal.event_slug,
                city_slug=signal.city_slug,
                event_date=signal.event_date,
                label=signal.label,
                direction=signal.direction,
                score=signal.score,
                entry_price=signal.market_prob,
                shares=shares,
                cost=cost,
                status="open",
                notes=notes,
                created_at=signal.created_at,
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
                payout = float(row["shares"]) if str(row["direction"]).upper() == resolution else 0.0
                pnl = round(payout - float(row["cost"]), 6)
                total_payout += payout
                total_pnl += pnl
                self.conn.execute(
                    """
                    UPDATE paper_positions
                    SET status = 'resolved',
                        resolution = ?,
                        realized_pnl = ?,
                        resolved_at = ?
                    WHERE id = ?
                    """,
                    (resolution, pnl, resolved_at, int(row["id"])),
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
        rows = self.conn.execute("SELECT * FROM paper_positions WHERE status = 'open' ORDER BY created_at DESC").fetchall()
        return [dict(row) for row in rows]

    def get_recent_paper_positions(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM paper_positions ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
        return [dict(row) for row in rows]

    def get_dashboard_paper_positions(self, limit: int = 20, *, status: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if status:
            where = "WHERE p.status = ?"
            params.append(str(status))
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
        return [_serialize_dashboard_position(row) for row in rows]

    def get_recent_resolutions(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM resolution_events ORDER BY resolved_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_recent_operator_actions(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM operator_events ORDER BY created_at DESC LIMIT ?",
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
        if market_type:
            rows = self.conn.execute(
                "SELECT * FROM signals WHERE market_type = ? ORDER BY created_at DESC LIMIT ?",
                (market_type, int(limit)),
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM signals ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
        return [dict(row) for row in rows]

    def get_signal_summary(self) -> dict[str, Any]:
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
        initial, available = self.get_paper_capital()
        summary = self.conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'resolved' AND realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN status = 'resolved' AND realized_pnl < 0 THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_positions,
                COALESCE(SUM(CASE WHEN status = 'resolved' THEN realized_pnl ELSE 0 END), 0.0) AS realized_pnl,
                COALESCE(SUM(CASE WHEN status = 'open' THEN cost ELSE 0 END), 0.0) AS open_cost
            FROM paper_positions
            """
        ).fetchone()
        wins = int(summary["wins"] or 0)
        losses = int(summary["losses"] or 0)
        open_positions = int(summary["open_positions"] or 0)
        realized_pnl = float(summary["realized_pnl"] or 0.0)
        open_cost = float(summary["open_cost"] or 0.0)
        current_equity = float(available) + open_cost
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
    created_at = _parse_iso_datetime(payload.get("created_at"))
    resolved_at = _parse_iso_datetime(payload.get("resolved_at"))
    now = datetime.now(timezone.utc)
    anchor = resolved_at or now
    holding_seconds = None
    if created_at is not None:
        holding_seconds = max(0.0, (anchor - created_at).total_seconds())

    time_to_resolution_s = payload.get("signal_time_to_resolution_s")
    remaining_to_resolution_s = None
    if time_to_resolution_s is not None:
        try:
            total_window = float(time_to_resolution_s)
        except (TypeError, ValueError):
            total_window = None
        if total_window is not None:
            if holding_seconds is None:
                remaining_to_resolution_s = max(0.0, total_window)
            else:
                remaining_to_resolution_s = max(0.0, total_window - holding_seconds)

    outcome_probability = _as_float(payload.get("signal_forecast_prob"))
    entry_price = _as_float(payload.get("entry_price")) or 0.0
    shares = _as_float(payload.get("shares")) or 0.0
    cost = _as_float(payload.get("cost")) or 0.0
    if outcome_probability is not None:
        expected_payout = round(shares * outcome_probability, 6)
        expected_value_pnl = round(expected_payout - cost, 6)
    else:
        expected_payout = None
        expected_value_pnl = None

    decision_metadata = {}
    try:
        if payload.get("decision_metadata_json"):
            decision_metadata = json.loads(str(payload["decision_metadata_json"]))
    except json.JSONDecodeError:
        decision_metadata = {}

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
        "entry_price": entry_price,
        "market_probability": _as_float(payload.get("signal_market_prob")),
        "outcome_probability": outcome_probability,
        "shares": shares,
        "cost": cost,
        "realized_pnl": _as_float(payload.get("realized_pnl")),
        "expected_payout": expected_payout,
        "expected_value_pnl": expected_value_pnl,
        "edge": _as_float(payload.get("signal_edge")),
        "edge_abs": _as_float(payload.get("signal_edge_abs")),
        "edge_size": str(payload.get("signal_edge_size") or ""),
        "confidence": str(payload.get("signal_confidence") or ""),
        "source_count": int(payload.get("signal_source_count") or 0),
        "liquidity": _as_float(payload.get("signal_liquidity")),
        "source_dispersion_pct": _as_float(payload.get("signal_source_dispersion_pct")),
        "time_to_resolution_s": _as_float(payload.get("signal_time_to_resolution_s")),
        "remaining_to_resolution_s": remaining_to_resolution_s,
        "holding_seconds": holding_seconds,
        "created_at": str(payload.get("created_at") or ""),
        "resolved_at": str(payload.get("resolved_at") or ""),
        "resolution": str(payload.get("resolution") or ""),
    }


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
