"""SQLite-backed persistence for restart-safe weather provider caches."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import WEATHER_CACHE_DB_PATH


CACHE_DB_PATH: str | Path = WEATHER_CACHE_DB_PATH
_CONNECTION_LOCK = threading.RLock()
_CONNECTION: sqlite3.Connection | None = None
_LAST_PRUNE_MONOTONIC = 0.0
_PRUNE_INTERVAL_SECONDS = 300.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connection() -> sqlite3.Connection:
    global _CONNECTION
    with _CONNECTION_LOCK:
        if _CONNECTION is not None:
            return _CONNECTION
        db_path = Path(CACHE_DB_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS weather_cache (
                namespace TEXT NOT NULL,
                provider TEXT NOT NULL,
                cache_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                expires_at REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(namespace, provider, cache_key)
            );

            CREATE INDEX IF NOT EXISTS idx_weather_cache_expires_at
            ON weather_cache(expires_at);
            """
        )
        conn.commit()
        _CONNECTION = conn
        return conn


def close_weather_cache() -> None:
    global _CONNECTION
    with _CONNECTION_LOCK:
        if _CONNECTION is None:
            return
        _CONNECTION.close()
        _CONNECTION = None


def clear_weather_cache() -> None:
    with _CONNECTION_LOCK:
        conn = _connection()
        conn.execute("DELETE FROM weather_cache")
        conn.commit()


def load_cached_payload(namespace: str, provider: str, cache_key: str) -> tuple[dict[str, Any], float] | None:
    now_epoch = time.time()
    with _CONNECTION_LOCK:
        conn = _connection()
        _prune_expired_locked(conn, now_epoch)
        row = conn.execute(
            """
            SELECT payload_json, expires_at
            FROM weather_cache
            WHERE namespace = ? AND provider = ? AND cache_key = ?
            """,
            (str(namespace), str(provider), str(cache_key)),
        ).fetchone()
        if row is None:
            return None
        expires_at = float(row["expires_at"] or 0.0)
        if expires_at <= now_epoch:
            conn.execute(
                """
                DELETE FROM weather_cache
                WHERE namespace = ? AND provider = ? AND cache_key = ?
                """,
                (str(namespace), str(provider), str(cache_key)),
            )
            conn.commit()
            return None
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except json.JSONDecodeError:
            conn.execute(
                """
                DELETE FROM weather_cache
                WHERE namespace = ? AND provider = ? AND cache_key = ?
                """,
                (str(namespace), str(provider), str(cache_key)),
            )
            conn.commit()
            return None
        if not isinstance(payload, dict):
            return None
        return payload, max(0.0, expires_at - now_epoch)


def store_cached_payload(
    namespace: str,
    provider: str,
    cache_key: str,
    payload: dict[str, Any],
    ttl_seconds: float,
) -> None:
    ttl = max(1.0, float(ttl_seconds))
    now_epoch = time.time()
    with _CONNECTION_LOCK:
        conn = _connection()
        _prune_expired_locked(conn, now_epoch)
        conn.execute(
            """
            INSERT INTO weather_cache(namespace, provider, cache_key, payload_json, expires_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(namespace, provider, cache_key)
            DO UPDATE SET
                payload_json = excluded.payload_json,
                expires_at = excluded.expires_at,
                updated_at = excluded.updated_at
            """,
            (
                str(namespace),
                str(provider),
                str(cache_key),
                json.dumps(payload, sort_keys=True),
                now_epoch + ttl,
                _utc_now_iso(),
            ),
        )
        conn.commit()


def _prune_expired_locked(conn: sqlite3.Connection, now_epoch: float) -> None:
    global _LAST_PRUNE_MONOTONIC
    current_monotonic = time.monotonic()
    if current_monotonic - _LAST_PRUNE_MONOTONIC < _PRUNE_INTERVAL_SECONDS:
        return
    conn.execute("DELETE FROM weather_cache WHERE expires_at <= ?", (float(now_epoch),))
    conn.commit()
    _LAST_PRUNE_MONOTONIC = current_monotonic
