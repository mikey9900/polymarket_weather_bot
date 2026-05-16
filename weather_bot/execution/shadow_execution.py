"""Execution-grade shadow order simulator.

This layer never places trades. It mirrors shadow order intents into a separate
ledger and only credits fills when public market data makes them executable.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from ..models import iso_now
from .models import ShadowOrderIntent
from .shadow_fill import fetch_clob_order_book


BookFetcher = Callable[[str], dict[str, Any] | None]
TradeFetcher = Callable[[str, int], list[dict[str, Any]]]
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_DATA_API_TRADES_URL = "https://data-api.polymarket.com/trades"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ShadowExecutionRuntimeConfig:
    enabled: bool = True
    entry_ttl_seconds: int = 1800
    exit_ttl_seconds: int = 300
    queue_fill_fraction: float = 0.50
    rest_fallback_seconds: int = 5
    show_taker_exit_estimate: bool = True
    entry_price_improvement_enabled: bool = True
    entry_min_edge_abs: float = 0.12
    exit_repricing_enabled: bool = True
    exit_ladder_step_seconds: int = 60
    exit_concession_steps: tuple[float, ...] = (0.005, 0.01)
    exit_urgent_concession_steps: tuple[float, ...] = (0.005, 0.01, 0.02, 0.03)
    exit_urgent_reason_codes: tuple[str, ...] = ("same_day_price_collapse", "no_stop_loss", "score_breakdown")

    @classmethod
    def from_settings(cls, settings: Any) -> "ShadowExecutionRuntimeConfig":
        return cls(
            enabled=bool(getattr(settings, "enabled", True)),
            entry_ttl_seconds=max(1, int(getattr(settings, "entry_ttl_seconds", 1800) or 1800)),
            exit_ttl_seconds=max(1, int(getattr(settings, "exit_ttl_seconds", 300) or 300)),
            queue_fill_fraction=max(0.0, min(1.0, float(getattr(settings, "queue_fill_fraction", 0.50) or 0.0))),
            rest_fallback_seconds=max(1, int(getattr(settings, "rest_fallback_seconds", 5) or 5)),
            show_taker_exit_estimate=bool(getattr(settings, "show_taker_exit_estimate", True)),
            entry_price_improvement_enabled=bool(getattr(settings, "entry_price_improvement_enabled", True)),
            entry_min_edge_abs=max(0.0, float(getattr(settings, "entry_min_edge_abs", 0.12) or 0.0)),
            exit_repricing_enabled=bool(getattr(settings, "exit_repricing_enabled", True)),
            exit_ladder_step_seconds=max(1, int(getattr(settings, "exit_ladder_step_seconds", 60) or 60)),
            exit_concession_steps=_float_sequence(getattr(settings, "exit_concession_steps", (0.005, 0.01))),
            exit_urgent_concession_steps=_float_sequence(
                getattr(settings, "exit_urgent_concession_steps", (0.005, 0.01, 0.02, 0.03))
            ),
            exit_urgent_reason_codes=_string_sequence(
                getattr(settings, "exit_urgent_reason_codes", ("same_day_price_collapse", "no_stop_loss", "score_breakdown"))
            ),
        )


class ShadowExecutionEngine:
    def __init__(
        self,
        *,
        tracker,
        config: ShadowExecutionRuntimeConfig | Any,
        book_fetcher: BookFetcher = fetch_clob_order_book,
        trade_fetcher: TradeFetcher | None = None,
    ):
        self.tracker = tracker
        self.config = config if isinstance(config, ShadowExecutionRuntimeConfig) else ShadowExecutionRuntimeConfig.from_settings(config)
        self.book_fetcher = book_fetcher
        self.trade_fetcher = trade_fetcher or fetch_polymarket_market_trades
        self._ws_lock = threading.RLock()
        self._ws_stop = threading.Event()
        self._ws_thread: threading.Thread | None = None
        self._ws_tokens: tuple[str, ...] = ()
        self._ws_error: str | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.config.enabled)

    def mirror_intent(self, shadow_intent_id: int, intent: ShadowOrderIntent) -> int | None:
        if not self.enabled:
            return None
        execution_intent = self._execution_priced_intent(intent)
        taker_estimate = self._build_taker_estimate(execution_intent)
        order_id = self.tracker.create_shadow_exec_order_from_intent(
            shadow_intent_id=int(shadow_intent_id),
            intent=execution_intent,
            entry_ttl_seconds=int(self.config.entry_ttl_seconds),
            exit_ttl_seconds=int(self.config.exit_ttl_seconds),
            queue_fill_fraction=float(self.config.queue_fill_fraction),
            taker_estimate=taker_estimate,
        )
        self._apply_intent_rehearsal_fill(order_id, execution_intent)
        order = self.tracker.get_shadow_exec_order(order_id)
        if order is not None and order.get("status") in {"resting", "partial_fill"}:
            self._apply_book_cross_fill(order, liquidity_source="immediate_book")
        return order_id

    def price_entry_intent(self, intent: ShadowOrderIntent) -> ShadowOrderIntent:
        return self._execution_priced_intent(intent)

    def run_cycle(self, *, price_fetcher: Callable[[str], float | None] | None = None) -> dict[str, Any]:
        if not self.enabled:
            return {"enabled": False, "processed_orders": 0, "fills": 0, "expired": 0, "exit_retries_queued": 0, "marked_positions": 0}
        exit_retries = self.queue_exit_retries()
        self.sync_market_stream()
        trade_tape = self.apply_trade_tape_fallback()
        expired = self.tracker.expire_shadow_exec_orders()
        processed = 0
        fills = 0
        for order in self.tracker.get_active_shadow_exec_orders(limit=500):
            processed += 1
            order = self._apply_order_repricing(order)
            fill = self._apply_book_cross_fill(order, liquidity_source="resting_book")
            if fill is not None:
                fills += 1
        marked = self.mark_open_positions(price_fetcher=price_fetcher)
        return {
            "enabled": True,
            "processed_orders": processed,
            "fills": fills,
            "expired": expired,
            "exit_retries_queued": exit_retries,
            "marked_positions": marked,
            "rest_trade_tape_conditions": trade_tape.get("conditions", 0),
            "rest_trade_tape_events": trade_tape.get("events", 0),
            "rest_trade_tape_fills": trade_tape.get("fills", 0),
            "rest_trade_tape_errors": trade_tape.get("errors", 0),
        }

    def apply_trade_tape_fallback(self, *, limit_per_condition: int = 1000) -> dict[str, int]:
        active_orders = [
            order
            for order in self.tracker.get_active_shadow_exec_orders(limit=500)
            if str(order.get("condition_id") or "").strip() and str(order.get("clob_token_id") or "").strip()
        ]
        if not active_orders:
            return {"conditions": 0, "events": 0, "fills": 0, "errors": 0}
        by_condition: dict[str, list[dict[str, Any]]] = {}
        for order in active_orders:
            by_condition.setdefault(str(order.get("condition_id") or "").strip(), []).append(order)
        observed_at = iso_now()
        event_count = 0
        fill_count = 0
        error_count = 0
        for condition_id, orders in by_condition.items():
            active_tokens = {str(order.get("clob_token_id") or "").strip() for order in orders}
            created_times = [value for value in (_parse_iso_datetime(order.get("created_at")) for order in orders) if value is not None]
            expiry_times = [value for value in (_parse_iso_datetime(order.get("expires_at")) for order in orders) if value is not None]
            earliest = min(created_times) if created_times else None
            latest_expiry = max(expiry_times) if expiry_times else None
            try:
                trades = self.trade_fetcher(condition_id, int(limit_per_condition))
            except Exception as exc:
                error_count += 1
                logger.debug("shadow trade-tape fallback failed for %s: %s", condition_id, exc)
                continue
            for trade in sorted(_normalize_trade_tape_events(trades), key=lambda item: (item["trade_timestamp"], item["event_uid"])):
                token = str(trade.get("clob_token_id") or "").strip()
                if token not in active_tokens:
                    continue
                trade_dt = datetime.fromtimestamp(int(trade["trade_timestamp"]), tz=timezone.utc)
                if earliest is not None and trade_dt < earliest:
                    continue
                if latest_expiry is not None and trade_dt > latest_expiry:
                    continue
                stored = self.tracker.record_shadow_exec_trade_event(
                    condition_id=condition_id,
                    clob_token_id=token,
                    side=str(trade["side"]),
                    price=float(trade["price"]),
                    size=float(trade["size"]),
                    trade_timestamp=int(trade["trade_timestamp"]),
                    transaction_hash=str(trade.get("transaction_hash") or ""),
                    source="rest_trade_tape",
                    raw_event=trade.get("raw") if isinstance(trade.get("raw"), dict) else trade,
                    observed_at=observed_at,
                )
                if stored is None or not stored.get("inserted"):
                    continue
                event_count += 1
                traded_at = trade_dt.isoformat()
                fills = self.apply_trade_event(
                    clob_token_id=token,
                    side=str(trade["side"]),
                    price=float(trade["price"]),
                    size=float(trade["size"]),
                    traded_at=traded_at,
                    liquidity_source="rest_trade_tape",
                    evidence={
                        "event_uid": stored.get("event_uid"),
                        "condition_id": condition_id,
                        "transaction_hash": stored.get("transaction_hash"),
                        "trade_timestamp": int(trade["trade_timestamp"]),
                        "source": "polymarket_data_api",
                    },
                )
                fill_count += len(fills)
                self.tracker.mark_shadow_exec_trade_event_processed(str(stored.get("event_uid") or ""), processed_at=iso_now())
        return {
            "conditions": len(by_condition),
            "events": event_count,
            "fills": fill_count,
            "errors": error_count,
        }

    def queue_exit_retries(self, *, limit: int = 500) -> int:
        queued = 0
        for candidate in self.tracker.get_shadow_exec_exit_retry_candidates(limit=limit):
            exit_price = _bounded_price(
                candidate.get("paper_mark_price")
                if candidate.get("paper_mark_price") is not None
                else candidate.get("shadow_mark_price")
            )
            if exit_price is None:
                exit_price = _bounded_price(candidate.get("shadow_avg_entry_price"))
            if exit_price is None:
                exit_price = _bounded_price(candidate.get("paper_entry_price"))
            intent = self.tracker.preview_shadow_exit_intent(
                int(candidate["paper_position_id"]),
                execution_mode=str(candidate.get("execution_mode") or "paper_shadow"),
                exit_price=exit_price,
                reason=str(candidate.get("exit_reason") or "shadow_exit_retry"),
                reason_code=str(candidate.get("reason_code") or "shadow_exit_retry"),
                decision_final_score=_as_float(candidate.get("mark_final_score")),
            )
            if intent is None:
                continue
            payload = dict(intent.payload or {})
            payload["shadow_execution_retry"] = {
                "shadow_position_id": int(candidate["shadow_position_id"]),
                "reason": "open_shadow_position_still_has_exit_reason",
                "queued_at": iso_now(),
            }
            intent = replace(intent, payload=payload)
            intent_id = self.tracker.record_shadow_order_intent(intent)
            self.mirror_intent(intent_id, intent)
            queued += 1
        return queued

    def stop(self) -> None:
        self._stop_market_stream()

    def mark_open_positions(self, *, price_fetcher: Callable[[str], float | None] | None = None) -> int:
        marked = 0
        for position in self.tracker.get_shadow_exec_positions(limit=500, status="open"):
            mark_price = _as_float(position.get("mark_price"))
            source = "shadow_exec_existing_mark"
            evidence: dict[str, Any] = {}
            if price_fetcher is not None:
                try:
                    yes_price = _bounded_price(price_fetcher(str(position.get("market_slug") or "")))
                except Exception as exc:
                    evidence["error"] = str(exc)
                    yes_price = None
                if yes_price is not None:
                    mark_price = yes_price if str(position.get("direction") or "").upper() == "YES" else round(1.0 - yes_price, 6)
                    source = "price_fetcher"
                    evidence["yes_price"] = yes_price
            if mark_price is None:
                mark_price = _as_float(position.get("avg_entry_price"))
                source = "shadow_exec_entry_mark"
            if mark_price is None:
                continue
            self.tracker.mark_shadow_exec_position(
                int(position["id"]),
                mark_price=mark_price,
                source=source,
                evidence=evidence,
            )
            marked += 1
        return marked

    def apply_trade_event(
        self,
        *,
        clob_token_id: str,
        side: str,
        price: float,
        size: float,
        traded_at: str | None = None,
        liquidity_source: str = "trade_through",
        evidence: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Credit resting fills from a later public trade event.

        BUY orders only fill from SELL trades at or below their limit. SELL
        orders only fill from BUY trades at or above their limit. The configured
        queue fraction is applied once across oldest active orders.
        """

        if not self.enabled:
            return []
        token = str(clob_token_id or "").strip()
        trade_side = str(side or "").upper()
        trade_price = _bounded_price(price)
        remaining_trade_size = max(0.0, float(size or 0.0)) * float(self.config.queue_fill_fraction)
        if not token or trade_side not in {"BUY", "SELL"} or trade_price is None or remaining_trade_size <= 0:
            return []
        fills: list[dict[str, Any]] = []
        traded_at = str(traded_at or iso_now())
        traded_dt = _parse_iso_datetime(traded_at)
        for order in self.tracker.get_active_shadow_exec_orders(limit=500):
            if remaining_trade_size <= 0:
                break
            if str(order.get("clob_token_id") or "") != token:
                continue
            created_dt = _parse_iso_datetime(order.get("created_at"))
            expires_dt = _parse_iso_datetime(order.get("expires_at"))
            if traded_dt is not None and created_dt is not None and traded_dt < created_dt:
                continue
            if traded_dt is not None and expires_dt is not None and traded_dt > expires_dt:
                continue
            order = self._apply_order_repricing(order, now=traded_at)
            if not _trade_qualifies_order(order, trade_side=trade_side, trade_price=trade_price):
                continue
            fill_shares = min(remaining_trade_size, float(order.get("unfilled_shares") or 0.0))
            fill = self.tracker.record_shadow_exec_fill(
                int(order["id"]),
                price=trade_price,
                shares=fill_shares,
                liquidity_source=str(liquidity_source or "trade_through"),
                evidence={
                    **(evidence or {}),
                    "trade_side": trade_side,
                    "trade_price": trade_price,
                    "observed_trade_size": float(size or 0.0),
                    "queue_fill_fraction": float(self.config.queue_fill_fraction),
                },
                filled_at=traded_at,
            )
            if fill is not None:
                remaining_trade_size = max(0.0, remaining_trade_size - float(fill.get("shares") or 0.0))
                fills.append(fill)
        return fills

    def apply_market_channel_message(self, message: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        """Apply Polymarket market-channel trade messages to resting orders."""

        if isinstance(message, list):
            fills: list[dict[str, Any]] = []
            for item in message:
                if isinstance(item, dict):
                    fills.extend(self.apply_market_channel_message(item))
            return fills
        if not isinstance(message, dict):
            return []
        if str(message.get("event_type") or "") != "last_trade_price":
            return []
        traded_at = _timestamp_ms_to_iso(message.get("timestamp")) or iso_now()
        return self.apply_trade_event(
            clob_token_id=str(message.get("asset_id") or ""),
            side=str(message.get("side") or ""),
            price=float(message.get("price") or 0.0),
            size=float(message.get("size") or 0.0),
            traded_at=traded_at,
            liquidity_source="market_websocket",
            evidence={
                "event_type": "last_trade_price",
                "market": message.get("market"),
                "timestamp": message.get("timestamp"),
                "fee_rate_bps": message.get("fee_rate_bps"),
            },
        )

    def sync_market_stream(self) -> None:
        tokens = tuple(
            sorted(
                {
                    str(order.get("clob_token_id") or "").strip()
                    for order in self.tracker.get_active_shadow_exec_orders(limit=500)
                    if str(order.get("clob_token_id") or "").strip()
                }
            )
        )
        if not tokens:
            self._stop_market_stream()
            return
        with self._ws_lock:
            if self._ws_thread is not None and self._ws_thread.is_alive() and self._ws_tokens == tokens:
                return
        self._stop_market_stream()
        with self._ws_lock:
            self._ws_tokens = tokens
            self._ws_stop.clear()
            self._ws_thread = threading.Thread(
                target=self._market_stream_loop,
                args=(tokens,),
                name="weather-shadow-market-ws",
                daemon=True,
            )
            self._ws_thread.start()

    def _stop_market_stream(self) -> None:
        with self._ws_lock:
            thread = self._ws_thread
            self._ws_stop.set()
            self._ws_thread = None
            self._ws_tokens = ()
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)

    def _market_stream_loop(self, tokens: tuple[str, ...]) -> None:
        try:
            import websocket  # type: ignore[import-not-found]
        except Exception as exc:
            with self._ws_lock:
                self._ws_error = f"websocket-client unavailable: {exc}"
            return
        subscribe_payload = json.dumps(
            {
                "assets_ids": list(tokens),
                "type": "market",
                "custom_feature_enabled": True,
            }
        )
        while not self._ws_stop.is_set():
            ws = None
            try:
                ws = websocket.create_connection(MARKET_WS_URL, timeout=10)
                ws.settimeout(5)
                ws.send(subscribe_payload)
                with self._ws_lock:
                    self._ws_error = None
                while not self._ws_stop.is_set():
                    try:
                        raw = ws.recv()
                    except Exception:
                        if self._ws_stop.is_set():
                            break
                        raise
                    try:
                        parsed = json.loads(raw)
                    except (TypeError, json.JSONDecodeError):
                        continue
                    self.apply_market_channel_message(parsed)
            except Exception as exc:
                with self._ws_lock:
                    self._ws_error = str(exc)
                logger.debug("shadow market websocket reconnecting after error: %s", exc)
                if self._ws_stop.wait(5.0):
                    break
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass

    def _apply_intent_rehearsal_fill(self, order_id: int, intent: ShadowOrderIntent) -> dict[str, Any] | None:
        status = str(intent.simulated_fill_status or "")
        if status not in {"full_fill", "partial_fill"}:
            return None
        shares = _as_float(intent.simulated_fill_shares)
        price = _as_float(intent.simulated_avg_fill_price)
        if shares is None or price is None or shares <= 0:
            return None
        return self.tracker.record_shadow_exec_fill(
            int(order_id),
            price=price,
            shares=shares,
            liquidity_source="immediate_book",
            evidence={
                "simulated_fill_status": status,
                "execution_checked_at": intent.execution_checked_at,
                "source": "shadow_fill_rehearsal",
            },
            filled_at=intent.execution_checked_at or intent.created_at,
        )

    def _execution_priced_intent(self, intent: ShadowOrderIntent) -> ShadowOrderIntent:
        if str(intent.intent_kind or "").lower() != "entry":
            return intent
        if not self.config.entry_price_improvement_enabled:
            return intent
        original_target = _bounded_price(intent.target_price)
        if original_target is None or original_target <= 0:
            return intent
        payload = dict(intent.payload or {})
        forecast_contract_prob = _contract_probability(intent.outcome_side or intent.direction, payload.get("forecast_prob"))
        if forecast_contract_prob is None:
            return intent
        edge_floor = _as_float(payload.get("entry_edge_floor"))
        if edge_floor is None:
            edge_floor = float(self.config.entry_min_edge_abs)
        max_limit = _bounded_price(forecast_contract_prob - max(0.0, edge_floor))
        if max_limit is None or max_limit <= original_target:
            return intent
        adjusted_target = max(original_target, max_limit)
        requested_notional = max(0.0, float(intent.notional_usd or 0.0))
        adjusted_shares = float(intent.shares or 0.0)
        if requested_notional > 0 and adjusted_target > 0:
            adjusted_shares = round(requested_notional / adjusted_target, 6)
        pricing = {
            "kind": "entry_bid_improvement",
            "enabled": True,
            "original_target_price": original_target,
            "adjusted_target_price": adjusted_target,
            "forecast_contract_probability": forecast_contract_prob,
            "preserved_edge_abs": round(forecast_contract_prob - adjusted_target, 6),
            "entry_edge_floor": edge_floor,
            "original_requested_shares": float(intent.shares or 0.0),
            "adjusted_requested_shares": adjusted_shares,
            "requested_notional_usd": requested_notional,
            "reason": "Lifted the executable shadow bid only to the model edge cap.",
        }
        payload["shadow_execution_pricing"] = pricing
        return replace(
            intent,
            target_price=adjusted_target,
            shares=adjusted_shares,
            payload=payload,
        )

    def _apply_order_repricing(self, order: dict[str, Any], *, now: str | None = None) -> dict[str, Any]:
        if not self.config.exit_repricing_enabled:
            return order
        if str(order.get("intent_kind") or "").lower() != "exit":
            return order
        if str(order.get("order_action") or "").upper() != "SELL":
            return order
        current_target = _bounded_price(order.get("target_price"))
        original_target = _original_order_target_price(order)
        if current_target is None or original_target is None:
            return order
        age_seconds = _order_age_seconds(order, now=now)
        if age_seconds is None or age_seconds < self.config.exit_ladder_step_seconds:
            return order
        reason_code = _order_reason_code(order)
        steps = self.config.exit_urgent_concession_steps if reason_code in self.config.exit_urgent_reason_codes else self.config.exit_concession_steps
        if not steps:
            return order
        step_index = min(len(steps), int(age_seconds // self.config.exit_ladder_step_seconds)) - 1
        if step_index < 0:
            return order
        concession = max(0.0, float(steps[step_index]))
        adjusted_target = _bounded_price(original_target - concession)
        if adjusted_target is None or adjusted_target >= current_target:
            return order
        evidence = {
            "kind": "exit_sell_ladder",
            "original_target_price": original_target,
            "previous_target_price": current_target,
            "adjusted_target_price": adjusted_target,
            "concession": round(concession, 6),
            "age_seconds": round(age_seconds, 3),
            "ladder_step_seconds": int(self.config.exit_ladder_step_seconds),
            "reason_code": reason_code,
            "urgent_ladder": reason_code in self.config.exit_urgent_reason_codes,
        }
        updated = self.tracker.update_shadow_exec_order_target(
            int(order["id"]),
            target_price=adjusted_target,
            reason="exit_sell_ladder",
            evidence=evidence,
            updated_at=now,
        )
        return updated or {**order, "target_price": adjusted_target}

    def _apply_book_cross_fill(self, order: dict[str, Any], *, liquidity_source: str) -> dict[str, Any] | None:
        token = str(order.get("clob_token_id") or "").strip()
        if not token:
            return None
        try:
            book = self.book_fetcher(token)
        except Exception as exc:
            return self._mark_order_checked(order, error=str(exc))
        fill = _simulate_order_fill_from_book(order, book)
        if fill["shares"] <= 0:
            return self._mark_order_checked(order, evidence={"book_hash": fill.get("book_hash")})
        return self.tracker.record_shadow_exec_fill(
            int(order["id"]),
            price=float(fill["avg_price"]),
            shares=float(fill["shares"]),
            liquidity_source=liquidity_source,
            evidence=fill,
            filled_at=iso_now(),
        )

    def _mark_order_checked(
        self,
        order: dict[str, Any],
        *,
        error: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> None:
        # The tracker intentionally owns durable state; no-op here keeps the
        # fallback poller from fabricating fills when the book is not executable.
        return None

    def _build_taker_estimate(self, intent: ShadowOrderIntent) -> dict[str, Any] | None:
        if not self.config.show_taker_exit_estimate or str(intent.clob_token_id or "").strip() == "":
            return None
        try:
            book = self.book_fetcher(str(intent.clob_token_id))
        except Exception as exc:
            return {"error": str(exc)}
        estimate = _simulate_taker_fill(intent, book)
        if estimate is None:
            return None
        if str(intent.intent_kind or "").lower() == "exit":
            entry_cost = _as_float((intent.payload or {}).get("cost"))
            if entry_cost is not None:
                estimate["estimated_pnl"] = round(float(estimate.get("notional_usd") or 0.0) - entry_cost, 6)
        return estimate


def _simulate_order_fill_from_book(order: dict[str, Any], book: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(book, dict):
        return {"shares": 0.0, "avg_price": None, "book_empty": True}
    is_buy = str(order.get("order_action") or "").upper() == "BUY"
    target = _bounded_price(order.get("target_price"))
    levels = _sorted_levels(book.get("asks") if is_buy else book.get("bids"), reverse=not is_buy)
    executable = [level for level in levels if _level_crosses(level["price"], target, is_buy=is_buy)]
    return {
        **_fill_from_levels(executable, requested_shares=float(order.get("unfilled_shares") or 0.0)),
        "book_hash": str(book.get("hash") or ""),
        "book_timestamp": str(book.get("timestamp") or ""),
    }


def _simulate_taker_fill(intent: ShadowOrderIntent, book: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(book, dict):
        return None
    is_buy = str(intent.order_action or "").upper() == "BUY"
    levels = _sorted_levels(book.get("asks") if is_buy else book.get("bids"), reverse=not is_buy)
    fill = _fill_from_levels(levels, requested_shares=float(intent.shares or 0.0))
    if fill["shares"] <= 0:
        return {"fill_shares": 0.0, "avg_fill_price": None, "notional_usd": 0.0}
    return {
        "fill_shares": fill["shares"],
        "avg_fill_price": fill["avg_price"],
        "notional_usd": fill["notional_usd"],
        "book_hash": str(book.get("hash") or ""),
        "book_timestamp": str(book.get("timestamp") or ""),
    }


def _trade_qualifies_order(order: dict[str, Any], *, trade_side: str, trade_price: float) -> bool:
    order_action = str(order.get("order_action") or "").upper()
    target = _bounded_price(order.get("target_price"))
    if target is None:
        return False
    if order_action == "BUY":
        return trade_side == "SELL" and trade_price <= target
    if order_action == "SELL":
        return trade_side == "BUY" and trade_price >= target
    return False


def fetch_polymarket_market_trades(condition_id: str, limit: int = 1000) -> list[dict[str, Any]]:
    condition_id = str(condition_id or "").strip()
    if not condition_id:
        return []
    response = requests.get(
        POLYMARKET_DATA_API_TRADES_URL,
        params={
            "market": condition_id,
            "limit": max(1, min(10000, int(limit or 1000))),
            "offset": 0,
            "takerOnly": "true",
        },
        timeout=5,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, list) else []


def _normalize_trade_tape_events(trades: Any) -> list[dict[str, Any]]:
    if not isinstance(trades, list):
        return []
    normalized: list[dict[str, Any]] = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        token = str(trade.get("asset") or trade.get("asset_id") or trade.get("clob_token_id") or "").strip()
        side = str(trade.get("side") or "").strip().upper()
        price = _bounded_price(trade.get("price"))
        size = _as_float(trade.get("size"))
        timestamp = _trade_timestamp_seconds(trade.get("timestamp") or trade.get("trade_timestamp"))
        if not token or side not in {"BUY", "SELL"} or price is None or size is None or size <= 0 or timestamp <= 0:
            continue
        tx_hash = str(trade.get("transactionHash") or trade.get("transaction_hash") or "").strip()
        event_uid = "|".join(
            [
                token,
                side,
                f"{price:.6f}",
                f"{float(size):.6f}",
                str(timestamp),
                tx_hash,
            ]
        )
        normalized.append(
            {
                "event_uid": event_uid,
                "clob_token_id": token,
                "side": side,
                "price": price,
                "size": round(float(size), 6),
                "trade_timestamp": timestamp,
                "transaction_hash": tx_hash,
                "raw": trade,
            }
        )
    return normalized


def _original_order_target_price(order: dict[str, Any]) -> float | None:
    payload = order.get("payload") if isinstance(order.get("payload"), dict) else {}
    pricing = payload.get("shadow_execution_repricing") if isinstance(payload.get("shadow_execution_repricing"), dict) else {}
    if pricing.get("original_target_price") is not None:
        return _bounded_price(pricing.get("original_target_price"))
    shadow_intent = payload.get("shadow_intent") if isinstance(payload.get("shadow_intent"), dict) else {}
    original = _bounded_price(
        (shadow_intent.get("payload") or {}).get("shadow_execution_pricing", {}).get("original_target_price")
        if isinstance(shadow_intent.get("payload"), dict)
        else None
    )
    if original is not None:
        return original
    return _bounded_price(shadow_intent.get("target_price") if shadow_intent else order.get("target_price"))


def _order_reason_code(order: dict[str, Any]) -> str:
    payload = order.get("payload") if isinstance(order.get("payload"), dict) else {}
    shadow_intent = payload.get("shadow_intent") if isinstance(payload.get("shadow_intent"), dict) else {}
    return str(shadow_intent.get("reason_code") or "").strip()


def _order_age_seconds(order: dict[str, Any], *, now: str | None = None) -> float | None:
    created = _parse_iso_datetime(order.get("created_at"))
    if created is None:
        return None
    current = _parse_iso_datetime(now) if now else datetime.now(timezone.utc)
    if current is None:
        current = datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return max(0.0, (current.astimezone(timezone.utc) - created.astimezone(timezone.utc)).total_seconds())


def _contract_probability(outcome_side: Any, forecast_probability: Any) -> float | None:
    probability = _bounded_price(forecast_probability)
    if probability is None:
        return None
    return probability if str(outcome_side or "").upper() == "YES" else round(1.0 - probability, 6)


def _fill_from_levels(levels: list[dict[str, float]], *, requested_shares: float) -> dict[str, Any]:
    remaining = max(0.0, requested_shares)
    filled = 0.0
    notional = 0.0
    for level in levels:
        if remaining <= 0:
            break
        take = min(remaining, max(0.0, float(level.get("size") or 0.0)))
        price = _bounded_price(level.get("price"))
        if price is None or take <= 0:
            continue
        filled += take
        notional += take * price
        remaining -= take
    filled = round(filled, 6)
    notional = round(notional, 6)
    return {
        "shares": filled,
        "avg_price": round(notional / filled, 6) if filled > 0 else None,
        "notional_usd": notional,
    }


def _level_crosses(price: float, target: float | None, *, is_buy: bool) -> bool:
    if target is None:
        return False
    return price <= target if is_buy else price >= target


def _sorted_levels(levels: Any, *, reverse: bool) -> list[dict[str, float]]:
    normalized: list[dict[str, float]] = []
    if not isinstance(levels, list):
        return normalized
    for item in levels:
        if not isinstance(item, dict):
            continue
        price = _bounded_price(item.get("price"))
        size = _as_float(item.get("size"))
        if price is None or size is None or size <= 0:
            continue
        normalized.append({"price": price, "size": round(size, 6)})
    normalized.sort(key=lambda item: item["price"], reverse=reverse)
    return normalized


def _float_sequence(value: Any) -> tuple[float, ...]:
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple)):
        parts = list(value)
    else:
        parts = []
    values: list[float] = []
    for part in parts:
        parsed = _as_float(part)
        if parsed is None or parsed < 0:
            continue
        values.append(round(parsed, 6))
    return tuple(values)


def _string_sequence(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple, set)):
        parts = [str(part).strip() for part in value]
    else:
        parts = []
    return tuple(part for part in parts if part)


def _bounded_price(value: Any) -> float | None:
    raw = _as_float(value)
    if raw is None:
        return None
    return round(max(0.0, min(1.0, raw)), 6)


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp_ms_to_iso(value: Any) -> str | None:
    raw = _as_float(value)
    if raw is None or raw <= 0:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc).isoformat()


def _trade_timestamp_seconds(value: Any) -> int:
    raw = _as_float(value)
    if raw is None or raw <= 0:
        return 0
    if raw > 10_000_000_000:
        raw = raw / 1000.0
    return int(raw)


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
