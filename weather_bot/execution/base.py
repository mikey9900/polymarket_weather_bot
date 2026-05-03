"""Execution mode normalization and capability checks."""

from __future__ import annotations

PAPER_EXECUTION_MODE = "paper"
PAPER_SHADOW_EXECUTION_MODE = "paper_shadow"


def normalize_execution_mode(value: object) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace("+", "_")
    if raw in {"paper_shadow", "paper__shadow", "shadow", "shadow_live", "shadowlive"}:
        return PAPER_SHADOW_EXECUTION_MODE
    return PAPER_EXECUTION_MODE


def execution_mode_records_shadow_orders(value: object) -> bool:
    return normalize_execution_mode(value) == PAPER_SHADOW_EXECUTION_MODE


def execution_mode_creates_paper_positions(value: object) -> bool:
    return normalize_execution_mode(value) in {PAPER_EXECUTION_MODE, PAPER_SHADOW_EXECUTION_MODE}


def execution_mode_label(value: object) -> str:
    mode = normalize_execution_mode(value)
    if mode == PAPER_SHADOW_EXECUTION_MODE:
        return "PAPER+SHADOW"
    return "PAPER"
