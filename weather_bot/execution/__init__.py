"""Execution-mode helpers for paper, shadow, and future live adapters."""

from .base import (
    PAPER_EXECUTION_MODE,
    PAPER_SHADOW_EXECUTION_MODE,
    execution_mode_creates_paper_positions,
    execution_mode_label,
    execution_mode_records_shadow_orders,
    normalize_execution_mode,
)

__all__ = [
    "PAPER_EXECUTION_MODE",
    "PAPER_SHADOW_EXECUTION_MODE",
    "execution_mode_creates_paper_positions",
    "execution_mode_label",
    "execution_mode_records_shadow_orders",
    "normalize_execution_mode",
]
