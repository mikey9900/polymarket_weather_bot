# Phase 1 Shadow-Live Handoff

Updated: 2026-05-02

## What Shipped

This handoff covers the new Phase 1 live-trading scaffolding on top of the existing paper bot.

Current mode support:

- `paper`
- `paper_shadow`

`paper_shadow` keeps normal paper trading active and mirrors accepted paper entries and exits into a shadow-live intent ledger. No real orders are sent yet.

## Core Behavior

Paper behavior is intentionally unchanged in this phase:

- accepted entries still create normal paper positions
- paper bankroll, PnL, open positions, and exits still work the same
- the new shadow mirror logs only when the paper trade actually happened

What is new:

- execution mode setting in config and HA
- shadow order intent models
- shadow entry mirroring on accepted entries
- shadow exit mirroring on manual close and review-driven close
- dashboard controls and telemetry for shadow-live

## Main Files

Execution-mode scaffolding:

- [weather_bot/execution/base.py](C:/Users/micha/polymarket_weather_bot/weather_bot/execution/base.py)
- [weather_bot/execution/models.py](C:/Users/micha/polymarket_weather_bot/weather_bot/execution/models.py)
- [weather_bot/execution/__init__.py](C:/Users/micha/polymarket_weather_bot/weather_bot/execution/__init__.py)

Strategy / runtime / storage:

- [weather_bot/strategy.py](C:/Users/micha/polymarket_weather_bot/weather_bot/strategy.py)
- [weather_bot/runtime.py](C:/Users/micha/polymarket_weather_bot/weather_bot/runtime.py)
- [weather_bot/tracker.py](C:/Users/micha/polymarket_weather_bot/weather_bot/tracker.py)

HA / dashboard / snapshot:

- [weather_bot/control_plane.py](C:/Users/micha/polymarket_weather_bot/weather_bot/control_plane.py)
- [weather_bot/dashboard_state.py](C:/Users/micha/polymarket_weather_bot/weather_bot/dashboard_state.py)
- [weather_bot/live_api_dashboard.html](C:/Users/micha/polymarket_weather_bot/weather_bot/live_api_dashboard.html)
- [weather_bot/config.py](C:/Users/micha/polymarket_weather_bot/weather_bot/config.py)
- [weather_bot/config.default.yaml](C:/Users/micha/polymarket_weather_bot/weather_bot/config.default.yaml)
- [weather-bot/config.yaml](C:/Users/micha/polymarket_weather_bot/weather-bot/config.yaml)

Tests:

- [tests/test_tracker_strategy.py](C:/Users/micha/polymarket_weather_bot/tests/test_tracker_strategy.py)
- [tests/test_runtime_dashboard.py](C:/Users/micha/polymarket_weather_bot/tests/test_runtime_dashboard.py)

## New Config

HA / config now supports:

```yaml
paper_execution_mode: "paper"
```

Supported values right now:

- `paper`
- `paper_shadow`

## What `paper_shadow` Does

When `paper_execution_mode: "paper_shadow"`:

- paper entries still happen normally
- a matching shadow entry intent is recorded
- manual closes record a matching shadow exit intent
- review-driven closes record a matching shadow exit intent
- control payload exposes execution mode and shadow counts
- dashboard snapshot exposes recent shadow intents

## New Shadow Data

New DB table:

- `shadow_order_intents`

Stored summary data currently includes:

- entry vs exit counts
- execution mode
- market / city / event / direction
- target and reference price
- shares and notional
- decision score
- reason and reason code
- payload JSON with additional signal context

Dashboard state now includes:

- `controls.paper_execution_mode`
- `controls.shadow_order_count`
- `controls.shadow_entry_count`
- `controls.shadow_exit_count`
- `controls.last_shadow_order_at`
- `recent_shadow_orders`

## Dashboard / HA Changes

In the `STRATEGIES` block:

- new `EXEC MODE` selector
- current execution-mode readout
- shadow intent counts

This is phase one only. It is still not a live executor.

## Verification

Passed before push:

```text
python -m pytest -q tests/test_tracker_strategy.py tests/test_runtime_dashboard.py
92 passed in 11.98s

python -m pytest -q tests/test_ha_version_guard.py
6 passed in 0.20s
```

## Add-on Versions

This handoff ships with:

- weather bot add-on: `3.3.7`
- weather codex add-on: `0.3.7`

## What Did Not Ship Yet

Not in this phase:

- real venue auth
- real order placement
- manual-confirm live mode
- auto-live mode
- reconciliation against a real trading account
- dedicated shadow-order card/panel in the UI
- bundle/report export sections specifically for shadow intents

## Recommended Next Steps

Best next steps after this pull:

1. Update both add-ons in HA.
2. Switch execution mode to `paper_shadow`.
3. Let it run and collect shadow intent telemetry.
4. Add a dedicated shadow-order dashboard panel.
5. Add bundle/report export coverage for `shadow_order_intents`.
6. Build Phase 2 manual-confirm live scaffolding after that.

## Pull-On-Other-PC Notes

On the other PC:

1. Pull `main`.
2. Confirm this file exists:
   - [PHASE1_SHADOW_LIVE_HANDOFF.md](C:/Users/micha/polymarket_weather_bot/PHASE1_SHADOW_LIVE_HANDOFF.md)
3. Read this file first.
4. Then inspect the files in `weather_bot/execution/` and the strategy/runtime/tracker changes.

Important:

- scratch folders like `codex_*` and `tmp_bundle_analysis/` were intentionally not part of the commit
- this push is meant to be a clean repo state for continuing on another machine
