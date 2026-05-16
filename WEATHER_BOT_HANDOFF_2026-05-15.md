# Weather Bot Handoff - 2026-05-15

This is the current working handoff for moving the Polymarket weather bot work to another PC.

Local context when written:

- Repo: `mikey9900/polymarket_weather_bot`
- Branch: `main`
- Working directory used in this chat: `C:\polymarket_weather_bot`
- Bot timezone used for trade analysis: `America/Edmonton`
- Latest bundle analyzed: `WEATHER-BOT_latest_bundle (11).zip`
- Current strategy stance: **do not change strategy yet**

## Current State

The scanner/trading-stuck issue appears fixed. Bundle `(11)` showed live scans running, temperature signals arriving, and trades opening again.

Latest bundle `(11)` summary for May 15 MDT:

- `142` paper trade attempts
- `84` realistic entry fills
- `58` unfilled realistic entries
- Paper realized P/L: about `-$12.19`
- Realistic shadow realized P/L: about `-$12.19`
- Realistic shadow unrealized P/L: about `-$17.37`
- Realistic shadow total P/L: about `-$29.56`
- Open exposure at bundle time: about `$361`

The day was not mostly an execution miss. It was mostly a forecast/cluster-risk day:

- May 17 markets caused almost all the damage: about `-$28.44`
- May 16 markets were about flat/slightly positive
- May 17 `YES` temperature-tail trades were the biggest weakness
- Wide forecast dispersion and city clusters hurt: Denver, Atlanta, Dallas, and San Francisco did most of the damage

Do **not** implement strategy filters yet unless we explicitly decide to. Candidate future controls are documented below.

## What Was Implemented In This Work Session

### 1. Realistic Shadow Trade-Tape Fallback

The realistic shadow ledger now has a REST trade-tape fallback using Polymarket market trade history.

Purpose:

- Keep realistic shadow P/L closer to real-money execution.
- Catch fills that the WebSocket might miss.
- Prevent resting orders from being marked expired if public Polymarket trades prove they should have filled during TTL.

Core behavior:

- Every active realistic shadow order carries a `condition_id`.
- Before expiring resting orders, the shadow execution loop fetches recent Polymarket trades for each active condition.
- Trade events are deduped and stored.
- Existing conservative fill rules are reused:
  - resting `BUY` fills only from later public `SELL` trades at or below the limit
  - resting `SELL` fills only from later public `BUY` trades at or above the limit
  - `queue_fill_fraction` still applies, currently `50%`
- Duplicate REST results cannot double-fill an order.
- Trades after order expiry do not fill the order.

New/updated tracker tables:

- `shadow_exec_orders.condition_id`
- `shadow_exec_trade_events`
- `shadow_exec_trade_cursors`

New fill source labels:

- `rest_trade_tape`
- `market_websocket`
- existing immediate/book fill sources remain

Main files:

- `weather_bot/execution/shadow_execution.py`
- `weather_bot/tracker.py`
- `weather_bot/runtime.py`
- `weather_bot/control_plane.py`
- `weather_bot/dashboard_state.py`
- `weather_bot/analysis_bundle.py`
- `weather_bot/analysis_report.py`

### 2. Dashboard / Export Coverage

The analysis bundle and dashboard now expose the trade-tape evidence.

New bundle files:

- `shadow_exec_trade_events.json`
- `shadow_exec_trade_cursors.json`

New analysis workbook sheet:

- `Shadow Exec Trades`

New summary fields include:

- `trade_event_count`
- `trade_cursor_count`
- `rest_trade_tape_fill_count`
- `market_websocket_fill_count`
- `trade_tape_rescued_order_count`
- `last_trade_event_at`
- `fills_by_source`

### 3. Paper Open-Cap Persistence

Earlier in the session we found the runtime was using `paper_max_open_positions: 60`, but the exported active config still showed `max_open_positions: 10`.

This has been fixed so the open-position cap can persist through Home Assistant options and env vars.

Added:

- HA option: `paper_max_open_positions`
- env override: `WEATHER_PAPER_MAX_OPEN_POSITIONS`
- config loader mapping into `paper.max_open_positions`

Files:

- `weather_bot/config.py`
- `weather-bot/config.yaml`
- `weather-codex/config.yaml`
- `tests/test_runtime_dashboard.py`

Current add-on versions in this handoff:

- `weather-bot`: `3.3.22`
- `weather-codex`: `0.3.21`

## Important Recent Milestones From This Chat

These are the larger milestones from the broader thread, including changes that were committed before this handoff:

- Same-day temperature risk gate:
  - same-day temp entries require `20%` edge unless manual floor is higher
  - same-day collapse exit only applies to positions opened on the event date
- Forecast-aware NO stop-loss:
  - NO exits now compare bad price action against deteriorated forecast/model probability
- Two-bad-reviews behavior:
  - added additional exit confirmation behavior before acting on repeated bad reviews
- `BUNDLE DATA` export label:
  - export button label was changed to match the bundle function
- Shadow P/L dashboard:
  - added realistic shadow execution/P&L tracking and dashboard sections
- Resolved/expired market handling:
  - old resolved markets now settle instead of waiting forever for exit liquidity
  - May 10-style resolved positions should not remain stuck
- Manual sell fix:
  - manual sell path was repaired so user-triggered exits mirror into tracking correctly
- Scanner missing-slug cache fix:
  - current/future missing temperature slugs are not cached permanently
  - this fixed the no-trades-after-temp-sweep issue

## Findings From The Latest Trade Analysis

We checked whether paper entries missed good realistic fills by searching Polymarket trade history.

Result:

- May 15 MDT had `0` good paper winners in the unfilled realistic-entry bucket.
- Across the current execution ledger, good no-fill misses were not the issue.
- Older legacy missed rows had a few naive paper winners, but they were not fillable inside the 30-minute TTL at the bot's entry price.

Interpretation:

- The shadow ledger was not leaving big winners on the table.
- The day was mostly a real market/forecast repricing problem, not an execution-tracking illusion.
- The new trade-tape fallback is still useful because it closes small WebSocket/polling gaps and gives stronger audit evidence.

## Candidate Strategy Changes - Not Implemented Yet

Do not add these until we explicitly decide to change strategy.

Possible future risk controls based on May 15:

- Stricter gate for future-date temp `YES` tail trades, especially `36h+` away.
- Reduce/block entries when source dispersion is `>=10%`, especially for `YES`.
- Same-market re-entry cooldown to avoid repeated fills on the same market.
- Cluster cap by `city + event_date + direction` so one bad forecast cluster cannot stack losses.

Why not implement yet:

- We need a few more trading days with realistic execution data.
- May 15 may be a bad forecast/regime day rather than proof of a bad rule.
- The stated goal right now is to get shadow tracking close to real trading before putting real money behind it.

## Verification

Full test suite passed after the current changes:

```text
.\.venv\Scripts\python.exe -m pytest -q
173 passed in 33.13s
```

Focused sets also passed:

```text
.\.venv\Scripts\python.exe -m pytest tests/test_shadow_execution.py tests/test_runtime_dashboard.py -q
100 passed
```

## Pulling On Another PC

On the other PC:

```powershell
git clone https://github.com/mikey9900/polymarket_weather_bot.git
cd polymarket_weather_bot
git pull origin main
```

If the repo already exists:

```powershell
cd C:\path\to\polymarket_weather_bot
git checkout main
git pull origin main
```

Then read this file first:

```text
WEATHER_BOT_HANDOFF_2026-05-15.md
```

Recommended sanity check:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## Continue From Here

Suggested next steps after pulling:

1. Confirm Home Assistant is running the updated add-on version.
2. Confirm `paper_execution_mode` is still `paper_shadow`.
3. Confirm `paper_max_open_positions` is set to the intended cap.
4. Let the bot collect more realistic shadow execution data.
5. Review the next bundle before making strategy changes.
6. Only then decide whether to add cluster caps, dispersion gates, or future-date YES-tail filters.
