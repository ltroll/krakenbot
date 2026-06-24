# Kraken Bot

This repository is a small trading-bot workspace centered around Kraken BTC/USD automation.
The project is built around a simple pattern:

1. Environment variables in `.env` provide secrets, URLs, and runtime file locations.
2. JSON config files tune each bot's trading behavior.
3. Bot scripts fetch price and sentiment inputs, make trade decisions, and write state and logs locally.
4. Optional support tools inspect logs or summarize trades.

The active bot you have been working on is [`range_grid_bot.py`](/C:/Users/bgert/krakenbot/range_grid_bot.py).

## Repository layout

Core bot files:

- [`range_grid_bot.py`](/C:/Users/bgert/krakenbot/range_grid_bot.py): the current range/grid bot using recent BTC price range plus external sentiment.
- [`range_grid_bot_average.py`](/C:/Users/bgert/krakenbot/range_grid_bot_average.py): test/alternate range-grid variant.
- [`kraken_sentiment_executor.py`](/C:/Users/bgert/krakenbot/kraken_sentiment_executor.py): allocation-style sentiment trader.
- [`stats_trend_bot.py`](/C:/Users/bgert/krakenbot/stats_trend_bot.py): stats/trend trader that derives entries from price history instead of sentiment.
- [`competition_shadow_bot.py`](/C:/Users/bgert/krakenbot/competition_shadow_bot.py): shadow-only NEO competition decision monitor.
- [`kraken_bot.py`](/C:/Users/bgert/krakenbot/kraken_bot.py): older/general Kraken bot logic.
- [`account_value_usd.py`](/C:/Users/bgert/krakenbot/account_value_usd.py): account value helper.

Config and state:

- `range_grid_config.json`: tuning values for the range-grid bot.
- `stats_trend_strategy_default.json`: default dry-run stats/trend strategy profile.
- `bot_config.json`, `bot_config_prod.json`, `bot_config_experimental1.json`, `sentiment_bot_config.json`: tuning profiles for other bots.
- `.env`: secrets, URLs, and path overrides.
- `last_state.json` or the file pointed to by `BOT_STATE_FILE`: persisted bot state.

Logs and observability:

- `trade_log.jsonl` or the file pointed to by `TRADE_LOG_FILE`: structured bot event log.
- [`log_viewer.py`](/C:/Users/bgert/krakenbot/log_viewer.py): CLI log viewer for JSONL logs.

LLM and notifications:

- [`llm_trade_summary.py`](/C:/Users/bgert/krakenbot/llm_trade_summary.py): sends LLM-generated trade summaries to Discord.
- [`llm_hourly_trade_report.py`](/C:/Users/bgert/krakenbot/llm_hourly_trade_report.py): reporting helper.
- [`discord_notify.py`](/C:/Users/bgert/krakenbot/discord_notify.py): Discord webhook sender.
- [`llama3.py`](/C:/Users/bgert/krakenbot/llama3.py): local LLM wrapper.

Operations:

- [`requirements.txt`](/C:/Users/bgert/krakenbot/requirements.txt): Python dependencies.
- [`update_all_bots.sh`](/C:/Users/bgert/krakenbot/update_all_bots.sh): multi-bot update helper.
- [`SupportFilesSummarization.txt`](/C:/Users/bgert/krakenbot/SupportFilesSummarization.txt): notes describing support-file conventions and expected logging format.

## How configuration works

The repo intentionally splits runtime configuration into two layers.

`.env` is for operational concerns:

- API keys and secrets
- Kraken endpoints
- external support file URLs
- filenames and file paths
- per-bot strategy file selectors

Strategy JSON files are for bot-function tuning:

- thresholds
- position sizing
- grid depth
- profit targets
- refresh/lookback windows

That separation lets you tune behavior without hardcoding secrets into scripts, and it lets you move or clone bots with different support files by changing only environment variables.

### Example `.env`

Based on the support-file notes, a typical `.env` looks like this:

```env
KRAKEN_API_KEY=
KRAKEN_API_SECRET=
KRAKEN_API_URL=https://api.kraken.com
KRAKEN_TICKER_URL="https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD"
KRAKEN_ORDERBOOK_URL="https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=5"
KRAKEN_OHLC_URL="https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60"
LLM_SIGNAL_URL=http://192.168.50.211/bot/llm_signal.json
PRICE_LOG_URL=http://192.168.50.211/bot/btc_price_log.jsonl
BOT_CONFIG_FILE=range_grid_config.json
BOT_STATE_FILE=last_state.json
TRADE_LOG_FILE=trade_log.jsonl
BOT_DIR_LIST_FILE=/home/losttroll/tradingbot/bot_dirs.txt

RANGE_GRID_CONFIG_FILE=range_grid_config.json
RANGE_GRID_STATE_FILE=last_state.json
RANGE_GRID_TRADE_LOG_FILE=trade_log.jsonl
RANGE_GRID_STRATEGY_PROFILE=range_grid_strategy_default.json

SENTIMENT_CONFIG_FILE=sentiment_bot_config.json
SENTIMENT_STATE_FILE=sentiment_state.json
SENTIMENT_TRADE_LOG_FILE=sentiment_trade_log.jsonl
SENTIMENT_DECISION_CSV_FILE=sentiment_decisions.csv
SENTIMENT_STRATEGY_PROFILE=sentiment_strategy_default.json

KRAKEN_ORDERBOOK_URL="https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=50"
STATS_TREND_STATE_FILE=stats_trend_state.json
STATS_TREND_TRADE_LOG_FILE=stats_trend_trade_log.jsonl
STATS_TREND_DECISION_CSV_FILE=stats_trend_decisions.csv
STATS_TREND_STRATEGY_PROFILE=stats_trend_strategy_default.json
STATS_TREND_DRY_RUN=true
STATS_TREND_BACKTEST_MODE=false
STATS_TREND_BACKTEST_STEP_PATH=/admin/api/backtest/step
STATS_TREND_BACKTEST_STEPS_PER_CYCLE=1
STATS_TREND_BACKTEST_USE_API_MARKET_DATA=true
STATS_TREND_MARKET_HISTORY_SOURCE=price_log
COMPETITION_DECISION_URL=http://192.168.50.211/bot/competition_decision.json
COMPETITION_CONFIG_FILE=competition_bot_config.json
COMPETITION_TRADE_LOG_FILE=competition_shadow_trade_log.jsonl
COMPETITION_DECISION_CSV_FILE=competition_shadow_decisions.csv
COMPETITION_POLL_INTERVAL_SECONDS=60
COMPETITION_BACKTEST_SNAPSHOT_FILE=competition_backtest_snapshot_log.jsonl
COMPETITION_BACKTEST_OUTPUT_FILE=competition_backtest.json
COMPETITION_BACKTEST_WINDOW_HOURS=24
KRAKEN_PAIR=XXBTZUSD
SIGNAL_FILE=
REQUEST_TIMEOUT_SECONDS=10
KRAKEN_NONCE_RETRIES=2
KRAKEN_LOCKOUT_COOLDOWN_SECONDS=300
DISCORD_WEB_HOOK=
```

Notes:

- Both `range_grid_bot.py` and `kraken_sentiment_executor.py` can now use the same `.env`.
- `RANGE_GRID_STRATEGY_PROFILE` and `SENTIMENT_STRATEGY_PROFILE` point directly to each bot's strategy JSON file.
- The generic `BOT_CONFIG_FILE`, `BOT_STATE_FILE`, `TRADE_LOG_FILE`, and `STRATEGY_PROFILE` keys are kept for compatibility with older scripts and single-bot setups.
- Strategy tunables such as `grid_anchor`, `entry_step_pct`, and profit targets now live in the selected strategy file instead of `.env`.

## Support data model

The bots depend on a small set of external support files.

### Price history

The support directory described in [`SupportFilesSummarization.txt`](/C:/Users/bgert/krakenbot/SupportFilesSummarization.txt) exposes recent BTC prices in either CSV or JSONL form.

The range-grid bot uses the JSONL form:

```json
{"timestamp": "2026-04-08T03:00:02.718311+00:00", "btc_price_usd": 71320}
```

The bot loads the file pointed to by `PRICE_LOG_URL`, filters rows to the last `range_window_hours`, and derives:

- `range_low`
- `range_high`
- `range_mean`
- `range_median`

These are persisted into bot state after each refresh.

### Sentiment signal

The sentiment file is expected to look like this:

```json
{
  "execution_signal": -0.0238,
  "confidence": 0.7,
  "processed_at": "2026-04-09T04:30:18.343283+00:00"
}
```

For the active range-grid bot, only `execution_signal` is currently used directly.

### Competition decision

The NEO competition lane reads the bot-facing guardrail file from `COMPETITION_DECISION_URL`, defaulting to:

```text
http://192.168.50.211/bot/competition_decision.json
```

Run one shadow check:

```bash
./venv/bin/python competition_shadow_bot.py --once
```

Run continuously:

```bash
./venv/bin/python competition_shadow_bot.py
```

The script only records decisions. When the file says `risk.shadow_only` is true, `decision == "shadow_candidate"` is logged as `record_tradeable_shadow_candidate`; blocked decisions are logged with their reason. If `status != "ok"`, the source is stale, or the file is not explicitly shadow-only, it logs `do_nothing`.

Capture one backtest snapshot:

```bash
./venv/bin/python capture_competition_snapshot.py
```

Replay the captured snapshots for P&L:

```bash
./venv/bin/python competition_backtest.py
```

The competition backtest compares two scenarios:

- `competition_allowed`: enters only when the engine file is fresh, `status == "ok"`, `risk.shadow_only == true`, and `decision == "shadow_candidate"`.
- `simulated_buy_allowed`: enters on any fresh `status == "ok"` snapshot, ignoring whether the competition guardrail decision blocked buying.

The replay uses one simulated position at a time, mid price when present, otherwise last price, fixed USD notional capped by `risk.max_position_usd`, and exits by take-profit, stop-loss, max hold, or final mark-to-market.

Execution and re-entry assumptions are configurable from `.env`:

```env
COMPETITION_BACKTEST_FILL_MODEL=taker
COMPETITION_BACKTEST_TAKER_FEE_BPS=40
COMPETITION_BACKTEST_MAKER_FEE_BPS=20
COMPETITION_BACKTEST_MAKER_ORDER_TIMEOUT_MINUTES=5
COMPETITION_BACKTEST_MAKER_CANCEL_ON_SIGNAL_BLOCK=true
COMPETITION_BACKTEST_MIN_AGGRESSION_SCORE=0.15
COMPETITION_BACKTEST_MIN_TRADE_COUNT=5
COMPETITION_BACKTEST_MIN_TOTAL_NOTIONAL_USD=1000
COMPETITION_BACKTEST_COOLDOWN_MINUTES=5
COMPETITION_BACKTEST_REQUIRE_SIGNAL_RESET=true
```

`taker` buys at the inferred ask and sells at the inferred bid. `maker` places a post-only bid and requires a later snapshot's trade price to touch that limit before counting an entry. Unfilled bids expire after the configured timeout and pending bids are cancelled when the entry signal blocks. A filled maker position rests its take-profit at the target price; stop-loss, timeout, and final mark-to-market exits cross the spread as takers and use half maker plus half taker round-trip fees. `mid` retains the original mid/last-price behavior. With signal reset enabled, `competition_allowed` must observe a blocked snapshot after an exit before it can enter again; the continuous-buy baseline remains available for comparison. Reports separate realized closed-trade P&L from unrealized mark-to-market P&L.

The report also includes `competition_directional`. This scenario requires the competition guardrails plus minimum aggression score, recent trade count, and recent trade notional. Each trade records its entry-time directional values so profitable and losing thresholds can be compared without changing the collector.

## How `range_grid_bot.py` works

This bot combines a recent BTC trading range with a sentiment gate.

### Startup

On startup it:

- loads `.env`
- selects the config JSON from `RANGE_GRID_CONFIG_FILE`, then `BOT_CONFIG_FILE`, then `range_grid_config.json`
- loads the strategy JSON from `RANGE_GRID_STRATEGY_PROFILE`
- loads prior state from `RANGE_GRID_STATE_FILE`, then `BOT_STATE_FILE`
- initializes the Kraken client
- queries Kraken `AssetPairs` to discover valid price and volume precision
- writes a `BOT_START` record to the trade log

### Main loop

The bot then loops forever with a 120-second sleep between cycles.

Each cycle:

1. Fetches the current BTC/USD price.
2. Fetches the current sentiment signal from `LLM_SIGNAL_URL`.
3. Logs a `SIGNAL_UPDATE` record.
4. Refreshes the observed range if the stored range is older than one hour.
5. Checks existing open buy orders to see whether any were filled.
6. Places matching sell orders at a profit target when a buy has filled.
7. If the sentiment signal is above the configured threshold, computes a grid of buy levels and places buys when price is at or below those levels.
8. If no action is taken, writes a `TRADE_DECISION` hold record explaining why.

### Grid anchor behavior

Grid levels are derived from:

- the observed price range
- the configured grid depth
- the chosen anchor point

The selected strategy profile's `grid_anchor` controls that anchor:

- `grid_anchor: "mean"`: levels are centered off the recent average price
- `grid_anchor: "median"`: levels are centered off the recent median price
- `grid_anchor: "high"`: buys only when market price is within `entry_step_pct` below the observed high
- `grid_anchor: "low"`: levels are built downward from the observed low

This is useful because anchor placement strongly affects how aggressively the bot averages into a range.

For the current mean-reversion buy model, the buy ladder is centered off the selected anchor. The first rung sits at `entry_step_pct` below that anchor, and each additional rung steps another `entry_step_pct` lower. The `high` anchor is intentionally different: it treats `entry_step_pct` as a tight band below the observed high, so with a high of `81000` and `entry_step_pct=0.005`, the bot can buy from `80595` through `81000` but skips prices below that band.

Why `median` can help:

- it is less sensitive to short-lived spikes or wicks in the recent price log
- it can keep the ladder closer to where price spent most of its time
- it may produce steadier entries than `mean` in noisy markets

### Strategy values from `range_grid_config.json`

Important values in the selected strategy file, such as [`range_grid_strategy_default.json`](/C:/Users/bgert/krakenbot/range_grid_strategy_default.json):

- `range_window_hours`: how much recent history is used for the observed range
- `max_grid_size`: number of buy levels
- `profit_target_pct`: sell markup after a filled buy
- `entry_step_pct`: spacing between buy levels below the selected anchor
- `llm_target_proximity_pct`: how close market price must be to an LLM-provided target before the bot will act on it
- `paper_trading_enabled`: logs approved buy orders without placing new Kraken buy orders
- `high_anchor_buy_cooldown_minutes`: minimum minutes between `grid_anchor: "high"` buys
- `max_open_high_anchor_orders`: cap on active high-anchor buys and sells
- `high_anchor_profit_target_pct`: profit target used for high-anchor buys before fees
- `round_trip_fee_pct`: fee allowance added to sell pricing
- `position_size_pct`: fraction of available USD allocated per buy level
- `execution_signal_threshold`: minimum signal required before placing new buys

### State file

The bot persists state in JSON so it can survive restarts without forgetting working orders.

State includes:

- `open_buy_orders`
- `open_sell_orders`
- `range_low`
- `range_high`
- `range_mean`
- `range_median`
- `last_range_refresh`

## How `stats_trend_bot.py` works

This bot does not read the sentiment/LLM signal. It reads the recent BTC price log from `PRICE_LOG_URL`, computes a statistical trend score, and buys only when price action is strong enough under the selected profile.

The default profile is intentionally `dry_run: true`. Its signal uses:

- fast moving average versus slow moving average
- recent momentum
- breakout distance versus the recent high
- range position inside the recent price window
- realized volatility as a dampener

The bot also reads the Kraken order book and scores several possible limit-buy entries below the current price. For each entry it builds a profit-target grid, defaulting to `0.005` through `0.015` in `0.001` steps, then estimates:

- probability price reaches the entry
- probability price reaches each configured exit target after that entry, including round-trip fee in the exit price
- joint probability of entry plus exit
- expected value after allowing for failed exits

The order-book probabilities are heuristic, not a market forecast with statistical guarantees. They are intended as a ranking/gating model over live liquidity, support, resistance, expected target distance, the configured exit horizon, and the trend signal.

The stats bot can also consume the optional `STATS_TREND_PRICE_INTELLIGENCE_URL` feed, currently pointed at `http://screenpi.local/bot/btc_price_intelligence.json` in `env.stats`. When the file is fresh, it adds a small regime-aware nudge from the 200-day SMA context, short/medium RSI, range position, volatility regime, recent momentum, and volume. This does not replace the order-book model; it slightly adjusts trend score, entry probability, exit probability, and expected value so the bot is more willing to buy oversold support in a constructive regime and more cautious when the broader setup is weak, stale, or illiquid.

The buy gate requires the best entry/exit pair to clear minimum entry probability, exit probability, and expected value thresholds, while also enforcing volatility, cooldown, inventory cap, daily buy cap, optional open-buy cap, open-sell cap, total open-order cap, and minimum order size checks. The bot places a limit buy at the selected entry, and after it fills, places a limit sell at the selected best exit.

`max_open_buy_orders_per_day` controls how many currently open buy orders may have been placed on the current UTC day. The default is `2`; if one of today's buy orders fills, cancels, or expires, that slot is freed and the bot may place another buy the same day. `max_open_buy_orders: 0` disables the separate all-days open-buy cap, while `max_open_orders` remains the hard resource ceiling across all open buy and sell orders.

Open buy orders are also rechecked while they wait to fill. `max_open_buy_age_minutes` cancels stale buy orders after the configured age, and `revalidate_open_buys` can cancel still-open buys when the current order-book model no longer clears the configured expected-value or exit-probability thresholds. Revalidation now waits for `open_buy_revalidation_grace_minutes` and will not cancel while market price remains within `open_buy_revalidation_hold_near_entry_pct` above the entry, so near-fills get room to complete. Buy orders store their predicted entry/exit probabilities and expected value at placement so later backtests can compare the prediction to the actual result.

If Kraken returns `EGeneral:Temporary lockout` on a private API call, the bot pauses private Kraken requests for `KRAKEN_LOCKOUT_COOLDOWN_SECONDS` and records `kraken_private_paused_until` in state. Public market-data checks can continue, but buy placement and balance/order private calls are skipped until the pause expires.

Runtime files use their own names by default:

- `stats_trend_state.json`
- `stats_trend_trade_log.jsonl`
- `stats_trend_decisions.csv`

Example launch:

```bash
set -a
source env.stats
set +a
python stats_trend_bot.py
```

### Backtesting

The stats/trend bot can run against a Kraken-compatible sandbox by switching on `STATS_TREND_BACKTEST_MODE`. In backtest mode, ticker, order book, pair metadata, balances, and order calls are derived from `KRAKEN_API_URL`; the bot ignores fixed real-Kraken ticker/depth URLs when `STATS_TREND_BACKTEST_USE_API_MARKET_DATA=true`.

`env.stats.backtest` sets `STATS_TREND_DRY_RUN=false` so orders go to the sandbox instead of the local dry-run simulator. For paper-only backtests, set it back to `true`.

After each bot cycle, it can advance the sandbox with:

```bash
POST {KRAKEN_API_URL}/admin/api/backtest/step?steps=1
```

Use the included backtest env template:

```bash
set -a
source env.stats.backtest
set +a
python stats_trend_bot.py
```

Or run a finite backtest batch from the repo root:

```bash
./run_stats_backtest.sh 100 1
```

The first argument is cycle count. The second argument is sandbox steps per cycle. The script lets `stats_trend_bot.py` read `KRAKEN_API_URL` from `.env` and writes to the backtest state/log/CSV files by default.

Backtest-specific outputs default to:

- `stats_trend_backtest_state.json`
- `stats_trend_backtest_trade_log.jsonl`
- `stats_trend_backtest_decisions.csv`

## Logging

Bots are expected to write structured JSONL logs to `trade_log.jsonl`.
The range-grid bot also writes trading-focused records to `range_grid_activity.jsonl`
or the file pointed to by `RANGE_GRID_ACTIVITY_LOG_FILE`.

Each line is one JSON object, for example:

```json
{"ts":"2026-04-09T03:30:02.093553+00:00","event":"SIGNAL_UPDATE","message":"","execution_signal":-0.0861,"price":70918.2}
```

The active range-grid bot currently writes events such as:

- `BOT_START`
- `SIGNAL_UPDATE`
- `RANGE_REFRESH`
- `RANGE_REFRESH_SKIPPED`
- `GRID_LEVEL_EVAL`
- `TRADE_DECISION`
- `BUY_ORDER_FILLED`
- `BUY_ORDER_PLACED`
- `SELL_ORDER_FILLED`
- `SELL_ORDER_PLACED`
- `ORDER_REJECTED`
- `ORDER_CANCELED`
- `ORDER_EXPIRED`
- `ORDER_STATUS_ERROR`
- `CYCLE_SUMMARY`
- `PRICE_ERROR`
- `SENTIMENT_ERROR`
- `RANGE_REFRESH_ERROR`
- `KRAKEN_EXCEPTION`
- `KRAKEN_API_ERROR`
- `LOOP_ERROR`

This structure is important because other utilities can consume it without regex parsing.

## Viewing logs

[`log_viewer.py`](/C:/Users/bgert/krakenbot/log_viewer.py) is a simple JSONL log inspector.

Example usage:

```powershell
python .\log_viewer.py --tail
python .\log_viewer.py --hours 24
python .\log_viewer.py --event SIGNAL_UPDATE
python .\log_viewer.py --days 7 --summary
```

It can:

- tail the log live
- filter by hours or days
- filter by event name
- print a quick event summary

## LLM and Discord helpers

The repo includes helpers for summarizing trade actions with an LLM and sending those summaries to Discord.

[`llm_trade_summary.py`](/C:/Users/bgert/krakenbot/llm_trade_summary.py):

- builds a short structured prompt around a trade event
- sends that prompt to the local LLM wrapper in `llama3.py`
- forwards the result through `discord_notify.py`

These helpers are optional. The support-file notes explicitly say not to assume Discord is always wanted.

## Standalone LLM Target Bot

`llm_target_bot.py` uses its own strategy file via `LLM_TARGET_STRATEGY_PROFILE`.
Keep these settings separate from the range-grid strategy profiles.

Useful strategy files:

- `llm_target_strategy_conservative.json`: strict quality gates and small exposure
- `llm_target_strategy_balanced.json`: default-quality gates with paper-trade sizing
- `llm_target_strategy_extreme_rebound.json`: contrarian rebound test profile, paper only
- `llm_target_strategy_quality_only_probe.json`: ignores sentiment in backtest and measures quality-approved targets
- `llm_target_strategy_sentiment_discount_loose.json`: allows watch/bearish setups only after a smaller discount
- `llm_target_strategy_price_target_probe.json`: ignores sentiment and quality in backtest to benchmark raw targets

Probe profiles should include `probe_only: true`; `llm_target_bot.py` refuses to
start with `probe_only: true` unless `dry_run: true`.

Compare LLM target strategy files with:

```bash
source env.llm-target-backtest
LLM_TARGET_BACKTEST_STRATEGY_SET_FILE=llm_target_strategy_test_set.txt ./venv/bin/python llm_target_backtest.py
```

The report includes `strategy_comparison`; ranked CSV output defaults to
`llm_target_strategy_ranked.csv`.

Snapshot rotation is configured with `LLM_TARGET_BACKTEST_SNAPSHOT_DIR` and
`LLM_TARGET_BACKTEST_SNAPSHOT_BASENAME`. For example, with
`LLM_TARGET_BACKTEST_SNAPSHOT_DIR=/home/ben/krakenbot/backtests` and
`LLM_TARGET_BACKTEST_SNAPSHOT_BASENAME=llm_target_snapshots.jsonl`, daily rotation
writes files such as `/home/ben/krakenbot/backtests/llm_target_snapshots_20260623.jsonl`.

## Running the project

Install dependencies:

```powershell
pip install -r .\requirements.txt
```

Run the active range-grid bot:

```powershell
python .\range_grid_bot.py
```

## Development notes

The design goal of this repo is flexibility for experimenting with different bot ideas without having to rewrite infrastructure every time.

A good rule of thumb is:

- put secrets, URLs, file paths, and quickly-switched runtime toggles in `.env`
- put strategy tuning in a bot-specific JSON file
- keep structured logs in JSONL
- persist enough state that a restart does not lose the bot's context

For new bots, following the same pattern will keep them easier to compare, debug, and operate.

## Troubleshooting

If the bot appears inactive:

- check that `LLM_SIGNAL_URL` is reachable
- check that `PRICE_LOG_URL` points at JSONL price history
- inspect `TRADE_LOG_FILE` for `PRICE_ERROR`, `SENTIMENT_ERROR`, `RANGE_REFRESH_ERROR`, or `LOOP_ERROR`
- confirm the state file path is writable
- confirm Kraken credentials are loaded from `.env`

If logging stops:

- verify `TRADE_LOG_FILE` points to the expected file
- confirm the directory exists or can be created
- look for `LOG_WRITE_ERROR` output in the console

If the grid feels too aggressive or too passive:

- adjust `execution_signal_threshold`
- adjust `position_size_pct`
- adjust `max_grid_size`
- change `grid_anchor` in the selected strategy profile
- adjust `profit_target_pct` to move the buy ladder closer to or farther from the anchor
- try `grid_anchor: "median"` if `mean` is being pushed around by noisy price spikes

## Current assumptions

This README describes the repo based on the code and support-file notes currently present in the workspace.
Some helper scripts may still be experimental or partially wired together, but the config, logging, and workflow sections above match the current code paths in the active bot.
# Altcoin scout bot foundation

`altcoin_bot.py` is the fail-closed SOL observer/paper-trading foundation. It accepts
only the versioned top-level scout decision documented in
`docs/asset_scout_decision_contract.md`; adaptive overlays and the three supporting
feeds are never authorization inputs. The Kraken integration is read-only and exposes
no order mutation methods. `live` is intentionally rejected even if live-related
environment settings are present.

Copy the values from `env.altcoin` into the instance `.env`, then start with:

```bash
python altcoin_bot.py --env-file .env --once
python altcoin_bot.py --env-file .env
```

Strategy/risk/fee knobs live in `altcoin_strategy_default.json`. Runtime paths, URLs,
credentials, mode, and safety files live in `.env`. Keep
`ALTCOIN_READ_ONLY_RECONCILIATION=false` until a Kraken key with query-only permissions
is configured. The dry-run mode uses public ticker/pair metadata but never calls Kraken
order submission or cancellation endpoints.

Run its tests with:

```bash
python -m unittest tests.test_altcoin_bot -v
```
