# Installing Bots As Services

These instructions show how to run either `range_grid_bot.py` or
`kraken_sentiment_executor.py` as a long-running Linux `systemd` service.

The examples assume the bot is installed at:

```bash
/home/<user>/tradingbot/krakenbot
```

Adjust paths, user names, and Python paths for your server.

## 1. Prepare The Bot Directory

Clone or copy the repo onto the server:

```bash
cd /home/<user>/tradingbot
git clone <repo-url> krakenbot
cd /home/<user>/tradingbot/krakenbot
```

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Create your runtime `.env`:

```bash
cp .env_example .env
nano .env
```

At minimum, set:

```env
KRAKEN_API_KEY=
KRAKEN_API_SECRET=
KRAKEN_API_URL=https://api.kraken.com
KRAKEN_TICKER_URL=https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD
LLM_SIGNAL_URL=http://<host>/bot/llm_signal.json
# For multi-asset sentiment:
# LLM_SIGNAL_URL=http://<host>/bot/multi_asset_signal.json
# SIGNAL_ASSET_ID=BTC
BOT_POLICY_BACKTEST_URL=http://<host>/bot/bot_policy_backtest.json
BOT_REPLAY_BACKTEST_URL=http://<host>/bot/bot_replay_backtest.json
SIGNAL_FORWARD_BACKTEST_URL=http://<host>/bot/signal_forward_backtest.json
PRICE_LOG_URL=http://<host>/bot/btc_price_log.jsonl
RANGE_GRID_STRATEGY_PROFILE=range_grid_strategy_default.json
SENTIMENT_STRATEGY_PROFILE=sentiment_strategy_default.json
```

## 2. Choose One Bot Configuration

Use one service per bot, but point both services at the same `.env`. The shared
file holds Kraken secrets, URLs, support-file paths, and the per-bot operation
paths. Each bot then loads its own strategy JSON file.

### Shared Bot Env

Create `/home/<user>/tradingbot/krakenbot/.env`:

```env
KRAKEN_API_KEY=<key>
KRAKEN_API_SECRET=<secret>
KRAKEN_API_URL=https://api.kraken.com
KRAKEN_TICKER_URL=https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD
KRAKEN_ORDERBOOK_URL=https://api.kraken.com/0/public/Depth?pair=XBTUSD&count=5
KRAKEN_OHLC_URL=https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=60
LLM_SIGNAL_URL=http://<host>/bot/llm_signal.json
# For multi-asset sentiment:
# LLM_SIGNAL_URL=http://<host>/bot/multi_asset_signal.json
# SIGNAL_ASSET_ID=BTC
BOT_POLICY_BACKTEST_URL=http://<host>/bot/bot_policy_backtest.json
BOT_REPLAY_BACKTEST_URL=http://<host>/bot/bot_replay_backtest.json
SIGNAL_FORWARD_BACKTEST_URL=http://<host>/bot/signal_forward_backtest.json
PRICE_LOG_URL=http://<host>/bot/btc_price_log.jsonl
BOT_DIR_LIST_FILE=/home/<user>/tradingbot/bot_dirs.txt

BOT_CONFIG_FILE=range_grid_config.json
BOT_STATE_FILE=last_state.json
TRADE_LOG_FILE=trade_log.jsonl

RANGE_GRID_CONFIG_FILE=range_grid_config.json
RANGE_GRID_STATE_FILE=last_state.json
RANGE_GRID_TRADE_LOG_FILE=trade_log.jsonl
RANGE_GRID_STRATEGY_PROFILE=range_grid_strategy_default.json

SENTIMENT_CONFIG_FILE=sentiment_bot_config.json
SENTIMENT_STATE_FILE=sentiment_state.json
SENTIMENT_TRADE_LOG_FILE=sentiment_trade_log.jsonl
SENTIMENT_DECISION_CSV_FILE=sentiment_decisions.csv
SENTIMENT_STRATEGY_PROFILE=sentiment_strategy_default.json

LLM_TARGET_CONFIG_FILE=sentiment_bot_config.json
LLM_TARGET_STATE_FILE=llm_target_state.json
LLM_TARGET_TRADE_LOG_FILE=llm_target_trade_log.jsonl
LLM_TARGET_STRATEGY_PROFILE=llm_target_strategy_weather_dryrun.json

KRAKEN_PAIR=XXBTZUSD
SIGNAL_FILE=
REQUEST_TIMEOUT_SECONDS=10
REQUEST_RETRY_ATTEMPTS=2
REQUEST_RETRY_BACKOFF_SECONDS=1.5
ORDER_TRACKER_TIMEOUT_SECONDS=5
ORDER_TRACKER_CHECKIN_TIMEOUT_SECONDS=5
KRAKEN_NONCE_RETRIES=2
KRAKEN_LOCKOUT_COOLDOWN_SECONDS=300
```

Bot tuning values such as grid anchor, entry spacing, position sizing,
profit targets, thresholds, dry-run mode, and loop intervals now live in the
strategy JSON files named by `RANGE_GRID_STRATEGY_PROFILE` and
`SENTIMENT_STRATEGY_PROFILE`.

For one-off sentiment test runs, command-line arguments can override the
strategy file or backtest gate without editing tracked JSON:

```bash
.venv/bin/python kraken_sentiment_executor.py --strategy-profile sentiment_strategy_default.json
.venv/bin/python kraken_sentiment_executor.py --no-backtest-health-gate
.venv/bin/python kraken_sentiment_executor.py --backtest-health-gate --backtest-min-trades 10
.venv/bin/python kraken_sentiment_executor.py --bot-policy-backtest-url http://<host>/bot/bot_policy_backtest.json
.venv/bin/python kraken_sentiment_executor.py --bot-replay-backtest-url http://<host>/bot/bot_replay_backtest.json
.venv/bin/python kraken_sentiment_executor.py --run-backtest --backtest-min-trades 1 --bot-replay-backtest-url http://<host>/bot/bot_replay_backtest.json
.venv/bin/python kraken_sentiment_executor.py --run-backtest --usd 50 --backtest-min-trades 1 --bot-replay-backtest-url http://<host>/bot/bot_replay_backtest.json
.venv/bin/python kraken_sentiment_executor.py --buynow --usd 25
```

`--run-backtest` is a one-shot report mode. It fetches the configured backtest
artifact, prints the findings, and exits without starting the live trading loop.
`--usd` emulates a fixed USD allocation for each filled backtest trade.
`--buynow` is a one-shot test helper. It submits a post-only limit buy at the
best bid or one tick inside the spread, records it as an open buy, and exits.
Start the service loop afterward to track the fill and place the profit-taking
sell.

The sentiment bot can use `risk_context.weather_report` from
`multi_asset_signal.json` as an advisory market-weather layer. When
`USE_RISK_CONTEXT_POLICY=true` and the payload is present/fresh, the bot keeps
trade authority, logs the weather condition, applies size/target multipliers,
and only treats `weather_emergency_bell=true` as a hard pause. Legacy
`action_recommendation`, `recommended_posture`, and hard-safety fields are
diagnostics, not the main trade gate.

```bash
USE_RISK_CONTEXT_POLICY=true
RISK_CONTEXT_HARD_SAFETY_BLOCK=false
# RISK_CONTEXT_MIN_BUY_SCORE=0.50  # Legacy risk-context fallback only.
RISK_CONTEXT_POSITION_SIZE_ENABLED=true
RISK_CONTEXT_TARGET_PROFIT_ENABLED=true
```

The sentiment bot can consume optional `market_structure` fields from the signal
payload. With `USE_MARKET_STRUCTURE_FILTER=false` it only logs support,
resistance, upside, downside, and risk/reward diagnostics. To let structure
block otherwise-eligible buys, enable it from `.env`:

```bash
USE_MARKET_STRUCTURE_FILTER=true
STRUCTURE_RESISTANCE_BUFFER_PCT=0.0025
STRUCTURE_MIN_RISK_REWARD=1.25
STRUCTURE_REQUIRE_SUPPORT_PROXIMITY=false
```

The filter blocks a buy when upside to resistance is less than the trade's
gross target plus the resistance buffer, or when structure risk/reward is below
the configured minimum. Support proximity can be made mandatory later with
`STRUCTURE_REQUIRE_SUPPORT_PROXIMITY=true`.

To create one daily file containing both backtest summaries and the last 24
hours of bot trading logs:

```bash
.venv/bin/python daily_backtesting_report.py
```

By default it runs `/home/ben/sentiment_engine/bot_policy_backtest.py` and
`/home/ben/sentiment_engine/bot_replay_backtest.py` with a 24 hour window, then
writes `backtesting_report_YYYY-MM-DD.md` under
`/var/www/html/bot/backtesting_reports`. Configure paths with
`BACKTEST_SCRIPT_DIR`, `BACKTEST_REPORT_DIR`, `SENTIMENT_TRADE_LOG_FILE`, and
the `BOT_*_BACKTEST_OUTPUT_FILE` variables in `.env`. If another host publishes
the backtest JSON, set `BOT_POLICY_BACKTEST_URL`, `BOT_REPLAY_BACKTEST_URL`, and
optionally `SIGNAL_FORWARD_BACKTEST_URL`; the report will read those URLs for
the summaries. The signal-forward section calls out inverted bearish signals,
contrarian value, and thin sample buckets. Use `--skip-backtests` when the
remote host is already producing the JSON and this bot should only build the
daily report.

Lock down env file permissions because they contain Kraken secrets:

```bash
chmod 600 /home/<user>/tradingbot/krakenbot/.env
```

## 3. Install The Range Grid Service

Create `/etc/systemd/system/kraken-range-grid.service`:

```ini
[Unit]
Description=Kraken Range Grid Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=<user>
Group=<user>
WorkingDirectory=/home/<user>/tradingbot/krakenbot
EnvironmentFile=/home/<user>/tradingbot/krakenbot/.env
ExecStart=/home/<user>/tradingbot/krakenbot/.venv/bin/python /home/<user>/tradingbot/krakenbot/range_grid_bot.py
Restart=always
RestartSec=15
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kraken-range-grid.service
sudo systemctl start kraken-range-grid.service
```

Check status and logs:

```bash
sudo systemctl status kraken-range-grid.service
journalctl -u kraken-range-grid.service -f
tail -f /home/<user>/tradingbot/krakenbot/range_grid_trade_log.jsonl
```

## 4. Install The Sentiment Executor Service

Create `/etc/systemd/system/kraken-sentiment.service`:

```ini
[Unit]
Description=Kraken Sentiment Executor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=<user>
Group=<user>
WorkingDirectory=/home/<user>/tradingbot/krakenbot
EnvironmentFile=/home/<user>/tradingbot/krakenbot/.env
ExecStart=/home/<user>/tradingbot/krakenbot/.venv/bin/python /home/<user>/tradingbot/krakenbot/kraken_sentiment_executor.py
Restart=always
RestartSec=15
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kraken-sentiment.service
sudo systemctl start kraken-sentiment.service
```

Check status and logs:

```bash
sudo systemctl status kraken-sentiment.service
journalctl -u kraken-sentiment.service -f
tail -f /home/<user>/tradingbot/krakenbot/sentiment_trade_log.jsonl
```

## 5. Install The LLM Target Service

Create `/etc/systemd/system/kraken-llm-target.service`:

```ini
[Unit]
Description=Kraken LLM Target Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=<user>
Group=<user>
WorkingDirectory=/home/<user>/tradingbot/krakenbot
EnvironmentFile=/home/<user>/tradingbot/krakenbot/.env
ExecStart=/home/<user>/tradingbot/krakenbot/.venv/bin/python /home/<user>/tradingbot/krakenbot/llm_target_bot.py
Restart=always
RestartSec=15
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Start with the dry-run weather profile:

```env
LLM_SIGNAL_URL=http://<host>/bot/multi_asset_signal.json
SIGNAL_ASSET_ID=BTC
LLM_TARGET_STRATEGY_PROFILE=llm_target_strategy_weather_dryrun.json
LLM_TARGET_STATE_FILE=llm_target_state.json
LLM_TARGET_TRADE_LOG_FILE=llm_target_trade_log.jsonl
```

Enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kraken-llm-target.service
sudo systemctl start kraken-llm-target.service
```

Check status and logs:

```bash
sudo systemctl status kraken-llm-target.service
journalctl -u kraken-llm-target.service -f
tail -f /home/<user>/tradingbot/krakenbot/llm_target_trade_log.jsonl
```

After dry-run service logs show clean signal reads, target-quality reads,
order-ledger checkins, and sane `LEGACY_ACTION_GATE_BYPASSED` events, switch
to the tiny live profile:

```env
LLM_TARGET_STRATEGY_PROFILE=llm_target_strategy_weather_tiny_live.json
```

Then restart:

```bash
sudo systemctl restart kraken-llm-target.service
```

## 6. Install The OLED Status Display

Create `/etc/systemd/system/kraken-status-display.service`:

```ini
[Unit]
Description=Kraken Bot OLED Status Display
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=<user>
Group=<user>
WorkingDirectory=/home/<user>/tradingbot/krakenbot
EnvironmentFile=/home/<user>/tradingbot/krakenbot/.env
Environment=BOT_DISPLAY_SERVICES=kraken-range-grid.service,kraken-sentiment.service,kraken-llm-target.service
Environment=BOT_DISPLAY_LOG_FILES=trade_log.jsonl,sentiment_trade_log.jsonl,llm_target_trade_log.jsonl,stats_trend_trade_log.jsonl
ExecStart=/home/<user>/tradingbot/krakenbot/.venv/bin/python /home/<user>/tradingbot/krakenbot/bot_status_display.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kraken-status-display.service
sudo systemctl start kraken-status-display.service
```

The display rotates through simple screens:

```text
status:
up
```

```text
last event:
12 minutes
```

```text
errors:
0
```

```text
IP Address:
192.168.50.211
```

```text
Bot Name:
range-grid-bot
```

```text
Uptime:
3d 4h
```

`status` is `up` only when every service listed in `BOT_DISPLAY_SERVICES` is
active. `last event` is the age of the newest JSONL log timestamp. `errors` is
the number of error events in the last hour. `Bot Name` is read from the
`ORDER_TRACKER_USER_AGENT` environment variable.

## 7. Common Operations

Stop a bot:

```bash
sudo systemctl stop kraken-range-grid.service
sudo systemctl stop kraken-sentiment.service
sudo systemctl stop kraken-llm-target.service
sudo systemctl stop kraken-status-display.service
```

Restart after changing `.env` or config:

```bash
sudo systemctl restart kraken-range-grid.service
sudo systemctl restart kraken-sentiment.service
sudo systemctl restart kraken-llm-target.service
sudo systemctl restart kraken-status-display.service
```

Disable a bot from starting on boot:

```bash
sudo systemctl disable kraken-range-grid.service
sudo systemctl disable kraken-sentiment.service
sudo systemctl disable kraken-llm-target.service
sudo systemctl disable kraken-status-display.service
```

View recent service logs:

```bash
journalctl -u kraken-range-grid.service -n 100 --no-pager
journalctl -u kraken-sentiment.service -n 100 --no-pager
journalctl -u kraken-llm-target.service -n 100 --no-pager
journalctl -u kraken-status-display.service -n 100 --no-pager
```

## 8. Safety Checklist

Before enabling live trading:

- Set `dry_run` to `true` in the selected sentiment strategy profile until logs look correct.
- Confirm the bot writes to the intended `TRADE_LOG_FILE`.
- Confirm the bot writes to the intended `BOT_STATE_FILE`.
- Confirm only one service is controlling the same strategy state file.
- Confirm `LLM_SIGNAL_URL` and `PRICE_LOG_URL` are reachable from the server.
- Confirm Kraken API keys have only the permissions the bot needs.
- For the LLM target bot, confirm `LLM_TARGET_STRATEGY_PROFILE` is
  `llm_target_strategy_weather_dryrun.json` before the first service run.
- Before switching the LLM target bot live, confirm the logs contain weather
  fields and no repeated `weather_report_missing`, `SIGNAL_ERROR`, or
  `TARGET_QUALITY_EVAL` failures.

Do not run both bots against the same account unless you intentionally want both
strategies managing inventory at the same time.
