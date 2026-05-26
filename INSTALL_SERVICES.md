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
BOT_POLICY_BACKTEST_URL=http://<host>/bot/bot_policy_backtest.json
BOT_REPLAY_BACKTEST_URL=http://<host>/bot/bot_replay_backtest.json
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
BOT_POLICY_BACKTEST_URL=http://<host>/bot/bot_policy_backtest.json
BOT_REPLAY_BACKTEST_URL=http://<host>/bot/bot_replay_backtest.json
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

KRAKEN_PAIR=XXBTZUSD
SIGNAL_FILE=
REQUEST_TIMEOUT_SECONDS=10
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
```

`--run-backtest` is a one-shot report mode. It fetches the configured backtest
artifact, prints the findings, and exits without starting the live trading loop.
`--usd` emulates a fixed USD allocation for each filled backtest trade.

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

## 5. Install The OLED Status Display

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
Environment=BOT_DISPLAY_SERVICES=kraken-range-grid.service,kraken-sentiment.service
Environment=BOT_DISPLAY_LOG_FILES=trade_log.jsonl,sentiment_trade_log.jsonl,stats_trend_trade_log.jsonl
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

## 6. Common Operations

Stop a bot:

```bash
sudo systemctl stop kraken-range-grid.service
sudo systemctl stop kraken-sentiment.service
sudo systemctl stop kraken-status-display.service
```

Restart after changing `.env` or config:

```bash
sudo systemctl restart kraken-range-grid.service
sudo systemctl restart kraken-sentiment.service
sudo systemctl restart kraken-status-display.service
```

Disable a bot from starting on boot:

```bash
sudo systemctl disable kraken-range-grid.service
sudo systemctl disable kraken-sentiment.service
sudo systemctl disable kraken-status-display.service
```

View recent service logs:

```bash
journalctl -u kraken-range-grid.service -n 100 --no-pager
journalctl -u kraken-sentiment.service -n 100 --no-pager
journalctl -u kraken-status-display.service -n 100 --no-pager
```

## 7. Safety Checklist

Before enabling live trading:

- Set `dry_run` to `true` in the selected sentiment strategy profile until logs look correct.
- Confirm the bot writes to the intended `TRADE_LOG_FILE`.
- Confirm the bot writes to the intended `BOT_STATE_FILE`.
- Confirm only one service is controlling the same strategy state file.
- Confirm `LLM_SIGNAL_URL` and `PRICE_LOG_URL` are reachable from the server.
- Confirm Kraken API keys have only the permissions the bot needs.

Do not run both bots against the same account unless you intentionally want both
strategies managing inventory at the same time.
