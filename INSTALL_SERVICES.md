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
BOT_CONFIG_FILE=
BOT_STATE_FILE=
TRADE_LOG_FILE=
PRICE_CHECK_INTERVAL_SECONDS=60
```

## 2. Choose One Bot Configuration

Use one service per bot. The easiest pattern is to create a dedicated env file
for each bot, then point the service at that file.

### Range Grid Bot Env

Create `/home/<user>/tradingbot/krakenbot/.env.range_grid`:

```env
KRAKEN_API_KEY=<key>
KRAKEN_API_SECRET=<secret>
KRAKEN_API_URL=https://api.kraken.com
KRAKEN_TICKER_URL=https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD
LLM_SIGNAL_URL=http://<host>/bot/llm_signal.json
PRICE_LOG_URL=http://<host>/bot/btc_price_log.jsonl

BOT_CONFIG_FILE=range_grid_config.json
BOT_STATE_FILE=range_grid_state.json
TRADE_LOG_FILE=range_grid_trade_log.jsonl

GRID_ANCHOR=median
ENTRY_STEP_PCT=0.005
HIGH_ANCHOR_BUY_COOLDOWN_MINUTES=15
MAX_OPEN_HIGH_ANCHOR_ORDERS=3
HIGH_ANCHOR_PROFIT_TARGET_PCT=0.006
PRICE_CHECK_INTERVAL_SECONDS=60
RANGE_REFRESH_INTERVAL_MINUTES=15
```

### Sentiment Executor Env

Create `/home/<user>/tradingbot/krakenbot/.env.sentiment`:

```env
KRAKEN_API_KEY=<key>
KRAKEN_API_SECRET=<secret>
KRAKEN_API_URL=https://api.kraken.com
KRAKEN_TICKER_URL=https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD
LLM_SIGNAL_URL=http://<host>/bot/llm_signal.json

BOT_CONFIG_FILE=sentiment_bot_config.json
BOT_STATE_FILE=sentiment_state.json
TRADE_LOG_FILE=sentiment_trade_log.jsonl
SENTIMENT_DECISION_CSV_FILE=sentiment_decisions.csv

KRAKEN_PAIR=XXBTZUSD
PRICE_CHECK_INTERVAL_SECONDS=60
KRAKEN_NONCE_RETRIES=2
MIN_TRADE_USD=30
POSITION_SIZE_PCT=0.10
MAX_TRADE_USD=60
CONFIDENCE_THRESHOLD=0.45
CONFIDENCE_WEIGHTING=true
DRY_RUN=false
EXECUTION_BUFFER_PCT=0.0025
REBALANCE_COOLDOWN_MINUTES=15
COOLDOWN_OVERRIDE_SIGNAL_ABS=0.20
SENTIMENT_BUY_THRESHOLD=0.03
TARGET_PROFIT_PCT=0.006
ROUND_TRIP_FEE_PCT=0.0032
MAX_OPEN_SELL_ORDERS=1
MAX_INVENTORY_USD=250
PREVENT_BUY_ABOVE_LAST_SELL=true
BUY_AFTER_SELL_DISCOUNT_PCT=0.0
HIGH_PRICE_BUY_BLOCK_PCT=0.0005
```

Lock down env file permissions because they contain Kraken secrets:

```bash
chmod 600 /home/<user>/tradingbot/krakenbot/.env.range_grid
chmod 600 /home/<user>/tradingbot/krakenbot/.env.sentiment
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
EnvironmentFile=/home/<user>/tradingbot/krakenbot/.env.range_grid
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
EnvironmentFile=/home/<user>/tradingbot/krakenbot/.env.sentiment
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

## 5. Common Operations

Stop a bot:

```bash
sudo systemctl stop kraken-range-grid.service
sudo systemctl stop kraken-sentiment.service
```

Restart after changing `.env` or config:

```bash
sudo systemctl restart kraken-range-grid.service
sudo systemctl restart kraken-sentiment.service
```

Disable a bot from starting on boot:

```bash
sudo systemctl disable kraken-range-grid.service
sudo systemctl disable kraken-sentiment.service
```

View recent service logs:

```bash
journalctl -u kraken-range-grid.service -n 100 --no-pager
journalctl -u kraken-sentiment.service -n 100 --no-pager
```

## 6. Safety Checklist

Before enabling live trading:

- Run with `DRY_RUN=true` for the sentiment executor until logs look correct.
- Confirm the bot writes to the intended `TRADE_LOG_FILE`.
- Confirm the bot writes to the intended `BOT_STATE_FILE`.
- Confirm only one service is controlling the same strategy state file.
- Confirm `LLM_SIGNAL_URL` and `PRICE_LOG_URL` are reachable from the server.
- Confirm Kraken API keys have only the permissions the bot needs.

Do not run both bots against the same account unless you intentionally want both
strategies managing inventory at the same time.
