#!/usr/bin/env bash
set -euo pipefail

BOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BOT_DIR"

CYCLES="${1:-100}"
STEPS_PER_CYCLE="${2:-1}"

if [[ -x "venv/bin/python" ]]; then
    PYTHON="venv/bin/python"
else
    PYTHON="${PYTHON:-python3}"
fi

export STATS_TREND_DRY_RUN="${STATS_TREND_DRY_RUN:-false}"
export STATS_TREND_BACKTEST_MODE=true
export STATS_TREND_BACKTEST_STEP_AFTER_CYCLE=true
export STATS_TREND_BACKTEST_STEPS_PER_CYCLE="$STEPS_PER_CYCLE"
export STATS_TREND_BACKTEST_USE_API_MARKET_DATA=true
export STATS_TREND_MARKET_HISTORY_SOURCE="${STATS_TREND_MARKET_HISTORY_SOURCE:-ohlc}"
export STATS_TREND_STATE_FILE="${STATS_TREND_STATE_FILE:-stats_trend_backtest_state.json}"
export STATS_TREND_TRADE_LOG_FILE="${STATS_TREND_TRADE_LOG_FILE:-stats_trend_backtest_trade_log.jsonl}"
export STATS_TREND_DECISION_CSV_FILE="${STATS_TREND_DECISION_CSV_FILE:-stats_trend_backtest_decisions.csv}"
export STATS_TREND_STRATEGY_PROFILE="${STATS_TREND_STRATEGY_PROFILE:-stats_trend_strategy_default.json}"

"$PYTHON" -B - "$CYCLES" <<'PY'
import sys

import stats_trend_bot as bot

cycles = int(sys.argv[1])

bot.require_runtime_config()
for index in range(cycles):
    print(f"backtest cycle {index + 1}/{cycles}", flush=True)
    bot.run_cycle()
    bot.step_backtest()

print("backtest complete")
PY
