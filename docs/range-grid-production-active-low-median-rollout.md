# Active low/median production rollout

This runbook promotes the validated low/median resting grid without changing
or repricing any existing exchange order.

## Required production environment

Update the existing definitions in the production `.env`; do not add duplicate
keys:

```bash
RANGE_GRID_STRATEGY_PROFILE=range_grid_strategy_production_active_low_median_fear_greed.json
RANGE_GRID_ANCHOR_ROUTER_ENABLED=false
```

The router is disabled for the initial trial because ranked strategy replay
tests profiles in isolation. Other router settings may remain in `.env`; they
are ignored while the router is disabled.

## Deploy

From the production repository:

```bash
git pull --ff-only
jq '{paper_trading_enabled,grid_anchor,entry_placement_mode_by_source,max_grid_size,minimum_order_floor_usd,minimum_order_floor_cash_reserve_usd,sell_repricing_enabled}' range_grid_strategy_production_active_low_median_fear_greed.json
grep -nE '^(RANGE_GRID_STRATEGY_PROFILE|RANGE_GRID_ANCHOR_ROUTER_ENABLED)=' .env
sudo systemctl restart kraken-range-grid.service
sudo systemctl status kraken-range-grid.service --no-pager
```

The profile check must report paper trading disabled, `grid_anchor` equal to
`low,median`, resting placement for low and median, four grid levels, a $100
order floor, a $100 reserve, and sell repricing disabled.

## Verify startup

```bash
grep '"event": "BOT_START"' /var/www/html/bot/range_grid_activity.jsonl | tail -1 | jq '{ts,strategy_profile,paper_trading_enabled,grid_anchor,max_grid_size,minimum_order_floor_usd,minimum_order_floor_cash_reserve_usd,sell_repricing_enabled,anchor_strategy_router_enabled}'
```

Expected identity:

```text
strategy_profile: range_grid_strategy_production_active_low_median_fear_greed.json
paper_trading_enabled: false
grid_anchor: low,median
max_grid_size: 4
minimum_order_floor_usd: 100
minimum_order_floor_cash_reserve_usd: 100
sell_repricing_enabled: false
anchor_strategy_router_enabled: false
```

New managed buys will use `range_low:price_band:...` or
`range_median:price_band:...` slot identities. Existing depth-based orders are
legacy recovery inventory; they remain open at their current exchange prices
and do not occupy new price bands.

## Rollback

Restore the prior two `.env` values and restart the service. A profile switch
or service restart does not cancel, amend, or reprice existing exchange orders.
