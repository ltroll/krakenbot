# Range Grid Strategy Files

- `range_grid_strategy_default.json`
  Normal operations. Uses `range_plus_llm` and keeps backlog guardrails enabled.

- `range_grid_strategy_recovery_range_only.json`
  Recovery mode for long-held underwater sells. Uses `range_only`, disables backlog buy blocking, and allows range buys to continue through liquidity/confidence-only sentiment blocks so the bot can keep working the range during testing.

- `range_grid_strategy_sell_only.json`
  Inventory-management mode. No new buys, but existing sells remain active and managed.

- `range_grid_strategy_observe_only.json`
  Observation mode. No new buys and no sell management changes; useful for monitoring, dry-runs, and service validation.

The active strategy file is selected by:

```bash
RANGE_GRID_STRATEGY_PROFILE=<strategy-file>.json
```

Current `env.range` points to:

```bash
RANGE_GRID_STRATEGY_PROFILE=range_grid_strategy_recovery_range_only.json
```
