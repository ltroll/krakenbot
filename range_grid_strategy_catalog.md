# Range Grid Strategy Files

- `range_grid_strategy_default.json`
  Normal operations. Uses `range_plus_llm` and keeps backlog guardrails enabled.

- `range_grid_strategy_recovery_range_only.json`
  Recovery mode for long-held underwater sells. Uses `range_only` and disables backlog buy blocking so the bot can keep working the range.

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
