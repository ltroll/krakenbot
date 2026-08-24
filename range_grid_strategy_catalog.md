# Range Grid Strategy Files

- `range_grid_strategy_default.json`
  Normal operations. Uses `range_plus_llm` and keeps backlog guardrails enabled.

- `range_grid_strategy_recovery_range_only.json`
  Recovery mode for long-held underwater sells. Uses `range_only`, `sentiment_control_mode=risk_modulated`, disables backlog buy blocking, allows range buys to continue through liquidity/confidence-only sentiment blocks, enables dynamic anchor selection across `low,median,high`, widens entry spacing automatically when realized volatility is elevated, and tapers new-buy size as deployed inventory approaches the configured inventory cap.

- `range_grid_strategy_price_first_source_policy_candidate.json`
  Paper-only recovery-first comparison profile. Low entries are price-first and weather-sized, median entries prefer stabilization but continue at reduced size, and high entries require confirmed range chop. Legacy inventory, sell backlog, and flow pressure soften sizing without imposing broad hard buy blocks. It is included in `range_grid_strategy_test_set.txt` but is not selected by `env.range`.

- `range_grid_strategy_recovery_resting_low_median.json`
  Deterministic resting-grid candidate built from the recovery source-policy
  and Fear & Greed profit-target work. Low and median maintain fixed nearby
  resting rungs with hard-safety-only entry authority and stable managed slot
  identities. A slot remains occupied through its paired immutable sell, while
  legacy orders without slot identities do not block the working grid. Dynamic
  anchor selection, volatility-adaptive spacing, and stale reanchoring are
  disabled. High remains triggered and chop-confirmed. Included in
  `range_grid_strategy_test_set.txt` for ranked replay before production use.

Sentiment control modes:

- `strict_sentiment`
  Existing conservative behavior. `llm_target` needs `bullish_allowed`, and ordinary range buys only proceed on permissive sentiment recommendations.

- `risk_modulated`
  Production-oriented compromise. `llm_target` stays strict, lower/mid range buys can continue during ordinary `blocked` states, and upper-range/high-anchor buys remain sentiment-sensitive. `risk_off` still blocks new longs.

- `price_first`
  Future-facing option for tests. Treats sentiment mostly as a hard-stop/risk-off layer rather than a general range-entry gate.

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
