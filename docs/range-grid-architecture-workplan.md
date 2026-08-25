# Range-grid architecture work plan

This document is the durable checklist for the range-grid architecture work. It
exists to keep short-term market moves and individual blocked candidates from
pulling the work away from the underlying system problems.

## Non-negotiable behavior

- Existing sell orders remain open and continue waiting for their configured
  profitable exits. This work must not cancel, reprice, stop-loss, or liquidate
  legacy inventory.
- Legacy recovery inventory must not prevent new trading when spendable USD is
  available. It may appear in total-exposure reporting, but it must not consume
  the working strategy's complete inventory budget.
- Live execution and replay must evaluate the same effective strategy for the
  same source and inputs.
- Insufficient recent performance history may reduce a new order to a small
  probe; it must not create a permanent no-trade state.
- High-band protections remain stricter than low/median accumulation rules.

## P0 — Establish one authoritative effective strategy

The router currently ranks complete profiles while live execution combines a
base profile and routed payloads. Snapshot replay records only the base profile.
Until this is corrected, ranked results do not describe the exact policy used
in production.

- [x] Record the architectural problems and rollout order in this document.
- [x] Add a deterministic base-plus-route strategy resolver.
- [x] Fingerprint the base, route, and effective payloads.
- [x] Use the resolved payload during live candidate evaluation.
- [x] Publish resolved route metadata and payloads in the runtime status file.
- [x] Capture resolved route data in backtest snapshots.
- [x] Make replay select the captured effective payload for each buy source.
- [x] Ensure strategy comparisons discard production routes and test the
      requested profile in isolation.
- [x] Add parity tests covering routed and fallback sources.

Acceptance criteria:

- Every live buy decision and order can identify one effective-strategy hash.
- A captured snapshot replay produces the same candidate source, level,
  approval result, and block reason as live decision logic for fixture inputs.
- A router winner cannot silently change safety or sizing behavior without
  changing the recorded effective-strategy hash.

## P1 — Share the decision engine

Live and replay currently contain separate candidate-building and gate logic.
They will continue to drift until both are adapters around one pure decision
engine.

- [ ] Extract market/config/state input models independent of Kraken I/O.
- [ ] Extract candidate generation and candidate evaluation into pure code.
- [ ] Make the live bot execute the resulting decision plan.
- [ ] Make backtest replay feed snapshots into the same decision plan.
- [ ] Retain a simulated broker for capital, placement, fills, and exits.
- [ ] Add golden parity fixtures for low, median, high, reanchored, blocked,
      and router-fallback decisions.

Acceptance criteria:

- Candidate approval and block-reason code has one implementation.
- Live-only code performs I/O and order submission, not strategy decisions.
- Replay-only code performs simulated exchange execution, not gate decisions.

## P1A — Restore deterministic low/median grid placement

The legacy entry engine calculated grid levels but treated them as market
timing triggers. It would not submit a buy while price remained above the
level, then required several forecast and recovery mechanisms to align after
price crossed it. This produced long silent periods followed by order bursts.

- [x] Add an explicit per-source `resting_grid` placement mode shared by live
      execution and replay.
- [x] Make resting slot 1 equal its low/median anchor while preserving the
      legacy one-step-below calculation for triggered entries.
- [x] Keep high-band entry in the existing strict triggered mode.
- [x] Give the initial resting rungs a stable source-and-depth slot identity.
- [x] Replace source-and-depth-only slot identities with quantized price-band
      identities so a materially lower grid level can trade while an older,
      higher sell waits without permitting duplicates in the same band.
- [x] Keep a managed slot occupied through its paired sell and reopen it only
      after that sell fills.
- [x] Exclude legacy orders without a managed slot identity from slot blocking.
- [x] Reduce resting low/median entry authority to hard safety only; forecast
      phases, sentiment opinions, falling-tape labels, flow pressure, inventory
      pressure, above-last-sell checks, and post-sell cooldowns cannot veto a
      nearby resting slot.
- [x] Retain cash reserve, exchange minimums, duplicate open-buy protection,
      short source pacing, explicit risk-off/danger blocks, operating mode, and
      private-API failure protection.
- [x] Disable dynamic one-anchor selection, volatility-shifting grid spacing,
      and stale-level reanchoring in the resting-grid candidate.
- [x] Add replay reporting for placement modes, resting distance, and occupied
      slots.
- [x] Add the candidate to the ranked strategy set.

Acceptance criteria:

- When price enters the configured low or median anchor zone, the first nearby
  rung can rest before price crosses it.
- A current managed sell blocks only its own price band; legacy recovery and
  pre-price-band orders do not block any new managed band.
- A resting low/median candidate is blocked only by price being outside its
  anchor zone, an occupied managed slot, short pacing, insufficient spendable
  cash above reserve, exchange constraints, or hard safety/runtime failures.
- Existing open orders are never canceled, repriced, or migrated during rollout.

## P2 — Separate legacy recovery from working inventory

Existing sells currently count at full buy cost in inventory-pressure sizing.
This can reduce new orders below exchange minimums even when hard caps are off
and new USD is available.

- [ ] Add an inventory cohort/policy identifier to newly placed orders.
- [ ] Classify orders without a current policy identifier as
      `legacy_recovery` without modifying their exchange orders.
- [ ] Report total, legacy-recovery, and working inventory separately.
- [ ] Base normal inventory pressure on working inventory.
- [ ] Continue counting open buys as committed USD.
- [ ] Use actual spendable USD, the configured cash reserve, source pacing, and
      working-inventory pressure for new-order sizing.
- [ ] Keep an optional explicit account-wide emergency exposure guard separate
      from normal strategy pacing.
- [ ] Add migration and regression tests proving legacy orders remain intact.

Acceptance criteria:

- Existing sells are unchanged on Kraken and continue filling normally.
- Legacy recovery inventory alone cannot yield `below_min_notional`,
  `max_inventory_usd`, or bucket-inventory blocks for an otherwise valid small
  buy when cash above the reserve is available.
- Working inventory still produces gradual size pressure and source cooldowns.

## P3 — Remove cold-start deadlocks

- [ ] Replace fail-closed insufficient-sample behavior with a reduced-size
      probe path.
- [ ] Apply the probe path only to qualified low/median setups.
- [ ] Backtest `range_chop_stabilizing` stale-level recovery using the shared
      decision engine.
- [ ] Keep post-jump and distribution-phase high entries guarded.
- [ ] Promote thresholds only after fee-adjusted replay and simulated execution
      agree.

Acceptance criteria:

- A quiet bot can collect new performance samples using bounded small orders.
- Missing samples cannot indefinitely block all qualifying accumulation.
- The probe path respects cash reserve, cooldown, and emergency risk signals.

## Rollout order

1. Complete P0 and verify on fixtures plus the captured 40-hour window.
2. Rank the P1A resting-grid candidate on captured windows before selecting it
   in production.
3. Complete P1 parity before further strategy tuning.
4. Migrate existing orders into the legacy-recovery cohort and complete P2.
5. Replay P3 candidates, then enable bounded live probes.

No stage requires waiting for existing sell orders to close. Those orders stay
in recovery while the architecture work proceeds.

## Active backtest experiments

Two paper-only hybrids isolate the useful low/median activity from the
168-hour `super_aggressive` result without inheriting its unrestricted high
exposure or small fragmented order sizing:

- `range_grid_strategy_recovery_hybrid_active_low_median_fear_greed.json`
  expands the low selection band and maintains four fixed-spacing,
  anchor-first resting levels for both low and median. It retains $100 orders,
  the $100 reserve, double Fear & Greed targets, immutable sells, and excludes
  high-band trading.
- `range_grid_strategy_recovery_hybrid_active_low_median_high_probe.json`
  uses the same resting low/median grid and adds a separate triggered high-band
  probe above 90% of the range. High has no $100 floor, is limited to one open
  order, uses 60/90-minute pacing, and still requires a bounded chop setup plus
  risk-context confirmation.

Both candidates use the same $600 starting cash and 1.20% round-trip fees as
the current double Fear & Greed control. Promotion requires more than nine
completed low/median trades, greater than 1.8567% net return, at least a 90%
close rate, drawdown no worse than roughly -2.5%, and no accumulating high-band
inventory over the captured 168-hour window.

## Verification log

### 2026-08-25 — Price-band slots and active hybrid correction

- Replaced resting source-and-depth slot identities with logarithmic price
  bands sized to each source's configured entry step. Rolling-anchor drift in
  one band cannot create duplicates, while a materially lower rung can trade
  even if an older higher sell remains open.
- Triggered entries, including high-band probes, retain their existing
  source-and-depth identities and strict entry behavior.
- Converted both paper-only active hybrids to anchor-first resting low/median
  placement, fixed spacing, hard-safety-only entry authority, and no stale
  reanchor. Preserved four levels, $100 low/median orders, the $100 reserve,
  immutable sells, and double Fear & Greed targets.
- Removed dormant aging/repricing parameters from both immutable-sell hybrids
  so their configuration no longer advertises behavior that cannot run.
- Existing exchange orders are not canceled, repriced, or migrated. Earlier
  source-and-depth resting orders are treated as legacy for price-band
  occupancy and continue waiting for their original exits.
- `python -m unittest discover -s tests`: 303 tests passed.
- Python compilation, JSON validation, and `git diff --check` passed.

### 2026-08-24 — Anchor-first resting-grid correction

- The first 40-hour replay of the resting-grid candidate placed one low and
  one median order but filled neither because the inherited grid calculation
  put slot 1 one full step below its anchor.
- Resting low/median grids now start at the anchor and descend from there;
  triggered grids retain their existing one-step-below behavior and high-band
  construction is unchanged.
- Live and replay now use the same shared entry-grid calculation.
- Existing orders, slot lifecycle, placement windows, sizing, profit targets,
  and sell behavior are unchanged.
- `python -m unittest discover -s tests`: 297 tests passed.
- Python compilation and `git diff --check` passed.

### 2026-08-24 — Deterministic low/median resting-grid candidate

- Replaced price-crossing entry behavior with explicit nearby resting orders
  for low and median only; high remains triggered and chop-confirmed.
- Added stable managed slot identities that persist from buy placement through
  the paired sell. Legacy orders carry no slot identity and cannot block the
  working grid.
- Removed soft forecast, sentiment, flow, inventory-pressure, above-last-sell,
  and post-sell blockers from resting low/median entries while retaining hard
  risk and runtime safety.
- Disabled dynamic anchor selection, volatility-adaptive spacing, and stale
  reanchoring in `range_grid_strategy_recovery_resting_low_median.json`.
- Retained the $100 low/median order floor, $100 cash reserve, immutable sells,
  observed 1.20% fee schedule, source pacing, and strict high-band policy.
- Added live/replay placement metadata plus managed-slot lifecycle coverage.
- `python -m unittest discover -s tests`: 293 tests passed.
- Python compilation, JSON validation, and `git diff --check` passed.


### 2026-08-23 — Production order sizing floor

- Added an enforced $100 source-specific floor to production and its matching
  paper candidate for low and median entries.
- Kept the general $8 exchange minimum separate so high-band sizing remains
  risk-adjusted and is neither rounded up nor blocked by the $100 floor.
- Retained the existing source cooldowns for signal pacing and a $100 spendable
  cash reserve; the floor cannot spend the reserve.
- High-band entries are not rounded up and must reach $100 through normal
  risk-adjusted sizing before they can trade.
- Existing buys and sells remain unchanged; the sizing floor applies only to
  future buy submissions.
- Added unit and simulated-lifecycle coverage for $100 low/median orders, cash
  reserve preservation, and rejection of undersized high-band orders.
- `python -m unittest discover -s tests`: 272 tests passed.
- JSON validation and `git diff --check` passed.

### 2026-08-23 — Correct live and replay fee economics

- Reconciled eight completed production round trips against Kraken execution
  data: buys paid 0.80% taker fees and resting sells paid 0.40% maker fees.
- Updated production and its matching paper candidate to model a 1.20%
  maker-plus-taker round trip.
- Normalized every profile in the active ranked strategy set to the same fee
  schedule so candidate comparisons cannot benefit from stale fee assumptions.
- Existing exchange sell orders remain unchanged; corrected fees affect only
  targets computed for future filled buys.
- `python -m unittest discover -s tests`: 269 tests passed.
- JSON validation and `git diff --check` passed.

### 2026-08-22 — Live sell repricing disabled

- Added a fail-closed live sell-repricing control; omitted configuration now
  means disabled.
- The production recovery profile explicitly disables sell repricing.
- Existing exchange orders are not canceled, amended, or recreated during
  deployment; their current Kraken prices remain unchanged.
- Sell-extension analysis remains shadow-only and cannot amend an order.
- Added regression coverage proving the disabled guard dominates the live
  `AMEND_SELL` path and production cannot enable it accidentally.
- `python -m unittest discover -s tests`: 268 tests passed.
- Python compilation and `git diff --check` passed.

### 2026-08-19 — P0 effective-strategy contract

- Added deterministic deep route-over-base composition with stable SHA-256
  fingerprints for the base, route, and resulting effective policy.
- Live candidate evaluation and flow control use the resolved source policy.
- Runtime status, captured snapshots, replay decisions, and placed-order state
  carry effective-policy identity.
- Profile-comparison replay replaces captured production routes with the
  requested comparison profile.
- Added routed-source, fallback-source, comparison-isolation, status-capture,
  fingerprint, and live-flow wiring regression tests.
- `python -m unittest discover -s tests`: 253 tests passed.
- Python compilation and `git diff --check` passed.

Deployment verification remains: after production is updated and restarted,
confirm the status file exposes `effective_strategy`, capture at least one new
snapshot containing it, and replay that snapshot. Historical snapshots without
this contract can only fall back to their recorded base profile; they cannot
reconstruct an unrecorded historical router payload.
