# Asset scout decision contract

The bot authorizes candidates from one input only: the top-level current decision in
`asset_scout_decision.json`. `adaptive_policy_overlay` is deliberately not parsed.
The other three sentiment documents are recorded as advisory source-health evidence only.

Supported version: `asset-scout-decision-v1`.

```json
{
  "schema_version": "asset-scout-decision-v1",
  "decision_id": "SOL-20260622T140000Z-01",
  "status": "ok",
  "generated_at": "2026-06-22T14:00:00Z",
  "asset": "SOL",
  "decision": "allow_scout_long",
  "limit_entry_plan": {
    "limit_price": "132.50",
    "quantity": "0.180",
    "reference_price": "132.60",
    "spread_bps": "4",
    "slippage_bps": "3",
    "expires_at": "2026-06-22T14:15:00Z"
  },
  "adaptive_policy_overlay": {
    "decision": "allow_scout_long",
    "shadow_only": true
  }
}
```

All timestamps must be timezone-aware ISO-8601 values. Numeric order fields must be
positive finite values. A missing plan is valid JSON but can never authorize a trade.
Unknown schema versions, missing identifiers, malformed values, future timestamps,
expired plans, and stale decisions fail closed.

Before deployment, capture the Pi's actual document and run the contract tests against it.
If its field names differ, add a new explicit schema version; do not add permissive aliases
to the authorization path.
