import unittest

from range_grid_effective_strategy import (
    EFFECTIVE_STRATEGY_COMPOSITION_MODE,
    merge_strategy_configs,
    resolve_effective_strategy,
    strategy_config_fingerprint,
)


class RangeGridEffectiveStrategyTests(unittest.TestCase):
    def test_route_overrides_base_and_preserves_nested_source_fallbacks(self):
        base = {
            "position_size_pct": 0.12,
            "entry_step_pct_by_source": {
                "range_low": 0.0028,
                "range_median": 0.0045,
            },
            "entry_policy_by_source": {
                "range_low": {
                    "authority": "price_first",
                    "position_size_multiplier": 0.75,
                }
            },
        }
        route = {
            "position_size_pct": 0.09,
            "entry_step_pct_by_source": {
                "range_low": 0.0036,
            },
            "entry_policy_by_source": {
                "range_low": {
                    "position_size_multiplier": 0.5,
                }
            },
        }

        merged = merge_strategy_configs(base, route)

        self.assertEqual(merged["position_size_pct"], 0.09)
        self.assertEqual(
            merged["entry_step_pct_by_source"],
            {"range_low": 0.0036, "range_median": 0.0045},
        )
        self.assertEqual(
            merged["entry_policy_by_source"]["range_low"],
            {
                "authority": "price_first",
                "position_size_multiplier": 0.5,
            },
        )
        self.assertEqual(base["position_size_pct"], 0.12)
        self.assertEqual(route["position_size_pct"], 0.09)

    def test_fingerprint_is_order_independent_and_changes_with_policy(self):
        first = {"b": 2, "a": {"x": 1}}
        reordered = {"a": {"x": 1}, "b": 2}
        changed = {"a": {"x": 2}, "b": 2}

        self.assertEqual(
            strategy_config_fingerprint(first),
            strategy_config_fingerprint(reordered),
        )
        self.assertNotEqual(
            strategy_config_fingerprint(first),
            strategy_config_fingerprint(changed),
        )

    def test_resolver_records_identity_and_override_paths(self):
        resolved = resolve_effective_strategy(
            {"max_inventory_usd": 2500, "minimum_order_floor_enabled": True},
            {"max_inventory_usd": 675},
            buy_source="range_low",
            base_label="production-source-policy.json",
            route_label="high-reversion-hybrid",
            route_file="/bot/high-reversion-hybrid.json",
        )

        self.assertEqual(
            resolved["composition_mode"],
            EFFECTIVE_STRATEGY_COMPOSITION_MODE,
        )
        self.assertEqual(resolved["buy_source"], "range_low")
        self.assertEqual(resolved["route_label"], "high-reversion-hybrid")
        self.assertEqual(resolved["payload"]["max_inventory_usd"], 675)
        self.assertTrue(resolved["payload"]["minimum_order_floor_enabled"])
        self.assertEqual(
            resolved["route_override_paths"],
            ["max_inventory_usd"],
        )
        self.assertNotEqual(
            resolved["base_fingerprint"],
            resolved["effective_fingerprint"],
        )

    def test_base_only_resolution_has_matching_fingerprints(self):
        resolved = resolve_effective_strategy(
            {"grid_anchor": "low,median,high"},
            buy_source="range_median",
            base_label="base.json",
        )

        self.assertIsNone(resolved["route_fingerprint"])
        self.assertEqual(resolved["route_override_paths"], [])
        self.assertEqual(
            resolved["base_fingerprint"],
            resolved["effective_fingerprint"],
        )


if __name__ == "__main__":
    unittest.main()
