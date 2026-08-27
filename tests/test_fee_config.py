import unittest

from fee_config import effective_round_trip_fee_pct


class EffectiveRoundTripFeeTests(unittest.TestCase):
    def test_general_fee_schedule_overrides_legacy_round_trip_value(self):
        self.assertEqual(
            effective_round_trip_fee_pct({
                "maker_fee_pct": 0.004,
                "taker_fee_pct": 0.008,
                "round_trip_fee_pct": 0.008,
            }),
            0.012,
        )

    def test_explicit_round_trip_is_used_without_per_side_fees(self):
        self.assertEqual(
            effective_round_trip_fee_pct({
                "round_trip_fee_pct": 0.008,
            }),
            0.008,
        )

    def test_fallback_is_used_without_fee_configuration(self):
        self.assertEqual(effective_round_trip_fee_pct({}, 0.0065), 0.0065)


if __name__ == "__main__":
    unittest.main()
