import unittest

from range_grid_assets import (
    asset_balance_from_kraken,
    default_asset_balance_keys,
    infer_asset_id_from_pair,
    kraken_pair_matches,
    kraken_result_for_pair,
    parse_asset_balance_keys,
)


class RangeGridAssetTests(unittest.TestCase):
    def test_infers_asset_id_from_pair(self):
        self.assertEqual(infer_asset_id_from_pair("XXBTZUSD"), "BTC")
        self.assertEqual(infer_asset_id_from_pair("XETHZUSD"), "ETH")
        self.assertEqual(infer_asset_id_from_pair("SOLUSD"), "SOL")

    def test_default_balance_keys_cover_btc_and_eth(self):
        self.assertEqual(default_asset_balance_keys("BTC"), ["XXBT", "XBT", "BTC"])
        self.assertEqual(default_asset_balance_keys("ETH"), ["XETH", "ETH"])

    def test_configured_balance_keys_override_defaults(self):
        self.assertEqual(parse_asset_balance_keys("XETH,ETH", "ETH"), ["XETH", "ETH"])
        self.assertEqual(parse_asset_balance_keys("", "ETH"), ["XETH", "ETH"])

    def test_asset_balance_uses_configured_keys(self):
        balances = {"ZUSD": "100.00", "XETH": "1.25", "XXBT": "0.05"}
        self.assertEqual(asset_balance_from_kraken(balances, ["XETH", "ETH"]), 1.25)
        self.assertEqual(asset_balance_from_kraken(balances, ["XXBT", "XBT"]), 0.05)
        self.assertIsNone(asset_balance_from_kraken(balances, ["SOL"]))

    def test_kraken_pair_matches_configured_usd_asset_aliases(self):
        self.assertTrue(kraken_pair_matches("XETHZUSD", "XETHZUSD", asset_id="ETH"))
        self.assertTrue(kraken_pair_matches("ETH/USD", "XETHZUSD", asset_id="ETH"))
        self.assertFalse(kraken_pair_matches("XXBTZUSD", "XETHZUSD", asset_id="ETH"))
        self.assertTrue(kraken_pair_matches("XBT/USD", "XXBTZUSD", asset_id="BTC"))
        self.assertTrue(kraken_pair_matches("BTCUSD", "XXBTZUSD", asset_id="BTC"))

    def test_kraken_result_for_pair_selects_alias_result(self):
        result = {
            "ETHUSD": {"pair_decimals": 2, "lot_decimals": 8},
            "XXBTZUSD": {"pair_decimals": 1, "lot_decimals": 8},
        }
        self.assertEqual(
            kraken_result_for_pair(result, "XETHZUSD", asset_id="ETH"),
            {"pair_decimals": 2, "lot_decimals": 8},
        )


if __name__ == "__main__":
    unittest.main()
