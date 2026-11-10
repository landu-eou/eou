from __future__ import annotations

import sys
import sqlite3
import tempfile
import unittest
from decimal import Decimal, ROUND_HALF_UP, localcontext
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from extract_jita_reference import (  # noqa: E402
    STATION_ID,
    build_prices,
    calculate_market_indicators,
    filter_reference_orders,
    write_market_database,
)


def make_order(order_id: int, type_id: int, price: str, volume: int, buy: bool) -> dict:
    return {
        "order_id": order_id,
        "type_id": type_id,
        "location_id": STATION_ID,
        "is_buy_order": buy,
        "volume_remain": volume,
        "price": float(price),
    }


class MarketMetricsTests(unittest.TestCase):
    def test_build_filters_once_for_each_type_and_side(self):
        buy = [make_order(1, 34, "10", 5, True), make_order(2, 34, "9", 3, True)]
        sell = [make_order(3, 34, "11", 7, False), make_order(4, 34, "12", 2, False)]
        with patch("extract_jita_reference.filter_reference_orders", wraps=filter_reference_orders) as filter_spy:
            row = build_prices(buy, sell)[0]
        self.assertEqual(filter_spy.call_count, 2)
        self.assertEqual(row["buy_liquidity"]["orders"], 2)
        self.assertEqual(row["sell_liquidity"]["orders"], 2)

    def test_all_indicators_use_filtered_orders(self):
        buy = [make_order(1, 34, "1", 80, True)] + [
            make_order(index, 34, "100", 2, True) for index in range(2, 11)
        ]
        sell = [make_order(20, 34, "1000", 80, False)] + [
            make_order(index, 34, "10", 2, False) for index in range(21, 30)
        ]
        filtered_buy, buy_metrics = filter_reference_orders(buy, True)
        filtered_sell, sell_metrics = filter_reference_orders(sell, False)
        self.assertEqual(buy_metrics["excluded_orders"], 1)
        self.assertEqual(sell_metrics["excluded_orders"], 1)

        metrics = calculate_market_indicators(filtered_buy, filtered_sell)
        self.assertEqual(metrics["best_bid"], Decimal("100.00"))
        self.assertEqual(metrics["best_ask"], Decimal("10.00"))
        self.assertEqual(metrics["spread"], {"absolute": Decimal("-90.0000"), "percentage": Decimal("-163.6364")})
        self.assertEqual(metrics["buy_liquidity"], {"orders": 9, "volume": 18, "value_isk": Decimal("1800.00")})
        self.assertEqual(metrics["sell_liquidity"], {"orders": 9, "volume": 18, "value_isk": Decimal("180.00")})
        self.assertEqual(metrics["buy_distribution"], {
            "median": Decimal("100.00"), "p10": Decimal("100.00"), "p90": Decimal("100.00"),
            "top_5_volume_pct": Decimal("55.5556"),
        })
        self.assertEqual(metrics["market_balance"], {"imbalance_5pct": Decimal("0.0000")})

        row = build_prices(buy, sell)[0]
        self.assertEqual(row["best_bid"], Decimal("100.00"))
        self.assertEqual(row["best_ask"], Decimal("10.00"))
        self.assertEqual(row["spread"]["absolute"], Decimal("-90.0000"))
        self.assertEqual(row["buy_depth"][5], {"volume": 18, "orders": 9})
        self.assertEqual(row["sell_depth"][5], {"volume": 18, "orders": 9})
        self.assertEqual(row["buy_distribution"]["median"], Decimal("100.00"))
        self.assertEqual(row["sell_distribution"]["median"], Decimal("10.00"))
        self.assertEqual(row["buy_execution"][100]["volume"], row["buy_liquidity"]["volume"])
        self.assertEqual(row["sell_execution"][100]["volume"], row["sell_liquidity"]["volume"])

    def test_spread_rounding_and_crossed_market(self):
        metrics = calculate_market_indicators(
            [make_order(1, 1, "10.005", 1, True)],
            [make_order(2, 1, "10.015", 1, False)],
        )
        self.assertEqual(metrics["best_bid"], Decimal("10.01"))
        self.assertEqual(metrics["best_ask"], Decimal("10.02"))
        self.assertEqual(metrics["spread"], {"absolute": Decimal("0.0100"), "percentage": Decimal("0.0999")})

        crossed = calculate_market_indicators(
            [make_order(3, 1, "12", 1, True)], [make_order(4, 1, "8", 1, False)]
        )
        self.assertEqual(crossed["spread"]["absolute"], Decimal("-4.0000"))

    def test_depth_includes_exact_one_two_and_five_percent_boundaries(self):
        buy = [make_order(i, 1, price, 10, True) for i, price in enumerate(("100", "99", "98", "95"), 1)]
        sell = [make_order(i + 10, 1, price, 10, False) for i, price in enumerate(("100", "101", "102", "105"), 1)]
        metrics = calculate_market_indicators(buy, sell)
        self.assertEqual(metrics["buy_depth"], {
            1: {"volume": 20, "orders": 2},
            2: {"volume": 30, "orders": 3},
            5: {"volume": 40, "orders": 4},
        })
        self.assertEqual(metrics["sell_depth"], {
            1: {"volume": 20, "orders": 2},
            2: {"volume": 30, "orders": 3},
            5: {"volume": 40, "orders": 4},
        })
        self.assertEqual(metrics["market_balance"]["imbalance_5pct"], Decimal("0.0000"))

    def test_distribution_interpolates_by_order_and_concentration_by_volume(self):
        buy = [
            make_order(1, 3, "10", 100, True),
            make_order(2, 3, "20", 40, True),
            make_order(3, 3, "30", 30, True),
            make_order(4, 3, "40", 20, True),
            make_order(5, 3, "50", 10, True),
            make_order(6, 3, "60", 5, True),
        ]
        dist = calculate_market_indicators(buy, [])["buy_distribution"]
        self.assertEqual(dist["median"], Decimal("35.00"))
        self.assertEqual(dist["p10"], Decimal("15.00"))
        self.assertEqual(dist["p90"], Decimal("55.00"))
        self.assertEqual(dist["top_5_volume_pct"], Decimal("97.5610"))

    def test_empty_sides_and_empty_book(self):
        one_sided = calculate_market_indicators([make_order(1, 7, "10", 4, True)], [])
        self.assertEqual(one_sided["best_bid"], Decimal("10.00"))
        self.assertIsNone(one_sided["best_ask"])
        self.assertEqual(one_sided["spread"], {"absolute": None, "percentage": None})
        self.assertIsNotNone(one_sided["buy_depth"])
        self.assertIsNone(one_sided["sell_depth"])
        self.assertEqual(one_sided["sell_liquidity"], {"orders": 0, "volume": 0, "value_isk": Decimal("0.00")})
        self.assertEqual(one_sided["sell_distribution"], {
            "median": None, "p10": None, "p90": None, "top_5_volume_pct": None,
        })
        self.assertIsNone(one_sided["market_balance"]["imbalance_5pct"])

        empty = calculate_market_indicators([], [])
        self.assertIsNone(empty["best_bid"])
        self.assertIsNone(empty["best_ask"])
        self.assertIsNone(empty["buy_depth"])
        self.assertIsNone(empty["sell_depth"])

    def test_extreme_decimal_prices_and_integer_volumes_remain_precise(self):
        buy_volume = 10**30
        sell_volume = 2 * 10**30
        metrics = calculate_market_indicators(
            [{"price": Decimal("1e30"), "volume_remain": buy_volume}],
            [{"price": Decimal("1.01e30"), "volume_remain": sell_volume}],
        )
        self.assertEqual(metrics["spread"]["absolute"], Decimal("1e28"))
        self.assertEqual(metrics["spread"]["percentage"], Decimal("0.9950"))
        self.assertEqual(metrics["buy_liquidity"]["value_isk"], Decimal("1e60"))
        self.assertEqual(metrics["sell_liquidity"]["value_isk"], Decimal("2.02e60"))
        self.assertEqual(metrics["buy_depth"][5]["volume"], buy_volume)
        self.assertEqual(metrics["sell_depth"][5]["volume"], sell_volume)
        self.assertEqual(metrics["market_balance"]["imbalance_5pct"], Decimal("-0.3333"))

        precise_price = Decimal("123456789012345678901234567890.12345")
        with localcontext() as context:
            context.prec = 100
            expected_value = (precise_price * buy_volume).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        precise = calculate_market_indicators(
            [{"price": precise_price, "volume_remain": buy_volume}], []
        )
        self.assertEqual(precise["buy_liquidity"]["value_isk"], expected_value)

    def test_sqlite_snapshot_schema_and_fixed_point_values(self):
        rows = build_prices(
            [make_order(1, 8, "10", 2, True)],
            [make_order(2, 8, "12", 3, False)],
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "referenceMarketPrices.db"
            write_market_database(rows, path)
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
                result = connection.execute(
                    """SELECT buy_price_cents, sell_price_cents, best_bid_cents, best_ask_cents,
                              spread_absolute_1e4, spread_percentage_1e4,
                              buy_at_reference_volume, buy_at_reference_price_cents,
                              sell_at_reference_volume, sell_at_reference_price_cents, imbalance_5pct_1e4
                       FROM market_prices WHERE type_id = 8"""
                ).fetchone()
                self.assertEqual(result, (1000, 1200, 1000, 1200, 20000, 181818, 2, 1000, 3, 1200, -2000))
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM execution_levels").fetchone()[0], 12)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM depth_levels").fetchone()[0], 6)
                self.assertEqual(
                    connection.execute("SELECT side, orders, volume, value_isk_cents FROM liquidity ORDER BY side").fetchall(),
                    [("buy", 1, 2, 2000), ("sell", 1, 3, 3600)],
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT side, median_cents, p10_cents, p90_cents FROM price_distributions ORDER BY side"
                    ).fetchall(),
                    [("buy", 1000, 1000, 1000), ("sell", 1200, 1200, 1200)],
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
