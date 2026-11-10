from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from extract_jita_reference import (  # noqa: E402
    EXECUTION_PERCENTAGES,
    STATION_ID,
    build_prices,
    calculateBuyReferencePrice,
    calculateSellReferencePrice,
    calculate_execution_levels,
    calculate_reference_depth,
    round_reference_price,
)


def make_order(order_id: int, price: float, volume: int, buy: bool, **overrides):
    order = {
        "order_id": order_id,
        "type_id": 34,
        "location_id": STATION_ID,
        "is_buy_order": buy,
        "volume_remain": volume,
        "price": price,
        "min_volume": 1,
        "range": "station",
    }
    order.update(overrides)
    return order


class ExecutionLevelsTests(unittest.TestCase):
    def test_buy_consumes_highest_prices_first(self):
        orders = [make_order(1, 10, 5, True), make_order(2, 8, 95, True)]
        levels = calculate_execution_levels(orders, buy_side=True)
        self.assertEqual(levels[1], {"volume": 1, "price": Decimal("10.00")})
        self.assertEqual(levels[10], {"volume": 10, "price": Decimal("9.00")})
        self.assertEqual(levels[30], {"volume": 30, "price": Decimal("8.33")})

    def test_sell_consumes_lowest_prices_first(self):
        orders = [make_order(1, 5, 5, False), make_order(2, 8, 95, False)]
        levels = calculate_execution_levels(orders, buy_side=False)
        self.assertEqual(levels[1], {"volume": 1, "price": Decimal("5.00")})
        self.assertEqual(levels[10], {"volume": 10, "price": Decimal("6.50")})
        self.assertEqual(levels[30], {"volume": 30, "price": Decimal("7.50")})

    def test_partially_consumes_the_order_at_the_target_boundary(self):
        orders = [make_order(1, 10, 4, True), make_order(2, 8, 96, True)]
        levels = calculate_execution_levels(orders, buy_side=True)
        self.assertEqual(levels[10], {"volume": 10, "price": Decimal("8.80")})

    def test_target_volume_uses_integer_floor(self):
        levels = calculate_execution_levels([make_order(1, 10, 99, True)], buy_side=True)
        self.assertEqual([levels[p]["volume"] for p in EXECUTION_PERCENTAGES], [0, 4, 9, 19, 29, 99])

    def test_100_percent_consumes_the_entire_valid_book(self):
        buy = calculate_execution_levels(
            [make_order(1, 10, 5, True), make_order(2, 8, 95, True)], buy_side=True
        )
        sell = calculate_execution_levels(
            [make_order(1, 5, 5, False), make_order(2, 8, 95, False)], buy_side=False
        )
        self.assertEqual(buy[100], {"volume": 100, "price": Decimal("8.10")})
        self.assertEqual(sell[100], {"volume": 100, "price": Decimal("7.85")})

    def test_weighted_mean_uses_executed_units_not_order_count(self):
        orders = [make_order(1, 10, 2, True), make_order(2, 6, 98, True)]
        levels = calculate_execution_levels(orders, buy_side=True)
        self.assertEqual(levels[10]["price"], Decimal("6.80"))
        self.assertNotEqual(levels[10]["price"], Decimal("8.00"))

    def test_price_rounding_is_decimal_half_up(self):
        buy_orders = [
            make_order(1, 1.01, 1, True),
            make_order(2, 1.00, 1, True),
            make_order(3, 0.50, 198, True),
        ]
        sell_orders = [
            make_order(1, 1.00, 1, False),
            make_order(2, 1.01, 1, False),
            make_order(3, 2.00, 198, False),
        ]
        self.assertEqual(calculate_execution_levels(buy_orders, True)[1]["price"], Decimal("1.01"))
        self.assertEqual(calculate_execution_levels(sell_orders, False)[1]["price"], Decimal("1.01"))

    def test_zero_target_has_null_price(self):
        levels = calculate_execution_levels([make_order(1, 10, 99, True)], buy_side=True)
        self.assertEqual(levels[1], {"volume": 0, "price": None})

    def test_no_valid_orders_returns_null_block(self):
        invalid = [make_order(1, 10, 0, True)]
        self.assertIsNone(calculate_execution_levels(invalid, buy_side=True))
        self.assertIsNone(calculate_reference_depth(invalid, True, Decimal("10.00")))
        rows = build_prices(invalid, [])
        self.assertIsNone(rows[0]["buy"])
        self.assertIsNone(rows[0]["buy_execution"])
        self.assertIsNone(rows[0]["sell_execution"])
        self.assertIsNone(rows[0]["buy_at_reference"])
        self.assertIsNone(rows[0]["sell_at_reference"])

    def test_reference_depth_includes_orders_at_the_cutoff(self):
        buy = [
            make_order(1, 10, 2, True),
            make_order(2, 9, 3, True),
            make_order(3, 8, 5, True),
        ]
        sell = [
            make_order(1, 8, 2, False),
            make_order(2, 9, 3, False),
            make_order(3, 10, 5, False),
        ]
        self.assertEqual(
            calculate_reference_depth(buy, True, Decimal("9.00")),
            {"volume": 5, "price": Decimal("9.40")},
        )
        self.assertEqual(
            calculate_reference_depth(sell, False, Decimal("9.00")),
            {"volume": 5, "price": Decimal("8.60")},
        )

    def test_reference_cutoff_uses_the_displayed_four_decimal_value(self):
        self.assertEqual(round_reference_price(Decimal("28.761251")), Decimal("28.7613"))

    def test_execution_includes_statistical_outliers(self):
        buy_orders = [make_order(1, 1, 80, True)] + [make_order(i, 100, 2, True) for i in range(2, 11)]
        buy_levels = calculate_execution_levels(buy_orders, buy_side=True)
        buy_mean = calculateBuyReferencePrice(buy_orders)
        self.assertIn(1, buy_mean["excluded_order_ids"])
        self.assertEqual(buy_levels[30]["price"], Decimal("62.45"))

        sell_orders = [make_order(1, 1000, 80, False)] + [make_order(i, 10, 2, False) for i in range(2, 11)]
        sell_levels = calculate_execution_levels(sell_orders, buy_side=False)
        sell_mean = calculateSellReferencePrice(sell_orders)
        self.assertIn(1, sell_mean["excluded_order_ids"])
        self.assertEqual(sell_levels[30]["price"], Decimal("385.52"))

    def test_reference_fields_are_unchanged_by_execution_fields(self):
        buy_orders = [make_order(1, price, 10, True) for price in (1, 2, 3, 4, 5)]
        sell_orders = [make_order(10 + i, price, 10, False) for i, price in enumerate((5, 6, 7, 8, 100))]
        rows = build_prices(buy_orders, sell_orders)
        self.assertEqual(rows[0]["buy"], calculateBuyReferencePrice(buy_orders)["final_mean"])
        self.assertEqual(rows[0]["sell"], calculateSellReferencePrice(sell_orders)["final_mean"])

    def test_buy_price_is_nonincreasing_across_positive_levels(self):
        levels = calculate_execution_levels(
            [make_order(1, 15, 20, True), make_order(2, 10, 80, True)], buy_side=True
        )
        prices = [levels[p]["price"] for p in EXECUTION_PERCENTAGES if levels[p]["volume"] > 0]
        self.assertEqual(prices, sorted(prices, reverse=True))

    def test_sell_price_is_nondecreasing_across_positive_levels(self):
        levels = calculate_execution_levels(
            [make_order(1, 5, 20, False), make_order(2, 10, 80, False)], buy_side=False
        )
        prices = [levels[p]["price"] for p in EXECUTION_PERCENTAGES if levels[p]["volume"] > 0]
        self.assertEqual(prices, sorted(prices))

    def test_execution_volumes_are_nondecreasing(self):
        levels = calculate_execution_levels(
            [make_order(1, 10, 101, True)], buy_side=True
        )
        volumes = [levels[p]["volume"] for p in EXECUTION_PERCENTAGES]
        self.assertEqual(volumes, sorted(volumes))


if __name__ == "__main__":
    unittest.main()
