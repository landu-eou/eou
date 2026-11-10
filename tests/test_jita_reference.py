from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from extract_jita_reference import (  # noqa: E402
    STATION_ID,
    calculateBuyReferencePrice,
    calculateSellReferencePrice,
)


def make_order(price: float, order_id: int, buy: bool = True, **overrides):
    order = {
        "order_id": order_id,
        "type_id": 34,
        "location_id": STATION_ID,
        "is_buy_order": buy,
        "volume_remain": 10,
        "price": price,
    }
    order.update(overrides)
    return order


class BuyReferenceTests(unittest.TestCase):
    def test_example_distribution_keeps_all_orders(self):
        orders = [make_order(price, index) for index, price in enumerate([2, 3, 4, 5, 5.1, 5.2, 5.3, 5.4], 1)]
        result = calculateBuyReferencePrice(orders)
        self.assertEqual(result["initial_mean"], Decimal("4.375"))
        self.assertEqual(result["final_mean"], Decimal("4.375"))
        self.assertEqual(result["q1"], Decimal("3.75"))
        self.assertEqual(result["q3"], Decimal("5.225"))
        self.assertEqual(result["lower_fence"], Decimal("1.5375"))
        self.assertEqual(result["excluded_orders"], 0)

    def test_excludes_only_lower_outlier(self):
        orders = [make_order(1, 1), *[make_order(100, index) for index in range(2, 9)]]
        result = calculateBuyReferencePrice(orders)
        self.assertEqual(result["excluded_order_ids"], [1])
        self.assertEqual(result["retained_orders"], 7)
        self.assertEqual(result["final_mean"], Decimal(100))

    def test_does_not_exclude_upper_buy_price(self):
        orders = [*[make_order(10, index) for index in range(1, 8)], make_order(1000, 8)]
        result = calculateBuyReferencePrice(orders)
        self.assertEqual(result["excluded_orders"], 0)
        self.assertEqual(result["retained_orders"], 8)

    def test_single_identical_and_empty_prices(self):
        self.assertEqual(calculateBuyReferencePrice([make_order(42, 1)])["final_mean"], Decimal(42))
        identical = calculateBuyReferencePrice([make_order(42, i) for i in range(1, 6)])
        self.assertEqual(identical["excluded_orders"], 0)
        self.assertEqual(identical["final_mean"], Decimal(42))
        empty = calculateBuyReferencePrice([])
        self.assertIsNone(empty["final_mean"])
        self.assertEqual(empty["valid_orders"], 0)

    def test_filters_wrong_side_station_volume_and_price(self):
        orders = [
            make_order(20, 1),
            make_order(1, 2, buy=False),
            make_order(1, 3, location_id=1),
            make_order(1, 4, volume_remain=0),
            make_order(0, 5),
        ]
        result = calculateBuyReferencePrice(orders)
        self.assertEqual(result["total_orders"], 5)
        self.assertEqual(result["valid_orders"], 1)
        self.assertEqual(result["final_mean"], Decimal(20))


class SellReferenceTests(unittest.TestCase):
    def test_excludes_only_upper_outlier(self):
        orders = [*[make_order(10, index, buy=False) for index in range(1, 8)], make_order(1000, 8, buy=False)]
        result = calculateSellReferencePrice(orders)
        self.assertEqual(result["excluded_order_ids"], [8])
        self.assertEqual(result["upper_fence"], Decimal(10))
        self.assertEqual(result["final_mean"], Decimal(10))

    def test_does_not_exclude_lower_sell_price(self):
        orders = [make_order(1, 1, buy=False), *[make_order(100, i, buy=False) for i in range(2, 9)]]
        result = calculateSellReferencePrice(orders)
        self.assertEqual(result["excluded_orders"], 0)
        self.assertEqual(result["final_mean"], Decimal("87.625"))


if __name__ == "__main__":
    unittest.main()
