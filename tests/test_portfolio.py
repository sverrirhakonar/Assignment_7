import unittest
import pandas as pd
import numpy as np
from parallel_fin.portfolio import aggregate_portfolio_sequential, aggregate_portfolio_multiprocessing

# Mock helper to generate fake price data
def make_symbol_prices():
    np.random.seed(42)
    dates = pd.date_range("2020-01-01", periods=10)
    return {
        "AAPL": pd.Series(np.linspace(100, 110, 10), index=dates),
        "GOOG": pd.Series(np.linspace(200, 210, 10), index=dates),
    }

# Simple portfolio JSON structure
TEST_PORTFOLIO = {
    "name": "Root Portfolio",
    "positions": [
        {"symbol": "AAPL", "quantity": 10},
        {"symbol": "GOOG", "quantity": 5},
    ],
    "sub_portfolios": [
        {
            "name": "Tech Sub",
            "positions": [{"symbol": "AAPL", "quantity": 2}],
            "sub_portfolios": []
        }
    ]
}


class TestPortfolioAggregation(unittest.TestCase):
    def setUp(self):
        self.symbol_prices = make_symbol_prices()

    def test_sequential_aggregation(self):
        """Ensure sequential aggregation returns expected structure."""
        result = aggregate_portfolio_sequential(TEST_PORTFOLIO, self.symbol_prices)
        self.assertIn("total_value", result)
        self.assertIn("aggregate_volatility", result)
        self.assertIn("max_drawdown", result)
        self.assertGreater(result["total_value"], 0)
        self.assertIsInstance(result["positions"], list)
        print(" Sequential aggregation OK")

    def test_multiprocessing_aggregation(self):
        """Ensure multiprocessing aggregation returns consistent results."""
        seq_result = aggregate_portfolio_sequential(TEST_PORTFOLIO, self.symbol_prices)
        mp_result = aggregate_portfolio_multiprocessing(TEST_PORTFOLIO, self.symbol_prices)
        self.assertAlmostEqual(seq_result["total_value"], mp_result["total_value"], places=4)
        print(" Multiprocessing aggregation OK")

    def test_nested_sub_portfolios(self):
        """Check nested portfolio calculations are consistent."""
        result = aggregate_portfolio_sequential(TEST_PORTFOLIO, self.symbol_prices)
        sub = result["sub_portfolios"][0]
        self.assertGreater(result["total_value"], sub["total_value"])
        print(" Nested sub-portfolio OK")


if __name__ == "__main__":
    unittest.main(verbosity=2)
