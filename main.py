"""
main.py
Entry point for the parallel_fin project.
Demonstrates data loading, metric computation, and portfolio aggregation.
"""

import os
import json
from parallel_fin.data_loader import load_market_data_pandas, load_market_data_polars
from parallel_fin.metrics import compute_rolling_pandas, compute_rolling_polars, build_symbol_price_map_pandas
from parallel_fin.portfolio import aggregate_portfolio_sequential
from parallel_fin.parallel import run_threaded

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "data", "market_data-1.csv")
PORTFOLIO_PATH = os.path.join(BASE_DIR, "data", "portfolio_structure-1.json")

def main():
    print("=== Parallel Finance Project Demo ===")

    # Load market data (you can switch to polars if preferred)
    df = load_market_data_pandas(CSV_PATH)
    print(f"Loaded data: {df.shape[0]:,} rows")

    # Compute rolling metrics
    df_metrics = compute_rolling_pandas(df)
    print("Computed rolling metrics (ma20, vol20, ret_vol20)")

    # Build symbol-price mapping
    symbol_prices = build_symbol_price_map_pandas(df_metrics)

    # Load and aggregate portfolio sequentially
    with open(PORTFOLIO_PATH, "r") as f:
        portfolio = json.load(f)
    result = aggregate_portfolio_sequential(portfolio, symbol_prices)

    # Display summary
    print("\nPortfolio Aggregation Summary:")
    print(f"Total Value: {result['total_value']:.2f}")
    print(f"Aggregate Volatility: {result['aggregate_volatility']:.4f}")
    print(f"Max Drawdown: {result['max_drawdown']:.4f}")
    print("✅ Completed successfully.")

    # Optional: Run threaded demo for speed
    print("\nRunning threaded performance test (pandas)...")
    run_threaded(df, "pandas", 20, 4)
    print("✅ Threaded computation finished.")

if __name__ == "__main__":
    main()
