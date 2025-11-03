# benchmark_all.py
import os
import time
import psutil
import json
import pandas as pd
import polars as pl

# 🧩 Local imports (match your current naming in data_loader)
from parallel_fin.data_loader import (
    load_market_data_pandas,
    load_market_data_polars,
)
from parallel_fin.metrics import compute_rolling_pandas, compute_rolling_polars, build_symbol_price_map_pandas
from parallel_fin.parallel import run_threaded, run_multiprocess
from parallel_fin.portfolio import aggregate_portfolio_sequential, aggregate_portfolio_multiprocessing

# -----------------------------------
# Setup paths (auto-detect)
# -----------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "data", "market_data-1.csv")
PORTFOLIO_PATH = os.path.join(BASE_DIR, "data", "portfolio_structure-1.json")

# -----------------------------------
# Helper to profile runtime, memory, and CPU
# -----------------------------------
def profile_block(func, *args, **kwargs):
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / (1024 * 1024)
    cpu_before = psutil.cpu_percent(interval=None)
    start = time.perf_counter()

    result = func(*args, **kwargs)

    end = time.perf_counter()
    cpu_after = psutil.cpu_percent(interval=None)
    mem_after = process.memory_info().rss / (1024 * 1024)

    stats = {
        "time_sec": round(end - start, 4),
        "mem_diff_mb": round(mem_after - mem_before, 4),
        "cpu_percent": round((cpu_before + cpu_after) / 2, 2),
    }
    return stats, result


# -----------------------------------
# Run benchmarks
# -----------------------------------
if __name__ == "__main__":
    print("=== Running Benchmarks ===")

    # 1️⃣ Data ingestion
    pandas_stats, df_pandas = profile_block(load_market_data_pandas, CSV_PATH)
    polars_stats, df_polars = profile_block(load_market_data_polars, CSV_PATH)
    print("\n[Ingestion Results]")
    print("Pandas:", pandas_stats)
    print("Polars:", polars_stats)

    # 2️⃣ Rolling metrics
    pandas_roll_stats, _ = profile_block(compute_rolling_pandas, df_pandas)
    polars_roll_stats, _ = profile_block(compute_rolling_polars, df_polars)
    print("\n[Rolling Metrics Results]")
    print("Pandas:", pandas_roll_stats)
    print("Polars:", polars_roll_stats)

    # 3️⃣ Parallel computation (threaded vs multiprocessing)
    pandas_thread_stats, _ = profile_block(run_threaded, df_pandas, "pandas", 20, 4)
    pandas_mp_stats, _ = profile_block(run_multiprocess, df_pandas, "pandas", 20, 4)
    print("\n[Parallel Performance Results]")
    print("Threaded Pandas:", pandas_thread_stats)
    print("Multiprocessing Pandas:", pandas_mp_stats)

    # 4️⃣ Portfolio aggregation
    with open(PORTFOLIO_PATH, "r") as f:
        portfolio_json = json.load(f)

    symbol_prices = build_symbol_price_map_pandas(df_pandas)
    seq_stats, _ = profile_block(aggregate_portfolio_sequential, portfolio_json, symbol_prices)
    mp_stats, _ = profile_block(aggregate_portfolio_multiprocessing, portfolio_json, symbol_prices)
    print("\n[Portfolio Aggregation Results]")
    print("Sequential:", seq_stats)
    print("Multiprocess:", mp_stats)

    # 5️⃣ Summary JSON
    summary = {
        "ingestion": {"pandas": pandas_stats, "polars": polars_stats},
        "rolling": {"pandas": pandas_roll_stats, "polars": polars_roll_stats},
        "parallel": {
            "pandas_thread": pandas_thread_stats,
            "pandas_multiprocess": pandas_mp_stats,
        },
        "portfolio": {
            "sequential": seq_stats,
            "multiprocess": mp_stats,
        },
    }

    print("\n=== SUMMARY JSON ===")
    print(json.dumps(summary, indent=4))

    # Save results to file
    results_path = os.path.join(BASE_DIR, "benchmark_results.json")
    with open(results_path, "w") as f:
        json.dump(summary, f, indent=4)
    print(f"\n✅ Results saved to {results_path}")
