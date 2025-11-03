import time
import psutil
import pandas as pd
import polars as pl
import statistics


# -----------------------------
# Utility: optional profiling decorator
# -----------------------------
def profile_memory(func):
    """
    Decorator to measure memory usage and execution time of a function.
    Returns a tuple: (result, metrics_dict)
    """
    def wrapper(*args, **kwargs):
        process = psutil.Process()
        mem_before = process.memory_info().rss / (1024 ** 2)  # MB
        start = time.perf_counter()

        result = func(*args, **kwargs)

        end = time.perf_counter()
        mem_after = process.memory_info().rss / (1024 ** 2)
        metrics = {
            "time_sec": round(end - start, 4),
            "mem_diff_mb": round(mem_after - mem_before, 4),
        }
        return result, metrics
    return wrapper


# -----------------------------
# Data loading functions
# -----------------------------
def load_market_data_pandas(csv_path: str) -> pd.DataFrame:
    """
    Load market data using pandas.
    Expected columns: timestamp, symbol, price
    """
    df = pd.read_csv(
        csv_path,
        parse_dates=["timestamp"],
        dtype={"symbol": "category", "price": "float64"},
    )
    df = df.set_index("timestamp").sort_index()
    return df


def load_market_data_polars(csv_path: str) -> pl.DataFrame:
    """
    Load market data using polars.
    Expected columns: timestamp, symbol, price
    """
    df = (
        pl.read_csv(csv_path, try_parse_dates=True)
        .with_columns([
            pl.col("symbol").cast(pl.Categorical),
            pl.col("price").cast(pl.Float64),
        ])
        .sort("timestamp")
    )
    return df


# -----------------------------
# Benchmark helper
# -----------------------------
def compare_ingestion(csv_path: str, repeats: int = 3):
    """
    Compare ingestion time and memory usage between pandas and polars.

    Runs each loader multiple times, averages the results,
    and prints a clean summary table.
    """
    pandas_times, pandas_mems = [], []
    polars_times, polars_mems = [], []

    for _ in range(repeats):
        # time + memory for pandas
        process = psutil.Process()
        mem_before = process.memory_info().rss / (1024 ** 2)
        start = time.perf_counter()
        _ = load_market_data_pandas(csv_path)
        end = time.perf_counter()
        mem_after = process.memory_info().rss / (1024 ** 2)
        pandas_times.append(end - start)
        pandas_mems.append(mem_after - mem_before)

        # time + memory for polars
        mem_before = process.memory_info().rss / (1024 ** 2)
        start = time.perf_counter()
        _ = load_market_data_polars(csv_path)
        end = time.perf_counter()
        mem_after = process.memory_info().rss / (1024 ** 2)
        polars_times.append(end - start)
        polars_mems.append(mem_after - mem_before)

    summary = {
        "pandas_time_avg": round(statistics.mean(pandas_times), 4),
        "pandas_mem_avg": round(statistics.mean(pandas_mems), 4),
        "polars_time_avg": round(statistics.mean(polars_times), 4),
        "polars_mem_avg": round(statistics.mean(polars_mems), 4),
    }

    print("\n=== Ingestion Performance Comparison ===")
    print(f"Pandas: {summary['pandas_time_avg']} s, {summary['pandas_mem_avg']} MB")
    print(f"Polars: {summary['polars_time_avg']} s, {summary['polars_mem_avg']} MB")
    print(f"Speed-up: {round(summary['pandas_time_avg'] / summary['polars_time_avg'], 2)}× faster\n")

    return summary


# -----------------------------
# Test Runner (optional)
# -----------------------------
if __name__ == "__main__":
    import os
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(BASE_DIR, "data", "market_data-1.csv")

    print("Testing Data Loaders...\n")
    df_pd = load_market_data_pandas(csv_path)
    df_pl = load_market_data_polars(csv_path)

    print("✅ Pandas loaded:", df_pd.shape)
    print("✅ Polars loaded:", df_pl.shape)
    compare_ingestion(csv_path)
