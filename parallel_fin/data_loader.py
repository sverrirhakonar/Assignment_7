import time
import psutil
import pandas as pd
import polars as pl


def profile_memory(func):
    """
    Decorator to measure memory usage and execution time of a function.
    Returns (result, metrics_dict)
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


@profile_memory
def load_pandas(csv_path: str) -> pd.DataFrame:
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


@profile_memory
def load_polars(csv_path: str) -> pl.DataFrame:
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


import statistics

def compare_ingestion(csv_path: str, repeats: int = 3):
    """
    Compare ingestion time and memory usage between pandas and polars.

    Runs each loader multiple times, averages the results,
    and prints a clean summary table.
    """
    pandas_times, pandas_mems = [], []
    polars_times, polars_mems = [], []

    for _ in range(repeats):
        _, pd_metrics = load_pandas(csv_path)
        _, pl_metrics = load_polars(csv_path)

        pandas_times.append(pd_metrics["time_sec"])
        pandas_mems.append(pd_metrics["mem_diff_mb"])

        polars_times.append(pl_metrics["time_sec"])
        polars_mems.append(pl_metrics["mem_diff_mb"])

    summary = {
        "pandas_time_avg": round(statistics.mean(pandas_times), 4),
        "pandas_mem_avg": round(statistics.mean(pandas_mems), 4),
        "polars_time_avg": round(statistics.mean(polars_times), 4),
        "polars_mem_avg": round(statistics.mean(polars_mems), 4),
    }

    print("\n=== Ingestion Performance Comparison ===")
    print(f"Pandas: {summary['pandas_time_avg']} s, {summary['pandas_mem_avg']} MB")
    print(f"Polars: {summary['polars_time_avg']} s, {summary['polars_mem_avg']} MB")
    print(f"Speed-up: {round(summary['pandas_time_avg']/summary['polars_time_avg'], 2)}× faster\n")

    return summary

