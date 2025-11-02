import pandas as pd
import numpy as np
import time
import psutil
import polars as pl
import matplotlib.pyplot as plt
from parallel_fin import data_loader

def profile_resources(func):
    """
    Measures execution time, memory, and average CPU%.
    Returns (result, metrics_dict)
    """
    def wrapper(*args, **kwargs):
        process = psutil.Process()
        mem_before = process.memory_info().rss / (1024 ** 2)
        start = time.perf_counter()

        # Start CPU measurement
        cpu_start = process.cpu_times()
        cpu_percent_start = psutil.cpu_percent(interval=None)

        result = func(*args, **kwargs)

        end = time.perf_counter()
        cpu_percent_end = psutil.cpu_percent(interval=None)
        cpu_end = process.cpu_times()
        mem_after = process.memory_info().rss / (1024 ** 2)

        # Compute elapsed time and average CPU%
        elapsed = end - start
        cpu_user = cpu_end.user - cpu_start.user
        cpu_system = cpu_end.system - cpu_start.system
        total_cpu_time = cpu_user + cpu_system
        avg_cpu_percent = round((total_cpu_time / elapsed) * 100, 2) if elapsed > 0 else 0

        metrics = {
            "time_sec": round(elapsed, 4),
            "mem_diff_mb": round(mem_after - mem_before, 4),
            "cpu_percent": avg_cpu_percent,
        }
        return result, metrics
    return wrapper


@profile_resources
def compute_rolling_pandas(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Compute rolling metrics for each symbol using pandas.
    Metrics: moving average, std dev, and Sharpe ratio.
    Assumes df has columns ['symbol', 'price'] and datetime index.
    """
    # Percent returns per symbol
    df["return"] = df.groupby("symbol")["price"].pct_change()

    # Rolling calculations per symbol
    df["ma20"] = df.groupby("symbol")["price"].transform(
        lambda x: x.rolling(window).mean()
    )
    df["vol20"] = df.groupby("symbol")["price"].transform(
        lambda x: x.rolling(window).std()
    )
    df["sharpe20"] = (
        df.groupby("symbol")["return"]
        .transform(lambda x: x.rolling(window).mean() / x.rolling(window).std())
        .replace([np.inf, -np.inf], np.nan)
    )

    return df

@profile_resources
def compute_rolling_polars(df: pl.DataFrame, window: int = 20) -> pl.DataFrame:
    """
    Compute rolling metrics per symbol using Polars.
    Metrics: moving average, std dev, Sharpe ratio (risk-free = 0).
    Assumes df has columns ['timestamp', 'symbol', 'price'].
    """

    # Compute percent returns (grouped by symbol)
    df = df.with_columns(
        (pl.col("price").pct_change().over("symbol")).alias("return")
    )

    # Rolling mean and std for price per symbol
    df = df.with_columns([
        pl.col("price")
        .rolling_mean(window_size=window)
        .over("symbol")
        .alias("ma20"),
        pl.col("price")
        .rolling_std(window_size=window)
        .over("symbol")
        .alias("vol20"),
    ])

    # Rolling Sharpe ratio (mean of returns / std of returns)
    df = df.with_columns(
        (
            (pl.col("return")
             .rolling_mean(window_size=window)
             .over("symbol"))
            /
            (pl.col("return")
             .rolling_std(window_size=window)
             .over("symbol"))
        )
        .alias("sharpe20")
    )

    return df




def compare_rolling_performance(csv_path: str, window: int = 20, symbol: str = "AAPL"):
    """
    Compare pandas vs polars rolling performance and visualize results.
    """

    print("\n=== Rolling Metrics Performance ===")

    # --- Load Data ---
    pd_df, _ = data_loader.load_pandas(csv_path)
    pl_df, _ = data_loader.load_polars(csv_path)

    # --- Compute pandas metrics ---
    pd_result, pd_stats = compute_rolling_pandas(pd_df.copy(), window)
    print(f"Pandas:  {pd_stats}")

    # --- Compute polars metrics ---
    pl_result, pl_stats = compute_rolling_polars(pl_df.clone(), window)
    print(f"Polars:  {pl_stats}")

    # --- Create summary table ---
    summary = pd.DataFrame(
        [
            ["pandas", pd_stats["time_sec"], pd_stats["mem_diff_mb"], pd_stats["cpu_percent"]],
            ["polars", pl_stats["time_sec"], pl_stats["mem_diff_mb"], pl_stats["cpu_percent"]],
        ],
        columns=["Library", "Time (s)", "Mem Δ (MB)", "CPU (%)"],
    )
    print("\nPerformance Summary:")
    print(summary.to_string(index=False))

    # --- Visualization for one symbol (AAPL) ---
    df_plot = pd_result.reset_index()
    df_plot = df_plot[df_plot["symbol"] == symbol]

    plt.figure(figsize=(10, 5))
    plt.plot(df_plot["timestamp"], df_plot["price"], label="Price", alpha=0.6)
    plt.plot(df_plot["timestamp"], df_plot["ma20"], label="20-period MA", linewidth=2)
    plt.title(f"{symbol} – Rolling Metrics (20 periods)")
    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.legend()
    plt.tight_layout()
    plt.show()

    return summary