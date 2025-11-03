import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from parallel_fin.metrics import profile_resources
from parallel_fin.portfolio import aggregate_portfolio_sequential, aggregate_portfolio_multiprocessing
import json
import math
import numpy as np
from typing import Any



def summarize_performance(results_dict):
    summary_df = pd.DataFrame(results_dict).T
    summary_df.index.name = "Library / Method"
    return summary_df


def print_performance_table(summary_df):
    print("\n=== Performance Summary ===")
    print(tabulate(summary_df, headers="keys", tablefmt="github", floatfmt=".4f"))


def plot_performance(summary_df):
    metrics = ["time_sec", "mem_diff_mb", "cpu_percent"]
    titles = ["Execution Time (s)", "Memory Usage Δ (MB)", "CPU Utilization (%)"]

    for i, metric in enumerate(metrics):
        plt.figure(figsize=(6, 4))
        summary_df[metric].plot(kind="bar", edgecolor="black")
        plt.title(titles[i])
        plt.ylabel(metric)
        plt.xticks(rotation=20)
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout()
        plt.show()

@profile_resources
def time_portfolio_seq(portfolio_tree, sym_prices, vol_window=20):
    return aggregate_portfolio_sequential(portfolio_tree, sym_prices, vol_window=vol_window)

@profile_resources
def time_portfolio_mp(portfolio_tree, sym_prices, vol_window=20, max_workers=None):
    return aggregate_portfolio_multiprocessing(
        portfolio_tree, sym_prices, vol_window=vol_window, max_workers=max_workers
    )

def _nan_to_none(obj: Any):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_nan_to_none(v) for v in obj]
    return obj

def save_portfolio_json(obj: Any, path):
    """Write portfolio hierarchy to JSON, converting NaN/inf to null."""
    clean = _nan_to_none(obj)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)

def save_portfolio_json(obj: Any, path):
    """Write portfolio hierarchy to JSON, converting NaN/inf to null."""
    clean = _nan_to_none(obj)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)
