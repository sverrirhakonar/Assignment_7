import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate


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
