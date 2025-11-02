from parallel_fin import data_loader, metrics

csv_path = "data/market_data-1.csv"

# Load data (pandas version)
df, _ = data_loader.load_pandas(csv_path)

# Compute rolling metrics
df_metrics, stats = metrics.compute_rolling_pandas(df)

print("Rolling metrics computed in:", stats)
print(df_metrics.head(100))



# Load with Polars
pl_df, _ = data_loader.load_polars(csv_path)

# Compute rolling metrics
pl_df_metrics, stats = metrics.compute_rolling_polars(pl_df)
print("Polars metrics computed in:", stats)
print(pl_df_metrics.head(100))


csv_path = "data/market_data-1.csv"
metrics.compare_rolling_performance(csv_path)