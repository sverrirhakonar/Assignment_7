from parallel_fin import data_loader

csv_path = "data/market_data-1.csv"

# Pandas
pd_df, pd_stats = data_loader.load_pandas(csv_path)
print("Pandas metrics:", pd_stats)
print(pd_df.head())

# Polars
pl_df, pl_stats = data_loader.load_polars(csv_path)
print("Polars metrics:", pl_stats)
print(pl_df.head())


summary = data_loader.compare_ingestion(csv_path)
print(summary)