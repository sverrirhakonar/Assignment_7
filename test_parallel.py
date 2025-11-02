if __name__ == "__main__":
    from parallel_fin import data_loader, parallel

    csv_path = "data/market_data-1.csv"
    df, _ = data_loader.load_pandas(csv_path)

    result, stats = parallel.parallel_rolling_multiprocess(df, window=20, max_workers=4)

    print("Multiprocessing Rolling Stats:", stats)
    print(result.head())
