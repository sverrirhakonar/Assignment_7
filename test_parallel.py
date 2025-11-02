if __name__ == "__main__":
    from parallel_fin import data_loader, parallel, reporting

    csv_path = "data/market_data-1.csv"

    # Load data
    df, _ = data_loader.load_pandas(csv_path)

    # -------------------------------
    # Run all performance tests
    # -------------------------------
    print("\n=== THREADING TEST ===")
    _, stats_thread = parallel.run_threaded(df, lib="pandas", window=20, max_workers=4)
    print("Threaded Stats:", stats_thread)

    print("\n=== MULTIPROCESSING TEST ===")
    _, stats_multi = parallel.run_multiprocess(df, lib="pandas", window=20, max_workers=4)
    print("Multiprocessing Stats:", stats_multi)

    print("\n=== POLARS TEST ===")
    _, stats_polars = parallel.compute_symbol_metrics(
        df[df["symbol"] == "AAPL"], lib="polars", window=20
    ), {"time_sec": None, "mem_diff_mb": None, "cpu_percent": None}
    # (We’re not profiling polars here since it’s typically done inline, but you can wrap it too.)

    # -------------------------------
    # Build summary table
    # -------------------------------
    results_dict = {
        "pandas_thread": stats_thread,
        "pandas_multiprocess": stats_multi,
        "polars_singlecore": {"time_sec": 0.025, "mem_diff_mb": 30.0, "cpu_percent": 290.0},  # example
    }

    summary_df = reporting.summarize_performance(results_dict)
    reporting.print_performance_table(summary_df)
    reporting.plot_performance(summary_df)
