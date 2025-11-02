from parallel_fin import data_loader, metrics, parallel, reporting

def main():
    df, _ = data_loader.load_pandas("data/market_data-1.csv")
    _, stats_thread = parallel.run_threaded(df)
    _, stats_multi = parallel.run_multiprocess(df)
    results = {"threaded": stats_thread, "multiprocess": stats_multi}
    summary = reporting.summarize_performance(results)
    reporting.print_performance_table(summary)

if __name__ == "__main__":
    main()