from parallel_fin import data_loader, parallel

def test_thread_vs_process_equivalence():
    df, _ = data_loader.load_pandas("data/market_data-1.csv")
    df_thread, _ = parallel.run_threaded(df, lib="pandas", window=10, max_workers=2)
    df_proc, _ = parallel.run_multiprocess(df, lib="pandas", window=10, max_workers=2)
    assert set(df_thread.columns) == set(df_proc.columns)
