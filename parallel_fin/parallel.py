import pandas as pd
import polars as pl
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from .metrics import compute_rolling_pandas, compute_rolling_polars
from .metrics import profile_resources 



def compute_symbol_metrics(df_for_one_symbol, lib: str = "pandas", window: int = 20):
    """
    Compute rolling metrics for a single symbol using the selected library.
    Supports both pandas and polars.
    """
    if lib == "pandas":
        res, _ = compute_rolling_pandas(df_for_one_symbol, window)
    elif lib == "polars":
        pl_df = pl.from_pandas(df_for_one_symbol)
        res, _ = compute_rolling_polars(pl_df, window)
        res = res.to_pandas()
    else:
        raise ValueError("lib must be 'pandas' or 'polars'")
    return res



@profile_resources
def run_threaded(df_all: pd.DataFrame, lib: str = "pandas", window: int = 20, max_workers: int = 4):
    """
    Compute rolling metrics for all symbols in parallel using threads.
    Each thread processes one symbol subset.
    """

    symbols = df_all["symbol"].unique()
    results = []

    def worker(symbol):
        sub_df = df_all[df_all["symbol"] == symbol].copy()
        return compute_symbol_metrics(sub_df, lib, window)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"Error in {symbol}: {e}")

    combined = pd.concat(results).sort_index()
    return combined



def worker_process(symbol, df_all, lib, window):
    """
    Worker function executed in a separate process.
    """
    sub_df = df_all[df_all["symbol"] == symbol].copy()
    return compute_symbol_metrics(sub_df, lib, window)


@profile_resources
def run_multiprocess(df_all: pd.DataFrame, lib: str = "pandas", window: int = 20, max_workers: int = 4):
    """
    Compute rolling metrics for all symbols using multiple processes.
    Each process works independently on one symbol.
    """

    symbols = df_all["symbol"].unique()
    results = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker_process, sym, df_all, lib, window): sym for sym in symbols}
        for fut in as_completed(futures):
            symbol = futures[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"Error in {symbol}: {e}")

    combined = pd.concat(results).sort_index()
    return combined
