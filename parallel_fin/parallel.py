import pandas as pd
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from .metrics import compute_rolling_pandas
from .metrics import profile_resources


@profile_resources
def parallel_rolling_threaded(df: pd.DataFrame, window: int = 20, max_workers: int = 4) -> pd.DataFrame:
    """
    Compute rolling metrics for each symbol using threads.
    Each thread handles one symbol subset.
    """

    symbols = df["symbol"].unique()
    results = []

    # Define per-symbol computation
    def process_symbol(symbol):
        sub_df = df[df["symbol"] == symbol].copy()
        res, _ = compute_rolling_pandas(sub_df, window)
        return res

    # Thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_symbol, sym): sym for sym in symbols}

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error in {symbol}: {e}")

    combined = pd.concat(results).sort_index()
    return combined

