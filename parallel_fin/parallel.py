import pandas as pd
import polars as pl
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from .metrics import compute_rolling_pandas, compute_rolling_polars
from .metrics import profile_resources 
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
from .metrics import rolling_return_volatility, max_drawdown, build_symbol_price_map_pandas




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

@dataclass
class PositionMetrics:
    symbol: str
    quantity: float
    value: float
    volatility: float
    drawdown: float

def _pack_series(s: Optional[pd.Series]) -> Optional[List[Tuple[pd.Timestamp, float]]]:
    if s is None or s.empty:
        return None
    return list(zip(s.index.to_pydatetime(), s.tolist()))

def _position_worker(args: Tuple[str, float, Optional[List[Tuple[pd.Timestamp, float]]], Optional[float], int]) -> PositionMetrics:
    symbol, quantity, packed, fallback_price, vol_window = args
    if packed:
        idx, vals = zip(*packed)
        prices = pd.Series(vals, index=pd.to_datetime(idx), dtype=float)
        latest = float(prices.iloc[-1])
        vol = rolling_return_volatility(prices, window=vol_window)
        dd = max_drawdown(prices)
        value = quantity * latest
    else:
        latest = float(fallback_price) if fallback_price is not None else float("nan")
        value = quantity * latest if not np.isnan(latest) else float("nan")
        vol, dd = float("nan"), float("nan")
    return PositionMetrics(symbol, float(quantity), float(value), float(vol), float(dd))

@profile_resources
def compute_positions_multiprocess(
    positions_spec: List[Dict[str, Any]],
    symbol_prices: Dict[str, pd.Series],
    vol_window: int = 20,
    max_workers: Optional[int] = None,
) -> List[PositionMetrics]:
    tasks: List[Tuple[str, float, Optional[List[Tuple[pd.Timestamp, float]]], Optional[float], int]] = []
    for pos in positions_spec:
        sym = pos["symbol"]
        qty = float(pos.get("quantity", 0.0))
        fallback = pos.get("price")
        s = symbol_prices.get(sym)
        tasks.append((sym, qty, _pack_series(s), fallback, vol_window))

    out: List[PositionMetrics] = []
    if tasks:
        with ProcessPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_position_worker, t) for t in tasks]
            for f in as_completed(futs):
                out.append(f.result())
        order = {pos["symbol"]: idx for idx, pos in enumerate(positions_spec)}
        out.sort(key=lambda m: order.get(m.symbol, 10**9))
    return out

