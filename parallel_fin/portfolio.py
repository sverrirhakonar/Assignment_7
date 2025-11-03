from typing import Any, Dict, List, Tuple, Optional
import math
import numpy as np
import pandas as pd
from .parallel import compute_positions_multiprocess, PositionMetrics
from .metrics import build_symbol_price_map_pandas

def _weighted_average(pairs: List[Tuple[float, float]]) -> float:
    if not pairs:
        return float("nan")
    w = np.array([w for w, _ in pairs], dtype=float)
    v = np.array([v for _, v in pairs], dtype=float)
    tot = np.nansum(w)
    return float(np.nansum(w * v) / tot) if tot > 0 else float("nan")

def _combine_node(positions: List[PositionMetrics], subs: List[Dict[str, Any]]) -> Tuple[float, float, float]:
    total_pos = np.nansum([p.value for p in positions]) if positions else 0.0
    total_sub = np.nansum([c.get("total_value", 0.0) for c in subs]) if subs else 0.0
    total_value = float(total_pos + total_sub)

    vol_terms = [(p.value, p.volatility) for p in positions if not math.isnan(p.value) and not math.isnan(p.volatility)]
    vol_terms += [
        (c.get("total_value", 0.0), c.get("aggregate_volatility", float("nan")))
        for c in subs
        if c.get("total_value", 0.0) > 0 and not math.isnan(c.get("aggregate_volatility", float("nan")))
    ]
    agg_vol = _weighted_average(vol_terms)

    dd_vals = [p.drawdown for p in positions if not math.isnan(p.drawdown)]
    dd_vals += [c.get("max_drawdown", float("nan")) for c in subs if not math.isnan(c.get("max_drawdown", float("nan")))]
    max_dd = float(np.nanmin(dd_vals)) if dd_vals else float("nan")

    return total_value, agg_vol, max_dd

def aggregate_portfolio_sequential(node: Dict[str, Any], symbol_prices: Dict[str, pd.Series], vol_window: int = 20) -> Dict[str, Any]:
    positions_spec = node.get("positions", []) or []
    subs_spec = node.get("sub_portfolios", []) or []

    # reuse the MP function in single process by limiting workers to 1
    pm_list, _ = compute_positions_multiprocess(positions_spec, symbol_prices, vol_window=vol_window, max_workers=1)
    sub_aggs = [aggregate_portfolio_sequential(sub, symbol_prices, vol_window) for sub in subs_spec]

    total_value, agg_vol, max_dd = _combine_node(pm_list, sub_aggs)

    return {
        "name": node.get("name", "Unnamed"),
        "total_value": total_value,
        "aggregate_volatility": agg_vol,
        "max_drawdown": max_dd,
        "positions": [{"symbol": p.symbol, "value": p.value, "volatility": p.volatility, "drawdown": p.drawdown} for p in pm_list],
        "sub_portfolios": sub_aggs,
    }

def aggregate_portfolio_multiprocessing(node: Dict[str, Any], symbol_prices: Dict[str, pd.Series], vol_window: int = 20, max_workers: Optional[int] = None) -> Dict[str, Any]:
    positions_spec = node.get("positions", []) or []
    subs_spec = node.get("sub_portfolios", []) or []

    pm_list, _ = compute_positions_multiprocess(positions_spec, symbol_prices, vol_window=vol_window, max_workers=max_workers)
    sub_aggs = [aggregate_portfolio_multiprocessing(sub, symbol_prices, vol_window, max_workers) for sub in subs_spec]

    total_value, agg_vol, max_dd = _combine_node(pm_list, sub_aggs)

    return {
        "name": node.get("name", "Unnamed"),
        "total_value": total_value,
        "aggregate_volatility": agg_vol,
        "max_drawdown": max_dd,
        "positions": [{"symbol": p.symbol, "value": p.value, "volatility": p.volatility, "drawdown": p.drawdown} for p in pm_list],
        "sub_portfolios": sub_aggs,
    }
