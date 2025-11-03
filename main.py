from parallel_fin import data_loader, metrics, parallel, reporting
import json
from parallel_fin.metrics import build_symbol_price_map_pandas
from pathlib import Path


def main():
    df, _ = data_loader.load_pandas("data/market_data-1.csv")
    sym_prices = build_symbol_price_map_pandas(df)
    with open("data/portfolio_structure-1.json", "r") as f:
        portfolio_tree = json.load(f)
    _, stats_thread = parallel.run_threaded(df)
    _, stats_multi = parallel.run_multiprocess(df)
    results = {"threaded": stats_thread, "multiprocess": stats_multi}

    seq_out, stats_port_seq = reporting.time_portfolio_seq(portfolio_tree, sym_prices, vol_window=20)
    mp_out, stats_port_mp  = reporting.time_portfolio_mp(portfolio_tree, sym_prices, vol_window=20, max_workers=None)

    results.update({
        "portfolio_sequential": stats_port_seq,
        "portfolio_multiprocess": stats_port_mp,
    })

    summary = reporting.summarize_performance(results)
    reporting.print_performance_table(summary)

    outdir = Path("outputs")
    outdir.mkdir(exist_ok=True)
    reporting.save_portfolio_json(seq_out, outdir / "portfolio_sequential.json")
    reporting.save_portfolio_json(mp_out,  outdir / "portfolio_multiprocessing.json")

if __name__ == "__main__":
    main()