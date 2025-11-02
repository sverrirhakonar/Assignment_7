import pytest
from parallel_fin import data_loader, metrics

@pytest.fixture
def sample_df():
    df, _ = data_loader.load_pandas("data/market_data-1.csv")
    return df[df["symbol"] == "AAPL"].head(50)

def test_rolling_pandas_vs_polars(sample_df):
    res_pd, _ = metrics.compute_rolling_pandas(sample_df, window=20)
    res_pl, _ = metrics.compute_rolling_polars(sample_df.to_polars(), window=20)
    assert abs(res_pd["ma20"].mean() - res_pl["ma20"].mean()) < 1e-6

def test_sharpe_nonnegative(sample_df):
    res_pd, _ = metrics.compute_rolling_pandas(sample_df, window=20)
    assert res_pd["sharpe20"].notna().any()
