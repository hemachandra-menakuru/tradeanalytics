# Deflated Sharpe Ratio (DSR) & Performance Attribution

## Deflated Sharpe Ratio

Bailey & Lopez de Prado (2014). The DSR answers: given that you tested N strategies
and selected the best, what is the probability that the observed Sharpe is real?

It adjusts for:
1. Number of trials (the more you tested, the higher the expected maximum by luck)
2. Non-normality of returns (skewness and excess kurtosis inflate the naive Sharpe)
3. Serial autocorrelation (smoothed returns look better than they are)

```python
import numpy as np
from scipy.stats import norm

def expected_max_sharpe(n_trials: int, n_obs: int,
                         sr_mean: float = 0.0, sr_std: float = 1.0) -> float:
    """
    Expected maximum Sharpe ratio across n_trials independent trials.
    Lopez de Prado (2018) eq. 8.1.
    """
    euler_mascheroni = 0.5772156649
    e_max = sr_mean + sr_std * (
        (1 - euler_mascheroni) * norm.ppf(1 - 1/n_trials) +
        euler_mascheroni * norm.ppf(1 - 1/(n_trials * np.e))
    )
    return e_max


def sharpe_ratio_adjusted(returns: pd.Series,
                           periods_per_year: int = 252) -> float:
    """
    Sharpe adjusted for autocorrelation (Opdyke 2007).
    Accounts for the fact that autocorrelated returns have lower effective N.
    """
    sr = returns.mean() / returns.std() * np.sqrt(periods_per_year)
    # First-order autocorrelation correction
    rho = returns.autocorr(lag=1)
    if abs(rho) > 0.99:
        return sr
    correction = np.sqrt((1 + rho) / (1 - rho))
    return sr / correction * np.sqrt(1)


def deflated_sharpe_ratio(observed_sr: float,
                           n_trials: int,
                           n_obs: int,
                           skewness: float,
                           excess_kurtosis: float,
                           sr_std_across_trials: float = 1.0) -> float:
    """
    DSR: probability that the observed SR is above the expected maximum by luck.

    observed_sr:            Sharpe ratio of the best strategy found (annualised)
    n_trials:               total number of parameter combinations / strategies tested
    n_obs:                  number of observations (daily returns) in backtest
    skewness:               skewness of the best strategy's returns
    excess_kurtosis:        excess kurtosis (kurtosis - 3)
    sr_std_across_trials:   standard deviation of SR across all tested strategies

    Returns: DSR in [0, 1]. DSR > 0.95 → strong evidence of real edge.
    """
    # Step 1: annualised SR to per-period
    sr_per_period = observed_sr / np.sqrt(252)

    # Step 2: adjust for non-normality (Mertens 2002 variance formula)
    sr_var = (1 / n_obs) * (
        1
        - skewness * sr_per_period
        + ((excess_kurtosis - 1) / 4) * sr_per_period**2
    )
    sr_std_single = np.sqrt(sr_var) * np.sqrt(252)

    # Step 3: expected maximum SR given n_trials
    e_max = expected_max_sharpe(n_trials, n_obs, sr_mean=0, sr_std=sr_std_across_trials)

    # Step 4: DSR = P(SR_hat > E[max SR | chance])
    dsr = norm.cdf(
        (observed_sr - e_max) / sr_std_single
    )
    return dsr


def dsr_gate(returns: pd.Series, n_trials: int) -> dict:
    """
    Full DSR gate check for one strategy.
    Returns dict with all metrics and pass/fail.
    """
    import pandas as pd
    gross_sr = returns.mean() / returns.std() * np.sqrt(252)
    adj_sr = sharpe_ratio_adjusted(returns)
    dsr = deflated_sharpe_ratio(
        observed_sr=gross_sr,
        n_trials=n_trials,
        n_obs=len(returns),
        skewness=float(returns.skew()),
        excess_kurtosis=float(returns.kurtosis()),
    )
    return {
        "gross_sr":   round(gross_sr, 4),
        "adj_sr":     round(adj_sr, 4),
        "dsr":        round(dsr, 4),
        "n_trials":   n_trials,
        "passes":     dsr > 0.95,
        "verdict":    "PASS" if dsr > 0.95 else "FAIL"
    }
```

## Performance Attribution (Brinson-Hood-Beebower)

Decompose backtest return into:
- **Allocation effect**: did we over/underweight the right sectors/clusters?
- **Selection effect**: within each cluster, did we pick the right instruments?
- **Interaction effect**: joint contribution of allocation and selection decisions

```python
def brinson_attribution(portfolio_weights: pd.DataFrame,
                         benchmark_weights: pd.DataFrame,
                         asset_returns: pd.DataFrame,
                         asset_clusters: pd.Series) -> pd.DataFrame:
    """
    Single-period Brinson attribution.

    portfolio_weights:   {ticker: weight} for portfolio
    benchmark_weights:   {ticker: weight} for benchmark (e.g. equal weight)
    asset_returns:       {ticker: return} for period
    asset_clusters:      {ticker: cluster} mapping

    Returns: attribution by cluster.
    """
    results = []
    for cluster in asset_clusters.unique():
        tickers = asset_clusters[asset_clusters == cluster].index

        wp = portfolio_weights.reindex(tickers).fillna(0).sum()
        wb = benchmark_weights.reindex(tickers).fillna(0).sum()
        rp = (portfolio_weights.reindex(tickers).fillna(0) *
              asset_returns.reindex(tickers).fillna(0)).sum() / wp if wp > 0 else 0
        rb = (benchmark_weights.reindex(tickers).fillna(0) *
              asset_returns.reindex(tickers).fillna(0)).sum() / wb if wb > 0 else 0
        rb_total = (benchmark_weights * asset_returns.reindex(
            benchmark_weights.index).fillna(0)).sum()

        allocation  = (wp - wb) * (rb - rb_total)
        selection   = wb * (rp - rb)
        interaction = (wp - wb) * (rp - rb)

        results.append({
            "cluster":     cluster,
            "port_weight": wp,
            "bench_weight": wb,
            "port_return": rp,
            "bench_return": rb,
            "allocation":  allocation,
            "selection":   selection,
            "interaction": interaction,
            "total":       allocation + selection + interaction
        })

    return pd.DataFrame(results).set_index("cluster")
```

## Risk Decomposition

Decompose portfolio variance into factor contributions:

```python
def factor_risk_decomposition(weights: pd.Series,
                               factor_loadings: pd.DataFrame,
                               factor_cov: pd.DataFrame,
                               idio_var: pd.Series) -> pd.DataFrame:
    """
    Barra-style risk decomposition.

    weights:          portfolio weights (indexed by ticker)
    factor_loadings:  ticker × factor matrix (e.g. beta, momentum, size)
    factor_cov:       factor × factor covariance matrix
    idio_var:         ticker-specific idiosyncratic variance

    Returns: risk contribution per factor + idiosyncratic.
    """
    # Systematic variance
    Bw = factor_loadings.T @ weights          # factor exposures of portfolio
    factor_var_contrib = Bw * (factor_cov @ Bw)  # element-wise

    # Idiosyncratic variance
    idio_contrib = (weights**2 * idio_var).sum()

    # Total portfolio variance
    total_var = factor_var_contrib.sum() + idio_contrib
    total_vol = np.sqrt(total_var)

    results = pd.DataFrame({
        "variance_contribution": factor_var_contrib,
        "pct_of_total":         factor_var_contrib / total_var * 100
    })
    results.loc["idiosyncratic"] = [idio_contrib, idio_contrib / total_var * 100]
    results.loc["TOTAL"] = [total_var, 100.0]

    return results.round(4)
```
