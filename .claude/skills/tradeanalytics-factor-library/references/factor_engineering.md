# Factor Engineering — Normalisation, Decay, Combination

## 1. The Full Factor Engineering Pipeline

```
Raw OHLCV data
    ↓
Universe filter (dollar volume > $5M ADV)
    ↓
Raw factor computation (per factor family)
    ↓
Winsorise (1st / 99th percentile per cross-section)
    ↓
Cross-sectional z-score (mean=0, std=1 across tickers per date)
    ↓
Missing value imputation (fill with 0 = cross-sectional mean)
    ↓
IC calculation per factor (spearman vs forward returns)
    ↓
Multiple-testing correction (BHY) — drop non-significant factors
    ↓
IC-weighted composite (or feed all to XGBoost directly)
    ↓
Feature matrix: rows=dates × tickers, cols=factors
```

## 2. Point-in-Time Correctness Checklist

Before writing any factor to the feature store, verify:

```python
def verify_no_lookahead(factor: pd.DataFrame, close: pd.DataFrame) -> bool:
    """
    Spot-check: at date t, factor value should only use close[t] and earlier.
    Test by computing factor with data truncated at t and comparing.
    """
    test_date_idx = len(close) // 2  # mid-point
    test_date = close.index[test_date_idx]
    
    # Factor computed with full history (suspect)
    full_value = factor.loc[test_date]
    
    # Factor computed with data only up to test_date
    truncated_close = close.iloc[:test_date_idx + 1]
    # Re-run your factor function on truncated_close and compare
    # If they differ → look-ahead bug
    return True  # replace with actual comparison
```

**Most common look-ahead bugs:**
1. `shift()` applied in wrong direction
2. Scaling/normalisation uses future data (e.g. sklearn StandardScaler fit on full history)
3. `fillna()` with forward-fill across the train/test boundary
4. `pct_change()` lag off by one

## 3. Efficient Vectorised Beta Computation

Rolling OLS for the entire universe at once:

```python
def vectorised_rolling_beta(ticker_returns: pd.DataFrame,
                             market_returns: pd.Series,
                             window: int = 60) -> pd.DataFrame:
    """
    Vectorised: computes all tickers simultaneously via matrix operations.
    O(T × window × N) but with numpy vectorisation — much faster than loops.
    """
    betas = pd.DataFrame(np.nan, index=ticker_returns.index,
                         columns=ticker_returns.columns)
    
    market_arr = market_returns.values
    ticker_arr = ticker_returns.values

    for i in range(window, len(ticker_returns)):
        m_window = market_arr[i-window:i]
        t_window = ticker_arr[i-window:i, :]
        
        m_demeaned = m_window - m_window.mean()
        t_demeaned = t_window - t_window.mean(axis=0)
        
        m_var = np.dot(m_demeaned, m_demeaned)
        if m_var == 0:
            continue
        
        cov = np.dot(m_demeaned, t_demeaned)  # shape: (N,)
        betas.iloc[i] = cov / m_var
    
    return betas
```

## 4. Factor Decay Analysis

Before including any factor, measure how quickly its IC decays as the prediction
horizon extends. This tells you the optimal rebalance frequency.

```python
def factor_decay_profile(factor: pd.DataFrame,
                          close: pd.DataFrame,
                          horizons: list[int] = [1, 5, 10, 21, 42, 63]) -> pd.Series:
    """
    IC of factor vs forward returns at each horizon.
    Plot this to see when IC drops below 0.03 (practical significance floor).
    """
    from scipy.stats import spearmanr
    
    ic_by_horizon = {}
    for h in horizons:
        forward_ret = np.log(close.shift(-h) / close)
        ic_series = []
        for date in factor.index[:-h]:
            f = factor.loc[date].dropna()
            r = forward_ret.loc[date].reindex(f.index).dropna()
            common = f.index.intersection(r.index)
            if len(common) < 10:
                continue
            ic, _ = spearmanr(f[common], r[common])
            if not np.isnan(ic):
                ic_series.append(ic)
        ic_by_horizon[h] = np.mean(ic_series) if ic_series else 0
    
    return pd.Series(ic_by_horizon, name="IC_by_horizon")
```

Interpretation:
- IC still > 0.03 at 21d → monthly rebalance is fine
- IC drops below 0.03 at 5d → need weekly or daily rebalance
- IC peaks at 5d then declines → optimal horizon is 5 days

## 5. Factor Orthogonalisation

When two factors are highly correlated (|ρ| > 0.6), including both adds noise.
Options:

```python
def orthogonalise_factor(primary: pd.DataFrame,
                          secondary: pd.DataFrame) -> pd.DataFrame:
    """
    Remove primary's influence from secondary via OLS residuals.
    Residual = pure secondary signal, uncorrelated with primary.
    Use when: adding a new factor that overlaps with existing ones.
    """
    residuals = pd.DataFrame(index=secondary.index, columns=secondary.columns)
    for date in secondary.index:
        p = primary.loc[date].dropna()
        s = secondary.loc[date].reindex(p.index).dropna()
        common = p.index.intersection(s.index)
        if len(common) < 10:
            residuals.loc[date] = np.nan
            continue
        slope = np.cov(p[common].values, s[common].values)[0, 1] / np.var(p[common].values)
        residuals.loc[date, common] = s[common] - slope * p[common]
    return residuals
```

Example: `reversal_21d` is correlated with `zscore_252d`. Orthogonalise reversal
against zscore to isolate the pure short-term reversal component.

## 6. Factor Correlation Monitoring

```python
def factor_correlation_matrix(features: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Spearman rank correlation matrix across all factors.
    Flag pairs with |ρ| > 0.6 for review.
    """
    corr = features.rank().corr(method="spearman")
    # Flag high-correlation pairs
    high_corr = []
    for i, col_i in enumerate(corr.columns):
        for j, col_j in enumerate(corr.columns):
            if i < j and abs(corr.iloc[i, j]) > 0.6:
                high_corr.append((col_i, col_j, round(corr.iloc[i, j], 3)))
    return corr, high_corr
```

## 7. Feature Matrix Assembly for Databricks

The final feature matrix written to `tradeanalytics.silver.feature_store`:

```python
def assemble_feature_matrix(
    close: pd.DataFrame,
    high: pd.DataFrame,
    low: pd.DataFrame,
    volume: pd.DataFrame,
    spy_close: pd.Series,
    as_of_date: str,
    cluster: str
) -> pd.DataFrame:
    """
    Assemble the complete feature matrix for one cluster.
    Returns long-format DataFrame ready for Delta write.
    """
    # 1. Universe filter
    dv = dollar_volume_log(close, volume, 21)
    universe = dv.columns[dv.iloc[-1] >= 15.4]
    close = close[universe]

    # 2. Compute all factors (already normalised internally)
    all_features = {}
    all_features.update(momentum_features)     # from momentum_factors.md
    all_features.update(reversion_features)    # from reversion_factors.md
    all_features.update(volatility_features)   # from volatility_factors.md
    all_features.update(volume_features)       # from volume_liquidity_factors.md

    # 3. Cross-sectional z-score each feature
    normalised = {
        name: cross_sectional_zscore(fdf)
        for name, fdf in all_features.items()
    }

    # 4. Stack to long format (date × ticker × features)
    rows = []
    for ticker in universe:
        row = {
            "as_of_date": as_of_date,
            "ticker": ticker,
            "cluster": cluster,
        }
        for name, fdf in normalised.items():
            row[name] = fdf[ticker].iloc[-1] if ticker in fdf.columns else None
        rows.append(row)

    return pd.DataFrame(rows)
```

## 8. Multiple Testing Correction in Practice

When you test 40 factors:
- At α=0.05: expect 2 false positives by random chance
- BHY correction is conservative (valid under arbitrary dependence)
- Benjamini-Hochberg is less conservative (valid under independence)
- Use BHY for financial data (factors are always correlated)

```python
from statsmodels.stats.multitest import multipletests
from scipy.stats import ttest_1samp

def select_factors_bhy(factor_ic_dict: dict[str, pd.Series],
                        alpha: float = 0.05) -> dict[str, pd.Series]:
    """
    Apply BHY multiple-testing correction.
    Returns only factors whose IC is statistically significant after correction.
    """
    names = list(factor_ic_dict.keys())
    p_values = [ttest_1samp(ic.dropna(), 0)[1] for ic in factor_ic_dict.values()]
    
    reject, p_corrected, _, _ = multipletests(p_values, alpha=alpha, method="fdr_by")
    
    selected = {name: ic for name, ic, r in zip(names, factor_ic_dict.values(), reject) if r}
    rejected = [name for name, r in zip(names, reject) if not r]
    
    print(f"Selected {len(selected)}/{len(names)} factors after BHY correction")
    print(f"Rejected: {rejected}")
    return selected
```

## 9. Factor Performance Dashboard

Log these to MLflow after each feature engineering run:

```python
mlflow.log_metrics({
    f"ic_mean_{name}": ic.mean()
    for name, ic in factor_ic_dict.items()
})
mlflow.log_metrics({
    f"icir_{name}": ic.mean() / ic.std()
    for name, ic in factor_ic_dict.items()
})
mlflow.log_dict(
    {name: float(p) for name, p in zip(names, p_corrected)},
    "factor_p_values_bhy.json"
)
```
