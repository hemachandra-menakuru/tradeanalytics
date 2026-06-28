# Stress Testing, Scenario Analysis & Tail Risk

## Historical Scenario Analysis

Replay known crisis periods against your portfolio to check survivability.

```python
CRISIS_SCENARIOS = {
    "gfc_peak_to_trough":    ("2008-09-12", "2009-03-09"),
    "gfc_lehman_week":        ("2008-09-15", "2008-09-19"),
    "flash_crash_2010":       ("2010-05-06", "2010-05-06"),
    "euro_crisis_2011":       ("2011-04-29", "2011-10-03"),
    "china_devaluation_2015": ("2015-08-18", "2015-08-26"),
    "vol_shock_2018":         ("2018-02-05", "2018-02-08"),
    "q4_2018_selloff":        ("2018-09-20", "2018-12-24"),
    "covid_crash":            ("2020-02-19", "2020-03-23"),
    "covid_rebound":          ("2020-03-23", "2020-08-18"),
    "rate_shock_2022":        ("2022-01-03", "2022-10-12"),
}


def historical_stress_test(
    weights: pd.Series,
    price_history: pd.DataFrame,
    scenarios: dict = CRISIS_SCENARIOS
) -> pd.DataFrame:
    """
    Simulate portfolio P&L during each historical scenario.

    weights:       current portfolio weights {ticker: weight}
    price_history: rows=dates, cols=tickers (must cover all scenario periods)

    Returns: scenario drawdown and absolute return per period.
    """
    results = []
    for name, (start, end) in scenarios.items():
        try:
            period_prices = price_history.loc[start:end]
            if len(period_prices) < 2:
                continue
            period_returns = period_prices.pct_change().dropna()
            portfolio_returns = (period_returns * weights.reindex(
                period_returns.columns).fillna(0)
            ).sum(axis=1)
            cum_return = (1 + portfolio_returns).prod() - 1
            max_dd = ((1 + portfolio_returns).cumprod() /
                      (1 + portfolio_returns).cumprod().cummax() - 1).min()
            results.append({
                "scenario":      name,
                "start":         start,
                "end":           end,
                "cum_return":    f"{cum_return:.2%}",
                "max_drawdown":  f"{max_dd:.2%}",
                "trading_days":  len(period_returns),
                "passes":        max_dd > -0.20   # fail if >20% portfolio drawdown
            })
        except Exception as e:
            results.append({"scenario": name, "error": str(e)})

    df = pd.DataFrame(results)
    n_fail = len(df[df["passes"] == False]) if "passes" in df.columns else 0
    print(f"Stress test: {len(df) - n_fail}/{len(df)} scenarios passed")
    return df
```

## Factor Stress Tests (Correlation Shock)

Crisis periods cause correlation spikes. What happens when all correlations jump to 0.9?

```python
def correlation_stress_test(weights: pd.Series,
                             individual_vols: pd.Series,
                             stressed_correlation: float = 0.90) -> dict:
    """
    Replace actual correlation matrix with a stressed version.
    Shows portfolio vol and VaR under extreme co-movement.
    """
    n = len(weights)
    # Stressed correlation matrix: all pairs at stressed_correlation
    stressed_corr = np.full((n, n), stressed_correlation)
    np.fill_diagonal(stressed_corr, 1.0)

    # Stressed covariance matrix
    vols = individual_vols.reindex(weights.index).fillna(0).values
    stressed_cov = np.outer(vols, vols) * stressed_corr

    w = weights.values
    stressed_port_vol = np.sqrt(w @ stressed_cov @ w)

    # Compare to normal vol
    normal_corr = np.eye(n)  # assume independence as baseline
    normal_cov = np.outer(vols, vols) * normal_corr
    normal_port_vol = np.sqrt(w @ normal_cov @ w)

    return {
        "normal_portfolio_vol":    f"{normal_port_vol:.2%}",
        "stressed_portfolio_vol":  f"{stressed_port_vol:.2%}",
        "vol_multiplier":          round(stressed_port_vol / normal_port_vol, 2),
        "stressed_99_var_1d":     f"{stressed_port_vol / np.sqrt(252) * 2.326:.2%}",
    }
```

## Tail Risk — Extreme Value Theory (EVT)

For modelling the true tail beyond what historical data shows.
Generalized Pareto Distribution (GPD) fits the excess loss beyond a threshold.

```python
from scipy.stats import genpareto

def evt_tail_var(returns: pd.Series,
                 threshold_quantile: float = 0.90,
                 confidence: float = 0.99) -> dict:
    """
    Peaks-over-Threshold (POT) method.
    Fit GPD to exceedances above threshold.
    More reliable than empirical quantile for confidence > 99%.
    """
    threshold = returns.quantile(1 - threshold_quantile)
    exceedances = -(returns[returns < threshold] - threshold)  # positive exceedances

    if len(exceedances) < 20:
        return {"error": "Insufficient tail observations for EVT"}

    shape, loc, scale = genpareto.fit(exceedances, floc=0)
    n_total = len(returns)
    n_tail  = len(exceedances)

    # VaR beyond threshold
    u = threshold_quantile
    q = confidence
    evt_var = threshold - scale / shape * (
        1 - ((n_total / n_tail) * (1 - q))**(-shape)
    )

    # CVaR (Expected Shortfall) from GPD
    evt_cvar = evt_var / (1 - shape) - scale / (1 - shape)

    return {
        "gpd_shape":    round(shape, 4),   # > 0 → fat tails (typical for equities)
        "gpd_scale":    round(scale, 6),
        "evt_var_99":   f"{evt_var:.2%}",
        "evt_cvar_99":  f"{evt_cvar:.2%}",
        "threshold":    f"{threshold:.2%}",
        "n_exceedances": n_tail,
    }
```

**Interpretation of GPD shape:**
- shape > 0 (Fréchet): fat tails — typical for equity returns
- shape = 0 (Gumbel): thin tails (exponential decay)
- shape < 0 (Weibull): bounded tails — rare in finance

## Drawdown Monitoring

Continuous drawdown tracking, written to Delta table each session.

```python
def realtime_drawdown_monitor(portfolio_values: pd.Series) -> pd.DataFrame:
    """
    Compute running drawdown from all-time high.
    Use this output to feed CircuitBreaker.update().
    """
    running_max = portfolio_values.cummax()
    drawdown    = (portfolio_values - running_max) / running_max
    underwater  = (drawdown < 0).astype(int)

    # Duration of current drawdown streak
    streak = underwater * (underwater.groupby(
        (underwater != underwater.shift()).cumsum()).cumcount() + 1
    )

    return pd.DataFrame({
        "portfolio_value": portfolio_values,
        "peak_value":      running_max,
        "drawdown":        drawdown,
        "drawdown_days":   streak,
    })
```

## Scenario: What If VIX Doubles?

Regime-conditional risk — what is our portfolio VaR when VIX > 30?

```python
def conditional_risk_on_vix(portfolio_returns: pd.Series,
                              vix: pd.Series,
                              vix_threshold: float = 30.0) -> dict:
    """
    Compute VaR and vol conditional on high-VIX regime.
    Tells you: how bad can it get when markets are already stressed?
    """
    high_vix_mask = vix > vix_threshold
    normal_mask   = vix <= vix_threshold

    high_vix_returns  = portfolio_returns[high_vix_mask]
    normal_returns    = portfolio_returns[normal_mask]

    return {
        "n_days_high_vix":    int(high_vix_mask.sum()),
        "n_days_normal":      int(normal_mask.sum()),
        "vol_high_vix":       f"{high_vix_returns.std() * np.sqrt(252):.2%}",
        "vol_normal":         f"{normal_returns.std() * np.sqrt(252):.2%}",
        "var_99_high_vix":    f"{high_vix_returns.quantile(0.01):.2%}",
        "var_99_normal":      f"{normal_returns.quantile(0.01):.2%}",
        "vol_multiplier":     round(
            high_vix_returns.std() / normal_returns.std(), 2
        ) if normal_returns.std() > 0 else float("inf"),
    }
```
