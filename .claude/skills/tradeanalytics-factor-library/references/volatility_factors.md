# Volatility & Risk Factors — Implementation Reference

## 1. Realised Volatility (Multiple Windows)

The most fundamental risk factor. Annualised daily return standard deviation.
Always compute at multiple windows — each captures a different regime frequency.

```python
def realised_vol(close: pd.DataFrame,
                  windows: list[int] = [5, 20, 60, 126]) -> dict[str, pd.DataFrame]:
    """
    Annualised realised vol at multiple lookback windows.
    Returns dict of DataFrames.
    """
    daily_returns = close.pct_change()
    return {
        f"realised_vol_{w}d": daily_returns.rolling(w).std() * np.sqrt(252)
        for w in windows
    }
```

**Why multiple windows:**
- 5d: captures very recent vol spikes (earnings, event-driven)
- 20d: standard 1-month window; most commonly used
- 60d: medium-term vol regime
- 126d: half-year; smooths out event spikes

**As a feature:** low realised vol → positive signal for low-vol cluster (anomaly
predicts outperformance). High realised vol → negative signal for mega-cap cluster.

## 2. Volatility Ratio (Vol Regime Change)

Ratio of short-term vol to long-term vol. Rising ratio = vol expanding = risk-off.

```python
def vol_ratio(close: pd.DataFrame,
               short_window: int = 5,
               long_window: int = 63) -> pd.DataFrame:
    """
    realised_vol_short / realised_vol_long.
    > 1.0: vol expanding (regime shift risk)
    < 1.0: vol compressing (calm regime)
    """
    daily_returns = close.pct_change()
    vol_short = daily_returns.rolling(short_window).std()
    vol_long  = daily_returns.rolling(long_window).std()
    return vol_short / vol_long.replace(0, np.nan)
```

## 3. Volatility of Volatility (VoV)

Second-order risk signal. High VoV = volatility itself is unstable = regime
transitions likely. Frazzini & Pedersen (2014) show VoV predicts BAB crashes.

```python
def vol_of_vol(close: pd.DataFrame,
                vol_window: int = 20,
                vov_window: int = 60) -> pd.DataFrame:
    """
    Standard deviation of rolling realised vol over vov_window.
    High VoV → regime uncertainty → reduce all exposure.
    """
    daily_returns = close.pct_change()
    rv = daily_returns.rolling(vol_window).std() * np.sqrt(252)
    return rv.rolling(vov_window).std()
```

## 4. Beta to Market (SPY)

Systematic risk exposure. For TradeAnalytics, compute vs SPY as the market proxy.

```python
def rolling_beta(close: pd.DataFrame,
                  market: pd.Series,
                  window: int = 60) -> pd.DataFrame:
    """
    Rolling OLS beta of each ticker vs market.
    close: ticker DataFrame, market: SPY close Series.
    """
    ticker_ret = close.pct_change()
    market_ret = market.pct_change()

    betas = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    for col in close.columns:
        for i in range(window, len(close)):
            y = ticker_ret.iloc[i-window:i][col].dropna().values
            x = market_ret.iloc[i-window:i].reindex(ticker_ret.index[i-window:i]).dropna().values
            n = min(len(x), len(y))
            if n < 20:
                continue
            x, y = x[-n:], y[-n:]
            cov = np.cov(x, y)
            betas.iloc[i][col] = cov[0, 1] / cov[0, 0] if cov[0, 0] > 0 else np.nan
    return betas
```

**Efficient implementation:** for large universes, use vectorised regression via
`np.linalg.lstsq` in blocks rather than the loop above. See `factor_engineering.md`.

## 5. Low-Volatility Anomaly Feature

Frazzini & Pedersen (2014). Low-beta and low-vol stocks generate higher
risk-adjusted returns than the CAPM predicts. Counter-intuitive but robust.

```python
def low_vol_signal(close: pd.DataFrame, window: int = 126) -> pd.DataFrame:
    """
    Negative of realised vol — low vol is the LONG signal.
    Normalise cross-sectionally before use.
    """
    daily_returns = close.pct_change()
    rv = daily_returns.rolling(window).std() * np.sqrt(252)
    return -rv  # low vol → high signal value

def low_beta_signal(beta: pd.DataFrame) -> pd.DataFrame:
    """
    Negative of rolling beta — low beta is the LONG signal.
    """
    return -beta
```

**Economic rationale:** leverage-constrained investors overpay for high-beta stocks
(they need beta > 1.0 to hit return targets and can't lever). This creates persistent
overvaluation of high-beta relative to low-beta. The premium is structural.

## 6. Max Return (Lottery Effect — Negative Signal)

Bali, Cakici & Whitelaw (2011). The maximum daily return in the past month
predicts underperformance. Retail investors overpay for lottery-like stocks.

```python
def max_return(close: pd.DataFrame, window: int = 21) -> pd.DataFrame:
    """
    Maximum single-day return over the past `window` days.
    High max return → negative signal (lottery stocks underperform).
    Sign flip: high max return → negative feature.
    """
    daily_returns = close.pct_change()
    max_ret = daily_returns.rolling(window).max()
    return -max_ret  # high lottery characteristic → negative signal
```

## 7. Idiosyncratic Volatility

Ang, Hodrick, Xing & Zhang (2006). High idiosyncratic vol predicts
underperformance — a puzzle that persists. Represents unhedgeable residual risk
that investors irrationally overpay for.

```python
def idiosyncratic_vol(close: pd.DataFrame,
                       market: pd.Series,
                       window: int = 63) -> pd.DataFrame:
    """
    Vol of residuals from market regression. 
    High idiosyncratic vol → negative signal.
    """
    ticker_ret = close.pct_change()
    market_ret = market.pct_change()
    
    idio_vol = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    for col in close.columns:
        for i in range(window, len(close), 5):  # step=5 for efficiency
            y = ticker_ret.iloc[i-window:i][col].values
            x = market_ret.iloc[i-window:i].values
            valid = ~(np.isnan(x) | np.isnan(y))
            if valid.sum() < 20:
                continue
            slope = np.cov(x[valid], y[valid])[0, 1] / np.var(x[valid])
            residuals = y[valid] - slope * x[valid]
            idio_vol.iloc[i][col] = residuals.std() * np.sqrt(252)
    
    return idio_vol.ffill(limit=5)  # forward-fill the 5-day step
```

## 8. Skewness of Returns

Negative skewness → fat left tail → crash risk. High negative skewness stocks
underperform because investors dislike crash risk.

```python
def return_skewness(close: pd.DataFrame, window: int = 63) -> pd.DataFrame:
    """
    Rolling skewness of daily returns.
    Negative skewness → negative signal (crash risk premium).
    """
    daily_returns = close.pct_change()
    return -daily_returns.rolling(window).skew()  # flip: less negative skew = positive
```

## Feature Set Summary for Volatility / Low-Vol Cluster

```python
volatility_features = {
    "realised_vol_5d":    realised_vol(close, [5])["realised_vol_5d"],
    "realised_vol_20d":   realised_vol(close, [20])["realised_vol_20d"],
    "realised_vol_60d":   realised_vol(close, [60])["realised_vol_60d"],
    "vol_ratio_5_63":     vol_ratio(close, 5, 63),
    "vol_of_vol_60d":     vol_of_vol(close, 20, 60),
    "beta_60d":           rolling_beta(close, spy_close, 60),
    "low_vol_126d":       low_vol_signal(close, 126),         # negative of vol
    "low_beta_60d":       low_beta_signal(rolling_beta(...)),  # negative of beta
    "max_return_21d":     max_return(close, 21),              # lottery — negative signal
    "idio_vol_63d":       idiosyncratic_vol(close, spy_close, 63),
    "skewness_63d":       return_skewness(close, 63),
}
```

## Mega-Cap Cluster Specific

For mega-cap stocks, volatility factors are directionally inverted:
- Low vol → stability → LONG signal (institutional preference for predictable large caps)
- High vol in mega-caps → regime break → SHORT signal (earnings miss, CEO change, etc.)

```python
# The features are the same — XGBoost learns the directionality per cluster
# Ensure cluster label is a feature so the model can condition on it
features["is_mega_cap"] = (dollar_volume_log > np.percentile(dollar_volume_log, 80)).astype(int)
```
