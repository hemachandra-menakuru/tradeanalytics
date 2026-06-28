# Volume & Liquidity Factors — Implementation Reference

## 1. Amihud Illiquidity Ratio

Amihud (2002). The most widely used liquidity measure in academic research.
|return| / dollar_volume measures price impact per dollar traded.
High illiquidity → illiquidity premium (positive expected return) but also
higher transaction costs and harder to exit positions.

```python
def amihud_illiquidity(close: pd.DataFrame,
                        volume: pd.DataFrame,
                        window: int = 21) -> pd.DataFrame:
    """
    Daily: |return| / dollar_volume.
    Rolling mean over `window` days.
    Higher = more illiquid = larger illiquidity premium.
    """
    daily_return = close.pct_change().abs()
    dollar_volume = close * volume
    daily_illiq = daily_return / dollar_volume.replace(0, np.nan)
    rolling_illiq = daily_illiq.rolling(window).mean()
    return np.log1p(rolling_illiq * 1e6)  # scale to readable range, log-transform
```

**Log-transform is mandatory:** the raw ratio spans many orders of magnitude across
small-caps vs mega-caps. Log makes it cross-sectionally comparable.

**As a signal:** context-dependent.
- In the **momentum cluster**: high illiquidity amplifies momentum (return drift persists longer in illiquid stocks)
- In the **mean-reversion cluster**: high illiquidity = slower reversion (avoid or reduce position size)
- In the **mega-cap cluster**: use as a filter only — exclude stocks below liquidity threshold

## 2. Dollar Volume (Log)

Size-liquidity composite. The single most important filter variable.

```python
def dollar_volume_log(close: pd.DataFrame,
                       volume: pd.DataFrame,
                       window: int = 21) -> pd.DataFrame:
    """
    Log of rolling average dollar volume.
    Used as both a feature and a universe filter.
    Minimum threshold: log(5_000_000) ≈ 15.4 (i.e. $5M average daily dollar volume)
    """
    dv = (close * volume).rolling(window).mean()
    return np.log(dv.replace(0, np.nan))
```

**Universe filter (mandatory):** before computing any other factor, drop instruments
with `dollar_volume_log < 15.4` (< $5M ADV). This removes micro-caps where:
- Transaction costs would eliminate any alpha
- Bid-ask spreads make IBKR fills unreliable
- Amihud ratio and momentum signals are distorted

## 3. Volume Ratio (Abnormal Volume)

Abnormal volume signals informed trading — either institutional accumulation or
distribution. Predicts short-term directional continuation.

```python
def volume_ratio(volume: pd.DataFrame,
                  short_window: int = 5,
                  long_window: int = 21) -> pd.DataFrame:
    """
    Current volume / average volume. >1.5 = abnormal.
    Combined with price direction to determine accumulation vs distribution.
    """
    avg_vol = volume.rolling(long_window).mean()
    return volume.rolling(short_window).mean() / avg_vol.replace(0, np.nan)

def volume_price_confirmation(close: pd.DataFrame,
                               volume: pd.DataFrame,
                               window: int = 10) -> pd.DataFrame:
    """
    Rolling correlation between price return and volume.
    Positive = volume confirms price direction = institutional accumulation.
    Negative = price moving against volume = distribution (bearish).
    """
    price_ret = close.pct_change()
    vol_change = volume.pct_change()
    return price_ret.rolling(window).corr(vol_change)
```

## 4. Turnover Rate

High turnover = high speculative interest = negative signal (overvalued).
A proxy for investor enthusiasm that predicts lower future returns.

```python
def turnover_rate(volume: pd.DataFrame,
                   shares_outstanding: pd.DataFrame | None,
                   window: int = 21) -> pd.DataFrame:
    """
    volume / shares_outstanding (if available) or normalised by median volume.
    High turnover → lottery-type stock → negative signal.
    IBKR doesn't provide shares_outstanding via REST API — use volume/ADV proxy.
    """
    if shares_outstanding is not None:
        raw_turnover = volume / shares_outstanding.replace(0, np.nan)
    else:
        # Proxy: today's volume relative to long-run average
        long_avg = volume.rolling(126).mean()
        raw_turnover = volume / long_avg.replace(0, np.nan)
    
    rolling_turnover = raw_turnover.rolling(window).mean()
    return -np.log1p(rolling_turnover)  # flip: high turnover → negative signal
```

## 5. Price Impact (Kyle's Lambda Proxy)

Estimate of market impact cost — how much price moves per unit volume traded.

```python
def price_impact_proxy(close: pd.DataFrame,
                        volume: pd.DataFrame,
                        window: int = 21) -> pd.DataFrame:
    """
    Kyle's lambda proxy: regression slope of |return| on sqrt(volume).
    Higher slope = more price impact = less liquid.
    """
    price_ret = close.pct_change().abs()
    sqrt_vol = np.sqrt(volume)
    
    lambda_series = pd.DataFrame(index=close.index, columns=close.columns, dtype=float)
    for col in close.columns:
        y = price_ret[col].rolling(window)
        x = sqrt_vol[col].rolling(window)
        # Rolling covariance / rolling variance
        cov = price_ret[col].rolling(window).cov(sqrt_vol[col])
        var = sqrt_vol[col].rolling(window).var()
        lambda_series[col] = cov / var.replace(0, np.nan)
    
    return np.log1p(lambda_series.clip(lower=0) * 1e4)
```

## 6. On-Balance Volume (OBV) Trend

Granville (1963). Cumulative volume signed by price direction. Rising OBV with
flat price = accumulation (positive signal). Falling OBV with flat price = distribution.

```python
def obv_trend(close: pd.DataFrame,
               volume: pd.DataFrame,
               window: int = 20) -> pd.DataFrame:
    """
    OBV normalised trend: slope of OBV over rolling window, normalised by price.
    Positive slope + stable price = accumulation = positive signal.
    """
    daily_return = close.pct_change()
    signed_volume = volume * np.sign(daily_return)
    obv = signed_volume.cumsum()
    # Rolling linear trend slope
    def rolling_slope(series: pd.Series) -> pd.Series:
        x = np.arange(window)
        slopes = series.rolling(window).apply(
            lambda y: np.polyfit(x, y, 1)[0] if len(y) == window else np.nan,
            raw=True
        )
        return slopes
    
    obv_slope = obv.apply(rolling_slope)
    return obv_slope / close.replace(0, np.nan)  # normalise by price level
```

## Feature Set Summary for Volume / Liquidity

```python
volume_features = {
    "amihud_illiq_21d":      amihud_illiquidity(close, volume, 21),
    "dollar_volume_log_21d": dollar_volume_log(close, volume, 21),
    "volume_ratio_5_21":     volume_ratio(volume, 5, 21),
    "vol_price_corr_10d":    volume_price_confirmation(close, volume, 10),
    "turnover_proxy_21d":    turnover_rate(volume, None, 21),
    "obv_trend_20d":         obv_trend(close, volume, 20),
}

# Universe filter — apply BEFORE any other factor computation
universe_mask = dollar_volume_log(close, volume, 21) >= 15.4  # $5M ADV minimum
```

## Liquidity and Transaction Cost Interaction

For a live trading system, liquidity factors serve a dual role:

1. **As predictive features** in the XGBoost model (included in feature matrix)
2. **As position-sizing constraints** in the execution layer (Phase 5)

```python
# Phase 5 position size constraint example
max_participation_rate = 0.10  # never exceed 10% of ADV
adv = dollar_volume / close    # average daily shares
max_shares = adv * max_participation_rate
target_shares = signal_strength * capital / price
final_shares = min(target_shares, max_shares)
```

Encode this constraint in the signal contract's `meta` field so consumers know the
liquidity-adjusted capacity per signal.
