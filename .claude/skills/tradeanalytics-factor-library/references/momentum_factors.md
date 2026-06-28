# Momentum Factors — Implementation Reference

## 1. Cross-Sectional Momentum (12-1)

The oldest and most replicated factor in equity research (Jegadeesh & Titman 1993).
Skip the most recent month to avoid microstructure reversal contamination.

```python
def cross_sectional_momentum(close: pd.DataFrame,
                              lookback: int = 252,
                              skip: int = 21) -> pd.DataFrame:
    """
    close: DataFrame, rows=dates, cols=tickers.
    Returns: 12-1 month return (rank later via cross_sectional_zscore).
    """
    ret_12m = close.shift(skip) / close.shift(lookback) - 1
    return ret_12m
```

**Why skip 1 month:** at the 1-month horizon momentum reverses (short-term reversal
effect). Including it contaminates the signal with the opposite phenomenon.

**Holding period:** signal refreshes monthly. Daily recomputation is fine but
the economic content does not change materially intra-month.

## 2. Time-Series Momentum (TSMOM)

Moskowitz, Ooi & Pedersen (2012). Each asset follows its own trend regardless
of cross-sectional rank.

```python
def tsmom(close: pd.DataFrame, lookback: int = 252) -> pd.DataFrame:
    """
    Sign of trailing return. +1 = uptrend, -1 = downtrend.
    Used as a binary feature, not a ranking.
    """
    trailing_return = close / close.shift(lookback) - 1
    return np.sign(trailing_return)

def tsmom_strength(close: pd.DataFrame, lookback: int = 252) -> pd.DataFrame:
    """
    Continuous version: trailing return normalised by realised vol.
    = Sharpe of the past period. Accounts for vol-adjusted trend strength.
    """
    trailing_return = close / close.shift(lookback) - 1
    daily_returns = close.pct_change()
    vol = daily_returns.rolling(lookback).std() * np.sqrt(252)
    return trailing_return / vol.replace(0, np.nan)
```

Compute at three horizons and include all three as separate features:
- `tsmom_3` (63 days)
- `tsmom_6` (126 days)
- `tsmom_12` (252 days)

XGBoost will learn which horizon is most predictive in each regime.

## 3. 52-Week High Proximity

George & Hwang (2004). Proximity to 52-week high predicts momentum continuation.
Psychological anchoring: investors are reluctant to buy above the recent high,
creating underreaction → momentum.

```python
def price_to_52w_high(close: pd.DataFrame, window: int = 252) -> pd.DataFrame:
    """
    Ratio of current price to rolling 252-day maximum.
    Range [0, 1]. Near 1.0 → approaching/breaking resistance.
    """
    rolling_max = close.rolling(window).max()
    return close / rolling_max
```

**IC behaviour:** strongest IC during trending regimes (bull/bear). Near-zero IC
in sideways/high-vol. Include `hmm_state` as an interaction feature.

## 4. EMA Cross (Trend Confirmation)

Multi-period EMA crossover used as a continuous momentum confirmation signal.

```python
def ema_cross_signal(close: pd.DataFrame,
                     fast: int = 50, slow: int = 200) -> pd.DataFrame:
    """
    (EMA_fast - EMA_slow) / EMA_slow — normalised spread.
    Positive = uptrend, negative = downtrend.
    Not a trading rule — a continuous feature for XGBoost.
    """
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    return (ema_fast - ema_slow) / ema_slow
```

Also compute at 10/50 (medium-term) and 5/20 (short-term) — three separate features.

## 5. MACD as Feature

MACD histogram normalised by price — continuous momentum feature at medium frequency.

```python
def macd_feature(close: pd.DataFrame,
                 fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """
    MACD histogram normalised by price. Removes price-level bias across tickers.
    """
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return histogram / close  # normalise by price level
```

## 6. Momentum Volatility Scaling (Risk-Adjusted Momentum)

Barroso & Santa-Clara (2015). Scale momentum signal by realised volatility to
halve momentum crash risk while preserving expected return.

```python
def vol_scaled_momentum(close: pd.DataFrame,
                         lookback: int = 252,
                         vol_window: int = 126,
                         target_vol: float = 0.12) -> pd.DataFrame:
    """
    Vol-scaled momentum: trailing_return * (target_vol / realised_vol).
    Reduces momentum crash severity by ~50% vs unscaled.
    """
    trailing_return = close / close.shift(lookback) - 1
    daily_ret = close.pct_change()
    realised_vol = daily_ret.rolling(vol_window).std() * np.sqrt(252)
    scaling = (target_vol / realised_vol).clip(0, 2)  # cap at 2x
    return trailing_return * scaling
```

**Momentum crashes** (Danielsson et al.): momentum crashes worst in high-vol,
high-correlation market stress (March 2009, March 2020). Vol scaling is the
single most effective crash-protection mechanism.

## Feature Set Summary for Momentum Cluster

```python
momentum_features = {
    "cs_mom_12_1":        cross_sectional_momentum(close, 252, 21),
    "tsmom_12":           tsmom(close, 252),
    "tsmom_6":            tsmom(close, 126),
    "tsmom_3":            tsmom(close, 63),
    "tsmom_strength_12":  tsmom_strength(close, 252),
    "price_to_52w_high":  price_to_52w_high(close, 252),
    "ema_cross_50_200":   ema_cross_signal(close, 50, 200),
    "ema_cross_10_50":    ema_cross_signal(close, 10, 50),
    "macd_norm":          macd_feature(close),
    "vol_scaled_mom":     vol_scaled_momentum(close),
}
# Then cross-sectionally normalise all of them
```
