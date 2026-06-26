# Mean-Reversion Factors — Implementation Reference

## 1. Short-Term Reversal

Jegadeesh (1990). Over 1–4 week horizons, recent winners underperform and
recent losers outperform. Driven by market microstructure (bid-ask bounce,
inventory effects) and overreaction to short-term news.

```python
def short_term_reversal(close: pd.DataFrame,
                         horizon: int = 5) -> pd.DataFrame:
    """
    Negative of trailing return — high recent return → bearish feature.
    Horizons: 5 days (1 week), 10 days (2 weeks), 21 days (1 month).
    """
    trailing_return = close / close.shift(horizon) - 1
    return -trailing_return  # sign flip: past losers are the long signal
```

Compute at three horizons as separate features: 5d, 10d, 21d.
**Do not use at 63d or longer** — at that horizon momentum dominates reversal.

## 2. RSI-Based Reversion Feature

RSI as a continuous overextension signal, not a binary entry rule.

```python
def rsi_reversion_feature(close: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Centred RSI: (RSI - 50) / 50. Range [-1, 1].
    Negative value = oversold (reversal long candidate).
    Positive value = overbought (reversal short candidate).
    Sign flip applied so high values = bullish reversion signal.
    """
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    centred = (rsi - 50) / 50
    return -centred  # oversold (low RSI) → positive feature value
```

Also compute at RSI(5) for ultra-short-term reversion and RSI(28) for
intermediate-term. Three features total.

## 3. Bollinger Band %B

Deviation from 20-day mean, normalised by band width. Values <0 = below lower
band (strong reversion candidate), values >1 = above upper band.

```python
def bollinger_pct_b(close: pd.DataFrame,
                     period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
    """
    %B = (price - lower_band) / (upper_band - lower_band).
    Centred and sign-flipped so high values = long reversion candidate.
    """
    mid = close.rolling(period).mean()
    std = close.rolling(period).std(ddof=1)
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    pct_b = (close - lower) / (upper - lower).replace(0, np.nan)
    # Flip: low %B (below lower band) → positive feature (oversold)
    return 0.5 - pct_b
```

## 4. Z-Score to N-Day Mean

Distance from rolling mean in standard deviation units. More general than Bollinger.

```python
def zscore_to_mean(close: pd.DataFrame, window: int = 252) -> pd.DataFrame:
    """
    (price - rolling_mean) / rolling_std over `window` days.
    Negative = below mean (potential mean-reversion long).
    Sign flip: negative zscore → positive feature (below mean = buy signal).
    """
    rolling_mean = close.rolling(window).mean()
    rolling_std  = close.rolling(window).std(ddof=1)
    zscore = (close - rolling_mean) / rolling_std.replace(0, np.nan)
    return -zscore  # below mean = positive signal
```

Compute at 63d (quarter), 126d (half-year), 252d (annual). Three features.

## 5. VWAP Deviation

Intraday mean-reversion signal. Price significantly below VWAP → institutional
buyers likely to absorb the deviation.

```python
def vwap_deviation(close: pd.DataFrame, vwap: pd.DataFrame) -> pd.DataFrame:
    """
    (price - vwap) / vwap — signed percentage deviation.
    Negative = below VWAP (reversion long candidate).
    Sign flip applied.
    """
    deviation = (close - vwap) / vwap.replace(0, np.nan)
    return -deviation  # below VWAP → positive feature
```

**Note:** VWAP resets daily. Only use daily close vs daily VWAP. Do not compute
intraday VWAP from daily OHLCV — it is undefined. Use daily close relative to
a rolling VWAP proxy (close × volume cumulative) as an approximation.

## 6. Overnight Gap Reversion

Large overnight gaps tend to partially fill intraday. The open-to-previous-close
gap is a reversion signal for the intraday session.

```python
def overnight_gap(open_: pd.DataFrame, close: pd.DataFrame) -> pd.DataFrame:
    """
    Gap = (open_today - close_yesterday) / close_yesterday.
    Large positive gap → likely to fill down (reversion short).
    Sign flip: large positive gap → negative feature.
    """
    gap = (open_ - close.shift(1)) / close.shift(1)
    return -gap  # gap up → negative reversion feature
```

## 7. ATR-Normalised Drawdown from High

Distance from recent high, normalised by ATR — regime-adjusted reversion depth.

```python
def atr_normalised_drawdown(close: pd.DataFrame, high: pd.DataFrame,
                              low: pd.DataFrame, atr_period: int = 14,
                              high_window: int = 20) -> pd.DataFrame:
    """
    How many ATRs below the recent high is the current price?
    Deep drawdown in low-vol environment = strong reversion candidate.
    """
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/atr_period, adjust=False).mean()

    rolling_high = close.rolling(high_window).max()
    drawdown_pts = rolling_high - close
    return drawdown_pts / atr.replace(0, np.nan)  # already positive; high = more oversold
```

## Feature Set Summary for Mean-Reversion Cluster

```python
reversion_features = {
    "reversal_5d":          short_term_reversal(close, 5),
    "reversal_10d":         short_term_reversal(close, 10),
    "reversal_21d":         short_term_reversal(close, 21),
    "rsi_reversion_5":      rsi_reversion_feature(close, 5),
    "rsi_reversion_14":     rsi_reversion_feature(close, 14),
    "rsi_reversion_28":     rsi_reversion_feature(close, 28),
    "bollinger_pct_b_20":   bollinger_pct_b(close, 20, 2.0),
    "bollinger_pct_b_10":   bollinger_pct_b(close, 10, 1.5),
    "zscore_63d":           zscore_to_mean(close, 63),
    "zscore_252d":          zscore_to_mean(close, 252),
    "vwap_deviation":       vwap_deviation(close, vwap_series),
    "overnight_gap":        overnight_gap(open_, close),
    "atr_drawdown_20d":     atr_normalised_drawdown(close, high, low, 14, 20),
}
```

## Regime Interaction: When Reversion Works

Mean-reversion IC is strongly regime-dependent. Always include regime indicators
as features so XGBoost can condition reversion signals on regime:

```python
regime_interactions = {
    "hmm_state":              hmm_regime_label,       # bull/bear/sideways/high_vol
    "vix_level_norm":         normalised_vix,          # low VIX → reversion works better
    "realised_vol_20d":       realised_vol,            # low vol → reversion works better
    "hurst_252":              hurst_exponent_series,   # H < 0.45 → mean-reverting
}
```

Reversion factors have the strongest IC when: VIX < 20, HMM state = sideways,
H < 0.45. IC degrades sharply in trending high-vol regimes.
