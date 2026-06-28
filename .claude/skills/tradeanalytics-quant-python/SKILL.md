---
name: tradeanalytics-quant-python
description: >
  Implement quantitative finance algorithms correctly in Python for the TradeAnalytics
  pipeline. Use this skill whenever the user asks about implementing technical indicators
  (EMA, RSI, MACD, ATR, VWAP, Bollinger Bands), calculating Information Coefficient (IC)
  or ICIR, purged walk-forward cross-validation, forward return labels, GARCH volatility
  for position sizing, HMM regime detection, Sharpe/Sortino/MDD/Calmar performance
  metrics, statistical significance testing of returns, or Hurst exponent calculation.
  Also trigger for questions about look-ahead bias in feature construction, point-in-time
  correctness, pandas rolling window pitfalls, handling corporate action adjustments in
  features, or any "how do I implement X in Python" question where X is a quant finance
  concept. This is the implementation companion to ml-feature-engineer — that skill
  explains what to build; this skill shows how to build it without common bugs.
---

# TradeAnalytics Quantitative Python Patterns

## Core Rule: No Look-Ahead Bias

Every calculation that uses future data to predict the past is fatal.
The two most common sources:

```python
# ❌ WRONG — shift() applied after rolling mean contaminates with future data
df["ema_10"] = df["close"].ewm(span=10).mean()
df["label"] = df["close"].pct_change(5).shift(-5)  # OK: label uses future intentionally
# But if you then sort or merge incorrectly, label leaks into features

# ✅ CORRECT — always verify: at row t, feature uses only data up to t
assert df["ema_10"].iloc[i] == df["close"].iloc[:i+1].ewm(span=10).mean().iloc[-1]
```

**Golden rule:** features use `t` and earlier. Labels use `t+1` through `t+N`. They must
never overlap in the training set.

---

## Technical Indicators

### EMA (Exponential Moving Average)

```python
def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()
```

`adjust=False` matches most vendor implementations (recursive formula, not weighted sum).

### RSI (Relative Strength Index)

```python
def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, float("nan"))
    return 100 - (100 / (1 + rs))
```

Wilder smoothing = EWM with `alpha=1/period`. Do not use `rolling().mean()` — that's
Simple Moving Average smoothing and gives different values.

### MACD

```python
def macd(series: pd.Series, fast=12, slow=26, signal=9) -> pd.DataFrame:
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    histogram = macd_line - signal_line
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "hist": histogram})
```

### ATR (Average True Range)

```python
def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()
```

### Bollinger Bands

```python
def bollinger_bands(series: pd.Series, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
    mid = series.rolling(period).mean()
    std = series.rolling(period).std(ddof=1)
    return pd.DataFrame({
        "upper": mid + std_dev * std,
        "mid":   mid,
        "lower": mid - std_dev * std,
        "pct_b": (series - (mid - std_dev * std)) / (2 * std_dev * std)
    })
```

### VWAP (Volume-Weighted Average Price)

VWAP resets daily — never compute a rolling VWAP across session boundaries.

```python
def vwap(high: pd.Series, low: pd.Series, close: pd.Series,
         volume: pd.Series, date: pd.Series) -> pd.Series:
    typical_price = (high + low + close) / 3
    pv = typical_price * volume
    # Group by date, cumulative sum within each day
    result = pd.Series(index=close.index, dtype=float)
    for d, grp in pv.groupby(date):
        cum_pv = grp.cumsum()
        cum_vol = volume.loc[grp.index].cumsum()
        result.loc[grp.index] = cum_pv / cum_vol
    return result
```

---

## Forward Return Labels

```python
def forward_returns(close: pd.Series, horizon: int) -> pd.Series:
    """Log returns over the next `horizon` bars. NaN for last `horizon` rows."""
    return np.log(close.shift(-horizon) / close)

def forward_sign_label(close: pd.Series, horizon: int,
                       threshold: float = 0.0) -> pd.Series:
    """1 = up, -1 = down, 0 = flat (within threshold)."""
    ret = forward_returns(close, horizon)
    label = pd.Series(0, index=close.index, dtype=int)
    label[ret >  threshold] =  1
    label[ret < -threshold] = -1
    return label
```

**Critical:** Drop rows where `label.isna()` before training. The last `horizon` rows
have no label — including them as zeros is silent look-ahead contamination.

---

## Purged Walk-Forward Cross-Validation

Standard k-fold is invalid for financial time series — future data leaks into the past
through overlapping label windows. Use purged CV instead.

```python
from typing import Iterator
import numpy as np

def purged_walk_forward_splits(
    n: int,
    n_splits: int,
    embargo_periods: int,
    min_train_size: int
) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """
    Yields (train_idx, test_idx) pairs.
    - embargo_periods: rows dropped between train end and test start
      (prevents label overlap: set to your forward-return horizon)
    - min_train_size: minimum rows before first test fold
    """
    fold_size = (n - min_train_size) // n_splits
    for i in range(n_splits):
        test_start = min_train_size + i * fold_size
        test_end   = test_start + fold_size
        train_end  = test_start - embargo_periods
        if train_end <= 0 or test_end > n:
            continue
        train_idx = np.arange(0, train_end)
        test_idx  = np.arange(test_start, min(test_end, n))
        yield train_idx, test_idx
```

**Set `embargo_periods` = your label horizon** (e.g., 5 for 5-day forward returns).
This ensures no training label window overlaps with any test bar.

---

## Information Coefficient (IC) and ICIR

IC measures rank correlation between predicted scores and actual returns.
It's the primary model quality metric — not accuracy, not AUC.

```python
from scipy.stats import spearmanr

def information_coefficient(scores: pd.Series, returns: pd.Series) -> float:
    """Spearman rank correlation between signal scores and forward returns."""
    valid = scores.notna() & returns.notna()
    if valid.sum() < 10:
        return float("nan")
    ic, _ = spearmanr(scores[valid], returns[valid])
    return ic

def ic_series(scores_df: pd.DataFrame, returns_df: pd.DataFrame) -> pd.Series:
    """IC computed per time period (row = date). Use this for ICIR."""
    ics = []
    dates = []
    for date in scores_df.index:
        ic = information_coefficient(scores_df.loc[date], returns_df.loc[date])
        ics.append(ic)
        dates.append(date)
    return pd.Series(ics, index=dates)

def icir(ic_series: pd.Series) -> float:
    """Information Coefficient Information Ratio = mean(IC) / std(IC)."""
    return ic_series.mean() / ic_series.std()
```

**Interpretation:**
- IC > 0.05: weak but potentially exploitable signal
- IC > 0.10: good signal
- IC > 0.15: excellent (rare in equities)
- ICIR > 0.5: signal is consistent across time periods

---

## Performance Metrics

```python
import numpy as np

def sharpe_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
    """Annualised Sharpe. Input: daily log returns (or simple — be consistent)."""
    if returns.std() == 0:
        return 0.0
    return (returns.mean() / returns.std()) * np.sqrt(periods_per_year)

def sortino_ratio(returns: pd.Series, periods_per_year: int = 252,
                  target: float = 0.0) -> float:
    """Sortino: penalises only downside volatility."""
    downside = returns[returns < target]
    if len(downside) == 0 or downside.std() == 0:
        return float("inf")
    return (returns.mean() - target) / downside.std() * np.sqrt(periods_per_year)

def max_drawdown(returns: pd.Series) -> float:
    """Maximum peak-to-trough drawdown as a fraction (negative number)."""
    cumulative = (1 + returns).cumprod()
    rolling_max = cumulative.cummax()
    drawdown = (cumulative - rolling_max) / rolling_max
    return drawdown.min()

def calmar_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
    mdd = max_drawdown(returns)
    if mdd == 0:
        return float("inf")
    ann_return = (1 + returns.mean()) ** periods_per_year - 1
    return ann_return / abs(mdd)
```

---

## Hurst Exponent

Used weekly as the Meta-Router input. H > 0.5 → momentum cluster.
H < 0.5 → mean-reversion cluster. H ≈ 0.5 → random walk.

```python
def hurst_exponent(series: pd.Series, min_lag: int = 2,
                   max_lag: int = 100) -> float:
    """
    R/S analysis. Requires ≥100 bars to be statistically reliable.
    Returns H in [0, 1].
    """
    lags = range(min_lag, max_lag)
    tau = []
    for lag in lags:
        sub = [series.iloc[i:i+lag].values for i in range(0, len(series)-lag, lag)]
        rs_vals = []
        for seg in sub:
            mean = np.mean(seg)
            dev = np.cumsum(seg - mean)
            r = dev.max() - dev.min()
            s = np.std(seg, ddof=1)
            if s > 0:
                rs_vals.append(r / s)
        if rs_vals:
            tau.append(np.mean(rs_vals))

    log_lags = np.log(list(lags)[:len(tau)])
    log_tau  = np.log(tau)
    h = np.polyfit(log_lags, log_tau, 1)[0]
    return float(h)
```

Compute on a 252-bar rolling window (1 year), weekly. Do not compute daily —
it's too slow and the signal changes on a weekly cadence.

---

## GARCH(1,1) for Volatility-Based Position Sizing

GARCH is used for position sizing only — not entry signals.

```python
from arch import arch_model

def fit_garch(returns: pd.Series) -> float:
    """
    Fit GARCH(1,1) and return 1-day ahead conditional volatility (annualised).
    Input: daily log returns (no NaN).
    """
    model = arch_model(returns * 100, vol="Garch", p=1, q=1, mean="Zero")
    result = model.fit(disp="off", show_warning=False)
    forecast = result.forecast(horizon=1)
    daily_vol = np.sqrt(forecast.variance.values[-1, 0]) / 100
    return daily_vol * np.sqrt(252)  # annualised
```

Position size formula (GARCH-based):
```python
def garch_position_size(target_vol: float, garch_ann_vol: float,
                        capital: float, price: float) -> int:
    """Number of shares such that position vol ≈ target_vol × capital."""
    dollar_risk = target_vol * capital
    shares = dollar_risk / (garch_ann_vol * price)
    return int(shares)
```

---

## HMM Regime Detection

```python
from hmmlearn import hmm
import numpy as np

def fit_hmm(returns: pd.Series, n_states: int = 4) -> tuple:
    """
    Fit Gaussian HMM. Returns (model, state_sequence).
    States correspond to: bull, bear, sideways, high-vol (label assignment manual).
    Requires hmmlearn: pip install hmmlearn
    """
    X = returns.values.reshape(-1, 1)
    model = hmm.GaussianHMM(
        n_components=n_states,
        covariance_type="full",
        n_iter=1000,
        random_state=42
    )
    model.fit(X)
    states = model.predict(X)
    return model, states

def label_hmm_states(model, n_states: int) -> dict[int, str]:
    """
    Auto-label states by mean return and volatility.
    High mean → bull, Low mean → bear, Low vol → sideways, High vol → high_vol.
    """
    means = model.means_.flatten()
    vols  = np.sqrt(model.covars_.flatten())
    labels = {}
    for i in range(n_states):
        if means[i] > 0 and vols[i] < np.median(vols):
            labels[i] = "bull"
        elif means[i] < 0 and vols[i] < np.median(vols):
            labels[i] = "bear"
        elif vols[i] > np.percentile(vols, 75):
            labels[i] = "high_vol"
        else:
            labels[i] = "sideways"
    return labels
```

**Point-in-time correctness for HMM:** refit the HMM at each walk-forward fold boundary
using only training data. Never use the full-sample HMM for backtest regime labels.

---

## Common Pandas Pitfalls in Financial Code

```python
# ❌ WRONG — chained indexing, silent SettingWithCopyWarning
df["feature"] = df[df["volume"] > 0]["close"].rolling(10).mean()

# ✅ CORRECT — use .loc or .assign
df.loc[df["volume"] > 0, "feature"] = df.loc[df["volume"] > 0, "close"].rolling(10).mean()

# ❌ WRONG — rolling with min_periods not set — NaN rows silently dropped later
df["vol"] = df["close"].pct_change().rolling(20).std()

# ✅ CORRECT — be explicit; document when NaN rows are expected
df["vol"] = df["close"].pct_change().rolling(20, min_periods=20).std()

# ❌ WRONG — merge on date without checking for timezone mismatch
df = features.merge(labels, on="date")  # silently produces empty df if tz differs

# ✅ CORRECT — normalise timezone before merge
features["date"] = pd.to_datetime(features["date"]).dt.tz_localize(None)
labels["date"]   = pd.to_datetime(labels["date"]).dt.tz_localize(None)
```

---

## Packages Used in This Project

```python
# Core quant stack (all in conda env tradeanalytics)
import pandas as pd          # data manipulation
import numpy as np           # numerical
from scipy.stats import spearmanr  # IC calculation
from arch import arch_model  # GARCH — pip install arch
from hmmlearn import hmm     # HMM — pip install hmmlearn
import xgboost as xgb        # Phase 3 primary model
import lightgbm as lgb       # Phase 3 alternative
import shap                  # feature importance
import mlflow                # experiment tracking
```
