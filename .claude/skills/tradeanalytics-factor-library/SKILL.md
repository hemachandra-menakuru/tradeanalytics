---
name: tradeanalytics-factor-library
description: >
  Institutional-grade systematic factor library for the TradeAnalytics XGBoost/LightGBM
  signal engine. Use this skill whenever the user asks about which features to build,
  what factors to add to the feature matrix, how to implement momentum/mean-reversion/
  quality/volatility/liquidity factors, how to normalise cross-sectional features,
  how to combine factors, which factors survive multiple-testing correction, factor
  decay analysis, factor crowding, or how to map academic factors to OHLCV-computable
  features. Also trigger for questions about the Fama-French factor model, AQR research,
  post-earnings drift, the low-volatility anomaly, betting-against-beta, 52-week high
  proximity, short-term reversal, Amihud illiquidity, or any "what factors should I use
  for X cluster" question. This is the authoritative research reference for what goes
  into the TradeAnalytics feature matrix — combining 30 years of academic factor research
  with implementation constraints of IBKR OHLCV data.
---

# TradeAnalytics Institutional Factor Library

## How to Use This Skill

This skill has two layers:
- **This file** — taxonomy, cluster mapping, normalisation, and combination methodology
- **`references/`** — deep implementation guides per factor family

Read the relevant reference file when implementing a specific factor family.

| File | Contents |
|---|---|
| `references/momentum_factors.md` | Price momentum, time-series momentum, industry momentum, 52-week high |
| `references/reversion_factors.md` | Short-term reversal, RSI extremes, Bollinger mean-reversion, VWAP deviation |
| `references/volatility_factors.md` | Realised vol, GARCH vol, low-vol anomaly, vol-of-vol, BAB (Betting Against Beta) |
| `references/volume_liquidity_factors.md` | Amihud illiquidity, dollar volume, volume ratio, order flow imbalance proxy |
| `references/quality_factors.md` | Price-implied quality proxies (OHLCV only) — earnings quality deferred to Phase 5+ |
| `references/factor_engineering.md` | Normalisation, decay analysis, IC-weighted combination, multiple-testing correction |

---

## The Factor Zoo Problem

Harvey, Liu & Zhu (2016) documented 316 factors published in top journals.
After correcting for multiple testing, fewer than half survive at conventional
significance levels. The institutional standard:

> **A factor earns inclusion only if it has:**
> 1. Out-of-sample replication across markets and time periods
> 2. Economic rationale (risk-based or behavioural)
> 3. Survived in a post-publication live period (reduces data-mining risk)
> 4. IC > 0.03 in your own universe before inclusion

This skill includes only factors meeting all four criteria.

---

## Factor Taxonomy — Five Families

### 1. Momentum (Trend)
*Persistent outperformers continue to outperform over 1–12 month horizons*
- **Cross-sectional momentum:** 12-1 month return rank within universe
- **Time-series momentum (TSMOM):** sign of own trailing return
- **Industry momentum:** sector-relative momentum (requires sector mapping)
- **52-week high proximity:** price / 52-week-high ratio
- **MACD momentum:** trend confirmation at multiple frequencies

### 2. Short-Term Reversal
*Winners over 1–4 weeks tend to mean-revert*
- **1-week reversal:** 5-day return (sign flipped as feature)
- **1-month reversal:** 21-day return (sign flipped)
- **RSI extreme:** oversold/overbought reversion signal
- **Bollinger %B:** deviation from 20-day mean normalised by band width
- **VWAP deviation:** price distance from session VWAP

### 3. Low Volatility / Low Beta
*Low-risk stocks earn risk-adjusted returns exceeding high-risk stocks*
- **Realised volatility:** 20-day, 60-day rolling return std
- **Idiosyncratic volatility:** residual vol after market-factor regression
- **Betting Against Beta (BAB):** leverage-adjusted beta-neutral return
- **Max return:** highest single-day return in past month (lottery proxy — negative signal)
- **GARCH conditional vol:** forward-looking volatility estimate

### 4. Liquidity
*Illiquid stocks earn a premium; extreme illiquidity is a risk factor*
- **Amihud illiquidity ratio:** |return| / dollar_volume (daily, 21-day average)
- **Dollar volume:** log(price × volume) — size/liquidity composite
- **Volume ratio:** today's volume / 20-day average volume (abnormal activity signal)
- **Turnover rate:** volume / shares outstanding proxy (float turnover)

### 5. Quality (OHLCV-computable proxies — Phase 3)
*High-quality businesses earn persistent excess returns*
- **Price consistency:** ratio of up-days in trailing window (earnings stability proxy)
- **Gap frequency:** overnight gap size distribution (earnings surprise proxy)
- **Volume-price correlation:** positive = accumulation (institutional buying proxy)
- **52-week range position:** normalised price position within year's range

*Fundamental quality factors (ROE, accruals, F-Score) deferred to Phase 5+
when fundamental data feed is added.*

---

## Cluster → Factor Mapping

This is the authoritative mapping for TradeAnalytics model clusters.

### Momentum Cluster Features
```
Primary:   cross_sectional_momentum_12_1, tsmom_12, tsmom_6, tsmom_3
           macd_hist_norm, ema_cross_50_200, price_to_52w_high
Secondary: industry_momentum_3m (requires sector labels — Phase 5+)
           earnings_momentum_sue (requires EPS data — Phase 5+)
Regime:    hurst_exponent_252 (routing only — H > 0.55 → assign to this cluster)
Exclude:   all reversal factors (negatively correlated with momentum)
```

### Mean-Reversion Cluster Features
```
Primary:   rsi_14, bollinger_pct_b_20, vwap_deviation_norm
           short_term_reversal_5d, short_term_reversal_21d
           z_score_to_252d_mean
Secondary: amihud_illiquidity (reversion needs liquidity to close)
           realised_vol_20d (high vol → faster reversion)
Regime:    hurst_exponent_252 (H < 0.45 → assign to this cluster)
           hmm_state (sideways regime → favour this cluster)
Exclude:   long-horizon momentum (contradicts mean-reversion thesis)
```

### Mega-Cap / Defensive Cluster Features
```
Primary:   realised_vol_20d (low → favour), dollar_volume_log
           price_consistency_63d, volume_price_corr_21d
           beta_to_spy_60d (low → favour)
Secondary: bollinger_pct_b_20 (mild reversion signal)
           tsmom_3 (short-horizon trend)
Regime:    vix_level (high VIX → weight this cluster more)
           hmm_state (bear/high_vol regime → favour this cluster)
```

---

## Cross-Sectional Normalisation (Mandatory)

Raw factor values are meaningless across assets. Always normalise within the
cross-section before feeding into XGBoost.

```python
def cross_sectional_zscore(factor: pd.DataFrame,
                           winsorize_pct: float = 0.01) -> pd.DataFrame:
    """
    Normalise factor cross-sectionally at each time step.
    factor: DataFrame, rows=dates, cols=tickers.
    winsorize_pct: clip extreme values (0.01 = 1st/99th percentile).
    """
    def _normalise_row(row: pd.Series) -> pd.Series:
        lo = row.quantile(winsorize_pct)
        hi = row.quantile(1 - winsorize_pct)
        clipped = row.clip(lo, hi)
        mu = clipped.mean()
        sigma = clipped.std()
        if sigma == 0:
            return pd.Series(0, index=row.index)
        return (clipped - mu) / sigma
    return factor.apply(_normalise_row, axis=1)
```

**Why winsorise at 1%:** a single extreme outlier (e.g. a 50% gap-down on earnings)
can dominate the cross-section and distort all other z-scores. Winsorising before
normalising prevents this.

---

## Factor Combination (IC-Weighted Composite)

Naively averaging factors assumes equal predictive power. IC-weighting is better:

```python
def ic_weighted_composite(factor_df_dict: dict[str, pd.DataFrame],
                          returns: pd.DataFrame,
                          lookback_periods: int = 60) -> pd.DataFrame:
    """
    Combine multiple normalised factor DataFrames into a single composite score.
    Weights = rolling IC of each factor vs. forward returns.
    factor_df_dict: {'factor_name': factor_df, ...}
    returns: forward returns DataFrame (same index/cols as factors)
    """
    from scipy.stats import spearmanr
    import numpy as np

    weights = {}
    for name, fdf in factor_df_dict.items():
        ic_series = []
        for date in fdf.index[-lookback_periods:]:
            f = fdf.loc[date].dropna()
            r = returns.loc[date].reindex(f.index).dropna()
            common = f.index.intersection(r.index)
            if len(common) < 10:
                ic_series.append(0)
                continue
            ic, _ = spearmanr(f[common], r[common])
            ic_series.append(ic if not np.isnan(ic) else 0)
        weights[name] = max(np.mean(ic_series), 0)  # negative IC factors get 0 weight

    total = sum(weights.values())
    if total == 0:
        weights = {k: 1/len(weights) for k in weights}
    else:
        weights = {k: v/total for k, v in weights.items()}

    composite = sum(fdf * weights[name] for name, fdf in factor_df_dict.items())
    return composite
```

---

## Factor Decay — How Quickly Signals Die

Different factors have different half-lives. This drives your rebalance frequency.

| Factor Family | Typical IC Half-Life | Rebalance Frequency |
|---|---|---|
| Short-term reversal | 1–5 days | Daily |
| Momentum (1–3 month) | 2–4 weeks | Weekly |
| Momentum (12-1 month) | 4–8 weeks | Monthly |
| Low volatility | 4–12 weeks | Monthly |
| Liquidity (Amihud) | 2–4 weeks | Weekly |
| Quality (fundamental) | 3–6 months | Quarterly |
| HMM regime state | 1–3 weeks | Weekly |
| Hurst exponent | 3–8 weeks | Weekly |

**Implication for your pipeline:** daily-rebalance factors (reversal, RSI, VWAP
deviation) go in the mean-reversion cluster. Monthly-rebalance factors (12-1 momentum,
low-vol) go in the momentum/mega-cap clusters. Mixing frequencies within a single
model without accounting for decay degrades IC.

---

## Multiple Testing Correction

When you test 30 factors, at p=0.05 you expect 1.5 false positives by chance.
Apply Benjamini-Hochberg-Yekutieli (BHY) correction before including any factor:

```python
from statsmodels.stats.multitest import multipletests

def select_significant_factors(factor_names: list[str],
                                p_values: list[float],
                                alpha: float = 0.05) -> list[str]:
    """
    BHY correction (conservative, valid under arbitrary dependence).
    Returns factor names that survive multiple-testing correction.
    """
    reject, p_corrected, _, _ = multipletests(p_values, alpha=alpha, method="fdr_by")
    return [name for name, r in zip(factor_names, reject) if r]
```

Compute p-values from your IC t-statistics:
```python
from scipy.stats import ttest_1samp

def ic_pvalue(ic_series: pd.Series) -> float:
    """Two-sided t-test: H0 = IC mean is zero."""
    _, p = ttest_1samp(ic_series.dropna(), popmean=0)
    return p
```

---

## Factor Crowding Risk

When too many systematic funds hold the same factors, crowded unwinds create
left-tail events uncorrelated with fundamentals. Monitor:

```python
def factor_crowding_signal(factor: pd.Series,
                           volume_ratio: pd.Series) -> float:
    """
    Proxy: high factor-score stocks also show high abnormal volume.
    Rising correlation → crowding risk increasing.
    """
    from scipy.stats import spearmanr
    ic, _ = spearmanr(factor.dropna(), volume_ratio.reindex(factor.index).dropna())
    return ic  # IC > 0.15 → crowding warning
```

When crowding signal fires on your primary factors, reduce position sizing in
those clusters — do NOT disable the signal. The signal is still valid; the
risk-per-unit-of-signal has increased.

---

## OHLCV-Available vs Deferred Factors

### Available Now (Phase 3 — IBKR OHLCV)
- All momentum factors (price-based)
- All reversal factors
- All volatility factors
- Amihud illiquidity, dollar volume, volume ratio
- OHLCV quality proxies

### Deferred to Phase 5+ (requires additional data feeds)
| Factor | Data Required | Source |
|---|---|---|
| Book-to-market (value) | Balance sheet | Compustat / IBKR fundamentals |
| Earnings yield, P/E | Income statement | Same |
| Gross profitability (Novy-Marx) | Revenue, COGS | Same |
| Accruals (Sloan) | Balance sheet | Same |
| Piotroski F-Score | Full financials | Same |
| SUE (earnings surprise) | EPS estimates | IBES / Refinitiv |
| Analyst revision momentum | Analyst estimates | Same |
| Short interest | Short interest data | FINRA / S3 Partners |
| Insider buying | SEC Form 4 | SEC EDGAR API (free) |
| Options implied vol skew | Options chain | IBKR options data (available) |

**Note:** IBKR Client Portal REST API does provide options data. The
`get_option_data` MCP tool is available. Options-derived factors (IV rank, put/call
ratio, skew) can be added in Phase 4 without a separate data feed.

---

## Key Academic References

| Paper | Finding |
|---|---|
| Jegadeesh & Titman (1993) | Cross-sectional momentum: 12-1 month return predicts next 3-12 months |
| Carhart (1997) | 4-factor model: market + size + value + momentum |
| Fama & French (2015) | 5-factor: adds profitability + investment |
| Asness, Moskowitz & Pedersen (2013) | Momentum works across asset classes and geographies |
| Frazzini & Pedersen (2014) | Betting Against Beta: low-beta stocks outperform risk-adjusted |
| Novy-Marx (2013) | Gross profitability predicts returns out-of-sample |
| Sloan (1996) | Accruals anomaly: high accruals → negative returns |
| Jegadeesh & Titman (2001) | Short-term reversal: 1-week losers outperform |
| Amihud (2002) | Illiquidity ratio predicts future returns as risk premium |
| Harvey, Liu & Zhu (2016) | Factor zoo: most published factors don't survive multiple testing |
| McLean & Pontiff (2016) | Factor returns decay 26% post-publication (arbitrage erosion) |

Read the reference files for Python implementations of each factor family.
