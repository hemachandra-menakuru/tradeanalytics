---
name: tradeanalytics-portfolio-construction
description: >
  Institutional portfolio construction for the TradeAnalytics signal engine — converting
  signal scores into position weights. Use this skill whenever the user asks about
  position sizing, portfolio weights, risk parity, equal risk contribution (ERC),
  hierarchical risk parity (HRP), mean-variance optimisation (MVO), Black-Litterman,
  Kelly criterion, volatility targeting, correlation-aware allocation, maximum
  diversification, how to size positions from signal scores, or how to rebalance a
  portfolio. Also trigger for questions about portfolio-level constraints (max position,
  max sector concentration, leverage limits), turnover minimisation during rebalancing,
  or how the TradeAnalytics signal contract translates into actual trade sizes. This is
  the Phase 4/5 bridge between signal generation and execution — signals go in, position
  weights come out.
---

# TradeAnalytics Portfolio Construction

## Design Principle

The Signal Generator produces portfolio-agnostic signal scores. Portfolio construction
converts those scores into position weights. These are separate concerns — the signal
engine never sees portfolio state, and the portfolio engine can be swapped without
touching the signal engine.

```
signal_score ∈ [-1, +1]  (from SignalQualityEngine)
    ↓
PortfolioConstructor (this skill)
    ↓
position_weight ∈ [-max_weight, +max_weight]
    ↓
ExecutionEngine (Phase 5)
    ↓
actual_shares × price = dollar_exposure
```

## Reference Files

| File | Contents |
|---|---|
| `references/risk_parity.md` | Risk Parity, ERC, HRP — full implementations |
| `references/optimisation.md` | MVO, Black-Litterman, Kelly — implementations |
| `references/volatility_targeting.md` | Volatility targeting, dynamic position sizing, GARCH-based sizing |
| `references/constraints.md` | Portfolio constraints, turnover minimisation, rebalancing optimisation |

---

## Portfolio Construction Hierarchy (use in order)

Start simple. Add complexity only when simpler methods are proven insufficient.

```
Level 1: Signal-weighted (start here — Phase 4 initial)
Level 2: Volatility-targeted signal-weighted (add vol normalisation)
Level 3: Equal Risk Contribution (when correlation matters)
Level 4: HRP (when universe > 30 instruments)
Level 5: MVO / Black-Litterman (when you have stable covariance estimates)
```

---

## Level 1 — Signal-Weighted Allocation

The simplest allocation: weights proportional to signal scores.

```python
def signal_weighted_weights(scores: pd.Series,
                             max_weight: float = 0.10,
                             long_only: bool = False) -> pd.Series:
    """
    Convert signal scores to portfolio weights.
    scores: Series {ticker: score} where score ∈ [-1, +1]
    max_weight: maximum single-position weight (default 10%)
    """
    if long_only:
        scores = scores.clip(lower=0)

    # Normalise to sum to 1 (or -1 for shorts)
    total = scores.abs().sum()
    if total == 0:
        return pd.Series(0, index=scores.index)

    weights = scores / total
    return weights.clip(-max_weight, max_weight)
```

**When to use:** Phase 4 initial deployment. Easy to understand, easy to debug.
Weakness: ignores correlation — concentrated in correlated positions.

---

## Level 2 — Volatility-Targeted Signal-Weighted

Adjust signal weights by inverse volatility so each position contributes
equal volatility to the portfolio (before correlation).

```python
def vol_targeted_weights(scores: pd.Series,
                          realised_vol: pd.Series,
                          target_portfolio_vol: float = 0.10,
                          max_weight: float = 0.15) -> pd.Series:
    """
    Inverse-vol weight × signal score.
    target_portfolio_vol: annualised (e.g. 0.10 = 10% p.a.)
    """
    inv_vol = 1.0 / realised_vol.replace(0, np.nan)
    inv_vol_normalised = inv_vol / inv_vol.sum()

    # Signal direction × vol normalisation
    raw_weights = scores * inv_vol_normalised

    # Scale to target portfolio vol (approximation ignoring correlation)
    port_vol_estimate = (raw_weights.abs() * realised_vol).sum()
    if port_vol_estimate > 0:
        scale = target_portfolio_vol / port_vol_estimate
        raw_weights = raw_weights * min(scale, 2.0)  # cap leverage at 2×

    return raw_weights.clip(-max_weight, max_weight)
```

---

## Level 3 — Equal Risk Contribution (ERC)

Each position contributes equally to total portfolio variance.
Better than inverse-vol when assets are correlated.

See `references/risk_parity.md` for full implementation.

```python
from references.risk_parity import equal_risk_contribution

weights = equal_risk_contribution(
    cov_matrix=rolling_covariance,
    signal_scores=scores,      # signs from signal engine
    long_only=False
)
```

ERC requires a stable covariance matrix. Use 63-day rolling covariance with
Ledoit-Wolf shrinkage (see `references/optimisation.md`).

---

## Level 4 — Hierarchical Risk Parity (HRP)

Lopez de Prado (2016). HRP uses hierarchical clustering on the correlation
matrix rather than matrix inversion — making it robust to estimation error
and valid when the covariance matrix is near-singular (many assets, few observations).

See `references/risk_parity.md` for full implementation.

```python
from references.risk_parity import hrp_weights

weights = hrp_weights(
    returns=daily_returns,       # rows=dates, cols=tickers
    signal_scores=scores,        # for directional sign
    linkage_method="single"      # "single" | "complete" | "average"
)
```

**Use HRP when:**
- Universe > 30 instruments
- Observation/variable ratio < 3 (T/N < 3)
- Covariance matrix is near-singular (condition number > 1000)

---

## Level 5 — Black-Litterman

When you have strong prior views (from the signal engine) and a market equilibrium,
BL blends them into a posterior distribution for expected returns.

See `references/optimisation.md` for full implementation.

```python
from references.optimisation import black_litterman

posterior_returns = black_litterman(
    market_caps=market_cap_weights,
    signal_views=signal_scores,       # your "views" vector
    view_confidence=confidence_scores, # from SignalQualityEngine
    risk_aversion=2.5,
    tau=0.05
)
```

---

## Portfolio-Level Constraints

Always applied AFTER weight computation, BEFORE execution:

```python
def apply_portfolio_constraints(
    weights: pd.Series,
    max_single_position: float = 0.10,
    max_cluster_concentration: float = 0.40,
    max_gross_leverage: float = 1.5,
    cluster_map: pd.Series = None
) -> pd.Series:
    """
    Apply hard constraints to computed weights.
    Clip then renormalise — preserves relative signal but enforces limits.
    """
    # 1. Single position limit
    weights = weights.clip(-max_single_position, max_single_position)

    # 2. Cluster concentration limit
    if cluster_map is not None:
        for cluster in cluster_map.unique():
            tickers_in_cluster = cluster_map[cluster_map == cluster].index
            cluster_exposure = weights.reindex(tickers_in_cluster).abs().sum()
            if cluster_exposure > max_cluster_concentration:
                scale = max_cluster_concentration / cluster_exposure
                weights[tickers_in_cluster] *= scale

    # 3. Gross leverage limit
    gross_leverage = weights.abs().sum()
    if gross_leverage > max_gross_leverage:
        weights = weights / gross_leverage * max_gross_leverage

    return weights
```

---

## Rebalancing Optimisation (Turnover Minimisation)

Don't naively flip to the new target weights every rebalance. Minimise
trading while tracking the target within tolerance.

```python
def rebalance_with_tolerance(
    current_weights: pd.Series,
    target_weights: pd.Series,
    rebalance_threshold: float = 0.02,   # 2% drift before rebalancing
    max_turnover_per_rebalance: float = 0.20  # max 20% of portfolio per rebalance
) -> pd.Series:
    """
    Partial rebalancing: only trade positions that have drifted beyond threshold.
    Reduces turnover by ~40% vs full rebalancing, with minimal tracking error.
    """
    drift = (target_weights - current_weights).abs()
    to_rebalance = drift[drift > rebalance_threshold].index

    new_weights = current_weights.copy()
    new_weights[to_rebalance] = target_weights[to_rebalance]

    # Cap total turnover
    total_trade = (new_weights - current_weights).abs().sum() / 2
    if total_trade > max_turnover_per_rebalance:
        scale = max_turnover_per_rebalance / total_trade
        adjustment = (new_weights - current_weights) * scale
        new_weights = current_weights + adjustment

    return new_weights
```

---

## Phase 4 Implementation Path

```
Phase 4 launch:    Level 1 (signal-weighted) — simplest, easiest to attribute
After 3 months:    Level 2 (vol-targeted) — add vol normalisation
After 6 months:    Level 3/4 (ERC or HRP) — if Sharpe improves on paper track
Phase 5:           Add constraints + rebalancing optimisation before live
```

Never jump to Level 5 (BL, MVO) without stable covariance estimates from
at least 2 years of history across the full universe.
