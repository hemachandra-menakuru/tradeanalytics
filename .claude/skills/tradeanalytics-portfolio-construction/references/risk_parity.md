# Risk Parity, ERC & Hierarchical Risk Parity

## Equal Risk Contribution (ERC)

Maillard, Roncalli & Teïletche (2010). Each asset contributes equally to
total portfolio variance. More balanced than inverse-vol because it accounts
for correlations between assets.

```python
import numpy as np
import pandas as pd
from scipy.optimize import minimize

def equal_risk_contribution(cov_matrix: pd.DataFrame,
                             signal_scores: pd.Series = None,
                             long_only: bool = False) -> pd.Series:
    """
    ERC weights: each asset's risk contribution = total_risk / N.

    cov_matrix:    N×N covariance matrix of returns
    signal_scores: optional signal directional scores — if provided,
                   ERC weights are signed by signal direction.
    long_only:     constrain to positive weights only
    """
    n = len(cov_matrix)
    cov = cov_matrix.values

    def portfolio_vol(w):
        return np.sqrt(w @ cov @ w)

    def risk_contributions(w):
        pv = portfolio_vol(w)
        marginal_risk = cov @ w
        return w * marginal_risk / pv

    def objective(w):
        rc = risk_contributions(w)
        target = portfolio_vol(w) / n
        return np.sum((rc - target)**2)

    # Initial guess: inverse vol
    vols = np.sqrt(np.diag(cov))
    w0 = (1 / vols) / (1 / vols).sum()

    bounds = [(0, 1) if long_only else (-1, 1)] * n
    constraints = [{"type": "eq", "fun": lambda w: np.abs(w).sum() - 1}]

    result = minimize(objective, w0, method="SLSQP",
                      bounds=bounds, constraints=constraints,
                      options={"maxiter": 1000, "ftol": 1e-12})

    weights = pd.Series(result.x, index=cov_matrix.index)

    # Apply signal direction if provided
    if signal_scores is not None:
        signs = np.sign(signal_scores.reindex(weights.index).fillna(0))
        signs[signs == 0] = 1  # default to long for zero-score assets
        weights = weights.abs() * signs

    return weights


def risk_contribution_breakdown(weights: pd.Series,
                                 cov_matrix: pd.DataFrame) -> pd.DataFrame:
    """
    Diagnose: how much risk does each position contribute?
    Use after ERC to verify equal contribution.
    """
    w = weights.values
    cov = cov_matrix.values
    port_var = w @ cov @ w
    port_vol = np.sqrt(port_var)
    marginal = cov @ w
    rc = w * marginal
    pct_rc = rc / port_var * 100

    return pd.DataFrame({
        "weight": weights,
        "risk_contribution": rc,
        "pct_of_total_risk": pct_rc,
        "marginal_risk": marginal
    }).round(6)
```

## Ledoit-Wolf Covariance Shrinkage

Raw sample covariance is noisy with few observations. Always shrink before
using in ERC, MVO, or BL.

```python
from sklearn.covariance import LedoitWolf

def shrunk_covariance(returns: pd.DataFrame, window: int = 126) -> pd.DataFrame:
    """
    Ledoit-Wolf shrinkage estimator. Dramatically improves conditioning
    of the covariance matrix when T/N < 5.
    """
    recent = returns.tail(window).dropna(axis=1)
    lw = LedoitWolf()
    lw.fit(recent)
    return pd.DataFrame(lw.covariance_,
                         index=recent.columns,
                         columns=recent.columns)
```

## Hierarchical Risk Parity (HRP)

Lopez de Prado (2016). No matrix inversion — uses graph theory and hierarchical
clustering instead. Works when covariance matrix is near-singular.

```python
from scipy.cluster.hierarchy import linkage, dendrogram, to_tree
from scipy.spatial.distance import squareform

def hrp_weights(returns: pd.DataFrame,
                signal_scores: pd.Series = None,
                linkage_method: str = "single") -> pd.Series:
    """
    Full HRP implementation.
    Step 1: Compute correlation matrix
    Step 2: Convert to distance matrix
    Step 3: Hierarchical clustering
    Step 4: Quasi-diagonalise the covariance matrix
    Step 5: Top-down recursive bisection to allocate weights
    """
    # Step 1: correlation and covariance
    cov = returns.cov()
    corr = returns.corr()

    # Step 2: distance matrix (0 = perfectly correlated, 1 = uncorrelated)
    dist = np.sqrt((1 - corr) / 2)
    dist_condensed = squareform(dist.values)

    # Step 3: hierarchical clustering
    link = linkage(dist_condensed, method=linkage_method)

    # Step 4: sort assets by cluster order (quasi-diagonalisation)
    sorted_idx = _get_quasi_diag(link, len(returns.columns))
    sorted_tickers = [returns.columns[i] for i in sorted_idx]
    cov_sorted = cov.loc[sorted_tickers, sorted_tickers]

    # Step 5: recursive bisection
    weights = _recursive_bisection(cov_sorted)
    weights = weights.reindex(returns.columns).fillna(0)

    # Apply signal direction
    if signal_scores is not None:
        signs = np.sign(signal_scores.reindex(weights.index).fillna(0))
        signs[signs == 0] = 1
        weights = weights.abs() * signs

    return weights


def _get_quasi_diag(link: np.ndarray, n: int) -> list:
    """Extract asset order from linkage matrix (quasi-diagonalisation)."""
    link = link.astype(int)
    sort_idx = [link[-1, 0], link[-1, 1]]
    num_items = link[-1, 3]

    while max(sort_idx) >= n:
        i = sort_idx.index(max(sort_idx))
        max_val = sort_idx[i]
        idx = max_val - n
        sort_idx[i:i+1] = [link[idx, 0], link[idx, 1]]

    return sort_idx


def _recursive_bisection(cov_sorted: pd.DataFrame) -> pd.Series:
    """Top-down recursive bisection: allocate risk by cluster."""
    weights = pd.Series(1.0, index=cov_sorted.index)
    clusters = [cov_sorted.index.tolist()]

    while clusters:
        new_clusters = []
        for cluster in clusters:
            if len(cluster) == 1:
                continue
            # Split cluster in half
            mid = len(cluster) // 2
            left, right = cluster[:mid], cluster[mid:]

            # Compute cluster volatilities
            left_var  = _cluster_var(cov_sorted, left)
            right_var = _cluster_var(cov_sorted, right)

            # Allocate inversely proportional to variance
            alloc_left = 1 - left_var / (left_var + right_var)

            weights[left]  *= alloc_left
            weights[right] *= 1 - alloc_left

            if len(left) > 1:
                new_clusters.append(left)
            if len(right) > 1:
                new_clusters.append(right)

        clusters = new_clusters

    return weights / weights.sum()


def _cluster_var(cov: pd.DataFrame, cluster: list) -> float:
    """Inverse-vol weighted variance of a cluster."""
    sub_cov = cov.loc[cluster, cluster]
    inv_diag = 1 / np.diag(sub_cov.values)
    w = inv_diag / inv_diag.sum()
    return float(w @ sub_cov.values @ w)
```

## When to Use Which Method

| Universe size | T/N ratio | Recommended |
|---|---|---|
| < 10 assets | Any | Inverse-vol (Level 2) |
| 10–30 assets | T/N > 5 | ERC |
| 10–30 assets | T/N < 5 | HRP |
| > 30 assets | Any | HRP |
| Any | Stable history, views available | Black-Litterman |

**TradeAnalytics current universe:** 8 instruments → use Level 2 (vol-targeted).
Switch to ERC at 15+ instruments. Switch to HRP at 30+.
