# Combinatorial Purged Cross-Validation (CPCV)

Lopez de Prado (2018) — Advances in Financial Machine Learning, Chapter 12.

## Why Standard Walk-Forward Isn't Enough

Standard walk-forward gives ONE backtest path per parameter set. CPCV generates
ALL possible train/test splits (C(N,k) combinations), producing a distribution
of paths. From this distribution we can compute PBO — the probability that the
best in-sample parameter is a false discovery.

## Full CPCV Implementation

```python
from itertools import combinations
import numpy as np
import pandas as pd
from scipy.stats import norm

def cpcv_paths(returns: pd.DataFrame, n_splits: int = 10,
               n_test_splits: int = 2, embargo_pct: float = 0.01
               ) -> tuple[list, list]:
    """
    Generate all CPCV train/test split combinations.

    returns: DataFrame, rows=dates, cols=strategies (or param sets).
    n_splits: total number of time splits (N in the paper).
    n_test_splits: number of test splits per combination (k).
    embargo_pct: fraction of each split to embargo between train and test.

    Returns: (train_paths, test_paths) — lists of index arrays.
    """
    T = len(returns)
    split_size = T // n_splits
    embargo_size = int(split_size * embargo_pct)

    # Create N non-overlapping time splits
    splits = []
    for i in range(n_splits):
        start = i * split_size
        end = start + split_size if i < n_splits - 1 else T
        splits.append(np.arange(start, end))

    train_paths, test_paths = [], []

    # All C(N, k) combinations of test splits
    for test_combo in combinations(range(n_splits), n_test_splits):
        test_idx = np.concatenate([splits[i] for i in test_combo])
        train_splits = [splits[i] for i in range(n_splits) if i not in test_combo]

        # Apply embargo: remove `embargo_size` rows adjacent to test from train
        train_idx_list = []
        for s in train_splits:
            # Check adjacency to test splits
            embargoed = set()
            for ti in test_combo:
                # Embargo rows adjacent to each test split
                embargoed.update(range(splits[ti][0] - embargo_size,
                                       splits[ti][0]))
                embargoed.update(range(splits[ti][-1] + 1,
                                       splits[ti][-1] + 1 + embargo_size))
            clean = [idx for idx in s if idx not in embargoed]
            if clean:
                train_idx_list.append(np.array(clean))

        if train_idx_list:
            train_paths.append(np.concatenate(train_idx_list))
            test_paths.append(test_idx)

    return train_paths, test_paths


def cpcv_pbo(strategy_returns: pd.DataFrame, n_splits: int = 10,
             n_test_splits: int = 2, embargo_pct: float = 0.01) -> float:
    """
    Probability of Backtest Overfitting via CPCV.

    strategy_returns: DataFrame where each column is the return series of
    one parameter configuration (e.g., 50 different hyperparameter sets).
    Rows are dates.

    Returns: PBO in [0, 1]. Values > 0.10 indicate overfitting risk.
    """
    train_paths, test_paths = cpcv_paths(
        strategy_returns, n_splits, n_test_splits, embargo_pct
    )

    n_better_random = 0
    n_trials = len(train_paths)

    for train_idx, test_idx in zip(train_paths, test_paths):
        # In-sample: select best-performing strategy (highest mean return)
        in_sample = strategy_returns.iloc[train_idx]
        best_is = in_sample.mean().idxmax()

        # Out-of-sample: how does the best IS strategy rank?
        out_of_sample = strategy_returns.iloc[test_idx]
        oos_means = out_of_sample.mean()
        rank_of_best = (oos_means < oos_means[best_is]).sum()
        n_strategies = len(strategy_returns.columns)

        # Relative rank in [0, 1] — 0 = worst, 1 = best
        relative_rank = rank_of_best / n_strategies

        # PBO counts cases where IS winner performs worse than median OOS
        if relative_rank < 0.5:
            n_better_random += 1

    return n_better_random / n_trials


def cpcv_sharpe_distribution(strategy_returns: pd.DataFrame,
                              n_splits: int = 10, n_test_splits: int = 2,
                              embargo_pct: float = 0.01) -> pd.Series:
    """
    Distribution of OOS Sharpe ratios across all CPCV paths.
    Use to assess: is the IS-selected strategy consistently good OOS?
    """
    train_paths, test_paths = cpcv_paths(
        strategy_returns, n_splits, n_test_splits, embargo_pct
    )

    oos_sharpes = []
    for train_idx, test_idx in zip(train_paths, test_paths):
        in_sample = strategy_returns.iloc[train_idx]
        best_is = in_sample.mean().idxmax()
        oos = strategy_returns.iloc[test_idx][best_is]
        sr = oos.mean() / oos.std() * np.sqrt(252) if oos.std() > 0 else 0
        oos_sharpes.append(sr)

    return pd.Series(oos_sharpes, name="oos_sharpe")
```

## Generating Strategy Returns for CPCV

You need a returns matrix with one column per parameter configuration:

```python
def generate_strategy_returns_matrix(
    features: pd.DataFrame,
    labels: pd.Series,
    param_grid: list[dict],
    model_fn: callable
) -> pd.DataFrame:
    """
    For each parameter set, train on the FIRST half, predict on all,
    return the prediction-weighted daily return series.
    This is the input to CPCV — NOT the same as the model's training loop.
    """
    n = len(features)
    split_idx = n // 2

    X_train = features.iloc[:split_idx]
    y_train = labels.iloc[:split_idx]

    strategy_returns = {}
    for i, params in enumerate(param_grid):
        model = model_fn(**params)
        model.fit(X_train, y_train)

        # Signal: predicted score * actual forward return
        scores = model.predict(features)
        ret = labels  # actual forward return (not shifted — already aligned)
        strategy_returns[f"params_{i}"] = pd.Series(
            scores * ret.values, index=features.index
        )

    return pd.DataFrame(strategy_returns)
```

## Interpreting CPCV Results

```
PBO = 0.05  → 5% of paths show IS winner underperforming OOS median → LOW overfitting risk ✅
PBO = 0.15  → 15% → moderate — tighten the parameter search space
PBO = 0.35  → 35% → HIGH overfitting — strategy is likely data-mined ❌
```

Additionally inspect `cpcv_sharpe_distribution()`:
- Tight distribution with positive mean → consistent signal
- Wide distribution → high variance, regime-dependent
- Mean near zero even if IS Sharpe is high → overfitted
