---
name: tradeanalytics-backtest-robustness
description: >
  Institutional-grade backtest validation and anti-overfitting framework for the
  TradeAnalytics signal engine. Use this skill whenever the user asks about backtest
  validation, overfitting detection, combinatorial purged cross-validation (CPCV),
  deflated Sharpe ratio (DSR), probability of backtest overfitting (PBO), Monte Carlo
  robustness testing, bootstrap analysis, parameter stability, sensitivity analysis,
  walk-forward optimisation, or "is my backtest real?". Also trigger for questions
  about the multiple-testing problem in strategy research, why a strategy that looks
  good in-sample fails out-of-sample, or how to prove a signal has genuine predictive
  power before committing capital. This is the Phase 3/4 gate — no signal enters
  gold.signal_log without passing these tests. Grounded in Marcos Lopez de Prado's
  Advances in Financial Machine Learning and Bailey, Borwein, Lopez de Prado & Zhu (2016).
---

# TradeAnalytics Backtest Robustness Framework

## The Core Problem

A backtest is a search over parameter space. The more combinations you test,
the higher the probability that the best-performing one is a lucky draw, not
a real edge. Institutional quant desks reject strategies that cannot demonstrate
robustness beyond in-sample performance.

> "The more trials run, the more likely a false discovery." — Harvey, Liu & Zhu (2016)

This skill enforces the gates that separate real edge from overfitting.

## Reference Files

| File | Contents |
|---|---|
| `references/cpcv.md` | Combinatorial Purged Cross-Validation — full implementation |
| `references/dsr_pbo.md` | Deflated Sharpe Ratio + Probability of Backtest Overfitting |
| `references/monte_carlo.md` | Monte Carlo robustness, bootstrap, parameter stability |
| `references/transaction_costs.md` | TC modelling, slippage, capacity analysis, realistic net Sharpe |

---

## Validation Gate Sequence (mandatory order)

Every strategy / factor in TradeAnalytics must pass all four gates before
being registered in MLflow and promoted to `champion`:

```
Gate 1: Purged Walk-Forward IC > 0.03 (mean) and ICIR > 0.5
    ↓
Gate 2: CPCV — PBO < 0.10 (less than 10% probability of overfitting)
    ↓
Gate 3: DSR > 0 (Deflated Sharpe accounting for number of trials tested)
    ↓
Gate 4: Net Sharpe after realistic TC and slippage > 0.8 annualised
    ↓
Signal enters gold.signal_log and is eligible for champion promotion
```

If any gate fails: the strategy is redesigned, not tuned. Tuning a failed gate
is the textbook definition of data-snooping.

---

## Gate 1 — Purged Walk-Forward

Already in `tradeanalytics-quant-python`. Summary:
- Use `purged_walk_forward_splits()` with `embargo_periods` = label horizon
- IC must be > 0.03 on the out-of-sample test set of EVERY fold
- ICIR (IC mean / IC std across folds) must be > 0.5
- A single fold with IC < 0 is a red flag — investigate regime dependency

---

## Gate 2 — Combinatorial Purged CV (CPCV)

See `references/cpcv.md` for full implementation.

CPCV generates all possible combinations of training/test splits, giving a
distribution of backtest performances. PBO is the fraction of combinations
where a randomly selected parameter set outperforms the optimised one.

```python
from references.cpcv import cpcv_pbo

pbo = cpcv_pbo(
    returns_matrix=walk_forward_returns,   # shape: (n_paths, n_param_sets)
    n_splits=10,
    embargo_pct=0.01
)
print(f"PBO: {pbo:.1%}")  # must be < 10%
```

**Interpretation:**
- PBO < 10%: strong signal, unlikely to be overfitted
- PBO 10%–25%: borderline — reduce parameter space and retest
- PBO > 25%: strategy is overfitted — do not deploy

---

## Gate 3 — Deflated Sharpe Ratio (DSR)

See `references/dsr_pbo.md` for full implementation.

The DSR adjusts the observed Sharpe for the number of trials tested,
non-normality of returns, and serial correlation.

```python
from references.dsr_pbo import deflated_sharpe_ratio

dsr = deflated_sharpe_ratio(
    observed_sharpe=backtest_sharpe,
    n_trials=n_parameter_combinations_tested,
    n_obs=len(returns),
    skewness=returns.skew(),
    kurtosis=returns.kurtosis(),
    annual_sr_benchmark=0.0   # H0: SR = 0
)
print(f"DSR: {dsr:.4f}")  # must be > 0
```

**Why DSR matters:** If you tested 100 parameter combinations and the best
had Sharpe 1.2, the expected maximum Sharpe by random chance is ~2.0.
DSR < 0 means your backtest Sharpe is below what random noise would produce
given the number of trials — the signal is not real.

---

## Gate 4 — Net Sharpe after Transaction Costs

See `references/transaction_costs.md` for full modelling.

Gross Sharpe is meaningless. Net Sharpe after realistic costs is the only
metric that matters for live trading viability.

```python
def net_sharpe(gross_returns: pd.Series,
               turnover: pd.Series,
               one_way_cost_bps: float = 5.0) -> float:
    """
    one_way_cost_bps: bid-ask + commission + market impact in basis points.
    For IBKR US equities: ~3-7 bps total one-way for liquid large-caps.
    """
    tc_drag = turnover * one_way_cost_bps / 10000
    net_returns = gross_returns - tc_drag
    return net_returns.mean() / net_returns.std() * np.sqrt(252)
```

Minimum viable: Net Sharpe > 0.8 after costs. Target: > 1.2.

---

## Parameter Stability Testing (Cross-Gate Requirement)

After all four gates pass, verify that performance is stable around the
chosen parameters — not a sharp peak surrounded by poor performance.

```python
def parameter_sensitivity_surface(param_grid: dict,
                                   metric_fn: callable,
                                   data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute metric across full parameter grid.
    A robust strategy shows a plateau, not a spike.
    """
    results = []
    for params in itertools.product(*param_grid.values()):
        param_dict = dict(zip(param_grid.keys(), params))
        metric = metric_fn(data, **param_dict)
        results.append({**param_dict, "metric": metric})
    return pd.DataFrame(results)
```

**Red flag:** if the chosen parameter set is the only configuration that passes
all gates and all neighbouring configurations fail — it is almost certainly a
lucky draw. Require a plateau of at least ±20% variation in each parameter.

---

## MLflow Integration

Log all robustness metrics on every training run:

```python
mlflow.log_metrics({
    "pbo":           pbo,
    "dsr":           dsr,
    "net_sharpe":    net_sharpe_value,
    "gross_sharpe":  gross_sharpe,
    "n_trials":      n_param_combinations,
    "ic_mean_oos":   ic_mean,
    "icir_oos":      icir,
})
mlflow.log_dict(sensitivity_surface.to_dict(), "parameter_sensitivity.json")
```

A model version is only eligible for `challenger` alias if all four gate
metrics are logged and pass thresholds. The promotion check script reads
these metrics from MLflow before allowing alias assignment.
