# VaR, CVaR & Tail Risk — Full Implementation

## Three VaR Methods (use all three — they should agree)

```python
import numpy as np
import pandas as pd
from scipy.stats import norm, t as student_t

# ── 1. Historical Simulation ─────────────────────────────────────────────────

def historical_var(returns: pd.Series, confidence: float = 0.99,
                   horizon_days: int = 1) -> float:
    """
    Non-parametric. Most reliable for fat-tailed financial returns.
    Requires ≥ 250 observations for 99% VaR (need 2.5 obs in the tail).
    """
    daily_var = returns.quantile(1 - confidence)
    return daily_var * np.sqrt(horizon_days)   # scale by √t (assumes i.i.d.)

def historical_cvar(returns: pd.Series, confidence: float = 0.99) -> float:
    """Expected Shortfall (CVaR) = mean of returns beyond VaR threshold."""
    var = historical_var(returns, confidence)
    tail = returns[returns <= var]
    return tail.mean() if len(tail) > 0 else var


# ── 2. Parametric (Gaussian) ─────────────────────────────────────────────────

def parametric_var(returns: pd.Series, confidence: float = 0.99,
                   horizon_days: int = 1) -> float:
    """
    Gaussian assumption. Fast but underestimates tail risk.
    Use only as a sanity check alongside historical VaR.
    """
    mu    = returns.mean()
    sigma = returns.std()
    z     = norm.ppf(1 - confidence)
    return (mu + z * sigma) * np.sqrt(horizon_days)

def parametric_cvar(returns: pd.Series, confidence: float = 0.99) -> float:
    """Analytical CVaR under Gaussian assumption."""
    sigma = returns.std()
    mu    = returns.mean()
    z     = norm.ppf(1 - confidence)
    return mu - sigma * norm.pdf(z) / (1 - confidence)


# ── 3. Student-t (fat tails) ─────────────────────────────────────────────────

def student_t_var(returns: pd.Series, confidence: float = 0.99,
                  horizon_days: int = 1) -> float:
    """
    Fits Student-t distribution — better than Gaussian for equity returns.
    Typical equity return kurtosis implies df ≈ 4–6.
    """
    from scipy.stats import t as student_t
    df, loc, scale = student_t.fit(returns, floc=returns.mean())
    q = student_t.ppf(1 - confidence, df, loc, scale)
    return q * np.sqrt(horizon_days)

def student_t_cvar(returns: pd.Series, confidence: float = 0.99) -> float:
    """CVaR under Student-t."""
    from scipy.stats import t as student_t
    df, loc, scale = student_t.fit(returns, floc=returns.mean())
    var_q = student_t.ppf(1 - confidence, df, loc, scale)
    # Numerical integration of the left tail
    from scipy.integrate import quad
    numerator, _ = quad(
        lambda x: x * student_t.pdf(x, df, loc, scale),
        -np.inf, var_q
    )
    return numerator / (1 - confidence)


# ── Full VaR Dashboard ────────────────────────────────────────────────────────

def var_dashboard(returns: pd.Series,
                  portfolio_value: float,
                  confidence: float = 0.99) -> pd.DataFrame:
    """
    Run all three methods. They should roughly agree.
    If Historical VaR >> Parametric VaR, returns have fat tails.
    If Student-t >> Historical, the tail fit is picking up extreme events.
    """
    methods = {
        "historical":   historical_var(returns, confidence),
        "parametric":   parametric_var(returns, confidence),
        "student_t":    student_t_var(returns, confidence),
    }
    cvar_methods = {
        "historical":   historical_cvar(returns, confidence),
        "parametric":   parametric_cvar(returns, confidence),
        "student_t":    student_t_cvar(returns, confidence),
    }

    rows = []
    for method in methods:
        var_pct  = methods[method]
        cvar_pct = cvar_methods[method]
        rows.append({
            "method":        method,
            "var_pct":       f"{var_pct:.2%}",
            "cvar_pct":      f"{cvar_pct:.2%}",
            "var_dollar":    f"${var_pct * portfolio_value:,.0f}",
            "cvar_dollar":   f"${cvar_pct * portfolio_value:,.0f}",
        })

    return pd.DataFrame(rows).set_index("method")
```

## Monte Carlo VaR

For non-linear payoffs (e.g. options) or when you want scenario diversity:

```python
def monte_carlo_var(returns: pd.Series,
                    portfolio_value: float,
                    n_simulations: int = 10_000,
                    horizon_days: int = 10,
                    confidence: float = 0.99) -> dict:
    """
    Simulate future portfolio P&L using bootstrapped returns.
    Accounts for fat tails better than parametric when T is large.
    """
    mu    = returns.mean()
    sigma = returns.std()

    # Bootstrap from empirical distribution (preserves non-normality)
    simulated_paths = np.random.choice(
        returns.values,
        size=(n_simulations, horizon_days),
        replace=True
    )
    cumulative_returns = (1 + simulated_paths).prod(axis=1) - 1
    pnl_distribution  = cumulative_returns * portfolio_value

    var_dollar  = np.percentile(pnl_distribution, (1 - confidence) * 100)
    cvar_dollar = pnl_distribution[pnl_distribution <= var_dollar].mean()

    return {
        "horizon_days":  horizon_days,
        "confidence":    confidence,
        "var_dollar":    round(var_dollar, 2),
        "cvar_dollar":   round(cvar_dollar, 2),
        "var_pct":       round(var_dollar / portfolio_value, 6),
        "cvar_pct":      round(cvar_dollar / portfolio_value, 6),
        "worst_1pct":    round(np.percentile(pnl_distribution, 1), 2),
    }
```

## CVaR Decomposition by Position

Which positions drive tail risk?

```python
def cvar_decomposition(weights: pd.Series,
                        returns_matrix: pd.DataFrame,
                        confidence: float = 0.99) -> pd.DataFrame:
    """
    Marginal CVaR contribution per position.
    Returns: % of total CVaR each position contributes.
    """
    portfolio_returns = (returns_matrix * weights).sum(axis=1)
    portfolio_var = portfolio_returns.quantile(1 - confidence)
    tail_mask = portfolio_returns <= portfolio_var

    # Marginal CVaR: average asset return in the tail × weight
    tail_returns = returns_matrix[tail_mask]
    marginal_cvar = (tail_returns.mean() * weights)
    total_cvar = marginal_cvar.sum()

    return pd.DataFrame({
        "marginal_cvar":  marginal_cvar,
        "pct_of_total":   (marginal_cvar / total_cvar * 100).round(2),
        "weight":         weights,
    }).sort_values("pct_of_total", ascending=False)
```

## Daily Risk Monitoring Output

Log these to `control.job_run_log` (or a new `control.risk_monitor` table) daily:

```python
daily_risk_snapshot = {
    "date":          str(today),
    "portfolio_value": portfolio_value,
    "var_95_1d":     historical_var(returns_90d, 0.95),
    "var_99_1d":     historical_var(returns_90d, 0.99),
    "cvar_99_1d":    historical_cvar(returns_90d, 0.99),
    "current_drawdown": current_drawdown,
    "portfolio_vol":  returns_90d.std() * np.sqrt(252),
    "gross_leverage": gross_leverage,
}
mlflow.log_metrics(daily_risk_snapshot)
```
