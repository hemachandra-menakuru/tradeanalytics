---
name: tradeanalytics-risk-engine
description: >
  Institutional risk management framework for the TradeAnalytics signal engine and
  execution layer. Use this skill whenever the user asks about Value at Risk (VaR),
  Expected Shortfall (CVaR), risk budgeting, drawdown controls, stop-loss frameworks,
  trailing exits, time-based exits, stress testing, scenario analysis, tail risk,
  portfolio exposure limits, concentration limits, correlation limits, volatility scaling,
  position sizing rules, or how to protect capital from a worst-case drawdown. Also
  trigger for questions about kill switches, circuit breakers, maximum drawdown monitoring,
  or any "how do I limit my downside" question. This is the Phase 4/5 risk layer —
  sits between portfolio construction and execution, and runs continuously during live
  trading. No capital is risked without this layer being active.
---

# TradeAnalytics Risk Engine

## Architecture

The risk engine has two operational modes:

**Pre-trade (static):** checks run before any order is submitted
**Intra-session (dynamic):** monitors live positions and triggers circuit breakers

```
SignalQualityEngine → PortfolioConstructor → [RISK ENGINE] → ExecutionEngine
                                                   ↑
                                         Continuous monitoring
                                         (drawdown, vol, correlation)
```

## Reference Files

| File | Contents |
|---|---|
| `references/var_cvar.md` | VaR, CVaR, parametric + historical + Monte Carlo methods |
| `references/drawdown_controls.md` | Max drawdown monitoring, circuit breakers, kill switch |
| `references/stress_testing.md` | Historical scenarios, Monte Carlo, tail risk, CVaR decomposition |
| `references/position_rules.md` | Stop-loss, trailing stops, time exits, risk budgeting |

---

## Risk Budget Framework

Before any position is sized, assign a risk budget to each cluster.
All position sizing must stay within budget.

```python
RISK_BUDGET = {
    "momentum":       0.40,   # 40% of portfolio risk budget
    "mean_reversion": 0.30,   # 30%
    "mega_cap":       0.20,   # 20%
    "cash_buffer":    0.10,   # 10% kept in reserve
}

# Total annualised portfolio volatility target
TARGET_PORTFOLIO_VOL = 0.10   # 10% annualised

# Maximum drawdown before circuit breaker fires
MAX_DRAWDOWN_TRIGGER = 0.15   # 15% portfolio drawdown
```

---

## Pre-Trade Checks (mandatory, in order)

```python
def pre_trade_risk_check(
    proposed_weights: pd.Series,
    current_weights: pd.Series,
    cov_matrix: pd.DataFrame,
    portfolio_value: float,
    cluster_map: pd.Series
) -> dict:
    """
    Returns {passed: bool, violations: list[str], adjusted_weights: pd.Series}.
    All violations must be cleared before order submission.
    """
    violations = []

    # Check 1: Single position limit
    max_pos = proposed_weights.abs().max()
    if max_pos > 0.10:
        violations.append(f"Single position exceeds 10%: {max_pos:.1%}")

    # Check 2: Cluster concentration
    for cluster, budget in RISK_BUDGET.items():
        tickers = cluster_map[cluster_map == cluster].index
        cluster_weight = proposed_weights.reindex(tickers).abs().sum()
        cluster_vol_contribution = _cluster_vol_contribution(
            proposed_weights, tickers, cov_matrix
        )
        if cluster_vol_contribution > budget * TARGET_PORTFOLIO_VOL:
            violations.append(
                f"Cluster {cluster} exceeds risk budget: "
                f"{cluster_vol_contribution:.1%} > {budget * TARGET_PORTFOLIO_VOL:.1%}"
            )

    # Check 3: Gross leverage
    gross_leverage = proposed_weights.abs().sum()
    if gross_leverage > 1.5:
        violations.append(f"Gross leverage {gross_leverage:.2f}x exceeds 1.5x limit")

    # Check 4: Pairwise correlation of new positions
    new_positions = proposed_weights[
        (proposed_weights.abs() > 0.01) &
        (current_weights.abs() < 0.01)
    ].index
    if len(new_positions) > 1:
        new_corr = _pairwise_corr_from_cov(cov_matrix, new_positions)
        if new_corr.max() > 0.70:
            violations.append(
                f"New positions are highly correlated: max ρ = {new_corr.max():.2f}"
            )

    # Check 5: Portfolio vol estimate
    port_var = proposed_weights.values @ cov_matrix.values @ proposed_weights.values
    port_vol = np.sqrt(port_var)
    if port_vol > TARGET_PORTFOLIO_VOL * 1.25:
        violations.append(
            f"Portfolio vol {port_vol:.1%} exceeds target {TARGET_PORTFOLIO_VOL:.1%} × 1.25"
        )

    return {
        "passed":    len(violations) == 0,
        "violations": violations,
        "port_vol":  port_vol,
        "gross_lev": gross_leverage
    }


def _cluster_vol_contribution(weights, cluster_tickers, cov):
    w = weights.reindex(cluster_tickers).fillna(0).values
    sub_cov = cov.loc[cluster_tickers, cluster_tickers].values
    return np.sqrt(w @ sub_cov @ w)


def _pairwise_corr_from_cov(cov, tickers):
    sub_cov = cov.loc[tickers, tickers]
    d = np.sqrt(np.diag(sub_cov.values))
    return sub_cov / np.outer(d, d)
```

---

## Portfolio VaR and CVaR

See `references/var_cvar.md` for parametric + historical + Monte Carlo methods.

Quick reference — historical VaR/CVaR for daily monitoring:

```python
def historical_var_cvar(returns: pd.Series,
                         portfolio_value: float,
                         confidence: float = 0.95) -> dict:
    """
    Daily historical VaR and CVaR.
    Returns dollar amounts and percentages.
    """
    sorted_returns = returns.sort_values()
    var_pct  = sorted_returns.quantile(1 - confidence)
    cvar_pct = sorted_returns[sorted_returns <= var_pct].mean()

    return {
        "var_pct":     round(var_pct, 6),
        "cvar_pct":    round(cvar_pct, 6),
        "var_dollar":  round(var_pct * portfolio_value, 2),
        "cvar_dollar": round(cvar_pct * portfolio_value, 2),
        "confidence":  confidence
    }
```

---

## Circuit Breaker (Kill Switch)

Mandatory for Phase 5 (live trading). Monitors portfolio P&L in real time
and halts all new orders when drawdown exceeds threshold.

```python
class CircuitBreaker:
    """
    Stateful — single instance per trading session.
    Written to control.ingestion_watermark equivalent in Phase 5.
    """
    def __init__(self, max_drawdown: float = 0.15,
                 daily_loss_limit: float = 0.03):
        self.max_drawdown    = max_drawdown
        self.daily_loss_limit = daily_loss_limit
        self._peak_value     = None
        self._session_start  = None
        self._triggered      = False

    def update(self, current_value: float) -> dict:
        if self._peak_value is None:
            self._peak_value = current_value
        if self._session_start is None:
            self._session_start = current_value

        self._peak_value = max(self._peak_value, current_value)

        drawdown = (self._peak_value - current_value) / self._peak_value
        daily_loss = (self._session_start - current_value) / self._session_start

        triggered = drawdown > self.max_drawdown or daily_loss > self.daily_loss_limit

        if triggered and not self._triggered:
            self._triggered = True
            # Write suspension flag to control table (Phase 5)
            # INSERT INTO control.kill_switch VALUES ('triggered', now(), reason)

        return {
            "triggered":   triggered,
            "drawdown":    drawdown,
            "daily_loss":  daily_loss,
            "peak_value":  self._peak_value,
        }

    def reset(self):
        """Call at session start each day."""
        self._session_start = None
        self._triggered = False
        # Do NOT reset peak_value — drawdown is measured from all-time peak
```

---

## Stop-Loss and Exit Rules

See `references/position_rules.md` for full implementations.

TradeAnalytics uses three exit types, checked in order:

```
1. Signal expiry (preferred) — hold until signal_expiry timestamp passes
2. ATR-based stop-loss      — exit if price moves N × ATR against position
3. Time-based stop          — force exit after max_hold_days regardless of signal
```

```python
def check_exits(position: dict,
                current_price: float,
                current_atr: float,
                current_time: pd.Timestamp) -> dict:
    """
    Returns: {should_exit: bool, reason: str}
    """
    # 1. Signal expiry
    if current_time >= position["signal_expiry"]:
        return {"should_exit": True, "reason": "signal_expiry"}

    # 2. ATR stop-loss (default: 2× ATR from entry)
    stop_distance = 2.0 * current_atr
    if position["direction"] == "LONG":
        stop_price = position["entry_price"] - stop_distance
        if current_price <= stop_price:
            return {"should_exit": True, "reason": "atr_stop_loss"}
    else:
        stop_price = position["entry_price"] + stop_distance
        if current_price >= stop_price:
            return {"should_exit": True, "reason": "atr_stop_loss"}

    # 3. Time-based stop
    days_held = (current_time - position["entry_time"]).days
    if days_held >= position.get("max_hold_days", 21):
        return {"should_exit": True, "reason": "time_stop"}

    return {"should_exit": False, "reason": None}
```

---

## Stress Testing Summary

See `references/stress_testing.md` for full implementations.

Historical scenarios every strategy must survive (with pre-2010 data back-fill):

| Scenario | Period | Market drawdown | What it tests |
|---|---|---|---|
| GFC | Sep 2008 – Mar 2009 | -57% | Momentum crash, correlation spike |
| Flash Crash | May 6, 2010 | -9% intraday | Liquidity evaporation |
| Euro crisis | Apr – Oct 2011 | -21% | Correlated cross-asset crash |
| China devaluation | Aug 2015 | -11% | Overnight gap risk |
| Vol shock | Feb 5, 2018 | -10% | VIX ETF unwind, vol-of-vol |
| COVID crash | Feb – Mar 2020 | -34% | Fastest -30% in history |
| Rate shock | Jan – Oct 2022 | -25% | Bond-equity correlation breakdown |

Each scenario must produce a simulated portfolio drawdown < 2× `MAX_DRAWDOWN_TRIGGER`.
If any scenario breaches 30% simulated drawdown, the strategy requires redesign.
