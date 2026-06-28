# Transaction Cost Modelling & Capacity Analysis

## Why TC Modelling Is Non-Negotiable

A strategy with gross Sharpe 1.5 and 50% daily turnover at 10 bps one-way cost
has net Sharpe of approximately 0.3. Most retail backtests omit costs entirely.
All TradeAnalytics validation uses NET metrics only.

## One-Way Cost Components (IBKR US Equities)

| Component | Estimate | Notes |
|---|---|---|
| Commission | 0.5 bps | IBKR fixed: $0.005/share, min $1/order |
| Bid-ask spread (half) | 1–3 bps | Large cap: ~1 bps, mid-cap: 2–3 bps |
| Market impact | 1–5 bps | Depends on order size vs ADV |
| Total one-way | 3–8 bps | Use 5 bps as conservative base |

```python
IBKR_ONE_WAY_COST_BPS = {
    "mega_cap":   3.0,   # S&P 100 constituents, >$10B ADV
    "large_cap":  5.0,   # S&P 500, $1B–$10B ADV
    "mid_cap":    8.0,   # S&P 400, $100M–$1B ADV
    "small_cap": 15.0,   # Russell 2000 components
}
```

## Full TC Model

```python
def transaction_cost_model(
    signal: pd.DataFrame,           # signal scores (rows=dates, cols=tickers)
    prices: pd.DataFrame,           # closing prices
    volume: pd.DataFrame,           # daily volume
    target_capital: float = 100_000,
    one_way_cost_bps: float = 5.0,
    max_participation_rate: float = 0.10,
    spread_model: str = "fixed"     # "fixed" | "amihud"
) -> pd.DataFrame:
    """
    Compute daily transaction costs and net returns.
    Returns DataFrame with gross_return, tc_drag, net_return per day.
    """
    # Convert signals to weights (cross-sectionally normalised)
    raw_weights = signal.div(signal.abs().sum(axis=1), axis=0).fillna(0)

    # Turnover: absolute change in weights per day
    turnover = raw_weights.diff().abs().sum(axis=1) / 2

    # Capacity constraint: clip weights by max participation
    dollar_volume = prices * volume
    adv = dollar_volume.rolling(21).mean()
    max_weight = (adv * max_participation_rate) / target_capital
    constrained_weights = raw_weights.clip(-max_weight, max_weight)

    # Gross return from price changes
    daily_returns = prices.pct_change()
    gross_return = (constrained_weights.shift(1) * daily_returns).sum(axis=1)

    # TC drag
    if spread_model == "amihud":
        # Dynamic spread estimate based on Amihud illiquidity
        illiq = daily_returns.abs() / dollar_volume.replace(0, np.nan)
        spread_bps = (illiq.rolling(21).mean() * 1e6).clip(1, 30)
        tc_per_trade = spread_bps / 10000
        tc_drag = (constrained_weights.diff().abs() * tc_per_trade).sum(axis=1) / 2
    else:
        tc_drag = turnover * one_way_cost_bps / 10000

    net_return = gross_return - tc_drag

    return pd.DataFrame({
        "gross_return": gross_return,
        "tc_drag":      tc_drag,
        "net_return":   net_return,
        "turnover":     turnover,
    })
```

## Slippage Estimation (Almgren-Chriss)

For larger orders, price impact is not constant. Use Almgren-Chriss:

```python
def almgren_chriss_impact(order_size_shares: float,
                           adv_shares: float,
                           daily_vol: float,
                           eta: float = 0.142,      # temporary impact coefficient
                           gamma: float = 0.314,    # permanent impact coefficient
                           execution_days: int = 1) -> float:
    """
    Market impact cost as fraction of trade value.

    Almgren & Chriss (2001). eta and gamma are estimated from market data.
    Default values from Almgren et al. (2005) for US equities.

    Returns: expected cost as fraction of trade value (e.g. 0.005 = 50 bps).
    """
    participation = order_size_shares / (adv_shares * execution_days)

    # Temporary impact (execution-day cost)
    temp_impact = eta * daily_vol * np.sqrt(participation)

    # Permanent impact (price move that persists after execution)
    perm_impact = 0.5 * gamma * daily_vol * participation

    return temp_impact + perm_impact
```

## Capacity Analysis

The maximum AUM a strategy can run before market impact destroys alpha.

```python
def strategy_capacity(
    signal: pd.DataFrame,
    prices: pd.DataFrame,
    volume: pd.DataFrame,
    target_sharpe: float = 0.8,
    impact_erosion_per_10pct_participation: float = 0.15
) -> dict:
    """
    Estimate strategy capacity: max AUM where net Sharpe stays above target.

    Methodology: binary search over AUM levels, computing net Sharpe
    at each level. Capacity = max AUM where net Sharpe >= target_sharpe.
    """
    dollar_volume = (prices * volume).mean().mean()
    universe_adv = dollar_volume  # average daily dollar volume

    def net_sr_at_aum(aum: float) -> float:
        participation = aum / (universe_adv * 252)
        # Linear degradation: each 10% participation → erosion% Sharpe reduction
        sharpe_erosion = participation * 10 * impact_erosion_per_10pct_participation
        base_sharpe = 1.0  # placeholder — replace with actual backtest Sharpe
        return base_sharpe - sharpe_erosion

    # Binary search for capacity
    lo, hi = 1_000, 1_000_000_000  # $1K to $1B
    for _ in range(40):
        mid = (lo + hi) / 2
        if net_sr_at_aum(mid) >= target_sharpe:
            lo = mid
        else:
            hi = mid

    return {
        "capacity_usd":     round(lo),
        "capacity_comment": f"${lo/1e6:.1f}M AUM maintains net Sharpe ≥ {target_sharpe}",
        "daily_adv_usd":    round(universe_adv),
        "max_participation": round(lo / (universe_adv * 252) * 100, 2)
    }
```

## Realistic Net Sharpe Benchmarks

| Strategy type | Gross Sharpe | Realistic net Sharpe | Why |
|---|---|---|---|
| High turnover (daily rebalance, mean reversion) | 2.0+ | 0.5–0.8 | TC drag kills most of the edge |
| Medium turnover (weekly, momentum) | 1.2–1.8 | 0.8–1.2 | Reasonable net after costs |
| Low turnover (monthly, factor) | 0.8–1.2 | 0.6–1.0 | Low TC, but lower gross alpha |
| **TradeAnalytics target** | **>1.5** | **>0.8** | **Minimum to justify live deployment** |

## Turnover Budget

Different rebalance frequencies have different TC profiles:

```python
ANNUAL_TURNOVER_BY_FREQUENCY = {
    "daily":    500,    # 200% one-way / year
    "weekly":   100,    # 100% one-way / year
    "monthly":  24,     # 24% one-way / year
}

def annual_tc_drag_bps(turnover_pct: float, one_way_bps: float) -> float:
    """Annual TC drag in basis points."""
    return turnover_pct * one_way_bps  # e.g. 100% turnover * 5 bps = 500 bps/yr
```

At 5 bps one-way, daily rebalancing costs ~1000 bps/year (~10% of capital).
This means a daily-rebalance strategy needs gross alpha > 10% just to break even.
Weekly rebalancing costs ~500 bps. Monthly costs ~120 bps.

**Implication for TradeAnalytics:** mean-reversion cluster (daily rebalance) requires
exceptionally high gross IC to survive costs. Momentum cluster (monthly) is far
more TC-efficient. Enforce TC gate strictly before promoting any strategy.
