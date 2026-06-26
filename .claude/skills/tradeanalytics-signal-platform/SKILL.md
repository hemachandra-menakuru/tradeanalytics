---
name: tradeanalytics-signal-platform
description: >
  Design, implement, and troubleshoot the TradeAnalytics Gold layer signal platform:
  SignalQualityEngine (Type 1 filters), SignalLog (survivorship-bias-free logging),
  SignalPublisher (Delta → REST/WebSocket/Telegram), PaperTracker, and the
  portfolio-agnostic signal contract. Use this skill whenever the user asks about signal
  generation, signal quality filters, signal publishing, signal distribution, the signal
  log table, paper trading tracking, signal monetisation architecture, or how signals
  flow from ML models to consumers. Also trigger for questions about preventing
  survivorship bias in signal training data, how to log rejected signals, Type 1 vs
  Type 2 risk, the producer/consumer split, or how friends' execution bots consume
  signals. This is the authoritative Phase 4 guide for everything after the model outputs
  a score and before capital is committed.
---

# TradeAnalytics Signal Platform

## The Two Operating Models (non-negotiable)

**Model 1 — Signal Generator (Producer, HC owns this):**
Ingests market data → builds features → runs ML models → fuses signals → publishes
portfolio-agnostic signals. Has zero knowledge of who consumes or what they do.

**Model 2 — Signal Consumer (per user):**
Each consumer independently applies portfolio logic, position sizing, execution rules,
and risk overlays. Portfolio risk is the consumer's own responsibility.

The Signal Generator is completely decoupled from consumer portfolio state.

## Signal Contract (the portfolio-agnostic output)

```python
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

@dataclass
class TradingSignal:
    signal_id: str              # UUID
    generated_at: datetime      # UTC
    ticker: str
    direction: str              # "LONG" | "SHORT" | "FLAT"
    signal_score: float         # -1.0 to +1.0 (model output, not a trade size)
    confidence: float           # 0.0 to 1.0
    regime: str                 # "bull" | "bear" | "sideways" | "high_vol"
    cluster: str                # "momentum" | "mean_reversion" | "mega_cap"
    model_version: str          # e.g., "tradeanalytics.momentum.xgboost@champion"
    hold_period_days: int       # expected hold
    signal_expiry: datetime     # UTC — signal is stale after this
    type1_passed: bool          # passed SignalQualityEngine filters
    rejection_reason: str | None
    meta: dict                  # optional — SHAP top features, catalyst score, etc.
```

**What the signal does NOT contain:**
- Position size (consumer's job)
- Dollar amount (consumer's job)
- Stop-loss level (consumer's job)
- Portfolio context (consumer's job)

## SignalQualityEngine — Type 1 Filters

Type 1 filters are applied by the Signal Generator BEFORE publishing. They protect
signal quality, not portfolio risk (that's Type 2, the consumer's job).

```python
from src.shared.base.signal_quality_filter import SignalQualityFilter

class LiquidityFilter(SignalQualityFilter):
    """Reject signals on instruments with insufficient liquidity."""
    def evaluate(self, signal: TradingSignal, features: dict) -> tuple[bool, str | None]:
        avg_dollar_volume = features.get("avg_dollar_volume_20d", 0)
        if avg_dollar_volume < self._config.min_dollar_volume:
            return False, f"Insufficient liquidity: ${avg_dollar_volume:,.0f} < ${self._config.min_dollar_volume:,.0f}"
        return True, None

class RegimeSuitabilityFilter(SignalQualityFilter):
    """Reject momentum signals in mean-reversion regime and vice versa."""
    def evaluate(self, signal: TradingSignal, features: dict) -> tuple[bool, str | None]:
        if signal.cluster == "momentum" and signal.regime in ("sideways", "high_vol"):
            return False, f"Momentum signal rejected in {signal.regime} regime"
        return True, None

class VolatilityAdjustedConfidenceFilter(SignalQualityFilter):
    """Require higher confidence when realised vol is elevated."""
    def evaluate(self, signal: TradingSignal, features: dict) -> tuple[bool, str | None]:
        realised_vol = features.get("realised_vol_20d", 0)
        min_confidence = self._config.base_confidence + (realised_vol * self._config.vol_confidence_scale)
        if signal.confidence < min_confidence:
            return False, f"Confidence {signal.confidence:.2f} below vol-adjusted minimum {min_confidence:.2f}"
        return True, None
```

Filters run in order. First failure = signal rejected. All rejections are logged to
`SignalLog` (see below) — this is mandatory.

## SignalLog — Survivorship-Bias-Free Logging

**The rule:** EVERY signal is logged to `gold.signal_log` regardless of outcome.
Including signals rejected by Type 1 filters. Including signals the consumer never acts on.

Why: If you only log signals that resulted in trades, your retraining data has
survivorship bias. The model sees a world where every signal it generated was good enough
to trade — which is false. Rejected signals that would have been profitable are invisible.

```sql
CREATE TABLE tradeanalytics.gold.signal_log (
    signal_id           STRING NOT NULL,
    generated_at        TIMESTAMP NOT NULL,
    ticker              STRING NOT NULL,
    direction           STRING NOT NULL,
    signal_score        DOUBLE,
    confidence          DOUBLE,
    regime              STRING,
    cluster             STRING,
    model_version       STRING,
    hold_period_days    INT,
    signal_expiry       TIMESTAMP,
    type1_passed        BOOLEAN NOT NULL,
    rejection_reason    STRING,          -- NULL if passed
    was_published       BOOLEAN,         -- did it reach consumers?
    actual_return       DOUBLE,          -- backfilled after hold_period_days
    actual_direction    STRING,          -- backfilled: "UP" | "DOWN" | "FLAT"
    outcome_recorded_at TIMESTAMP,       -- when actual_return was backfilled
    meta                STRING           -- JSON blob: SHAP, catalyst score, etc.
) USING DELTA
PARTITIONED BY (DATE(generated_at), cluster);
```

**Backfill job:** A daily job reads `signal_log` where `actual_return IS NULL` and
`signal_expiry < now()`, fetches closing prices from `bronze.market_data_daily`, and
writes `actual_return` and `actual_direction`. This is what makes the log useful for
unbiased retraining.

## SignalPublisher — Transport Layer

The publisher is behind a `SignalPublisher` ABC so transport can be swapped.

```python
# Current transport options (Phase 4b)
SignalPublisherFactory.register("delta_cdc",  DeltaCDCPublisher)   # polling consumers
SignalPublisherFactory.register("rest",        RESTPublisher)       # pull API
SignalPublisherFactory.register("telegram",    TelegramPublisher)   # HC's personal bot
SignalPublisherFactory.register("websocket",   WebSocketPublisher)  # push, Phase 5+
```

Config selects transport:
```yaml
# config/signals/publishing.yml
publisher: telegram          # Phase 4b start
```

**Start with Telegram** for Phase 4b (HC + friends). Migrate to REST when consumers
need programmatic access for their own execution bots.

## PaperTracker

Tracks paper trading performance of published signals. Required before live execution.

```sql
CREATE TABLE tradeanalytics.gold.paper_track (
    signal_id           STRING NOT NULL,
    paper_entry_price   DOUBLE,
    paper_entry_date    DATE,
    paper_exit_price    DOUBLE,
    paper_exit_date     DATE,
    paper_return_pct    DOUBLE,
    paper_hold_days     INT,
    exit_reason         STRING    -- "signal_expiry" | "stop_hit" | "target_hit"
) USING DELTA;
```

**Promotion gate to live (locked):**
- ≥ 6 months of paper signals with `actual_return` backfilled
- Annualised Sharpe > 1.0 on paper trades
- Max drawdown < 20% on paper portfolio
- Win rate > 50% directional accuracy

## Phase 4b Signal Flow (Telegram → Friend's Bot)

```
SignalQualityEngine
    ↓ (passes Type 1)
gold.signal_log (logged regardless)
    ↓
SignalPublisher → Telegram channel
    ↓
Friend reads signal → applies their own position sizing → executes via their bot
    ↓ (after hold_period_days)
Backfill job → writes actual_return to signal_log
```

## What Belongs Here vs in the Consumer

| Concern | Owner |
|---|---|
| Signal score calculation | Signal Generator (this platform) |
| Type 1 quality filters | Signal Generator (this platform) |
| Signal logging | Signal Generator (this platform) |
| Signal publishing | Signal Generator (this platform) |
| Position sizing | Consumer |
| Stop-loss / take-profit levels | Consumer |
| Portfolio exposure limits | Consumer |
| Order execution | Consumer |
| Portfolio-level drawdown limits | Consumer |

## Build Order (Phase 4)

1. `SignalQualityFilter` ABC + first 3 implementations
2. `SignalQualityEngine` — runs filters in sequence, returns pass/fail + reason
3. `gold.signal_log` Delta table + DDL notebook
4. `SignalLog` writer (always writes, regardless of type1_passed)
5. `SignalPublisher` ABC + `TelegramPublisher` implementation
6. Backfill job for `actual_return`
7. `PaperTracker` — reads signal_log, computes paper P&L
8. Monitoring: daily signal count, rejection rate by filter, IC of published signals
