---
name: tradeanalytics-abc-enforcer
description: >
  Enforce the TradeAnalytics universal component pattern: ABC → Registry → Factory → Config.
  Use this skill whenever the user is adding a new component, new provider, new model,
  new strategy, new broker, new indicator, new signal publisher, or any new class to the
  TradeAnalytics codebase. Also trigger when someone asks "how do I add a new X", "where
  does Y go", "how do I wire this in", "do I need to register this", or shows code that
  introduces a concrete class without a corresponding ABC. This is the non-negotiable
  architectural law of the project — trigger even if the user doesn't explicitly ask
  about pattern compliance. Zero exceptions.
---

# TradeAnalytics ABC → Registry → Factory → Config Pattern

## The Law

Every new component in TradeAnalytics follows exactly this four-step pattern.
Adding a new thing = these four steps, nothing more, nothing less.

```
ABC (contract)
  ↓
Registry (catalogue — maps string key → class)
  ↓
Factory (creator — reads Registry, calls constructor)
  ↓
Config (YAML — selects which implementation to use)
```

**Why:** No if/else chains for component selection anywhere in the codebase. Config YAML
is the only selector. New implementations slot in without touching existing code.

## Step-by-Step: Adding a New Component

### Step 1 — Define the ABC

Location: `src/shared/base/` (for cross-cutting ABCs) or `src/<layer>/base/` (layer-specific)

```python
# src/shared/base/universe_reader.py
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class InstrumentInfo:
    instrument_id: int
    symbol: str
    exchange: str

class UniverseReader(ABC):
    @abstractmethod
    def get_instruments(self, universe: str) -> list[InstrumentInfo]:
        """Return all active instruments for a given universe."""
        ...

    @abstractmethod
    def is_active(self, instrument_id: int) -> bool:
        """Check if an instrument is currently active."""
        ...
```

Rules for ABCs:
- Pure interface — no business logic, no state
- One ABC per concern (ISP: don't bundle historical + realtime + options into one ABC)
- `@abstractmethod` on every method a concrete class must implement
- Type-annotate all inputs and outputs
- Use `@dataclass` for value objects returned by the ABC

### Step 2 — Implement the concrete class

Location: `src/<layer>/<category>/`

```python
# src/reference/managers/ticker_feed_config_manager.py
from src.shared.base.universe_reader import UniverseReader, InstrumentInfo

class TickerFeedConfigManager(UniverseReader):
    def __init__(self, spark, config):
        self._spark = spark
        self._catalog = config.databricks.catalog

    def get_instruments(self, universe: str) -> list[InstrumentInfo]:
        rows = self._spark.sql(f"""
            SELECT instrument_id, symbol, exchange
            FROM {self._catalog}.reference.ticker_feed_config
            WHERE universe = '{universe}' AND is_active = true
        """).collect()
        return [InstrumentInfo(**row.asDict()) for row in rows]

    def is_active(self, instrument_id: int) -> bool:
        ...
```

### Step 3 — Register in the Factory

Location: `src/<layer>/factory/` — one factory per layer

```python
# src/reference/factory/universe_reader_factory.py
from src.shared.base.universe_reader import UniverseReader

class UniverseReaderFactory:
    _registry: dict[str, type[UniverseReader]] = {}

    @classmethod
    def register(cls, key: str, impl: type[UniverseReader]) -> None:
        cls._registry[key] = impl

    @classmethod
    def create(cls, key: str, **kwargs) -> UniverseReader:
        if key not in cls._registry:
            raise ValueError(f"UniverseReader '{key}' not registered. Known: {list(cls._registry)}")
        return cls._registry[key](**kwargs)

# Registration (in the notebook or job entry point — not in the factory itself)
UniverseReaderFactory.register("delta", TickerFeedConfigManager)
UniverseReaderFactory.register("csv",   TickerReader)              # legacy fallback
```

### Step 4 — Add a YAML config entry

```yaml
# config/dev.yml (or the relevant stream/model config)
reference:
  universe_reader: delta    # "delta" → TickerFeedConfigManager, "csv" → TickerReader
```

### Step 5 — Consume via Factory (never via direct import of concrete class)

```python
# In BronzeIngestionJob or any consumer:
from src.reference.factory.universe_reader_factory import UniverseReaderFactory

reader = UniverseReaderFactory.create(
    config.reference.universe_reader,
    spark=spark,
    config=config
)
instruments = reader.get_instruments(universe="us_equity")
```

## Existing ABCs and Their Locations

| ABC | Location | Implemented By |
|---|---|---|
| `MarketDataProvider` | `src/bronze/base/market_data_provider.py` | `IBKRProvider`, `YahooProvider` |
| `UniverseReader` | `src/shared/base/universe_reader.py` | `TickerReader` (CSV), `TickerFeedConfigManager` (Delta — Phase 2.5) |
| `TradingCalendar` | `src/shared/base/trading_calendar.py` | `USEquityCalendar` |
| `WatermarkStore` | `src/shared/base/watermark_store.py` | `WatermarkManager` (Bronze), future `ControlWatermarkManager` (Phase 2.5) |

**Phase 3 ABCs to build:**
- `IndicatorEngine` → `src/silver/base/`
- `FeatureEngineer` → `src/silver/base/`
- `FeatureScaler` → `src/silver/base/`
- `RegimeDetector` → `src/silver/base/`

**Phase 4 ABCs to build:**
- `MLModel` → `src/gold/base/`
- `LabelingEngine` → `src/gold/base/`
- `SignalFusion` → `src/gold/base/`
- `SignalPublisher` → `src/api/base/`

## Common Violations to Reject

```python
# ❌ WRONG — direct instantiation of concrete class in consumer
from src.reference.managers.ticker_feed_config_manager import TickerFeedConfigManager
reader = TickerFeedConfigManager(spark=spark, config=config)

# ✅ CORRECT — factory + config
reader = UniverseReaderFactory.create(config.reference.universe_reader, spark=spark, config=config)
```

```python
# ❌ WRONG — if/else selector in code
if config.source == "ibkr":
    provider = IBKRProvider(config)
elif config.source == "yahoo":
    provider = YahooProvider(config)

# ✅ CORRECT — registry lookup
provider = MarketDataFactory.create(config.sources.primary, config=config)
```

```python
# ❌ WRONG — concrete class with no ABC parent
class MyNewIndicator:
    def calculate(self, df): ...

# ✅ CORRECT — ABC first, then concrete
class IndicatorEngine(ABC):
    @abstractmethod
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame: ...

class EMAIndicator(IndicatorEngine):
    def calculate(self, df: pd.DataFrame) -> pd.DataFrame: ...
```

## Checklist for Any New Component

- [ ] ABC defined in `src/shared/base/` or `src/<layer>/base/`
- [ ] Concrete class implements the ABC (IDE will show missing `@abstractmethod` error if not)
- [ ] Factory has `register()` and `create()` classmethods
- [ ] Registration happens in the notebook/job entry point
- [ ] YAML config has a key that selects the implementation
- [ ] No direct import of the concrete class in any consumer — always through the factory
- [ ] Unit test mocks the ABC, not the concrete class
