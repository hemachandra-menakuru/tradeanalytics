"""
Tests for DeltaUniverseReader (local mode — no Spark required).

Scenarios:
  1. get_active_instruments returns all instruments in local mode
  2. get_active_instruments filtered by symbols
  3. get_active_instruments filtered by asset_class
  4. get_active_instruments with symbol filter that matches nothing
  5. get_instrument returns correct instrument by symbol
  6. get_instrument returns None for unknown symbol
  7. get_symbols returns symbol strings only
  8. get_symbols respects active_only=True (all local instruments are active)
  9. Empty local store returns empty list
 10. spark mode raises ValueError when spark=None
 11. DeltaUniverseReader implements UniverseReader ABC
 12. instrument_id is populated (not None)
 13. Symbol lookup is case-insensitive
"""

import pytest
from datetime import date

from src.reference.readers.delta_universe_reader import DeltaUniverseReader
from src.shared.base.universe_reader import UniverseReader, InstrumentInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _inst(symbol: str, asset_class: str = "equity", instrument_id: int = 1) -> InstrumentInfo:
    return InstrumentInfo(
        symbol        = symbol,
        name          = f"{symbol} Inc.",
        asset_class   = asset_class,
        active        = True,
        history_start = date(2020, 1, 2),
        instrument_id = instrument_id,
        exchange_mic  = "XNAS",
        currency      = "USD",
    )


@pytest.fixture
def reader():
    return DeltaUniverseReader(mode="local", instruments=[
        _inst("SPY",  asset_class="etf",    instrument_id=505),
        _inst("QQQ",  asset_class="etf",    instrument_id=506),
        _inst("AAPL", asset_class="equity", instrument_id=507),
        _inst("MSFT", asset_class="equity", instrument_id=508),
    ])


# ---------------------------------------------------------------------------
# get_active_instruments
# ---------------------------------------------------------------------------

def test_get_active_instruments_returns_all(reader):
    result = reader.get_active_instruments()
    assert len(result) == 4


def test_get_active_instruments_filtered_by_symbols(reader):
    result = reader.get_active_instruments(symbols=["SPY", "AAPL"])
    symbols = {r.symbol for r in result}
    assert symbols == {"SPY", "AAPL"}


def test_get_active_instruments_filtered_by_asset_class(reader):
    result = reader.get_active_instruments(asset_classes=["etf"])
    assert len(result) == 2
    assert all(r.asset_class == "etf" for r in result)


def test_get_active_instruments_symbol_filter_no_match(reader):
    result = reader.get_active_instruments(symbols=["NVDA"])
    assert result == []


def test_get_active_instruments_empty_store():
    reader = DeltaUniverseReader(mode="local", instruments=[])
    assert reader.get_active_instruments() == []


# ---------------------------------------------------------------------------
# get_instrument
# ---------------------------------------------------------------------------

def test_get_instrument_returns_correct_record(reader):
    inst = reader.get_instrument("AAPL")
    assert inst is not None
    assert inst.symbol == "AAPL"
    assert inst.instrument_id == 507
    assert inst.asset_class == "equity"


def test_get_instrument_returns_none_for_unknown(reader):
    assert reader.get_instrument("NVDA") is None


def test_get_instrument_case_insensitive(reader):
    assert reader.get_instrument("spy") is not None
    assert reader.get_instrument("SPY") is not None


# ---------------------------------------------------------------------------
# get_symbols
# ---------------------------------------------------------------------------

def test_get_symbols_returns_strings(reader):
    symbols = reader.get_symbols()
    assert isinstance(symbols, list)
    assert "SPY" in symbols
    assert "AAPL" in symbols


def test_get_symbols_count_matches_instruments(reader):
    assert len(reader.get_symbols()) == 4


# ---------------------------------------------------------------------------
# instrument_id populated
# ---------------------------------------------------------------------------

def test_instrument_id_is_populated(reader):
    instruments = reader.get_active_instruments()
    assert all(i.instrument_id is not None for i in instruments)


def test_instrument_id_correct_per_symbol(reader):
    spy = reader.get_instrument("SPY")
    assert spy.instrument_id == 505


# ---------------------------------------------------------------------------
# Spark mode guard
# ---------------------------------------------------------------------------

def test_spark_mode_requires_spark_session():
    with pytest.raises(ValueError, match="SparkSession required"):
        DeltaUniverseReader(mode="spark", spark=None)


# ---------------------------------------------------------------------------
# ABC compliance
# ---------------------------------------------------------------------------

def test_implements_universe_reader_abc():
    reader = DeltaUniverseReader(mode="local")
    assert isinstance(reader, UniverseReader)
