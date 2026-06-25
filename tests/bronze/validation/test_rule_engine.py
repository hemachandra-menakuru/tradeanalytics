"""
Tests for RuleEngine — verifies each rule fires correctly.
"""
import pytest
from pathlib import Path
from src.bronze.validation.rule_engine import RuleEngine
from src.bronze.validation.models import ValidationOutcome, RuleSeverity

RULES_PATH = Path(__file__).resolve().parents[3] / "src" / "reference" / "seed" / "data_quality_rules.yml"


@pytest.fixture
def engine():
    # Load shared rules + daily additional rules (includes no_weekend_dates)
    return RuleEngine(
        RULES_PATH,
        stream_name="daily",
        additional_rules=[
            {"rule": "no_weekend_dates", "severity": "critical", "action": "reject_record"},
            {"rule": "vwap_between_low_and_high", "severity": "critical", "action": "reject_record"},
        ],
    )


@pytest.fixture
def valid_record():
    """A clean record that passes all rules."""
    return {
        "symbol": "AAPL", "date": "2026-06-19", "interval": "1d",
        "source": "ibkr",
        "open": 150.0, "high": 152.5, "low": 149.75, "close": 151.25,
        "volume": 1_000_000,
        "prev_close": 149.50, "vwap": 150.87,
        "trade_count": 45_230,
        "pre_market_volume": 125_000,
        "fetch_duration_ms": 245,
        "is_amended": False,
    }


# ── Engine loading tests ───────────────────────────────────────────────────────

def test_engine_loads_rules(engine):
    assert engine.rule_count > 0


def test_engine_has_expected_rules(engine):
    assert "high_gte_low" in engine.rule_names
    assert "no_null_ohlcv" in engine.rule_names
    assert "price_positive" in engine.rule_names


def test_engine_fails_on_missing_rules_file():
    with pytest.raises(FileNotFoundError):
        RuleEngine(Path("/nonexistent/rules.yml"))


# ── Clean record passes all rules ─────────────────────────────────────────────

def test_valid_record_passes_all_rules(engine, valid_record):
    results = engine.apply(valid_record)
    assert results == [], f"Expected no failures, got: {results}"


# ── Price relationship rules ───────────────────────────────────────────────────

def test_high_gte_low_fires_when_high_less_than_low(engine, valid_record):
    valid_record["high"] = 148.0  # high < low
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "high_gte_low" in rule_names


def test_high_gte_low_outcome_is_rejected(engine, valid_record):
    valid_record["high"] = 148.0
    results = engine.apply(valid_record)
    result = next(r for r in results if r.rule_name == "high_gte_low")
    assert result.outcome == ValidationOutcome.REJECTED.value


def test_high_gte_open_fires_when_high_less_than_open(engine, valid_record):
    valid_record["high"] = 149.0  # high < open (150.0)
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "high_gte_open" in rule_names


def test_high_gte_close_fires_when_high_less_than_close(engine, valid_record):
    valid_record["high"] = 150.0  # high < close (151.25)
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "high_gte_close" in rule_names


def test_low_lte_open_fires_when_low_greater_than_open(engine, valid_record):
    valid_record["low"] = 151.0  # low > open (150.0)
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "low_lte_open" in rule_names


def test_low_lte_close_fires_when_low_greater_than_close(engine, valid_record):
    valid_record["low"] = 152.0  # low > close (151.25)
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "low_lte_close" in rule_names


# ── Volume rules ───────────────────────────────────────────────────────────────

def test_volume_positive_fires_for_zero_volume(engine, valid_record):
    valid_record["volume"] = 0
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "volume_positive" in rule_names


def test_volume_positive_outcome_is_flagged(engine, valid_record):
    valid_record["volume"] = 0
    results = engine.apply(valid_record)
    result = next(r for r in results if r.rule_name == "volume_positive")
    assert result.outcome == ValidationOutcome.FLAGGED.value


def test_volume_positive_does_not_fire_for_null_volume(engine, valid_record):
    valid_record["volume"] = None
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    # null_ohlcv should fire, but not volume_positive
    assert "no_null_ohlcv" in rule_names
    assert "volume_positive" not in rule_names


# ── Price positive rules ───────────────────────────────────────────────────────

def test_price_positive_fires_for_zero_close(engine, valid_record):
    valid_record["close"] = 0.0
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "price_positive" in rule_names


def test_price_positive_fires_for_negative_price(engine, valid_record):
    valid_record["open"] = -1.0
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "price_positive" in rule_names


# ── Weekend date rule ──────────────────────────────────────────────────────────

def test_no_weekend_dates_fires_for_saturday(engine, valid_record):
    valid_record["date"] = "2026-06-20"  # Saturday
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "no_weekend_dates" in rule_names


def test_no_weekend_dates_fires_for_sunday(engine, valid_record):
    valid_record["date"] = "2026-06-21"  # Sunday
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "no_weekend_dates" in rule_names


def test_no_weekend_dates_passes_for_friday(engine, valid_record):
    valid_record["date"] = "2026-06-19"  # Friday
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "no_weekend_dates" not in rule_names


# ── Null OHLCV rule ────────────────────────────────────────────────────────────

def test_no_null_ohlcv_fires_for_null_open(engine, valid_record):
    valid_record["open"] = None
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "no_null_ohlcv" in rule_names


def test_no_null_ohlcv_outcome_is_rejected(engine, valid_record):
    valid_record["close"] = None
    results = engine.apply(valid_record)
    result = next(r for r in results if r.rule_name == "no_null_ohlcv")
    assert result.outcome == ValidationOutcome.REJECTED.value


# ── Price change threshold rule ────────────────────────────────────────────────

def test_price_change_fires_for_large_move(engine, valid_record):
    valid_record["prev_close"] = 100.0
    valid_record["close"] = 160.0  # 60% move
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "price_change_pct_threshold" in rule_names


def test_price_change_passes_for_normal_move(engine, valid_record):
    valid_record["prev_close"] = 149.50
    valid_record["close"] = 151.25  # 1.17% move
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "price_change_pct_threshold" not in rule_names


# ── VWAP rule ─────────────────────────────────────────────────────────────────

def test_vwap_fires_when_outside_range(engine, valid_record):
    valid_record["vwap"] = 200.0  # way above high
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "vwap_between_low_and_high" in rule_names


def test_vwap_passes_when_within_range(engine, valid_record):
    valid_record["vwap"] = 151.0  # between 149.75 and 152.5
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "vwap_between_low_and_high" not in rule_names


def test_vwap_passes_when_null(engine, valid_record):
    valid_record["vwap"] = None  # optional field
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "vwap_between_low_and_high" not in rule_names


# ── Gap threshold rule ────────────────────────────────────────────────────────

def test_gap_threshold_fires_for_large_gap(engine, valid_record):
    valid_record["prev_close"] = 100.0
    valid_record["open"] = 120.0  # 20% gap
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "gap_threshold" in rule_names


def test_gap_threshold_passes_for_normal_gap(engine, valid_record):
    valid_record["prev_close"] = 149.50
    valid_record["open"] = 150.00  # 0.33% gap
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "gap_threshold" not in rule_names


# ── Pre-market volume rule ────────────────────────────────────────────────────

def test_pre_market_volume_fires_for_negative(engine, valid_record):
    valid_record["pre_market_volume"] = -100
    results = engine.apply(valid_record)
    rule_names = [r.rule_name for r in results]
    assert "pre_market_volume_non_negative" in rule_names


# ── Multiple rules can fire ───────────────────────────────────────────────────

def test_multiple_rules_can_fire_simultaneously(engine, valid_record):
    valid_record["high"] = 148.0   # triggers high_gte_low + high_gte_open + high_gte_close
    valid_record["volume"] = 0     # triggers volume_positive
    results = engine.apply(valid_record)
    assert len(results) >= 2


# ── Rule result fields ────────────────────────────────────────────────────────

def test_rule_result_has_message(engine, valid_record):
    valid_record["high"] = 148.0
    results = engine.apply(valid_record)
    result = next(r for r in results if r.rule_name == "high_gte_low")
    assert result.message is not None
    assert "High" in result.message
    assert "Low" in result.message
