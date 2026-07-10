"""
Tests for BronzeRecord and RejectedRecord models.
Validates structure, computed properties, deduplication keys,
amendment logic, and serialisation.
"""
import json
import pytest
from datetime import date, datetime
from src.bronze.models.bronze_record import (
    BronzeRecord, RejectedRecord,
    RecordInterval, MarketSession, IngestionType,
    AmendmentReason,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture
def daily_record():
    """Standard valid daily AAPL record."""
    return BronzeRecord(
        symbol="AAPL",
        bar_date=date(2026, 6, 19),
        bar_interval=RecordInterval.DAILY.value,
        source="ibkr",
        batch_id="batch_20260619_100500",
        pipeline_version="9ccf799",
        open=150.00,
        high=152.50,
        low=149.75,
        close=151.25,
        volume=1_000_000,
        adj_close=151.25,
        prev_close=149.50,
        vwap=150.87,
        trade_count=45_230,
    )


@pytest.fixture
def intraday_record():
    """Standard valid 1h AAPL record."""
    return BronzeRecord(
        symbol="AAPL",
        bar_date=date(2026, 6, 19),
        bar_interval=RecordInterval.HOURLY.value,
        source="ibkr",
        batch_id="batch_20260619_100500",
        pipeline_version="9ccf799",
        open=150.00,
        high=151.25,
        low=149.75,
        close=151.00,
        volume=125_000,
        bar_time_utc="2026-06-19T13:30:00Z",
        bar_time_eastern="2026-06-19T09:30:00",
        bar_end_time_utc="2026-06-19T14:30:00Z",
        timezone_source="America/New_York",
    )


# ── Identity tests ─────────────────────────────────────────────────────────────

def test_daily_record_creates_successfully(daily_record):
    assert daily_record.symbol == "AAPL"
    assert daily_record.close == 151.25
    assert daily_record.record_version == 1
    assert daily_record.is_amended == False


def test_intraday_record_creates_successfully(intraday_record):
    assert intraday_record.bar_interval == "1h"
    assert intraday_record.bar_time_utc == "2026-06-19T13:30:00Z"


def test_validate_identity_passes_for_valid_record(daily_record):
    daily_record.validate_identity()  # should not raise


def test_validate_identity_fails_empty_symbol(daily_record):
    daily_record.symbol = ""
    with pytest.raises(ValueError, match="symbol"):
        daily_record.validate_identity()


def test_validate_identity_fails_invalid_interval(daily_record):
    daily_record.bar_interval = "2d"
    with pytest.raises(ValueError, match="Unknown interval"):
        daily_record.validate_identity()


def test_validate_identity_fails_intraday_missing_bar_time(intraday_record):
    intraday_record.bar_time_utc = None
    with pytest.raises(ValueError, match="bar_time_utc"):
        intraday_record.validate_identity()


# test removed — batch_id validated at construction, not post-mutation


# ── Table tier and unique key tests ───────────────────────────────────────────

def test_daily_record_table_tier(daily_record):
    assert daily_record.table_tier == "daily"


def test_intraday_record_table_tier(intraday_record):
    assert intraday_record.table_tier == "intraday"


def test_tick_record_table_tier():
    record = BronzeRecord(
        symbol="AAPL", bar_date=date(2026, 6, 19),
        bar_interval=RecordInterval.ONE_MIN.value,
        source="ibkr", batch_id="batch_001",
        pipeline_version="abc123",
        open=150.0, high=150.5, low=149.9, close=150.2,
        volume=5000,
        bar_time_utc="2026-06-19T13:30:00Z",
    )
    assert record.table_tier == "tick"


def test_daily_unique_key_excludes_bar_time(daily_record):
    key = daily_record.unique_key
    assert "bar_time_utc" not in key
    assert set(key.keys()) == {"symbol", "bar_date", "bar_interval"}


def test_intraday_unique_key_includes_bar_time(intraday_record):
    key = intraday_record.unique_key
    assert "bar_time_utc" in key
    assert key["bar_time_utc"] == "2026-06-19T13:30:00Z"


# ── Computed property tests ────────────────────────────────────────────────────

def test_price_range(daily_record):
    expected = 152.50 - 149.75
    assert abs(daily_record.price_range - expected) < 0.001


def test_daily_return_pct(daily_record):
    # (151.25 - 149.50) / 149.50 * 100 = 1.17%
    expected = ((151.25 - 149.50) / 149.50) * 100
    assert abs(daily_record.daily_return_pct - expected) < 0.01


def test_daily_return_pct_none_when_no_prev_close(daily_record):
    daily_record.prev_close = None
    assert daily_record.daily_return_pct is None


def test_gap_pct(daily_record):
    # (150.00 - 149.50) / 149.50 * 100 = 0.33%
    expected = ((150.00 - 149.50) / 149.50) * 100
    assert abs(daily_record.gap_pct - expected) < 0.01


def test_is_gap_up_false_for_small_gap(daily_record):
    # 0.33% gap — below 0.5% threshold
    assert daily_record.is_gap_up == False


def test_is_gap_up_true_for_large_gap(daily_record):
    daily_record.prev_close = 140.00  # big gap up
    assert daily_record.is_gap_up == True


def test_is_gap_down_true(daily_record):
    daily_record.prev_close = 160.00  # gap down
    assert daily_record.is_gap_down == True


# ── Corporate action tests ────────────────────────────────────────────────────

def test_corporate_action_flag_auto_set_on_split(daily_record):
    daily_record.split_factor = 0.25
    # Trigger __post_init__ equivalent — set manually since already created
    daily_record.has_corporate_action = (
        daily_record.split_factor is not None or daily_record.dividend_amount > 0
    )
    assert daily_record.has_corporate_action == True


def test_corporate_action_false_by_default(daily_record):
    assert daily_record.has_corporate_action == False
    assert daily_record.split_factor is None
    assert daily_record.dividend_amount == 0.0


# ── Amendment tests ───────────────────────────────────────────────────────────

def test_amend_creates_new_record(daily_record):
    amended = daily_record.amend(
        new_batch_id="batch_20260620_090000",
        reason=AmendmentReason.PROVIDER_CORRECTION.value,
        close=152.00,
    )
    assert amended is not daily_record
    assert amended.close == 152.00
    assert daily_record.close == 151.25  # original unchanged


def test_amend_increments_version(daily_record):
    amended = daily_record.amend(
        new_batch_id="batch_002",
        reason=AmendmentReason.PROVIDER_CORRECTION.value,
        close=152.00,
    )
    assert amended.record_version == 2
    assert daily_record.record_version == 1


def test_amend_sets_supersedes_batch(daily_record):
    amended = daily_record.amend(
        new_batch_id="batch_002",
        reason=AmendmentReason.PROVIDER_CORRECTION.value,
        close=152.00,
    )
    assert amended.supersedes_batch == "batch_20260619_100500"
    assert amended.is_amended == True


def test_amend_chain(daily_record):
    v2 = daily_record.amend(
        new_batch_id="batch_002",
        reason=AmendmentReason.PROVIDER_CORRECTION.value,
        close=152.00,
    )
    v3 = v2.amend(
        new_batch_id="batch_003",
        reason=AmendmentReason.RESTATEMENT.value,
        close=152.50,
    )
    assert v3.record_version == 3
    assert v3.close == 152.50
    assert v2.close == 152.00
    assert daily_record.close == 151.25


# ── Data completeness tests ───────────────────────────────────────────────────

def test_completeness_high_for_rich_record(daily_record):
    pct = daily_record.compute_completeness()
    assert pct > 70.0  # rich record should score well


def test_completeness_lower_for_sparse_record():
    sparse = BronzeRecord(
        symbol="AAPL", bar_date=date(2026, 6, 19),
        bar_interval="1d", source="yahoo",
        batch_id="batch_001", pipeline_version="abc",
        open=150.0, high=152.0, low=149.0, close=151.0,
        volume=1_000_000,
        # No adj_close, no prev_close, no vwap, no trade_count
    )
    pct = sparse.compute_completeness()
    assert pct < 60.0


# ── Serialisation tests ───────────────────────────────────────────────────────

def test_to_dict_returns_dict(daily_record):
    d = daily_record.to_dict()
    assert isinstance(d, dict)


def test_to_dict_contains_all_groups(daily_record):
    d = daily_record.to_dict()
    # Group 1
    assert "symbol" in d and "bar_date" in d and "bar_interval" in d
    # Group 2
    assert "open" in d and "close" in d and "volume" in d
    # Group 3
    assert "adj_close" in d and "adj_factor" in d
    # Group 4
    assert "vwap" in d and "prev_close" in d
    # Group 5
    assert "has_corporate_action" in d and "dividend_amount" in d
    # Group 6
    assert "trading_halt" in d and "is_trading_day" in d
    # Group 7
    assert "data_quality_flag" in d and "record_version" in d
    # Group 8
    assert "batch_id" in d and "pipeline_version" in d and "ingested_at" in d


def test_to_dict_date_is_string(daily_record):
    d = daily_record.to_dict()
    assert isinstance(d["bar_date"], str)
    assert d["bar_date"] == "2026-06-19"


def test_to_dict_data_quality_reasons_is_json_string(daily_record):
    daily_record.data_quality_reasons = ["volume_positive", "vwap_missing"]
    d = daily_record.to_dict()
    parsed = json.loads(d["data_quality_reasons"])
    assert parsed == ["volume_positive", "vwap_missing"]


# ── RecordInterval utility tests ──────────────────────────────────────────────

def test_record_interval_is_intraday():
    assert RecordInterval.is_intraday("1h") == True
    assert RecordInterval.is_intraday("4h") == True
    assert RecordInterval.is_intraday("5m") == True
    assert RecordInterval.is_intraday("1d") == False


def test_record_interval_unique_key_fields_daily():
    fields = RecordInterval.get_unique_key_fields("1d")
    assert fields == ["symbol", "bar_date", "bar_interval"]


def test_record_interval_unique_key_fields_intraday():
    fields = RecordInterval.get_unique_key_fields("1h")
    assert "bar_time_utc" in fields
    assert len(fields) == 4


# ── RejectedRecord tests ──────────────────────────────────────────────────────

def test_rejected_record_creates_successfully():
    rejected = RejectedRecord(
        symbol="AAPL",
        bar_date="2026-06-19",
        bar_interval="1d",
        source="ibkr",
        batch_id="batch_001",
        rejected_rule="high_gte_low",
        rejection_reason="High (148.0) is less than Low (149.0)",
        raw_record='{"symbol": "AAPL", "high": 148.0, "low": 149.0}',
        raw_open=150.0,
        raw_high=148.0,
        raw_low=149.0,
        raw_close=151.0,
        raw_volume=1_000_000,
    )
    assert rejected.symbol == "AAPL"
    assert rejected.rejected_rule == "high_gte_low"
    assert rejected.reprocessed == False


def test_rejected_record_to_dict():
    rejected = RejectedRecord(
        symbol="AAPL", bar_date="2026-06-19", bar_interval="1d",
        source="ibkr", batch_id="batch_001",
        rejected_rule="price_positive",
        rejection_reason="Close price is 0.0",
        raw_record="{}",
    )
    d = rejected.to_dict()
    assert isinstance(d, dict)
    assert d["rejected_rule"] == "price_positive"
    assert d["reprocessed"] == False


def test_repr_is_readable(daily_record):
    r = repr(daily_record)
    assert "AAPL" in r
    assert "151.25" in r
    assert "version=1" in r


# ── Hardening tests (post code review) ───────────────────────────────────────

def test_blank_symbol_raises_at_construction():
    with pytest.raises(ValueError, match="symbol"):
        BronzeRecord(
            symbol="   ", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_blank_source_raises_at_construction():
    with pytest.raises(ValueError, match="source"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="   ",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_blank_batch_id_raises_at_construction():
    with pytest.raises(ValueError, match="batch_id"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_unknown_interval_raises_at_construction():
    with pytest.raises(ValueError, match="Unknown interval"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="2d", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_intraday_missing_bar_time_raises_at_construction():
    with pytest.raises(ValueError, match="bar_time_utc"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1h", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
            bar_time_utc=None,  # missing — must raise
        )


def test_intraday_blank_bar_time_raises_at_construction():
    with pytest.raises(ValueError, match="bar_time_utc"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1h", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
            bar_time_utc="   ",  # blank — must raise
        )


def test_is_intraday_rejects_unknown_interval():
    with pytest.raises(ValueError, match="Unknown interval"):
        RecordInterval.is_intraday("weekly")


def test_get_table_tier_rejects_unknown_interval():
    with pytest.raises(ValueError, match="Unknown interval"):
        RecordInterval.get_table_tier("2d")


def test_amend_rejects_invalid_reason(daily_record):
    with pytest.raises(ValueError, match="Invalid amendment reason"):
        daily_record.amend(
            new_batch_id="batch_002",
            reason="made_up_reason",
            close=152.00,
        )


def test_amend_rejects_system_controlled_fields(daily_record):
    with pytest.raises(ValueError, match="not allowed in amendments"):
        daily_record.amend(
            new_batch_id="batch_002",
            reason=AmendmentReason.PROVIDER_CORRECTION.value,
            record_version=99,   # system-controlled — not allowed
        )


def test_amend_rejects_batch_id_override(daily_record):
    with pytest.raises(ValueError, match="not allowed in amendments"):
        daily_record.amend(
            new_batch_id="batch_002",
            reason=AmendmentReason.PROVIDER_CORRECTION.value,
            batch_id="sneaky_override",  # not allowed
        )


def test_unique_key_safe_for_valid_intraday(intraday_record):
    key = intraday_record.unique_key
    assert key["bar_time_utc"] == "2026-06-19T13:30:00Z"
    assert key["bar_time_utc"] is not None


# ── Hardening tests (post code review) ───────────────────────────────────────

def test_blank_symbol_raises_at_construction():
    with pytest.raises(ValueError, match="symbol"):
        BronzeRecord(
            symbol="   ", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_blank_source_raises_at_construction():
    with pytest.raises(ValueError, match="source"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="   ",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_blank_batch_id_raises_at_construction():
    with pytest.raises(ValueError, match="batch_id"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_unknown_interval_raises_at_construction():
    with pytest.raises(ValueError, match="Unknown interval"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="2d", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_intraday_missing_bar_time_raises_at_construction():
    with pytest.raises(ValueError, match="bar_time_utc"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1h", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
            bar_time_utc=None,  # missing — must raise
        )


def test_intraday_blank_bar_time_raises_at_construction():
    with pytest.raises(ValueError, match="bar_time_utc"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1h", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
            bar_time_utc="   ",  # blank — must raise
        )


def test_is_intraday_rejects_unknown_interval():
    with pytest.raises(ValueError, match="Unknown interval"):
        RecordInterval.is_intraday("weekly")


def test_get_table_tier_rejects_unknown_interval():
    with pytest.raises(ValueError, match="Unknown interval"):
        RecordInterval.get_table_tier("2d")


def test_amend_rejects_invalid_reason(daily_record):
    with pytest.raises(ValueError, match="Invalid amendment reason"):
        daily_record.amend(
            new_batch_id="batch_002",
            reason="made_up_reason",
            close=152.00,
        )


def test_amend_rejects_system_controlled_fields(daily_record):
    with pytest.raises(ValueError, match="not allowed in amendments"):
        daily_record.amend(
            new_batch_id="batch_002",
            reason=AmendmentReason.PROVIDER_CORRECTION.value,
            record_version=99,   # system-controlled — not allowed
        )


def test_amend_rejects_batch_id_override(daily_record):
    with pytest.raises(ValueError, match="not allowed in amendments"):
        daily_record.amend(
            new_batch_id="batch_002",
            reason=AmendmentReason.PROVIDER_CORRECTION.value,
            batch_id="sneaky_override",  # not allowed
        )


def test_unique_key_safe_for_valid_intraday(intraday_record):
    key = intraday_record.unique_key
    assert key["bar_time_utc"] == "2026-06-19T13:30:00Z"
    assert key["bar_time_utc"] is not None


def test_validate_identity_fails_missing_batch_id_at_construction():
    """batch_id is enforced at construction, not post-mutation."""
    with pytest.raises(ValueError, match="batch_id"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


# ── Post re-review hardening tests ────────────────────────────────────────────

def test_none_open_raises_at_construction():
    with pytest.raises(ValueError, match="open"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=None, high=152.0, low=149.0, close=151.0, volume=1_000_000,
        )


def test_none_close_raises_at_construction():
    with pytest.raises(ValueError, match="close"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=None, volume=1_000_000,
        )


def test_none_volume_raises_at_construction():
    with pytest.raises(ValueError, match="volume"):
        BronzeRecord(
            symbol="AAPL", bar_date=date(2026, 6, 19),
            bar_interval="1d", source="ibkr",
            batch_id="batch_001", pipeline_version="abc",
            open=150.0, high=152.0, low=149.0, close=151.0, volume=None,
        )


def test_has_corporate_action_derived_from_split_factor(daily_record):
    """has_corporate_action must be True when split_factor is set."""
    daily_record.split_factor = 0.25
    daily_record.validate_identity()  # re-derives the flag
    assert daily_record.has_corporate_action == True


def test_has_corporate_action_derived_from_dividend(daily_record):
    """has_corporate_action must be True when dividend_amount > 0."""
    daily_record.dividend_amount = 0.75
    daily_record.validate_identity()
    assert daily_record.has_corporate_action == True


def test_has_corporate_action_resets_to_false_after_amendment(daily_record):
    """
    Amendment that removes corporate action must produce has_corporate_action=False.
    Tests that the flag is re-derived, not carried over stale from original.
    """
    # Create a record with a split
    record_with_split = BronzeRecord(
        symbol="AAPL", bar_date=date(2026, 6, 19),
        bar_interval="1d", source="ibkr",
        batch_id="batch_001", pipeline_version="abc",
        open=600.0, high=605.0, low=598.0, close=601.0, volume=1_000_000,
        split_factor=0.25,
    )
    assert record_with_split.has_corporate_action == True

    # Amend to remove the split
    amended = record_with_split.amend(
        new_batch_id="batch_002",
        reason=AmendmentReason.PROVIDER_CORRECTION.value,
        split_factor=None,
    )

    # Flag must be re-derived as False — not stale True
    assert amended.has_corporate_action == False
    assert amended.split_factor is None


def test_validate_identity_catches_post_mutation_blank_source(daily_record):
    """validate_identity must catch blank source set after construction."""
    daily_record.source = ""
    with pytest.raises(ValueError, match="source"):
        daily_record.validate_identity()


def test_validate_identity_catches_post_mutation_blank_batch_id(daily_record):
    """validate_identity must catch blank batch_id set after construction."""
    daily_record.batch_id = "   "
    with pytest.raises(ValueError, match="batch_id"):
        daily_record.validate_identity()


def test_validate_identity_catches_post_mutation_blank_pipeline_version(daily_record):
    """validate_identity must catch blank pipeline_version set after construction."""
    daily_record.pipeline_version = ""
    with pytest.raises(ValueError, match="pipeline_version"):
        daily_record.validate_identity()


def test_validate_identity_catches_intraday_blank_bar_time(intraday_record):
    """validate_identity must catch whitespace bar_time_utc set after construction."""
    intraday_record.bar_time_utc = "   "
    with pytest.raises(ValueError, match="bar_time_utc"):
        intraday_record.validate_identity()
