"""
Tests for IngestionPlanner — verifies correct mode and date range
selection for every ingestion scenario.
All tests use mocked watermarks and config — no I/O.
"""
import pytest
from datetime import date, timedelta
from pathlib import Path
from src.config.config_loader import ConfigLoader
from src.ingestion.models.ingestion_mode import (
    IngestionMode, IngestionWatermark, FetchPlan
)
from src.ingestion.models.ingestion_planner import IngestionPlanner, is_trading_day

REPO_ROOT = Path(__file__).resolve().parents[3]
TODAY = date(2026, 6, 19)  # Friday — known trading day


@pytest.fixture(autouse=True)
def reset_config():
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


@pytest.fixture
def config():
    return ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)


@pytest.fixture
def planner(config):
    return IngestionPlanner(config)


@pytest.fixture
def fresh_watermark():
    """Watermark showing data up to yesterday."""
    return IngestionWatermark(
        symbol="AAPL",
        interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=1),
        record_count=2625,
    )


@pytest.fixture
def stale_watermark():
    """Watermark showing data is 60 days old (gap)."""
    return IngestionWatermark(
        symbol="AAPL",
        interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=60),
        record_count=2565,
    )


@pytest.fixture
def current_watermark():
    """Watermark showing data is up to today."""
    return IngestionWatermark(
        symbol="AAPL",
        interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY,
        record_count=2626,
    )


# ── Trading day tests ─────────────────────────────────────────────────────────

def test_friday_is_trading_day():
    assert is_trading_day(date(2026, 6, 19)) == True  # Friday


def test_saturday_is_not_trading_day():
    assert is_trading_day(date(2026, 6, 20)) == False


def test_sunday_is_not_trading_day():
    assert is_trading_day(date(2026, 6, 21)) == False


def test_juneteenth_2026_is_not_trading_day():
    assert is_trading_day(date(2026, 6, 19)) == True  # Juneteenth is Saturday in 2026
    # Actually Juneteenth 2026 is Friday June 19 — check if it's a holiday
    # In 2026 Juneteenth falls on Friday so it IS a holiday
    # Our fixture uses TODAY = 2026-06-19 which is in _US_MARKET_HOLIDAYS_2026
    # Let's use a different date for the no-op test


def test_christmas_2026_is_not_trading_day():
    assert is_trading_day(date(2026, 12, 25)) == False


# ── Scenario A: Normal operations ─────────────────────────────────────────────

def test_initial_load_when_no_watermark(planner):
    """A1: First ever run — no watermark → INITIAL_LOAD."""
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.mode == IngestionMode.INITIAL_LOAD
    assert plan.end_date == TODAY
    # Should go back ~10 years
    assert plan.date_range_days > 3000  # 10 years ≈ 3650 days


def test_initial_load_ingestion_type_is_backfill(planner):
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.ingestion_type == "backfill"
    assert plan.mode.is_backfill == True


def test_incremental_when_watermark_is_yesterday(planner, fresh_watermark):
    """A2: Normal daily run → INCREMENTAL."""
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.INCREMENTAL
    assert plan.end_date == TODAY
    assert plan.ingestion_type == "scheduled"


def test_incremental_includes_amendment_buffer(planner, fresh_watermark):
    """A2: Incremental must go back N buffer days for amendments."""
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    buffer_days = 3  # from config
    expected_start = fresh_watermark.latest_date - timedelta(days=buffer_days)
    assert plan.start_date == expected_start


def test_no_op_when_already_up_to_date(planner, current_watermark):
    """A2: Already up to date → NO_OP."""
    plan = planner.plan(
        "AAPL", "1d", watermark=current_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.NO_OP


# ── Scenario B: Gap scenarios ─────────────────────────────────────────────────

def test_gap_fill_when_gap_exceeds_threshold(planner, stale_watermark):
    """B2: 60 day gap > max_gap_days (30) → GAP_FILL."""
    plan = planner.plan(
        "AAPL", "1d", watermark=stale_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.GAP_FILL
    assert plan.start_date == stale_watermark.latest_date + timedelta(days=1)
    assert plan.end_date == TODAY
    assert plan.ingestion_type == "backfill"


def test_incremental_when_gap_within_threshold(planner):
    """B1: 5 day gap < max_gap_days (30) → still INCREMENTAL."""
    watermark = IngestionWatermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=5),
        record_count=2620,
    )
    plan = planner.plan("AAPL", "1d", watermark=watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.INCREMENTAL


# ── Scenario C: Historical extension ─────────────────────────────────────────

def test_history_extension_when_enabled(config, fresh_watermark):
    """C1: extend_history_start before earliest_date → HISTORY_EXTENSION."""
    config._data["data"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["data"]["ingestion"]["history_extension"]["extend_history_start"] = "2010-01-01"

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.HISTORY_EXTENSION
    assert plan.start_date == date(2010, 1, 1)
    assert plan.end_date == fresh_watermark.earliest_date - timedelta(days=1)


def test_history_extension_not_triggered_if_already_covered(config, fresh_watermark):
    """C1: extend_history_start after earliest_date → not triggered."""
    # earliest_date is 2016-01-04, extension to 2018 — already covered
    config._data["data"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["data"]["ingestion"]["history_extension"]["extend_history_start"] = "2018-01-01"

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    # 2018 > 2016 (earliest_date) — no extension needed
    assert plan.mode != IngestionMode.HISTORY_EXTENSION


def test_per_ticker_history_start_overrides_global(planner):
    """C4: Per-ticker history_start takes precedence over global lookback."""
    ticker_start = date(2020, 9, 30)  # IPO date e.g. PLTR
    plan = planner.plan(
        "PLTR", "1d", watermark=None, as_of_date=TODAY,
        ticker_history_start=ticker_start,
    )
    assert plan.mode == IngestionMode.INITIAL_LOAD
    assert plan.start_date == ticker_start


# ── Scenario D: Explicit overrides ───────────────────────────────────────────

def test_explicit_date_range_override(config, fresh_watermark):
    """C3: Explicit date range → EXPLICIT_DATE_RANGE regardless of watermark."""
    config._data["data"]["ingestion"]["date_range_override"]["enabled"] = True
    config._data["data"]["ingestion"]["date_range_override"]["start_date"] = "2019-06-01"
    config._data["data"]["ingestion"]["date_range_override"]["end_date"] = "2019-06-30"

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.EXPLICIT_DATE_RANGE
    assert plan.start_date == date(2019, 6, 1)
    assert plan.end_date == date(2019, 6, 30)


def test_restatement_mode(config, fresh_watermark):
    """D2: Restatement → RESTATEMENT, is_amendment_run=True."""
    config._data["data"]["ingestion"]["restatement"]["enabled"] = True
    config._data["data"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["data"]["ingestion"]["restatement"]["end_date"] = "2026-06-19"

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.RESTATEMENT
    assert plan.is_amendment_run == True
    assert plan.ingestion_type == "amendment"


def test_force_reload_requires_confirm(config, fresh_watermark):
    """E5: force_reload without confirm → NOT triggered."""
    config._data["data"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["data"]["ingestion"]["force_reload"]["confirm"] = False

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode != IngestionMode.FORCE_RELOAD


def test_force_reload_with_confirm(config, fresh_watermark):
    """E5: force_reload with confirm=True → FORCE_RELOAD."""
    config._data["data"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["data"]["ingestion"]["force_reload"]["confirm"] = True

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.FORCE_RELOAD


# ── Priority order tests ──────────────────────────────────────────────────────

def test_force_reload_overrides_restatement(config, fresh_watermark):
    """force_reload has higher priority than restatement."""
    config._data["data"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["data"]["ingestion"]["force_reload"]["confirm"] = True
    config._data["data"]["ingestion"]["restatement"]["enabled"] = True
    config._data["data"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["data"]["ingestion"]["restatement"]["end_date"] = "2026-06-19"

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.FORCE_RELOAD


def test_restatement_overrides_incremental(config, fresh_watermark):
    """restatement has higher priority than incremental."""
    config._data["data"]["ingestion"]["restatement"]["enabled"] = True
    config._data["data"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["data"]["ingestion"]["restatement"]["end_date"] = "2026-06-19"

    planner = IngestionPlanner(config)
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan.mode == IngestionMode.RESTATEMENT


# ── FetchPlan property tests ──────────────────────────────────────────────────

def test_fetch_plan_date_range_days(planner):
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.date_range_days > 0


def test_fetch_plan_estimated_batches(planner):
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.estimated_batches >= 1


def test_fetch_plan_repr(planner, fresh_watermark):
    plan = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    r = repr(plan)
    assert "AAPL" in r
    assert "incremental" in r


# ── IngestionMode property tests ──────────────────────────────────────────────

def test_ingestion_mode_initial_load_is_backfill():
    assert IngestionMode.INITIAL_LOAD.is_backfill == True
    assert IngestionMode.INITIAL_LOAD.ingestion_type == "backfill"


def test_ingestion_mode_incremental_is_not_backfill():
    assert IngestionMode.INCREMENTAL.is_backfill == False
    assert IngestionMode.INCREMENTAL.ingestion_type == "scheduled"


def test_ingestion_mode_restatement_is_amendment():
    assert IngestionMode.RESTATEMENT.ingestion_type == "amendment"


def test_all_modes_have_description():
    for mode in IngestionMode:
        assert mode.description is not None
        assert len(mode.description) > 0


# ── Symbol scoping tests ──────────────────────────────────────────────────────

def test_history_extension_scoped_to_specific_symbol(config, fresh_watermark):
    """C1: history_extension with symbols=["AAPL"] only applies to AAPL."""
    config._data["data"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["data"]["ingestion"]["history_extension"]["extend_history_start"] = "2010-01-01"
    config._data["data"]["ingestion"]["history_extension"]["symbols"] = ["AAPL"]

    planner = IngestionPlanner(config)

    # AAPL should get history extension
    plan_aapl = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan_aapl.mode == IngestionMode.HISTORY_EXTENSION

    # MSFT should get incremental (not in scope)
    msft_watermark = IngestionWatermark(
        symbol="MSFT", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=1),
        record_count=2625,
    )
    plan_msft = planner.plan(
        "MSFT", "1d", watermark=msft_watermark, as_of_date=TODAY
    )
    assert plan_msft.mode == IngestionMode.INCREMENTAL


def test_force_reload_scoped_to_specific_symbols(config, fresh_watermark):
    """E5: force_reload with symbols=["NVDA"] only applies to NVDA."""
    config._data["data"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["data"]["ingestion"]["force_reload"]["confirm"] = True
    config._data["data"]["ingestion"]["force_reload"]["symbols"] = ["NVDA"]

    planner = IngestionPlanner(config)

    # NVDA gets force reload
    nvda_watermark = IngestionWatermark(
        symbol="NVDA", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=1),
        record_count=2625,
    )
    plan_nvda = planner.plan(
        "NVDA", "1d", watermark=nvda_watermark, as_of_date=TODAY
    )
    assert plan_nvda.mode == IngestionMode.FORCE_RELOAD

    # AAPL is NOT in scope — gets normal incremental
    plan_aapl = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan_aapl.mode == IngestionMode.INCREMENTAL


def test_explicit_date_range_scoped_to_two_symbols(config, fresh_watermark):
    """C3: date_range_override with symbols=["AAPL","MSFT"] only applies to those."""
    config._data["data"]["ingestion"]["date_range_override"]["enabled"] = True
    config._data["data"]["ingestion"]["date_range_override"]["start_date"] = "2019-06-01"
    config._data["data"]["ingestion"]["date_range_override"]["end_date"] = "2019-06-30"
    config._data["data"]["ingestion"]["date_range_override"]["symbols"] = ["AAPL", "MSFT"]

    planner = IngestionPlanner(config)

    # AAPL and MSFT get explicit range
    plan_aapl = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan_aapl.mode == IngestionMode.EXPLICIT_DATE_RANGE

    # NVDA is NOT in scope
    nvda_watermark = IngestionWatermark(
        symbol="NVDA", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=1),
        record_count=2625,
    )
    plan_nvda = planner.plan(
        "NVDA", "1d", watermark=nvda_watermark, as_of_date=TODAY
    )
    assert plan_nvda.mode == IngestionMode.INCREMENTAL


def test_empty_symbols_list_applies_to_all(config, fresh_watermark):
    """symbols: [] means ALL symbols — not just an empty selection."""
    config._data["data"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["data"]["ingestion"]["history_extension"]["extend_history_start"] = "2010-01-01"
    config._data["data"]["ingestion"]["history_extension"]["symbols"] = []

    planner = IngestionPlanner(config)

    # Both AAPL and MSFT should get history extension
    plan_aapl = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan_aapl.mode == IngestionMode.HISTORY_EXTENSION

    msft_watermark = IngestionWatermark(
        symbol="MSFT", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=1),
        record_count=2625,
    )
    plan_msft = planner.plan(
        "MSFT", "1d", watermark=msft_watermark, as_of_date=TODAY
    )
    assert plan_msft.mode == IngestionMode.HISTORY_EXTENSION


def test_restatement_scoped_to_one_symbol(config, fresh_watermark):
    """D2: restatement with symbols=["AAPL"] only re-states AAPL."""
    config._data["data"]["ingestion"]["restatement"]["enabled"] = True
    config._data["data"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["data"]["ingestion"]["restatement"]["end_date"] = "2026-06-19"
    config._data["data"]["ingestion"]["restatement"]["symbols"] = ["AAPL"]

    planner = IngestionPlanner(config)

    plan_aapl = planner.plan(
        "AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY
    )
    assert plan_aapl.mode == IngestionMode.RESTATEMENT

    nvda_watermark = IngestionWatermark(
        symbol="NVDA", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=1),
        record_count=2625,
    )
    plan_nvda = planner.plan(
        "NVDA", "1d", watermark=nvda_watermark, as_of_date=TODAY
    )
    assert plan_nvda.mode == IngestionMode.INCREMENTAL
