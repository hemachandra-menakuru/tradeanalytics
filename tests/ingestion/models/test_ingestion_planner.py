"""
Tests for IngestionPlanner — all ingestion scenarios and symbol scoping.
Uses stream config (config.daily.*) not the old config.data.* paths.
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
TODAY = date(2026, 6, 22)  # Monday — known trading day (avoiding Juneteenth)


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
    return IngestionPlanner(config, stream_name="daily")


@pytest.fixture
def fresh_watermark():
    return IngestionWatermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=1),
        record_count=2625,
    )


@pytest.fixture
def stale_watermark():
    return IngestionWatermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY - timedelta(days=60),
        record_count=2565,
    )


@pytest.fixture
def current_watermark():
    return IngestionWatermark(
        symbol="AAPL", interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=TODAY,
        record_count=2626,
    )


# Helper to make a fresh watermark for any symbol
def make_watermark(symbol, latest_date=None):
    return IngestionWatermark(
        symbol=symbol, interval="1d",
        earliest_date=date(2016, 1, 4),
        latest_date=latest_date or (TODAY - timedelta(days=1)),
        record_count=2625,
    )


# ── Trading day tests ─────────────────────────────────────────────────────────

def test_monday_is_trading_day():
    assert is_trading_day(date(2026, 6, 22)) == True


def test_saturday_is_not_trading_day():
    assert is_trading_day(date(2026, 6, 20)) == False


def test_sunday_is_not_trading_day():
    assert is_trading_day(date(2026, 6, 21)) == False


def test_christmas_2026_is_not_trading_day():
    assert is_trading_day(date(2026, 12, 25)) == False


# ── A: Normal operations ──────────────────────────────────────────────────────

def test_initial_load_when_no_watermark(planner):
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.mode == IngestionMode.INITIAL_LOAD
    assert plan.end_date == TODAY
    assert plan.date_range_days > 3000


def test_initial_load_ingestion_type_is_backfill(planner):
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.ingestion_type == "backfill"
    assert plan.mode.is_backfill == True


def test_incremental_when_watermark_is_yesterday(planner, fresh_watermark):
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.INCREMENTAL
    assert plan.end_date == TODAY
    assert plan.ingestion_type == "scheduled"


def test_incremental_includes_amendment_buffer(planner, fresh_watermark):
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    buffer_days = 3
    expected_start = fresh_watermark.latest_date - timedelta(days=buffer_days)
    assert plan.start_date == expected_start


def test_no_op_when_already_up_to_date(planner, current_watermark):
    plan = planner.plan("AAPL", "1d", watermark=current_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.NO_OP


# ── B: Gap scenarios ──────────────────────────────────────────────────────────

def test_gap_fill_when_gap_exceeds_threshold(planner, stale_watermark):
    plan = planner.plan("AAPL", "1d", watermark=stale_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.GAP_FILL
    assert plan.start_date == stale_watermark.latest_date + timedelta(days=1)
    assert plan.ingestion_type == "backfill"


def test_incremental_when_gap_within_threshold(planner):
    watermark = make_watermark("AAPL", latest_date=TODAY - timedelta(days=5))
    plan = planner.plan("AAPL", "1d", watermark=watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.INCREMENTAL


# ── C: History extension ──────────────────────────────────────────────────────

def test_history_extension_when_enabled(config, fresh_watermark):
    config._data["daily"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["daily"]["ingestion"]["history_extension"]["extend_history_start"] = "2010-01-01"
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.HISTORY_EXTENSION
    assert plan.start_date == date(2010, 1, 1)
    assert plan.end_date == fresh_watermark.earliest_date - timedelta(days=1)


def test_history_extension_not_triggered_if_already_covered(config, fresh_watermark):
    config._data["daily"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["daily"]["ingestion"]["history_extension"]["extend_history_start"] = "2018-01-01"
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode != IngestionMode.HISTORY_EXTENSION


def test_per_ticker_history_start_overrides_global(planner):
    ticker_start = date(2020, 9, 30)
    plan = planner.plan(
        "PLTR", "1d", watermark=None, as_of_date=TODAY,
        ticker_history_start=ticker_start,
    )
    assert plan.mode == IngestionMode.INITIAL_LOAD
    assert plan.start_date == ticker_start


# ── D: Explicit overrides ─────────────────────────────────────────────────────

def test_explicit_date_range_override(config, fresh_watermark):
    config._data["daily"]["ingestion"]["date_range_override"]["enabled"] = True
    config._data["daily"]["ingestion"]["date_range_override"]["start_date"] = "2019-06-01"
    config._data["daily"]["ingestion"]["date_range_override"]["end_date"] = "2019-06-30"
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.EXPLICIT_DATE_RANGE
    assert plan.start_date == date(2019, 6, 1)
    assert plan.end_date == date(2019, 6, 30)


def test_restatement_mode(config, fresh_watermark):
    config._data["daily"]["ingestion"]["restatement"]["enabled"] = True
    config._data["daily"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["daily"]["ingestion"]["restatement"]["end_date"] = "2026-06-22"
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.RESTATEMENT
    assert plan.is_amendment_run == True
    assert plan.ingestion_type == "amendment"


def test_force_reload_requires_confirm(config, fresh_watermark):
    config._data["daily"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["daily"]["ingestion"]["force_reload"]["confirm"] = False
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode != IngestionMode.FORCE_RELOAD


def test_force_reload_with_confirm(config, fresh_watermark):
    config._data["daily"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["daily"]["ingestion"]["force_reload"]["confirm"] = True
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.FORCE_RELOAD


# ── Priority order ────────────────────────────────────────────────────────────

def test_force_reload_overrides_restatement(config, fresh_watermark):
    config._data["daily"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["daily"]["ingestion"]["force_reload"]["confirm"] = True
    config._data["daily"]["ingestion"]["restatement"]["enabled"] = True
    config._data["daily"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["daily"]["ingestion"]["restatement"]["end_date"] = "2026-06-22"
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.FORCE_RELOAD


def test_restatement_overrides_incremental(config, fresh_watermark):
    config._data["daily"]["ingestion"]["restatement"]["enabled"] = True
    config._data["daily"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["daily"]["ingestion"]["restatement"]["end_date"] = "2026-06-22"
    planner = IngestionPlanner(config, stream_name="daily")
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan.mode == IngestionMode.RESTATEMENT


# ── FetchPlan properties ──────────────────────────────────────────────────────

def test_fetch_plan_date_range_days(planner):
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.date_range_days > 0


def test_fetch_plan_estimated_batches(planner):
    plan = planner.plan("AAPL", "1d", watermark=None, as_of_date=TODAY)
    assert plan.estimated_batches >= 1


def test_fetch_plan_repr(planner, fresh_watermark):
    plan = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    r = repr(plan)
    assert "AAPL" in r
    assert "incremental" in r


# ── IngestionMode properties ──────────────────────────────────────────────────

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


# ── Symbol scoping tests ──────────────────────────────────────────────────────

def test_history_extension_scoped_to_specific_symbol(config, fresh_watermark):
    config._data["daily"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["daily"]["ingestion"]["history_extension"]["extend_history_start"] = "2010-01-01"
    config._data["daily"]["ingestion"]["history_extension"]["symbols"] = ["AAPL"]
    planner = IngestionPlanner(config, stream_name="daily")

    plan_aapl = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan_aapl.mode == IngestionMode.HISTORY_EXTENSION

    plan_msft = planner.plan("MSFT", "1d", watermark=make_watermark("MSFT"), as_of_date=TODAY)
    assert plan_msft.mode == IngestionMode.INCREMENTAL


def test_force_reload_scoped_to_specific_symbols(config, fresh_watermark):
    config._data["daily"]["ingestion"]["force_reload"]["enabled"] = True
    config._data["daily"]["ingestion"]["force_reload"]["confirm"] = True
    config._data["daily"]["ingestion"]["force_reload"]["symbols"] = ["NVDA"]
    planner = IngestionPlanner(config, stream_name="daily")

    plan_nvda = planner.plan("NVDA", "1d", watermark=make_watermark("NVDA"), as_of_date=TODAY)
    assert plan_nvda.mode == IngestionMode.FORCE_RELOAD

    plan_aapl = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan_aapl.mode == IngestionMode.INCREMENTAL


def test_explicit_date_range_scoped_to_two_symbols(config, fresh_watermark):
    config._data["daily"]["ingestion"]["date_range_override"]["enabled"] = True
    config._data["daily"]["ingestion"]["date_range_override"]["start_date"] = "2019-06-01"
    config._data["daily"]["ingestion"]["date_range_override"]["end_date"] = "2019-06-30"
    config._data["daily"]["ingestion"]["date_range_override"]["symbols"] = ["AAPL", "MSFT"]
    planner = IngestionPlanner(config, stream_name="daily")

    plan_aapl = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan_aapl.mode == IngestionMode.EXPLICIT_DATE_RANGE

    plan_nvda = planner.plan("NVDA", "1d", watermark=make_watermark("NVDA"), as_of_date=TODAY)
    assert plan_nvda.mode == IngestionMode.INCREMENTAL


def test_empty_symbols_list_applies_to_all(config, fresh_watermark):
    config._data["daily"]["ingestion"]["history_extension"]["enabled"] = True
    config._data["daily"]["ingestion"]["history_extension"]["extend_history_start"] = "2010-01-01"
    config._data["daily"]["ingestion"]["history_extension"]["symbols"] = []
    planner = IngestionPlanner(config, stream_name="daily")

    plan_aapl = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan_aapl.mode == IngestionMode.HISTORY_EXTENSION

    plan_msft = planner.plan("MSFT", "1d", watermark=make_watermark("MSFT"), as_of_date=TODAY)
    assert plan_msft.mode == IngestionMode.HISTORY_EXTENSION


def test_restatement_scoped_to_one_symbol(config, fresh_watermark):
    config._data["daily"]["ingestion"]["restatement"]["enabled"] = True
    config._data["daily"]["ingestion"]["restatement"]["start_date"] = "2026-01-01"
    config._data["daily"]["ingestion"]["restatement"]["end_date"] = "2026-06-22"
    config._data["daily"]["ingestion"]["restatement"]["symbols"] = ["AAPL"]
    planner = IngestionPlanner(config, stream_name="daily")

    plan_aapl = planner.plan("AAPL", "1d", watermark=fresh_watermark, as_of_date=TODAY)
    assert plan_aapl.mode == IngestionMode.RESTATEMENT

    plan_nvda = planner.plan("NVDA", "1d", watermark=make_watermark("NVDA"), as_of_date=TODAY)
    assert plan_nvda.mode == IngestionMode.INCREMENTAL
