"""Tests for ConfigLoader — validates all config files load correctly."""
import pytest
from pathlib import Path
from src.shared.config.config_loader import ConfigLoader, ConfigNode

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def reset_config():
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


def test_config_loads_successfully():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert isinstance(config, ConfigNode)


def test_environment_is_dev():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.environment == "dev"


def test_config_is_cached():
    c1 = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    c2 = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert c1 is c2


def test_force_reload_returns_fresh_instance():
    c1 = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    c2 = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT, force_reload=True)
    assert c1 is not c2


# ── Infrastructure ────────────────────────────────────────────────────────────

def test_aws_region():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.aws.region == "us-east-1"


def test_aws_s3_buckets():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.aws.s3.raw == "handh-trade-raw-use1"
    assert config.aws.s3.refined == "handh-trade-refined-use1"
    assert config.aws.s3.mlflow == "handh-trade-mlflow-use1"


def test_databricks_catalog():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.databricks.catalog == "tradeanalytics"


def test_databricks_schemas():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.databricks.schemas.bronze == "bronze"
    assert config.databricks.schemas.silver == "silver"


# ── Sources ───────────────────────────────────────────────────────────────────

def test_sources_primary_is_ibkr():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.sources.primary == "ibkr"


def test_sources_fallback_is_yahoo():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.sources.fallback == "yahoo"


def test_sources_ibkr_config_exists():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.sources.ibkr.timeout_seconds == 30
    assert config.sources.ibkr.max_retries == 3


def test_sources_provider_fallback_enabled():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.sources.provider_fallback.enabled == True


# ── Daily stream ──────────────────────────────────────────────────────────────

def test_daily_stream_is_enabled():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.daily.enabled == True


def test_daily_stream_phase_is_2():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.daily.phase == 2


def test_daily_stream_table():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.daily.table == "market_data_daily"


def test_daily_stream_intervals():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert "1d" in config.daily.intervals


def test_daily_stream_lookback_years():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.daily.history.lookback_years == 10


def test_daily_stream_amendment_buffer():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.daily.history.amendment_buffer_days == 3


def test_daily_stream_validation_thresholds():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.daily.validation.thresholds.price_change_pct_threshold == 50.0
    assert config.daily.validation.thresholds.gap_threshold == 15.0


def test_daily_force_reload_disabled_by_default():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.daily.ingestion.force_reload.enabled == False
    assert config.daily.ingestion.force_reload.confirm == False


# ── Intraday stream ───────────────────────────────────────────────────────────

def test_intraday_stream_is_disabled():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.intraday.enabled == False


def test_intraday_stream_phase_is_3():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.intraday.phase == 3


def test_intraday_threshold_lower_than_daily():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    daily_threshold = config.daily.validation.thresholds.price_change_pct_threshold
    intraday_threshold = config.intraday.validation.thresholds.price_change_pct_threshold
    assert intraday_threshold < daily_threshold


# ── Tick stream ───────────────────────────────────────────────────────────────

def test_tick_stream_is_disabled():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.tick.enabled == False


def test_tick_stream_phase_is_5():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.tick.phase == 5


def test_tick_threshold_lower_than_intraday():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    intraday_threshold = config.intraday.validation.thresholds.price_change_pct_threshold
    tick_threshold = config.tick.validation.thresholds.price_change_pct_threshold
    assert tick_threshold < intraday_threshold


# ── Risk ──────────────────────────────────────────────────────────────────────

def test_risk_kill_switch_is_off():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.risk.kill_switch == False


def test_risk_position_limits_safe():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.risk.max_position_risk_pct <= 5.0
    assert config.risk.max_daily_loss_pct <= 5.0


def test_risk_min_price_filter():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.risk.min_price >= 5.0


# ── ConfigNode utilities ──────────────────────────────────────────────────────

def test_config_get_with_default():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    result = config.get("aws.s3.raw", default="fallback")
    assert result == "handh-trade-raw-use1"


def test_config_get_missing_key_returns_default():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    result = config.get("this.does.not.exist", default="fallback")
    assert result == "fallback"


def test_config_has_existing_key():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.has("aws.s3.raw") == True


def test_config_has_missing_key():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.has("this.does.not.exist") == False


def test_config_to_dict():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    d = config.to_dict()
    assert isinstance(d, dict)
    assert "aws" in d
    assert "daily" in d
    assert "sources" in d


def test_missing_key_raises_helpful_error():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    with pytest.raises(AttributeError) as exc_info:
        _ = config.aws.s3.nonexistent_bucket
    assert "nonexistent_bucket" in str(exc_info.value)
    assert "Available keys" in str(exc_info.value)
