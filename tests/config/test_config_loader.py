"""
Tests for ConfigLoader.
Validates that all config files load correctly and
key values are within safe bounds.
"""
import pytest
from pathlib import Path
from src.config.config_loader import ConfigLoader, ConfigNode


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def reset_config():
    """Reset singleton before and after each test."""
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


# ── Loading tests ──────────────────────────────────────────────────────────────

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


# ── AWS config tests ───────────────────────────────────────────────────────────

def test_aws_region():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.aws.region == "us-east-1"


def test_aws_s3_buckets():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.aws.s3.raw == "handh-trade-raw-use1"
    assert config.aws.s3.refined == "handh-trade-refined-use1"
    assert config.aws.s3.mlflow == "handh-trade-mlflow-use1"
    assert config.aws.s3.dbx_root == "handh-trade-dbx-root-use1"


# ── Databricks config tests ────────────────────────────────────────────────────

def test_databricks_catalog():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.databricks.catalog == "handh_trade"


def test_databricks_schemas():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.databricks.schemas.bronze == "bronze"
    assert config.databricks.schemas.silver == "silver"
    assert config.databricks.schemas.gold == "gold"


# ── Data source tests ──────────────────────────────────────────────────────────

def test_data_source_is_ibkr():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.data.source == "ibkr"
    assert config.data.fallback_source == "yahoo"


def test_data_timezone():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.data.timezone == "America/New_York"


# ── Risk config tests ──────────────────────────────────────────────────────────

def test_risk_kill_switch_is_off():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.risk.kill_switch == False, "Kill switch must be OFF on startup"


def test_risk_position_limits_are_safe():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.risk.max_position_risk_pct <= 5.0, \
        f"Position risk {config.risk.max_position_risk_pct}% exceeds safe limit of 5%"
    assert config.risk.max_daily_loss_pct <= 5.0, \
        f"Daily loss limit {config.risk.max_daily_loss_pct}% exceeds safe limit of 5%"
    assert config.risk.max_monthly_drawdown_pct <= 15.0, \
        f"Monthly drawdown {config.risk.max_monthly_drawdown_pct}% exceeds safe limit of 15%"


def test_risk_min_price_filter():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.risk.min_price >= 5.0, "Min price must be >= $5 to avoid penny stocks"


def test_risk_reward_ratio():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    assert config.risk.min_risk_reward_ratio >= 1.5, \
        "Minimum R:R must be >= 1.5 to ensure positive expectancy"


# ── ConfigNode utility tests ───────────────────────────────────────────────────

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
    assert "risk" in d


def test_missing_key_raises_helpful_error():
    config = ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)
    with pytest.raises(AttributeError) as exc_info:
        _ = config.aws.s3.nonexistent_bucket
    assert "nonexistent_bucket" in str(exc_info.value)
    assert "Available keys" in str(exc_info.value)
