"""
Tests for DataQualityValidator — stream-aware validation.
"""
import pytest
from pathlib import Path
from src.bronze.validation.validator import DataQualityValidator
from src.shared.config.config_loader import ConfigLoader

REPO_ROOT = Path(__file__).resolve().parents[3]
RULES_PATH = REPO_ROOT / "src" / "reference" / "seed" / "data_quality_rules.yml"


@pytest.fixture(autouse=True)
def reset_config():
    ConfigLoader.reset()
    yield
    ConfigLoader.reset()


@pytest.fixture
def config():
    return ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)


@pytest.fixture
def validator(config):
    return DataQualityValidator.for_stream(config, "daily", rules_path=RULES_PATH)


@pytest.fixture
def clean_record():
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


@pytest.fixture
def rejected_record():
    return {
        "symbol": "AAPL", "date": "2026-06-19", "interval": "1d",
        "source": "ibkr",
        "open": 150.0, "high": 148.0,
        "low": 149.75, "close": 151.25,
        "volume": 1_000_000,
    }


@pytest.fixture
def flagged_record():
    return {
        "symbol": "AAPL", "date": "2026-06-19", "interval": "1d",
        "source": "ibkr",
        "open": 150.0, "high": 152.5, "low": 149.75, "close": 151.25,
        "volume": 0,
        "prev_close": 149.50,
    }


# ── Validator setup ───────────────────────────────────────────────────────────

def test_validator_loads_for_daily_stream(validator):
    assert validator.rule_count > 0


def test_validator_for_stream_factory(config):
    v = DataQualityValidator.for_stream(config, "daily", rules_path=RULES_PATH)
    assert v is not None


def test_validator_for_daily_convenience(config):
    v = DataQualityValidator.for_daily(config, rules_path=RULES_PATH)
    assert v is not None


# ── Clean batch ───────────────────────────────────────────────────────────────

def test_clean_record_goes_to_clean_records(validator, clean_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [clean_record])
    assert summary.total_passed == 1
    assert summary.total_rejected == 0
    assert summary.total_flagged == 0


def test_clean_batch_has_100_pass_rate(validator, clean_record):
    weekday_dates = [
        "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-22"
    ]
    records = []
    for d in weekday_dates:
        r = clean_record.copy()
        r["date"] = d
        records.append(r)
    summary = validator.validate_batch("AAPL", "1d", "batch_001", records)
    assert summary.pass_rate_pct == 100.0


# ── Rejected records ──────────────────────────────────────────────────────────

def test_rejected_record_goes_to_rejected(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert summary.total_rejected == 1
    assert len(summary.rejected_records) == 1


def test_rejected_record_not_in_clean_or_flagged(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert len(summary.clean_records) == 0
    assert len(summary.flagged_records) == 0


def test_rejected_record_has_required_fields(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    r = summary.rejected_records[0]
    assert "rejected_rule" in r
    assert "rejection_reason" in r
    assert "raw_record" in r
    assert r["reprocessed"] == False


# ── Flagged records ───────────────────────────────────────────────────────────

def test_flagged_record_goes_to_flagged(validator, flagged_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    assert summary.total_flagged == 1


def test_flagged_record_has_quality_flag_true(validator, flagged_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    assert summary.flagged_records[0]["data_quality_flag"] == True


def test_flagged_record_has_reasons(validator, flagged_record):
    import json
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    reasons = json.loads(summary.flagged_records[0]["data_quality_reasons"])
    assert "volume_positive" in reasons


# ── Mixed batch ───────────────────────────────────────────────────────────────

def test_mixed_batch_classified_correctly(validator, clean_record, rejected_record, flagged_record):
    summary = validator.validate_batch(
        "AAPL", "1d", "batch_001",
        [clean_record, rejected_record, flagged_record]
    )
    assert summary.total_input == 3
    assert summary.total_passed == 1
    assert summary.total_rejected == 1
    assert summary.total_flagged == 1


def test_writable_records_includes_clean_and_flagged(validator, clean_record, flagged_record):
    summary = validator.validate_batch(
        "AAPL", "1d", "batch_001", [clean_record, flagged_record]
    )
    assert len(summary.writable_records) == 2


# ── Summary properties ────────────────────────────────────────────────────────

def test_pass_rate_100_for_empty_batch(validator):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [])
    assert summary.pass_rate_pct == 100.0


def test_rejection_rate_correct(validator, rejected_record, clean_record):
    summary = validator.validate_batch(
        "AAPL", "1d", "batch_001", [rejected_record, clean_record]
    )
    assert summary.rejection_rate_pct == 50.0


def test_has_issues_true_when_rejections(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert summary.has_issues == True


def test_has_issues_false_for_clean_batch(validator, clean_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [clean_record])
    assert summary.has_issues == False


# ── Audit log ─────────────────────────────────────────────────────────────────

def test_audit_log_for_rejections(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert len(summary.audit_log) > 0
    assert summary.audit_log[0]["outcome"] == "rejected"


def test_no_audit_entries_for_clean_records(validator, clean_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [clean_record])
    assert len(summary.audit_log) == 0


def test_to_summary_dict_no_record_data(validator, clean_record, rejected_record):
    summary = validator.validate_batch(
        "AAPL", "1d", "batch_001", [clean_record, rejected_record]
    )
    d = summary.to_summary_dict()
    assert "clean_records" not in d
    assert "total_input" in d
    assert "pass_rate_pct" in d
