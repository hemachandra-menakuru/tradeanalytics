"""
Tests for DataQualityValidator — validates batch processing behaviour.
"""
import pytest
from pathlib import Path
from src.ingestion.validation.validator import DataQualityValidator
from src.config.config_loader import ConfigLoader

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def config():
    ConfigLoader.reset()
    return ConfigLoader.load(environment="dev", repo_root=REPO_ROOT)


@pytest.fixture
def validator(config):
    return DataQualityValidator(config)


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
        "open": 150.0, "high": 148.0,  # high < low — critical
        "low": 149.75, "close": 151.25,
        "volume": 1_000_000,
    }


@pytest.fixture
def flagged_record():
    return {
        "symbol": "AAPL", "date": "2026-06-19", "interval": "1d",
        "source": "ibkr",
        "open": 150.0, "high": 152.5, "low": 149.75, "close": 151.25,
        "volume": 0,   # warning — flagged not rejected
        "prev_close": 149.50,
    }


# ── Validator setup tests ──────────────────────────────────────────────────────

def test_validator_loads_successfully(validator):
    assert validator.rule_count > 0


# ── Clean batch tests ──────────────────────────────────────────────────────────

def test_clean_record_goes_to_clean_records(validator, clean_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [clean_record])
    assert summary.total_passed == 1
    assert summary.total_rejected == 0
    assert summary.total_flagged == 0
    assert len(summary.clean_records) == 1


def test_clean_batch_has_100_pass_rate(validator, clean_record):
    # Use known weekdays (Mon-Fri) to avoid weekend rejections
    weekday_dates = [
        "2026-06-15", "2026-06-16", "2026-06-17", "2026-06-18", "2026-06-19"
    ]
    records = []
    for d in weekday_dates:
        r = clean_record.copy()
        r["date"] = d
        records.append(r)
    summary = validator.validate_batch("AAPL", "1d", "batch_001", records)
    assert summary.pass_rate_pct == 100.0


# ── Rejected record tests ──────────────────────────────────────────────────────

def test_rejected_record_goes_to_rejected_records(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert summary.total_rejected == 1
    assert summary.total_passed == 0
    assert len(summary.rejected_records) == 1


def test_rejected_record_not_in_clean_or_flagged(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert len(summary.clean_records) == 0
    assert len(summary.flagged_records) == 0


def test_rejected_record_dict_has_required_fields(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    rejected = summary.rejected_records[0]
    assert "symbol" in rejected
    assert "rejected_rule" in rejected
    assert "rejection_reason" in rejected
    assert "raw_record" in rejected
    assert "rejected_at" in rejected
    assert rejected["reprocessed"] == False


def test_rejection_reason_tracked(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert "high_gte_low" in summary.rejection_reasons


# ── Flagged record tests ───────────────────────────────────────────────────────

def test_flagged_record_goes_to_flagged_records(validator, flagged_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    assert summary.total_flagged == 1
    assert len(summary.flagged_records) == 1


def test_flagged_record_has_quality_flag_true(validator, flagged_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    flagged = summary.flagged_records[0]
    assert flagged["data_quality_flag"] == True


def test_flagged_record_has_quality_reasons(validator, flagged_record):
    import json
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    flagged = summary.flagged_records[0]
    reasons = json.loads(flagged["data_quality_reasons"])
    assert "volume_positive" in reasons


def test_flag_reason_tracked(validator, flagged_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    assert "volume_positive" in summary.flag_reasons


# ── Mixed batch tests ──────────────────────────────────────────────────────────

def test_mixed_batch_correctly_classified(
    validator, clean_record, rejected_record, flagged_record
):
    records = [clean_record, rejected_record, flagged_record]
    summary = validator.validate_batch("AAPL", "1d", "batch_001", records)
    assert summary.total_input == 3
    assert summary.total_passed == 1
    assert summary.total_rejected == 1
    assert summary.total_flagged == 1


def test_writable_records_includes_clean_and_flagged(
    validator, clean_record, flagged_record
):
    summary = validator.validate_batch(
        "AAPL", "1d", "batch_001", [clean_record, flagged_record]
    )
    assert len(summary.writable_records) == 2


# ── ValidationSummary property tests ─────────────────────────────────────────

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


# ── Audit log tests ───────────────────────────────────────────────────────────

def test_audit_log_populated_for_rejections(validator, rejected_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [rejected_record])
    assert len(summary.audit_log) > 0
    entry = summary.audit_log[0]
    assert entry["symbol"] == "AAPL"
    assert entry["outcome"] == "rejected"


def test_audit_log_populated_for_flags(validator, flagged_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [flagged_record])
    assert len(summary.audit_log) > 0
    entry = summary.audit_log[0]
    assert entry["outcome"] == "flagged"


def test_clean_records_have_no_audit_entries(validator, clean_record):
    summary = validator.validate_batch("AAPL", "1d", "batch_001", [clean_record])
    assert len(summary.audit_log) == 0


# ── to_summary_dict test ──────────────────────────────────────────────────────

def test_to_summary_dict_has_no_record_data(validator, clean_record, rejected_record):
    summary = validator.validate_batch(
        "AAPL", "1d", "batch_001", [clean_record, rejected_record]
    )
    d = summary.to_summary_dict()
    assert "clean_records" not in d
    assert "rejected_records" not in d
    assert "total_input" in d
    assert "pass_rate_pct" in d
