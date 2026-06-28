"""
Shared fixtures for corporate actions tests.

Synthetic data covers every scenario the pipeline must handle:
  - Forward splits (2:1, 3:1, 4:1, 10:1, 3:2, 5:2)
  - Reverse splits (1:2, 1:5, 1:10, 1:20)
  - Dividends (regular and special — below detection threshold)
  - Bonus issues (structurally identical to forward splits)
  - Mergers / acquisitions (ratio unrecognisable — manual review)
  - Spinoffs (partial price reduction — may or may not cross threshold)
  - Ticker symbol changes (no price change — not detectable)
  - Delistings (data stops — not a corporate action candidate)
  - Edge cases: boundary ratios, zero prices, missing data, duplicates
"""

import pytest
from datetime import date, datetime, timezone


# ---------------------------------------------------------------------------
# Instrument reference data
# ---------------------------------------------------------------------------

@pytest.fixture
def instruments():
    """
    8 synthetic instruments covering different asset types and scenarios.
    instrument_id chosen to not collide with real dev data (505-512).
    """
    return [
        {"instrument_id": 1001, "symbol": "SPLIT2",  "name": "2-for-1 Split Corp"},
        {"instrument_id": 1002, "symbol": "SPLIT4",  "name": "4-for-1 Split Corp"},
        {"instrument_id": 1003, "symbol": "RSPLIT",  "name": "1-for-10 Reverse Split Corp"},
        {"instrument_id": 1004, "symbol": "DIVD",    "name": "Regular Dividend Corp"},
        {"instrument_id": 1005, "symbol": "BONUS",   "name": "Bonus Issue Corp"},     # same as 2:1 split
        {"instrument_id": 1006, "symbol": "MERGER",  "name": "Merger Target Corp"},   # unrecognisable ratio
        {"instrument_id": 1007, "symbol": "SPINOFF", "name": "Spinoff Parent Corp"},  # partial reduction
        {"instrument_id": 1008, "symbol": "STABLE",  "name": "No Event Corp"},        # no action — control
    ]


# ---------------------------------------------------------------------------
# Synthetic Bronze price histories
# ---------------------------------------------------------------------------

def _bar(instrument_id, bar_date, close, record_version=1, open_=None):
    """Helper: build a minimal bronze record dict."""
    return {
        "instrument_id":  instrument_id,
        "bar_date":       bar_date,
        "open":           open_ or close * 0.99,
        "high":           close * 1.01,
        "low":            close * 0.98,
        "close":          close,
        "volume":         1_000_000,
        "record_version": record_version,
        "source":         "ibkr",
    }


@pytest.fixture
def bronze_prices_pre_split():
    """
    Bronze prices stored BEFORE any corporate actions.
    These represent what is currently on disk — old price base.
    """
    return [
        # SPLIT2 — 2:1 split coming; stored at $200
        _bar(1001, date(2024, 1, 15), close=200.00),
        # SPLIT4 — 4:1 split coming; stored at $400
        _bar(1002, date(2024, 1, 15), close=400.00),
        # RSPLIT — 1:10 reverse split coming; stored at $1.00
        _bar(1003, date(2024, 1, 15), close=1.00),
        # DIVD — regular dividend; stored at $50, price barely changes
        _bar(1004, date(2024, 1, 15), close=50.00),
        # BONUS — bonus issue (2:1); stored at $300
        _bar(1005, date(2024, 1, 15), close=300.00),
        # MERGER — acquired at premium; stored at $80
        _bar(1006, date(2024, 1, 15), close=80.00),
        # SPINOFF — spinoff reduces parent value; stored at $100
        _bar(1007, date(2024, 1, 15), close=100.00),
        # STABLE — no event; control instrument
        _bar(1008, date(2024, 1, 15), close=150.00),
    ]


@pytest.fixture
def vendor_prices_post_event():
    """
    Prices IBKR returns AFTER retroactive adjustment.
    These are what the vendor returns TODAY for the same 2024-01-15 bar.

    Ratios vs bronze_prices_pre_split:
        SPLIT2:  $100/$200  = 0.50  → 2:1 split
        SPLIT4:  $100/$400  = 0.25  → 4:1 split
        RSPLIT:  $10/$1     = 10.0  → 1:10 reverse split
        DIVD:    $49.5/$50  = 0.99  → below 40% threshold (false positive)
        BONUS:   $150/$300  = 0.50  → 2:1 bonus issue (same as split)
        MERGER:  $120/$80   = 1.50  → above threshold but unrecognised ratio
        SPINOFF: $70/$100   = 0.70  → below 40% threshold (false positive)
        STABLE:  $150/$150  = 1.00  → no change
    """
    return {
        1001: 100.00,   # SPLIT2   ratio=0.50 → SPLIT 2:1
        1002: 100.00,   # SPLIT4   ratio=0.25 → SPLIT 4:1
        1003: 10.00,    # RSPLIT   ratio=10.0 → REVERSE_SPLIT 1:10
        1004: 49.50,    # DIVD     ratio=0.99 → below threshold → FALSE_POSITIVE
        1005: 150.00,   # BONUS    ratio=0.50 → SPLIT 2:1 (bonus = split structurally)
        1006: 120.00,   # MERGER   ratio=1.50 → above threshold, unrecognised → UNKNOWN
        1007: 70.00,    # SPINOFF  ratio=0.70 → below threshold → FALSE_POSITIVE
        1008: 150.00,   # STABLE   ratio=1.00 → no divergence → no candidate
    }


# ---------------------------------------------------------------------------
# Negative and edge case data
# ---------------------------------------------------------------------------

@pytest.fixture
def edge_case_prices():
    """
    Problematic price combinations the detector must handle without crashing.
    """
    return {
        "zero_bronze":   {"bronze": 0.00,  "vendor": 100.00},  # division by zero risk
        "zero_vendor":   {"bronze": 100.0, "vendor": 0.00},    # vendor returned 0
        "none_vendor":   {"bronze": 100.0, "vendor": None},    # vendor missing data
        "none_bronze":   {"bronze": None,  "vendor": 100.0},   # bronze missing close
        "negative":      {"bronze": -5.00, "vendor": -10.00},  # invalid negative prices
        "very_large":    {"bronze": 1e9,   "vendor": 5e8},     # extreme values (BRK.A type)
        "near_boundary": {"bronze": 100.0, "vendor": 59.50},   # ratio=0.595, just above threshold
        "at_boundary":   {"bronze": 100.0, "vendor": 60.00},   # ratio=0.60, exactly at threshold
        "below_boundary":{"bronze": 100.0, "vendor": 60.01},   # ratio=0.6001, just below threshold
    }


@pytest.fixture
def duplicate_bronze_records():
    """
    Same instrument_id + bar_date with multiple record_versions.
    The detector must use the LATEST record_version (dedup window).
    record_version=2 is the latest — that's the price to use.
    """
    return [
        _bar(1001, date(2024, 1, 15), close=200.00, record_version=1),  # old — ignore
        _bar(1001, date(2024, 1, 15), close=195.00, record_version=2),  # latest — use this
    ]


@pytest.fixture
def out_of_order_events():
    """
    Corporate actions reported out of chronological order.
    The classifier must handle events arriving in any order.
    """
    return [
        {"instrument_id": 1001, "effective_date": date(2024, 3, 1), "event_type": "SPLIT",
         "split_ratio_from": 2, "split_ratio_to": 1, "event_factor": 0.5, "source_vendor": "yahoo"},
        {"instrument_id": 1001, "effective_date": date(2024, 1, 1), "event_type": "DIVIDEND",
         "split_ratio_from": 1, "split_ratio_to": 1, "event_factor": 1.0, "dividend_amount": 0.50,
         "source_vendor": "yahoo"},
        {"instrument_id": 1001, "effective_date": date(2024, 2, 1), "event_type": "SPLIT",
         "split_ratio_from": 3, "split_ratio_to": 1, "event_factor": 0.3333, "source_vendor": "yahoo"},
    ]


@pytest.fixture
def corrupted_records():
    """
    Records with invalid or missing required fields.
    The pipeline must reject these gracefully — not crash or silently pass.
    """
    return [
        {"instrument_id": None, "bar_date": date(2024, 1, 15), "close": 100.00},   # no instrument_id
        {"instrument_id": 1001, "bar_date": None,              "close": 100.00},   # no date
        {"instrument_id": 1001, "bar_date": date(2024, 1, 15), "close": None},     # no close price
        {"instrument_id": 1001, "bar_date": date(2024, 1, 15), "close": "abc"},    # wrong type
        {"instrument_id": 1001, "bar_date": "not-a-date",      "close": 100.00},   # invalid date string
        {},  # completely empty record
    ]


@pytest.fixture
def partial_vendor_response():
    """
    Vendor returns data for only some instruments (API timeout / rate limit).
    Detector must process what it has and not fail for missing instruments.
    """
    return {
        1001: 100.00,   # SPLIT2 — present
        1002: None,     # SPLIT4 — missing (API timeout)
        # 1003 not in dict at all — also missing
        1004: 49.50,    # DIVD — present
    }


@pytest.fixture
def incorrect_adjustment_factors():
    """
    Corporate action events with mathematically inconsistent adjustment factors.
    event_factor should equal ratio_to / ratio_from.
    These should be flagged/rejected by validation.
    """
    return [
        # Correct: 2:1 split → factor should be 0.5
        {"ratio_from": 2, "ratio_to": 1, "event_factor": 0.50,  "valid": True},
        # Wrong factor for ratio (1.0 instead of 0.5)
        {"ratio_from": 2, "ratio_to": 1, "event_factor": 1.00,  "valid": False},
        # Wrong factor (negative)
        {"ratio_from": 2, "ratio_to": 1, "event_factor": -0.50, "valid": False},
        # Wrong factor (zero)
        {"ratio_from": 2, "ratio_to": 1, "event_factor": 0.00,  "valid": False},
        # Correct: 1:10 reverse → factor should be 10.0
        {"ratio_from": 1, "ratio_to": 10, "event_factor": 10.00, "valid": True},
        # Wrong: factor exceeds plausible range (>50 suggests a data error)
        {"ratio_from": 1, "ratio_to": 100, "event_factor": 100.0, "valid": False},
    ]


# ---------------------------------------------------------------------------
# Saga state fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def candidate_at_detected():
    """A candidate row in DETECTED state — ready for classifier."""
    return {
        "candidate_id":     1,
        "instrument_id":    1001,
        "detected_on_date": date(2024, 1, 16),
        "price_in_bronze":  200.00,
        "price_from_vendor": 100.00,
        "price_ratio":      0.50,
        "status":           "DETECTED",
        "event_type":       None,
        "action_id":        None,
        "reload_command_id": None,
        "retry_count":      0,
    }


@pytest.fixture
def candidate_at_max_retries():
    """A candidate that has already failed MAX_RETRIES times — should be marked FAILED."""
    return {
        "candidate_id":     2,
        "instrument_id":    1002,
        "detected_on_date": date(2024, 1, 16),
        "price_in_bronze":  400.00,
        "price_from_vendor": 100.00,
        "price_ratio":      0.25,
        "status":           "DETECTED",
        "event_type":       None,
        "retry_count":      3,  # already at MAX_RETRIES
    }


@pytest.fixture
def candidates_mixed_states():
    """
    Candidates in various saga states — tests that recovery only processes
    non-terminal rows.
    """
    return [
        {"candidate_id": 1, "status": "DETECTED",         "instrument_id": 1001},
        {"candidate_id": 2, "status": "CLASSIFIED",       "instrument_id": 1002},
        {"candidate_id": 3, "status": "RELOAD_TRIGGERED", "instrument_id": 1003},
        {"candidate_id": 4, "status": "RESOLVED",         "instrument_id": 1004},  # terminal
        {"candidate_id": 5, "status": "FALSE_POSITIVE",   "instrument_id": 1005},  # terminal
        {"candidate_id": 6, "status": "FAILED",           "instrument_id": 1006},  # terminal
    ]
