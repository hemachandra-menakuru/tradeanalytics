"""
Unit tests for CorporateActionDetector.

Focus areas:
  1. Candidate generation — correct instruments flagged based on price ratio
  2. Deduplication — instruments already in open saga not re-inserted
  3. Recovery — recover_incomplete_sagas() counts non-terminal rows correctly
  4. Edge cases — zero prices, None prices, partial vendor responses, empty Bronze
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import date

from src.control.corporate_actions.detector import CorporateActionDetector


def _make_detector(spark, vendor_prices=None):
    """Build a detector with a mocked vendor provider."""
    vendor = MagicMock()
    vendor.get_corporate_actions.return_value = []
    return CorporateActionDetector(
        spark=spark,
        vendor_provider=vendor,
        catalog="tradeanalytics",
        detection_threshold=0.40,
    ), vendor


def _make_spark_with_bronze(bronze_rows, open_candidates=None):
    """
    Build a mock SparkSession that returns bronze_rows for the Bronze query
    and open_candidates for the existing open saga query.
    """
    spark = MagicMock()
    open_candidates = open_candidates or []

    bronze_df = MagicMock()
    bronze_df.collect.return_value = [
        _row(instrument_id=r["instrument_id"], close=r["close"])
        for r in bronze_rows
    ]

    open_df = MagicMock()
    open_df.collect.return_value = [
        _row(instrument_id=c) for c in open_candidates
    ]

    insert_df = MagicMock()
    insert_df.collect.return_value = []

    def sql_side_effect(query):
        q = query.strip()
        if "market_data_daily" in q:
            return bronze_df
        if "status NOT IN" in q and "corporate_action_candidates" in q:
            return open_df
        return insert_df

    spark.sql.side_effect = sql_side_effect
    return spark


def _row(**kwargs):
    """Build a mock Spark Row."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: kwargs.get(key)
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


# ---------------------------------------------------------------------------
# Group 1 — Candidate generation
# ---------------------------------------------------------------------------

class TestCandidateGeneration:

    def test_split_instrument_flagged_as_candidate(self, bronze_prices_pre_split, vendor_prices_post_event):
        """instrument_id=1001 has ratio=0.50 — must be written as DETECTED candidate."""
        bronze = [b for b in bronze_prices_pre_split if b["instrument_id"] == 1001]
        spark = _make_spark_with_bronze(bronze)
        detector, vendor = _make_detector(spark)

        # Inject vendor prices via the _fetch_vendor_prices override
        with patch.object(detector, "_fetch_vendor_prices", return_value={1001: 100.00}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 1001 in result

    def test_stable_instrument_not_flagged(self, bronze_prices_pre_split, vendor_prices_post_event):
        """instrument_id=1008 has ratio=1.00 — must NOT be flagged."""
        bronze = [b for b in bronze_prices_pre_split if b["instrument_id"] == 1008]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={1008: 150.00}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 1008 not in result

    def test_all_split_instruments_flagged(self, bronze_prices_pre_split, vendor_prices_post_event):
        """instrument_ids 1001, 1002, 1003 have ratios >40% — all must be flagged."""
        spark = _make_spark_with_bronze(bronze_prices_pre_split)
        detector, _ = _make_detector(spark)

        # Only instruments with ratio > 0.40 deviation should be flagged:
        # 1001: ratio=0.50 → deviation=0.50 ✓
        # 1002: ratio=0.25 → deviation=0.75 ✓
        # 1003: ratio=10.0 → deviation=9.0  ✓
        # 1004: ratio=0.99 → deviation=0.01 ✗
        # 1005: ratio=0.50 → deviation=0.50 ✓ (bonus issue)
        # 1006: ratio=1.50 → deviation=0.50 ✓ (merger)
        # 1007: ratio=0.70 → deviation=0.30 ✗
        # 1008: ratio=1.00 → deviation=0.00 ✗

        with patch.object(detector, "_fetch_vendor_prices", return_value=vendor_prices_post_event):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert set(result) == {1001, 1002, 1003, 1005, 1006}

    def test_dividend_not_flagged(self, bronze_prices_pre_split, vendor_prices_post_event):
        """Regular dividend causes <1% price change — below 40% threshold, not flagged."""
        bronze = [b for b in bronze_prices_pre_split if b["instrument_id"] == 1004]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={1004: 49.50}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 1004 not in result

    def test_spinoff_below_threshold_not_flagged(self, bronze_prices_pre_split, vendor_prices_post_event):
        """Spinoff reduces parent price by 30% — below 40% threshold, not flagged."""
        bronze = [b for b in bronze_prices_pre_split if b["instrument_id"] == 1007]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={1007: 70.00}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 1007 not in result


# ---------------------------------------------------------------------------
# Group 2 — Deduplication (open sagas not re-inserted)
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_existing_open_candidate_not_reinserted(self, bronze_prices_pre_split, vendor_prices_post_event):
        """If instrument_id=1001 already has an open saga, it must not be inserted again."""
        bronze = [b for b in bronze_prices_pre_split if b["instrument_id"] == 1001]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={1001: 100.00}):
            # 1001 already has an open saga
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value={1001}):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 1001 not in result

    def test_duplicate_detection_same_day_idempotent(self, bronze_prices_pre_split, vendor_prices_post_event):
        """Running detect_and_record twice on the same day produces 0 new candidates on second run."""
        bronze = [b for b in bronze_prices_pre_split if b["instrument_id"] == 1001]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)
        as_of = date(2024, 1, 16)

        with patch.object(detector, "_fetch_vendor_prices", return_value={1001: 100.00}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                first_run = detector.detect_and_record(as_of_date=as_of)

            # Second run — 1001 is now in open sagas
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value={1001}):
                second_run = detector.detect_and_record(as_of_date=as_of)

        assert 1001 in first_run
        assert 1001 not in second_run


# ---------------------------------------------------------------------------
# Group 3 — Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_empty_bronze_returns_empty(self):
        """No bronze data — detect_and_record must return empty list, not crash."""
        spark = _make_spark_with_bronze([])
        detector, _ = _make_detector(spark)
        result = detector.detect_and_record(as_of_date=date(2024, 1, 16))
        assert result == []

    def test_zero_bronze_price_skipped(self, edge_case_prices):
        """Bronze close=0 — detector must skip this instrument (division by zero protection)."""
        bronze = [{"instrument_id": 9001, "close": 0.00}]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={9001: 100.00}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 9001 not in result

    def test_zero_vendor_price_skipped(self, edge_case_prices):
        """Vendor returned price=0 — skip this instrument (no division by zero, no false flag)."""
        bronze = [{"instrument_id": 9002, "close": 100.00}]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={9002: 0.00}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 9002 not in result

    def test_none_vendor_price_skipped(self, edge_case_prices):
        """Vendor returned None (API timeout / missing data) — instrument skipped."""
        bronze = [{"instrument_id": 9003, "close": 100.00}]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={9003: None}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 9003 not in result

    def test_partial_vendor_response(self, bronze_prices_pre_split, partial_vendor_response):
        """Vendor returns data for only some instruments — others skipped, no crash."""
        spark = _make_spark_with_bronze(bronze_prices_pre_split)
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value=partial_vendor_response):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        # Only instruments in partial_vendor_response can possibly be in result
        for instrument_id in result:
            assert instrument_id in partial_vendor_response

    def test_duplicate_bronze_records_uses_latest_version(self, duplicate_bronze_records):
        """
        Two records for same instrument_id+date with different record_versions.
        Detector must use record_version=2 (close=195.00), not record_version=1 (close=200.00).
        Vendor returns 97.50 — ratio = 97.50/195.00 = 0.50 → SPLIT detected.
        If wrong version used: 97.50/200.00 = 0.49 — still detected but wrong base price stored.
        """
        # The Bronze SQL already uses ROW_NUMBER() for dedup — verify the query contains it
        spark = MagicMock()
        bronze_df = MagicMock()
        # Return only the record_version=2 row (as the SQL ROW_NUMBER dedup would)
        bronze_df.collect.return_value = [_row(instrument_id=1001, close=195.00)]
        open_df = MagicMock()
        open_df.collect.return_value = []
        insert_df = MagicMock()
        insert_df.collect.return_value = []

        def sql_side_effect(query):
            if "market_data_daily" in query:
                return bronze_df
            if "status NOT IN" in query:
                return open_df
            return insert_df

        spark.sql.side_effect = sql_side_effect
        detector, _ = _make_detector(spark)

        with patch.object(detector, "_fetch_vendor_prices", return_value={1001: 97.50}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                detector.detect_and_record(as_of_date=date(2024, 1, 16))

        # Verify the Bronze SQL uses ROW_NUMBER() for deduplication
        bronze_sql = spark.sql.call_args_list[0][0][0]
        assert "ROW_NUMBER" in bronze_sql
        assert "record_version DESC" in bronze_sql

    def test_very_large_prices_handled(self, edge_case_prices):
        """Berkshire Hathaway-style prices ($500,000) — ratio math must still work."""
        bronze = [{"instrument_id": 9009, "close": 500_000.00}]
        spark = _make_spark_with_bronze(bronze)
        detector, _ = _make_detector(spark)

        # 10:1 split: vendor now shows $50,000
        with patch.object(detector, "_fetch_vendor_prices", return_value={9009: 50_000.00}):
            with patch.object(detector, "_get_open_candidate_instrument_ids", return_value=set()):
                result = detector.detect_and_record(as_of_date=date(2024, 1, 16))

        assert 9009 in result


# ---------------------------------------------------------------------------
# Group 4 — Recovery
# ---------------------------------------------------------------------------

class TestRecovery:

    def test_recover_counts_non_terminal_only(self, candidates_mixed_states):
        """Only DETECTED, CLASSIFIED, RELOAD_TRIGGERED rows are non-terminal."""
        spark = MagicMock()
        recovery_df = MagicMock()
        # Simulate the SQL returning only non-terminal rows
        non_terminal = [c for c in candidates_mixed_states
                        if c["status"] not in ("RESOLVED", "FALSE_POSITIVE", "FAILED")]
        recovery_df.collect.return_value = [
            _row(candidate_id=c["candidate_id"], instrument_id=c["instrument_id"],
                 status=c["status"], detected_at="2024-01-16", retry_count=0)
            for c in non_terminal
        ]
        spark.sql.return_value = recovery_df

        detector, _ = _make_detector(spark)
        count = detector.recover_incomplete_sagas()

        assert count == 3  # DETECTED + CLASSIFIED + RELOAD_TRIGGERED

    def test_recover_returns_zero_when_all_resolved(self):
        """All candidates in terminal state — recovery returns 0."""
        spark = MagicMock()
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        spark.sql.return_value = empty_df

        detector, _ = _make_detector(spark)
        count = detector.recover_incomplete_sagas()
        assert count == 0
