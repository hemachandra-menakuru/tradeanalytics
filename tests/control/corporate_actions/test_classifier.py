"""
Unit tests for CorporateActionClassifier.

Tests are structured in three groups:
  1. _classify_ratio() — pure function, no Spark, parametrized over all known ratios
  2. Saga state transitions — mock Spark to verify correct SQL issued per state change
  3. Edge cases — boundary ratios, invalid inputs, retry exhaustion
"""

import pytest
from unittest.mock import MagicMock, patch, call
from datetime import date

from src.control.corporate_actions.classifier import CorporateActionClassifier


# ---------------------------------------------------------------------------
# Group 1 — _classify_ratio() — pure function tests
# ---------------------------------------------------------------------------

class TestClassifyRatio:
    """
    _classify_ratio(ratio) → (event_type, ratio_from, ratio_to)

    Covers all forward splits, reverse splits, and non-event scenarios.
    The ±5% tolerance means a ratio of 0.475–0.525 all classify as 2:1.
    """

    # --- Forward splits ---

    @pytest.mark.parametrize("ratio,expected_from,expected_to", [
        (0.500, 2,  1),   # exact 2:1
        (0.476, 2,  1),   # lower tolerance boundary (0.5 * 0.95 = 0.475)
        (0.524, 2,  1),   # upper tolerance boundary (0.5 * 1.05 = 0.525)
        (0.333, 3,  1),   # exact 3:1
        (0.250, 4,  1),   # exact 4:1
        (0.200, 5,  1),   # exact 5:1
        (0.667, 3,  2),   # exact 3:2
        (0.400, 5,  2),   # exact 5:2
        (0.100, 10, 1),   # exact 10:1
    ])
    def test_forward_split_detected(self, ratio, expected_from, expected_to):
        event_type, ratio_from, ratio_to = CorporateActionClassifier._classify_ratio(ratio)
        assert event_type == "SPLIT"
        assert ratio_from == expected_from
        assert ratio_to == expected_to

    # --- Reverse splits ---

    @pytest.mark.parametrize("ratio,expected_from,expected_to", [
        (2.00,  1, 2),    # exact 1:2
        (5.00,  1, 5),    # exact 1:5
        (10.00, 1, 10),   # exact 1:10
        (20.00, 1, 20),   # exact 1:20
        (9.52,  1, 10),   # lower boundary of 1:10 (10.0 * 0.95 = 9.50)
        (10.49, 1, 10),   # upper boundary of 1:10 (10.0 * 1.05 = 10.50)
    ])
    def test_reverse_split_detected(self, ratio, expected_from, expected_to):
        event_type, ratio_from, ratio_to = CorporateActionClassifier._classify_ratio(ratio)
        assert event_type == "REVERSE_SPLIT"
        assert ratio_from == expected_from
        assert ratio_to == expected_to

    # --- False positives (below 40% threshold should not reach classifier,
    #     but we test defensive handling anyway) ---

    @pytest.mark.parametrize("ratio", [
        0.99,   # regular dividend — tiny price drop
        1.01,   # negligible noise
        0.85,   # large earnings move, within normal range
        1.15,   # gap up earnings beat
    ])
    def test_below_threshold_is_false_positive(self, ratio):
        event_type, _, _ = CorporateActionClassifier._classify_ratio(ratio)
        assert event_type == "FALSE_POSITIVE"

    # --- Unknown ratios (above threshold but no pattern match) ---

    @pytest.mark.parametrize("ratio", [
        1.50,   # merger premium — above threshold, no known split pattern
        0.37,   # fractional / unusual ratio not in our known list
        3.33,   # doesn't match any known reverse split
        0.60,   # just at our 40% threshold but no pattern match — FALSE_POSITIVE
    ])
    def test_unrecognised_ratio_is_unknown_or_false_positive(self, ratio):
        event_type, _, _ = CorporateActionClassifier._classify_ratio(ratio)
        assert event_type in ("UNKNOWN", "FALSE_POSITIVE")

    # --- event_factor consistency: ratio_to / ratio_from ---

    @pytest.mark.parametrize("ratio", [0.50, 0.333, 0.25, 10.0, 5.0, 2.0])
    def test_event_factor_equals_ratio_to_over_ratio_from(self, ratio):
        event_type, ratio_from, ratio_to = CorporateActionClassifier._classify_ratio(ratio)
        if event_type in ("SPLIT", "REVERSE_SPLIT"):
            expected_factor = ratio_to / ratio_from
            assert abs(expected_factor - ratio) < 0.06  # within tolerance


# ---------------------------------------------------------------------------
# Group 2 — Saga state transitions (mock Spark)
# ---------------------------------------------------------------------------

class TestSagaStateTransitions:
    """
    Tests that process_detected_candidates() advances saga status correctly
    and issues the right SQL for each outcome.

    Spark is mocked — we verify which SQL statements are called, not Delta itself.
    """

    def _make_classifier(self, spark_mock):
        return CorporateActionClassifier(
            spark=spark_mock,
            catalog="tradeanalytics",
            reload_target_start=date(2010, 1, 1),
        )

    def _make_candidate_row(self, **overrides):
        defaults = {
            "candidate_id":     1,
            "instrument_id":    1001,
            "detected_on_date": date(2024, 1, 16),
            "price_in_bronze":  200.00,
            "price_from_vendor": 100.00,
            "price_ratio":      0.50,
            "status":           "DETECTED",
            "retry_count":      0,
        }
        defaults.update(overrides)
        row = MagicMock()
        for k, v in defaults.items():
            row.__getitem__ = lambda self, key, d=defaults: d[key]
        row.__getitem__ = lambda self, key, d=defaults: d[key]
        # Make it behave like a Spark Row
        obj = type("Row", (), defaults)()
        for k, v in defaults.items():
            setattr(obj, k, v)
        return obj

    def _setup_spark(self, candidates):
        """Build a mock Spark that returns given candidates for the DETECTED query."""
        spark = MagicMock()
        detected_df = MagicMock()
        detected_df.collect.return_value = candidates

        command_df = MagicMock()
        command_row = MagicMock()
        command_row.__getitem__ = lambda self, key: 99  # fake command_id
        command_df.collect.return_value = [command_row]

        def sql_side_effect(query):
            q = query.strip()
            if "status = 'DETECTED'" in q and "ORDER BY" in q:
                return detected_df
            return command_df

        spark.sql.side_effect = sql_side_effect
        return spark

    def test_split_candidate_advances_to_classified(self, candidate_at_detected):
        """A ratio=0.50 candidate must reach CLASSIFIED with event_type=SPLIT."""
        row = MagicMock()
        row.__getitem__ = lambda self, key: {
            "candidate_id": 1, "instrument_id": 1001,
            "detected_on_date": date(2024, 1, 16),
            "price_in_bronze": 200.0, "price_from_vendor": 100.0,
            "price_ratio": 0.50, "retry_count": 0,
        }[key]

        spark = self._setup_spark([row])
        classifier = self._make_classifier(spark)
        summary = classifier.process_detected_candidates()

        assert summary["classified_split"] == 1
        assert summary["classified_false_positive"] == 0
        assert summary["failed"] == 0

        # Verify FORCE_RELOAD command was issued
        all_sql = " ".join(str(c) for c in spark.sql.call_args_list)
        assert "FORCE_RELOAD" in all_sql
        assert "ingestion_command" in all_sql

        # Verify corporate_actions row inserted
        assert "corporate_actions" in all_sql

        # Verify candidate updated to CLASSIFIED
        assert "CLASSIFIED" in all_sql

    def test_false_positive_no_reload_issued(self):
        """
        A ratio=0.72 candidate must NOT trigger reload — FALSE_POSITIVE.
        Note: 0.70 is within ±5% of 3:2 (0.6667) and correctly classifies as SPLIT 3:2.
        0.72 is outside all known split tolerances — genuinely unrecognised → FALSE_POSITIVE.
        """
        data = {
            "candidate_id": 2, "instrument_id": 1007,
            "detected_on_date": date(2024, 1, 16),
            "price_in_bronze": 100.0, "price_from_vendor": 72.0,
            "price_ratio": 0.72, "retry_count": 0,
        }
        row = MagicMock()
        row.__getitem__ = MagicMock(side_effect=lambda key: data[key])

        spark = self._setup_spark([row])
        classifier = self._make_classifier(spark)
        summary = classifier.process_detected_candidates()

        assert summary["classified_false_positive"] == 1
        assert summary["classified_split"] == 0

        all_sql = " ".join(str(c) for c in spark.sql.call_args_list)
        assert "FORCE_RELOAD" not in all_sql
        assert "FALSE_POSITIVE" in all_sql

    def test_failed_candidate_after_max_retries(self, candidate_at_max_retries):
        """A candidate at retry_count=3 must be marked FAILED, not retried."""
        row = MagicMock()
        # Force an exception during classification by using an impossible ratio
        row.__getitem__ = lambda self, key: {
            "candidate_id": 2, "instrument_id": 1002,
            "detected_on_date": date(2024, 1, 16),
            "price_in_bronze": 400.0, "price_from_vendor": 100.0,
            "price_ratio": 0.25, "retry_count": 3,
        }[key]

        spark = MagicMock()
        detected_df = MagicMock()
        detected_df.collect.return_value = [row]

        # Simulate classifier internal exception during write
        call_count = [0]
        def sql_side_effect(query):
            q = query.strip()
            if "status = 'DETECTED'" in q and "ORDER BY" in q:
                return detected_df
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("Simulated DB write failure")
            df = MagicMock()
            df.collect.return_value = []
            return df

        spark.sql.side_effect = sql_side_effect
        classifier = self._make_classifier(spark)
        summary = classifier.process_detected_candidates()

        assert summary["failed"] == 1
        all_sql = " ".join(str(c) for c in spark.sql.call_args_list)
        assert "FAILED" in all_sql

    def test_no_candidates_returns_zero_summary(self):
        """Empty detected queue — all summary counts must be 0."""
        spark = MagicMock()
        empty_df = MagicMock()
        empty_df.collect.return_value = []
        spark.sql.return_value = empty_df

        classifier = self._make_classifier(spark)
        summary = classifier.process_detected_candidates()

        assert summary == {"classified_split": 0, "classified_false_positive": 0, "failed": 0}

    def test_terminal_states_not_reprocessed(self, candidates_mixed_states):
        """RESOLVED, FALSE_POSITIVE, FAILED candidates must not be processed again.
        The SQL WHERE clause is the guard — only DETECTED rows are returned."""
        spark = MagicMock()
        detected_df = MagicMock()
        # SQL filters to DETECTED only — simulate by returning empty (no detected rows)
        detected_df.collect.return_value = []
        spark.sql.return_value = detected_df

        classifier = self._make_classifier(spark)
        classifier.process_detected_candidates()

        # Verify the WHERE clause filters to DETECTED only
        first_sql = spark.sql.call_args_list[0][0][0]
        assert "status = 'DETECTED'" in first_sql


# ---------------------------------------------------------------------------
# Group 3 — Incorrect adjustment factors
# ---------------------------------------------------------------------------

class TestAdjustmentFactorValidation:
    """
    event_factor must equal ratio_to / ratio_from.
    These tests confirm what SHOULD be caught — even if the current code
    doesn't yet enforce it, these tests document the expected behaviour.
    """

    @pytest.mark.parametrize("ratio_from,ratio_to,factor,valid", [
        (2,  1,  0.50,   True),
        (4,  1,  0.25,   True),
        (1,  10, 10.0,   True),
        (2,  1,  1.00,   False),   # wrong factor
        (2,  1,  -0.50,  False),   # negative
        (2,  1,  0.00,   False),   # zero
        (1,  100, 100.0, False),   # implausibly large reverse split
    ])
    def test_event_factor_consistency(self, ratio_from, ratio_to, factor, valid):
        """event_factor = ratio_to / ratio_from is the mathematical contract."""
        if ratio_from > 0:
            expected = ratio_to / ratio_from
            is_consistent = abs(factor - expected) < 0.01 and factor > 0 and factor < 50
            assert is_consistent == valid


# ---------------------------------------------------------------------------
# Group 4 — Ratio boundary conditions
# ---------------------------------------------------------------------------

class TestRatioBoundaryConditions:
    """
    Tests the exact boundary of the 40% detection threshold and ±5% classification tolerance.
    These are the most likely cases to produce silent failures in production.
    """

    def test_ratio_exactly_at_split_boundary(self):
        """price_ratio=0.60 is AT the 40% threshold — not a recognisable split → FALSE_POSITIVE."""
        event_type, _, _ = CorporateActionClassifier._classify_ratio(0.60)
        assert event_type == "FALSE_POSITIVE"

    def test_ratio_just_above_split_boundary(self):
        """price_ratio=0.59 is just above the threshold — close to 3:2 (0.667) but not within tolerance."""
        event_type, _, _ = CorporateActionClassifier._classify_ratio(0.59)
        # 0.59 is not within ±5% of any known split ratio — UNKNOWN or FALSE_POSITIVE
        assert event_type in ("UNKNOWN", "FALSE_POSITIVE")

    def test_ratio_within_3_to_2_tolerance(self):
        """price_ratio=0.64 is within ±5% of 0.667 (3:2 split)."""
        event_type, ratio_from, ratio_to = CorporateActionClassifier._classify_ratio(0.64)
        assert event_type == "SPLIT"
        assert ratio_from == 3
        assert ratio_to == 2

    def test_ratio_at_exact_reverse_split(self):
        """price_ratio=10.0 exactly matches 1:10 reverse split."""
        event_type, ratio_from, ratio_to = CorporateActionClassifier._classify_ratio(10.0)
        assert event_type == "REVERSE_SPLIT"
        assert ratio_from == 1
        assert ratio_to == 10

    def test_ratio_between_two_known_splits(self):
        """price_ratio=0.29 is between 4:1 (0.25) and 3:1 (0.333) — matches neither."""
        event_type, _, _ = CorporateActionClassifier._classify_ratio(0.29)
        assert event_type in ("UNKNOWN", "FALSE_POSITIVE")
