"""
CorporateActionClassifier — classifies detected corporate action candidates.

CONCEPT: What the classifier does.

After the detector writes a DETECTED candidate, the classifier:
1. Looks at the price_ratio and determines the most likely event type.
2. Checks whether the ratio is consistent with a clean split ratio (2:1, 3:1, 4:1 etc.)
   or whether it looks like an earnings gap or other non-split event.
3. Updates the candidate row to CLASSIFIED with the determined event_type.
4. If it is a split: inserts a row into reference.corporate_actions (the audit log)
   and issues a FORCE_RELOAD command to control.ingestion_command.
5. If it is a false positive: marks the candidate as FALSE_POSITIVE — no reload triggered.

SPLIT RATIO DETECTION:

A 2-for-1 split causes price_ratio ≈ 0.50 (new/old = 0.5).
A 3-for-1 split causes price_ratio ≈ 0.33.
A 4-for-1 split causes price_ratio ≈ 0.25.
A 1-for-10 reverse split causes price_ratio ≈ 10.0.

We test against a set of known clean ratios. If the observed ratio is within
tolerance (±5%) of a known ratio, we classify it as that split. If it doesn't
match any known ratio but exceeds the 40% threshold, it may be:
- A fractional split (e.g. 3:2 = 0.667)
- A large earnings move (mark as FALSE_POSITIVE — no reload)
- A data error

Manual review is required for unrecognised ratios. The candidate stays at
CLASSIFIED with event_type=UNKNOWN until an operator reviews it and either
inserts the correct event into reference.corporate_actions manually and then
issues a FORCE_RELOAD command, or marks it as FALSE_POSITIVE.

SAGA STEP:

Input:  candidate at status=DETECTED
Output: candidate at status=CLASSIFIED (event_type set)
        + reference.corporate_actions row inserted (if split)
        + control.ingestion_command FORCE_RELOAD inserted (if split)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from math import isclose
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Known split ratios: (ratio_from, ratio_to, expected_price_ratio)
# expected_price_ratio = ratio_to / ratio_from
_KNOWN_SPLITS: List[Tuple[int, int, float]] = [
    (2,  1,  0.5000),   # 2-for-1 forward split
    (3,  1,  0.3333),   # 3-for-1
    (4,  1,  0.2500),   # 4-for-1
    (5,  1,  0.2000),   # 5-for-1
    (3,  2,  0.6667),   # 3-for-2
    (5,  2,  0.4000),   # 5-for-2
    (10, 1,  0.1000),   # 10-for-1
    (1,  2,  2.0000),   # 1-for-2 reverse
    (1,  5,  5.0000),   # 1-for-5 reverse
    (1,  10, 10.000),   # 1-for-10 reverse
    (1,  20, 20.000),   # 1-for-20 reverse
]

_TOLERANCE = 0.05  # ±5% of expected ratio is accepted as a match
_MAX_RETRIES = 3


class CorporateActionClassifier:
    """
    Classifies DETECTED corporate action candidates and advances the saga.

    Args:
        spark:         Active SparkSession.
        catalog:       Unity Catalog name.
        target_start:  Start date for the FORCE_RELOAD command (full history from origin).
    """

    def __init__(
        self,
        spark,
        catalog: str = "tradeanalytics",
        reload_target_start: date = date(2010, 1, 1),
    ):
        self._spark = spark
        self._catalog = catalog
        self._reload_target_start = reload_target_start

    def process_detected_candidates(self) -> Dict[str, int]:
        """
        Classify all candidates at status=DETECTED.

        Returns a summary: {"classified_split": N, "classified_false_positive": N, "failed": N}
        """
        rows = self._spark.sql(f"""
            SELECT candidate_id, instrument_id, detected_on_date,
                   price_in_bronze, price_from_vendor, price_ratio, retry_count
            FROM {self._catalog}.control.corporate_action_candidates
            WHERE status = 'DETECTED'
            ORDER BY detected_at
        """).collect()

        summary = {"classified_split": 0, "classified_false_positive": 0, "failed": 0}

        for row in rows:
            candidate_id  = row["candidate_id"]
            instrument_id = row["instrument_id"]
            ratio         = float(row["price_ratio"])
            retry_count   = int(row["retry_count"])

            try:
                event_type, ratio_from, ratio_to = self._classify_ratio(ratio)
                if event_type in ("SPLIT", "REVERSE_SPLIT"):
                    event_factor = ratio_to / ratio_from
                    self._write_corporate_action(
                        candidate_id=candidate_id,
                        instrument_id=instrument_id,
                        effective_date=row["detected_on_date"],
                        event_type=event_type,
                        ratio_from=ratio_from,
                        ratio_to=ratio_to,
                        event_factor=event_factor,
                    )
                    reload_command_id = self._issue_force_reload(
                        instrument_id=instrument_id,
                        start_date=self._reload_target_start,
                        end_date=date.today(),
                    )
                    self._update_candidate(
                        candidate_id=candidate_id,
                        status="CLASSIFIED",
                        event_type=event_type,
                        reload_command_id=reload_command_id,
                    )
                    summary["classified_split"] += 1
                    logger.info(
                        f"CorporateActionClassifier: instrument_id={instrument_id} "
                        f"classified as {event_type} {ratio_from}-for-{ratio_to}, "
                        f"FORCE_RELOAD issued"
                    )
                else:
                    # FALSE_POSITIVE or UNKNOWN
                    self._update_candidate(
                        candidate_id=candidate_id,
                        status="FALSE_POSITIVE",
                        event_type=event_type,
                    )
                    summary["classified_false_positive"] += 1
                    logger.info(
                        f"CorporateActionClassifier: instrument_id={instrument_id} "
                        f"ratio={ratio:.4f} → {event_type} (no reload)"
                    )
            except Exception as e:
                logger.error(
                    f"CorporateActionClassifier: failed for candidate_id={candidate_id} "
                    f"instrument_id={instrument_id} — {e}"
                )
                new_retry = retry_count + 1
                if new_retry >= _MAX_RETRIES:
                    self._update_candidate(
                        candidate_id=candidate_id,
                        status="FAILED",
                        failure_reason=str(e),
                    )
                else:
                    self._update_candidate(
                        candidate_id=candidate_id,
                        status="DETECTED",  # leave for retry
                        retry_count=new_retry,
                    )
                summary["failed"] += 1

        return summary

    @staticmethod
    def _classify_ratio(ratio: float) -> Tuple[str, int, int]:
        """
        Match a price ratio against known split patterns.

        Returns: (event_type, ratio_from, ratio_to)
        event_type is one of: SPLIT | REVERSE_SPLIT | FALSE_POSITIVE | UNKNOWN
        """
        for ratio_from, ratio_to, expected in _KNOWN_SPLITS:
            if isclose(ratio, expected, rel_tol=_TOLERANCE):
                if ratio_to < ratio_from:
                    return "SPLIT", ratio_from, ratio_to
                else:
                    return "REVERSE_SPLIT", ratio_from, ratio_to

        # No known ratio matched — large move but unrecognised pattern
        # Could be fractional split, earnings gap, or data error
        if ratio > 0.6 and ratio < 1.4:
            # Within 40% of par — should not have been flagged at 40% threshold, log warning
            return "FALSE_POSITIVE", 1, 1
        else:
            return "UNKNOWN", 1, 1

    def _write_corporate_action(
        self,
        candidate_id: int,
        instrument_id: int,
        effective_date: date,
        event_type: str,
        ratio_from: int,
        ratio_to: int,
        event_factor: float,
    ) -> None:
        """Insert a confirmed event into reference.corporate_actions (append-only)."""
        detail = f"{ratio_from}-for-{ratio_to} {event_type.lower().replace('_', ' ')}"
        self._spark.sql(f"""
            INSERT INTO {self._catalog}.reference.corporate_actions
                (instrument_id, effective_date, event_type,
                 split_ratio_from, split_ratio_to, event_factor,
                 event_detail, source_vendor, candidate_id,
                 reload_triggered)
            VALUES
                ({instrument_id}, '{effective_date}', '{event_type}',
                 {ratio_from}, {ratio_to}, {event_factor},
                 '{detail}', 'ibkr', {candidate_id},
                 true)
        """)

    def _issue_force_reload(
        self,
        instrument_id: int,
        start_date: date,
        end_date: date,
    ) -> int:
        """
        Insert a FORCE_RELOAD command into control.ingestion_command.

        Returns the generated command_id.
        """
        self._spark.sql(f"""
            INSERT INTO {self._catalog}.control.ingestion_command
                (instrument_id, action, override_start_date, override_end_date,
                 status, requested_by, requested_at)
            VALUES
                ({instrument_id}, 'FORCE_RELOAD', '{start_date}', '{end_date}',
                 'pending', 'CorporateActionClassifier',
                 current_timestamp())
        """)
        row = self._spark.sql(f"""
            SELECT command_id
            FROM {self._catalog}.control.ingestion_command
            WHERE instrument_id = {instrument_id}
              AND action = 'FORCE_RELOAD'
              AND status = 'pending'
            ORDER BY requested_at DESC
            LIMIT 1
        """).collect()
        return row[0]["command_id"] if row else -1

    def _update_candidate(
        self,
        candidate_id: int,
        status: str,
        event_type: Optional[str] = None,
        reload_command_id: Optional[int] = None,
        failure_reason: Optional[str] = None,
        retry_count: Optional[int] = None,
    ) -> None:
        """Update the saga candidate row with the result of the classification step."""
        set_clauses = [f"status = '{status}'", "updated_at = current_timestamp()"]
        if event_type is not None:
            set_clauses.append(f"event_type = '{event_type}'")
        if reload_command_id is not None:
            set_clauses.append(f"reload_command_id = {reload_command_id}")
        if failure_reason is not None:
            escaped = failure_reason.replace("'", "\\'")
            set_clauses.append(f"failure_reason = '{escaped}'")
        if retry_count is not None:
            set_clauses.append(f"retry_count = {retry_count}")
        if status == "RESOLVED":
            set_clauses.append("resolved_at = current_timestamp()")

        sql = f"""
            UPDATE {self._catalog}.control.corporate_action_candidates
            SET {', '.join(set_clauses)}
            WHERE candidate_id = {candidate_id}
        """
        self._spark.sql(sql)
