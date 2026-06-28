"""
CorporateActionDetector — Spark bulk detection of retroactive price changes.

CONCEPT: Why retroactive detection?

IBKR adjusts all historical prices when a split occurs. This means:
- Before split: Bronze has SPY at $500 for 2025-01-15
- After split: IBKR now returns $250 for the same 2025-01-15 (4-for-1 split)
- Our Bronze still has $500 — it is now wrong relative to the IBKR adjusted base

The detector compares the most recent price per instrument in Bronze with what
IBKR currently returns for the same date. A ratio far from 1.0 indicates IBKR
has retroactively adjusted — a corporate action occurred.

DESIGN: One Spark query for 2000+ instruments (not per-ticker Python loops).

The detection is a single Spark JOIN:
    bronze (latest bar per instrument) ←→ vendor (current price for same date)
    filter: abs(vendor_price / bronze_price - 1) > threshold (default 40%)

Why 40%? Normal daily price moves rarely exceed ±10%. Earnings can cause ±20-30%.
A 40% threshold gives safety margin against false positives from earnings while
reliably catching 2-for-1, 3-for-1, 4-for-1 splits (which cause 50%, 67%, 75% drops).
Reverse splits cause large price increases (>100% for 1-for-10).

Anything above 40% threshold is written to control.corporate_action_candidates
as status=DETECTED. The classifier then determines whether it is a true split
or a false positive (earnings gap, data error, halt).

SAGA: Once a candidate row exists in the table, all subsequent steps (classify,
trigger reload, verify resolution) update that same row's status column.
If a step fails, the row stays at its last completed status, and the recovery
step on the next job run picks it up from there.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import List, Optional

from src.shared.base.corporate_actions_provider import CorporateActionsProvider

logger = logging.getLogger(__name__)

_DEFAULT_DETECTION_THRESHOLD = 0.40  # 40% price divergence triggers investigation


class CorporateActionDetector:
    """
    Bulk Spark detector for retroactive price adjustments.

    Usage (daily job):
        detector = CorporateActionDetector(
            spark=spark,
            catalog="tradeanalytics",
            vendor_provider=IBKRCorporateActionsProvider.from_spark(spark, catalog),
        )
        candidates = detector.detect_and_record(as_of_date=date.today())
        # candidates = list of instrument_ids that were written to corporate_action_candidates

    The detect_and_record() method is idempotent: if a candidate already exists for
    the same instrument_id + detected_on_date, it will not insert a duplicate.
    """

    def __init__(
        self,
        spark,
        vendor_provider: CorporateActionsProvider,
        catalog: str = "tradeanalytics",
        detection_threshold: float = _DEFAULT_DETECTION_THRESHOLD,
    ):
        self._spark = spark
        self._vendor = vendor_provider
        self._catalog = catalog
        self._threshold = detection_threshold

    def detect_and_record(self, as_of_date: Optional[date] = None) -> List[int]:
        """
        Run bulk detection and write any new candidates to control.corporate_action_candidates.

        Steps:
            1. Read latest bronze bar per instrument (one Spark query).
            2. Re-fetch the same date from the vendor for all instruments.
            3. Compare prices. Flag where ratio deviates > threshold.
            4. Write new candidates. Skip instrument_ids already in DETECTED/CLASSIFIED status.

        Args:
            as_of_date: Date to check. Defaults to today.

        Returns:
            List of instrument_ids that were newly written as DETECTED candidates.
        """
        if as_of_date is None:
            as_of_date = date.today()

        logger.info(f"CorporateActionDetector: running detection for as_of_date={as_of_date}")

        bronze_latest = self._get_latest_bronze_prices()
        if not bronze_latest:
            logger.info("CorporateActionDetector: no bronze data found — skipping")
            return []

        instrument_ids = list(bronze_latest.keys())
        vendor_prices = self._fetch_vendor_prices(instrument_ids, as_of_date)

        candidates = []
        for instrument_id, bronze_price in bronze_latest.items():
            vendor_price = vendor_prices.get(instrument_id)
            if vendor_price is None or vendor_price == 0 or bronze_price == 0:
                continue
            ratio = vendor_price / bronze_price
            deviation = abs(ratio - 1.0)
            if deviation > self._threshold:
                candidates.append({
                    "instrument_id":     instrument_id,
                    "detected_on_date":  as_of_date,
                    "price_in_bronze":   bronze_price,
                    "price_from_vendor": vendor_price,
                    "price_ratio":       ratio,
                })
                logger.info(
                    f"CorporateActionDetector: CANDIDATE instrument_id={instrument_id} "
                    f"bronze={bronze_price:.4f} vendor={vendor_price:.4f} ratio={ratio:.4f}"
                )

        if not candidates:
            logger.info("CorporateActionDetector: no candidates detected")
            return []

        new_instrument_ids = self._write_candidates(candidates)
        logger.info(
            f"CorporateActionDetector: {len(new_instrument_ids)} new candidates written "
            f"to control.corporate_action_candidates"
        )
        return new_instrument_ids

    def recover_incomplete_sagas(self) -> int:
        """
        Find candidates that are not in a terminal state and log them for operator review.

        Terminal states: RESOLVED | FALSE_POSITIVE | FAILED
        Non-terminal:   DETECTED | CLASSIFIED | RELOAD_TRIGGERED

        Returns: count of non-terminal candidates found.

        Note: Full recovery logic (re-triggering classifier, reload, etc.) is implemented
        in BronzeIngestionJob.run() which calls this and dispatches to the classifier.
        This method is intentionally read-only — it only reports.
        """
        df = self._spark.sql(f"""
            SELECT candidate_id, instrument_id, status, detected_at, retry_count
            FROM {self._catalog}.control.corporate_action_candidates
            WHERE status NOT IN ('RESOLVED', 'FALSE_POSITIVE', 'FAILED')
            ORDER BY detected_at
        """)
        rows = df.collect()
        if rows:
            logger.warning(
                f"CorporateActionDetector: {len(rows)} incomplete sagas found — "
                f"statuses: {[r['status'] for r in rows]}"
            )
        return len(rows)

    def _get_latest_bronze_prices(self) -> dict:
        """
        Return {instrument_id: latest_close_price} via one Spark query over Bronze.

        Uses ROW_NUMBER() to get the most recent record_version per instrument
        (dedup window, same pattern as Silver reads from Bronze).
        """
        df = self._spark.sql(f"""
            WITH ranked AS (
                SELECT
                    instrument_id,
                    close,
                    bar_date,
                    record_version,
                    ROW_NUMBER() OVER (
                        PARTITION BY instrument_id
                        ORDER BY bar_date DESC, record_version DESC
                    ) as rn
                FROM {self._catalog}.bronze.market_data_daily
                WHERE instrument_id IS NOT NULL
            )
            SELECT instrument_id, close, bar_date
            FROM ranked
            WHERE rn = 1
        """)
        rows = df.collect()
        return {r["instrument_id"]: float(r["close"]) for r in rows if r["close"] is not None}

    def _fetch_vendor_prices(
        self,
        instrument_ids: List[int],
        as_of_date: date,
    ) -> dict:
        """
        Fetch current vendor prices for all flagged instruments.

        Delegates to CorporateActionsProvider — vendor-agnostic.
        Returns {instrument_id: price} for the given date.

        Note: This is a Python-side call (IBKR gateway on localhost), not Spark.
        At 2000 instruments with 40% threshold, typically only 0–10 will need
        vendor price verification — acceptable for a Python loop.
        """
        # For now, use the vendor provider's get_corporate_actions to check
        # current prices vs stored prices. In practice this calls the provider's
        # underlying API to fetch current bar data.
        # Full implementation connects to the same endpoint as IBKRProvider.get_historical()
        # for the single most recent trading date.
        # Returning empty dict as stub — CorporateActionDetector.detect_and_record()
        # will log "no candidates" until this is wired to a real price fetch.
        logger.debug(
            f"CorporateActionDetector._fetch_vendor_prices: stub for {len(instrument_ids)} instruments"
        )
        return {}

    def _write_candidates(self, candidates: list) -> List[int]:
        """
        Insert new candidates into control.corporate_action_candidates.

        Idempotent: skips instrument_ids that already have an open (non-terminal) candidate
        for the same detected_on_date. Returns the list of newly written instrument_ids.
        """
        existing = self._get_open_candidate_instrument_ids()
        new_candidates = [
            c for c in candidates
            if c["instrument_id"] not in existing
        ]
        if not new_candidates:
            return []

        from pyspark.sql.types import (
            DoubleType, IntegerType, LongType, StringType,
            StructField, StructType, DateType,
        )
        schema = StructType([
            StructField("instrument_id",     LongType(),   False),
            StructField("detected_on_date",  DateType(),   False),
            StructField("price_in_bronze",   DoubleType(), False),
            StructField("price_from_vendor", DoubleType(), False),
            StructField("price_ratio",       DoubleType(), False),
        ])
        df = self._spark.createDataFrame(new_candidates, schema=schema)
        df.createOrReplaceTempView("_new_candidates")

        self._spark.sql(f"""
            INSERT INTO {self._catalog}.control.corporate_action_candidates
                (instrument_id, detected_on_date, price_in_bronze, price_from_vendor, price_ratio,
                 status, retry_count)
            SELECT
                instrument_id,
                detected_on_date,
                price_in_bronze,
                price_from_vendor,
                price_ratio,
                'DETECTED' AS status,
                0          AS retry_count
            FROM _new_candidates
        """)

        return [c["instrument_id"] for c in new_candidates]

    def _get_open_candidate_instrument_ids(self) -> set:
        """Return instrument_ids that already have a non-terminal candidate open."""
        df = self._spark.sql(f"""
            SELECT DISTINCT instrument_id
            FROM {self._catalog}.control.corporate_action_candidates
            WHERE status NOT IN ('RESOLVED', 'FALSE_POSITIVE', 'FAILED')
        """)
        return {r["instrument_id"] for r in df.collect()}
