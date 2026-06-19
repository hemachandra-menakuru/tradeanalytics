"""
TradeAnalytics Data Quality Validator
======================================
Orchestrates validation of a batch of raw OHLCV records.
Uses RuleEngine to apply per-record rules and produces a ValidationSummary.

Usage:
    from src.ingestion.validation.validator import DataQualityValidator
    from src.config.config_loader import ConfigLoader

    config = ConfigLoader.load()
    validator = DataQualityValidator(config)

    raw_records = provider.get_historical("AAPL", start, end)
    summary = validator.validate_batch("AAPL", "1d", "batch_001", raw_records)

    # Write results
    spark.createDataFrame(summary.writable_records)...   # to main Bronze table
    spark.createDataFrame(summary.rejected_records)...   # to rejected table
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from src.config.config_loader import ConfigNode, _find_repo_root
from src.ingestion.validation.models import (
    ValidationSummary, ValidationOutcome
)
from src.ingestion.validation.rule_engine import RuleEngine

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Validates batches of raw OHLCV records against data quality rules.

    Responsibilities:
      - Apply per-record rules via RuleEngine
      - Classify each record as clean / flagged / rejected
      - Enrich accepted records with data_quality fields
      - Build RejectedRecord dicts for quarantine table
      - Produce ValidationSummary for logging and monitoring
      - Check batch-level continuity (missing days)
    """

    def __init__(self, config: ConfigNode, rules_path: Optional[Path] = None):
        """
        Args:
            config:     Loaded ConfigNode (from ConfigLoader.load())
            rules_path: Override path to data_quality_rules.yml (for testing)
        """
        if rules_path is None:
            repo_root = _find_repo_root()
            rules_path = repo_root / "src" / "reference" / "data_quality_rules.yml"

        self._engine = RuleEngine(rules_path)
        self._config = config
        logger.info(
            f"DataQualityValidator ready — "
            f"{self._engine.rule_count} rules loaded"
        )

    @property
    def rule_count(self) -> int:
        return self._engine.rule_count

    def validate_batch(
        self,
        symbol: str,
        interval: str,
        batch_id: str,
        raw_records: List[dict],
        pipeline_version: str = "unknown",
    ) -> ValidationSummary:
        """
        Validate a batch of raw OHLCV records for one symbol + interval.

        Args:
            symbol:           Ticker symbol e.g. "AAPL"
            interval:         Data interval e.g. "1d"
            batch_id:         Unique identifier for this ingestion run
            raw_records:      List of raw dicts from provider
            pipeline_version: Git SHA of current code (for audit trail)

        Returns:
            ValidationSummary with clean/flagged/rejected record lists
            and full audit log
        """
        summary = ValidationSummary(
            symbol=symbol,
            interval=interval,
            batch_id=batch_id,
            total_input=len(raw_records),
        )

        for record in raw_records:
            self._validate_record(
                record=record,
                symbol=symbol,
                interval=interval,
                batch_id=batch_id,
                pipeline_version=pipeline_version,
                summary=summary,
            )

        # Batch-level checks
        self._check_continuity(raw_records, symbol, interval, summary)

        logger.info(
            f"Validated {symbol}/{interval}: "
            f"{summary.total_passed} passed, "
            f"{summary.total_flagged} flagged, "
            f"{summary.total_rejected} rejected "
            f"({summary.pass_rate_pct}% pass rate)"
        )

        return summary

    def _validate_record(
        self,
        record: dict,
        symbol: str,
        interval: str,
        batch_id: str,
        pipeline_version: str,
        summary: ValidationSummary,
    ) -> None:
        """Apply all rules to a single record and update summary."""

        fired_results = self._engine.apply(record)

        rejections = [r for r in fired_results
                      if r.outcome == ValidationOutcome.REJECTED.value]
        flags      = [r for r in fired_results
                      if r.outcome == ValidationOutcome.FLAGGED.value]

        record_date = str(record.get("date", "unknown"))

        if rejections:
            primary_rejection = rejections[0]
            rejected_dict = self._build_rejected_record(
                record=record,
                rule_name=primary_rejection.rule_name,
                reason=primary_rejection.message or primary_rejection.rule_name,
                batch_id=batch_id,
                pipeline_version=pipeline_version,
            )
            summary.rejected_records.append(rejected_dict)
            summary.total_rejected += 1

            for r in rejections:
                summary.rejection_reasons[r.rule_name] = (
                    summary.rejection_reasons.get(r.rule_name, 0) + 1
                )
                summary.add_audit_entry(
                    record_date=record_date,
                    outcome=ValidationOutcome.REJECTED.value,
                    rule_name=r.rule_name,
                    message=r.message or "",
                )

        elif flags:
            enriched = self._enrich_record(
                record=record,
                flag_reasons=[f.rule_name for f in flags],
                data_quality_flag=True,
            )
            summary.flagged_records.append(enriched)
            summary.total_flagged += 1

            for f in flags:
                summary.flag_reasons[f.rule_name] = (
                    summary.flag_reasons.get(f.rule_name, 0) + 1
                )
                summary.add_audit_entry(
                    record_date=record_date,
                    outcome=ValidationOutcome.FLAGGED.value,
                    rule_name=f.rule_name,
                    message=f.message or "",
                )

        else:
            enriched = self._enrich_record(
                record=record,
                flag_reasons=[],
                data_quality_flag=False,
            )
            summary.clean_records.append(enriched)
            summary.total_passed += 1

    def _enrich_record(
        self,
        record: dict,
        flag_reasons: List[str],
        data_quality_flag: bool,
    ) -> dict:
        """Add data quality fields to an accepted record."""
        enriched = record.copy()
        enriched["data_quality_flag"]    = data_quality_flag
        enriched["data_quality_reasons"] = json.dumps(flag_reasons)
        return enriched

    def _build_rejected_record(
        self,
        record: dict,
        rule_name: str,
        reason: str,
        batch_id: str,
        pipeline_version: str,
    ) -> dict:
        """Build a dict for the bronze.market_data_rejected table."""
        return {
            "symbol":           record.get("symbol", "unknown"),
            "date":             str(record.get("date", "unknown")),
            "interval":         record.get("interval", "unknown"),
            "source":           record.get("source", "unknown"),
            "batch_id":         batch_id,
            "rejected_rule":    rule_name,
            "rejection_reason": reason,
            "raw_record":       json.dumps(record, default=str),
            "rejected_at":      datetime.now(timezone.utc).isoformat(),
            "raw_open":         record.get("open"),
            "raw_high":         record.get("high"),
            "raw_low":          record.get("low"),
            "raw_close":        record.get("close"),
            "raw_volume":       record.get("volume"),
            "reprocessed":      False,
            "reprocessed_at":   None,
            "reprocessed_batch": None,
            "pipeline_version": pipeline_version,
        }

    def _check_continuity(
        self,
        records: List[dict],
        symbol: str,
        interval: str,
        summary: ValidationSummary,
    ) -> None:
        """Check for consecutive missing trading days (daily only)."""
        if interval != "1d" or len(records) < 2:
            return

        from datetime import date

        accepted_dates = []
        for r in records:
            date_val = r.get("date")
            if date_val is None:
                continue
            try:
                if isinstance(date_val, str):
                    accepted_dates.append(date.fromisoformat(date_val))
                else:
                    accepted_dates.append(date_val)
            except ValueError:
                continue

        if len(accepted_dates) < 2:
            return

        accepted_dates.sort()
        max_gap = 0
        gap_start = None

        for i in range(1, len(accepted_dates)):
            delta = (accepted_dates[i] - accepted_dates[i-1]).days
            if delta > 3:
                trading_days_gap = delta - 2
                if trading_days_gap > max_gap:
                    max_gap = trading_days_gap
                    gap_start = accepted_dates[i-1]

        threshold = 5
        if max_gap > threshold:
            msg = (
                f"{symbol}/{interval}: {max_gap} consecutive trading days "
                f"missing after {gap_start} — possible ingestion gap"
            )
            logger.warning(msg)
            summary.add_audit_entry(
                record_date=str(gap_start),
                outcome="alert",
                rule_name="max_consecutive_missing_days",
                message=msg,
            )
