"""
TradeAnalytics Data Quality Validator
======================================
Orchestrates validation of a batch of raw OHLCV records.
Uses RuleEngine with stream-specific configuration.

Usage:
    from src.ingestion.validation.validator import DataQualityValidator
    from src.config.config_loader import ConfigLoader

    config = ConfigLoader.load()

    # Stream-aware validator
    validator = DataQualityValidator.for_stream(config, stream_name="daily")

    raw_records = provider.get_historical("AAPL", start, end)
    summary = validator.validate_batch("AAPL", "1d", "batch_001", raw_records)
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
    Validates batches of raw OHLCV records against stream-specific rules.
    Use DataQualityValidator.for_stream() to create a stream-aware instance.
    """

    def __init__(self, engine: RuleEngine, stream_name: str = "daily"):
        self._engine      = engine
        self._stream_name = stream_name

    @classmethod
    def for_stream(
        cls,
        config: ConfigNode,
        stream_name: str = "daily",
        rules_path: Optional[Path] = None,
    ) -> "DataQualityValidator":
        """
        Create a validator configured for a specific stream.
        Reads thresholds and rule overrides from streams/{stream_name}.yml

        Args:
            config:      Loaded ConfigNode
            stream_name: "daily" | "intraday" | "tick"
            rules_path:  Override rules file path (for testing)
        """
        if rules_path is None:
            repo_root  = _find_repo_root()
            rules_path = repo_root / "src" / "reference" / "data_quality_rules.yml"

        # Get stream config
        stream_cfg = getattr(config, stream_name, None)
        if stream_cfg is None:
            raise ValueError(
                f"Stream '{stream_name}' not found in config. "
                f"Available: check config/streams/*.yml"
            )

        # Extract validation settings from stream config
        validation_cfg = stream_cfg.get(f"{stream_name}.validation")

        thresholds       = {}
        additional_rules = []
        disabled_rules   = []

        if validation_cfg is not None:
            # Extract thresholds
            thresholds_cfg = stream_cfg.get(
                f"{stream_name}.validation.thresholds"
            )
            if thresholds_cfg is not None:
                thresholds = thresholds_cfg.to_dict() if hasattr(
                    thresholds_cfg, "to_dict"
                ) else dict(thresholds_cfg)

            # Extract additional rules
            additional_rules_raw = config.get(
                f"{stream_name}.validation.additional_rules",
                default=[]
            )
            if additional_rules_raw:
                additional_rules = list(additional_rules_raw)

            # Extract disabled rules
            disabled_rules_raw = config.get(
                f"{stream_name}.validation.disabled_rules",
                default=[]
            )
            if disabled_rules_raw:
                disabled_rules = list(disabled_rules_raw)

        engine = RuleEngine(
            rules_path=rules_path,
            stream_name=stream_name,
            stream_thresholds=thresholds,
            additional_rules=additional_rules,
            disabled_rules=disabled_rules,
        )

        logger.info(
            f"DataQualityValidator created for stream='{stream_name}' — "
            f"{engine.rule_count} rules, "
            f"{len(thresholds)} threshold overrides"
        )

        return cls(engine=engine, stream_name=stream_name)

    @classmethod
    def for_daily(cls, config: ConfigNode, rules_path: Optional[Path] = None):
        """Convenience constructor for daily stream."""
        return cls.for_stream(config, "daily", rules_path)

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

        self._check_continuity(raw_records, symbol, interval, summary)

        logger.info(
            f"[{self._stream_name}] Validated {symbol}/{interval}: "
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
        fired_results = self._engine.apply(record)
        rejections = [r for r in fired_results
                      if r.outcome == ValidationOutcome.REJECTED.value]
        flags      = [r for r in fired_results
                      if r.outcome == ValidationOutcome.FLAGGED.value]
        record_date = str(record.get("date", "unknown"))

        if rejections:
            primary = rejections[0]
            summary.rejected_records.append(self._build_rejected_record(
                record=record,
                rule_name=primary.rule_name,
                reason=primary.message or primary.rule_name,
                batch_id=batch_id,
                pipeline_version=pipeline_version,
            ))
            summary.total_rejected += 1
            for r in rejections:
                summary.rejection_reasons[r.rule_name] = (
                    summary.rejection_reasons.get(r.rule_name, 0) + 1
                )
                summary.add_audit_entry(record_date,
                    ValidationOutcome.REJECTED.value, r.rule_name, r.message or "")

        elif flags:
            enriched = self._enrich_record(record, [f.rule_name for f in flags], True)
            summary.flagged_records.append(enriched)
            summary.total_flagged += 1
            for f in flags:
                summary.flag_reasons[f.rule_name] = (
                    summary.flag_reasons.get(f.rule_name, 0) + 1
                )
                summary.add_audit_entry(record_date,
                    ValidationOutcome.FLAGGED.value, f.rule_name, f.message or "")

        else:
            summary.clean_records.append(
                self._enrich_record(record, [], False)
            )
            summary.total_passed += 1

    def _enrich_record(self, record, flag_reasons, flag):
        enriched = record.copy()
        enriched["data_quality_flag"]    = flag
        enriched["data_quality_reasons"] = json.dumps(flag_reasons)
        return enriched

    def _build_rejected_record(self, record, rule_name, reason, batch_id, pipeline_version):
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

    def _check_continuity(self, records, symbol, interval, summary):
        if interval != "1d" or len(records) < 2:
            return
        from datetime import date
        accepted_dates = []
        for r in records:
            date_val = r.get("date")
            if date_val is None:
                continue
            try:
                accepted_dates.append(
                    date.fromisoformat(date_val)
                    if isinstance(date_val, str) else date_val
                )
            except ValueError:
                continue
        if len(accepted_dates) < 2:
            return
        accepted_dates.sort()
        max_gap, gap_start = 0, None
        for i in range(1, len(accepted_dates)):
            delta = (accepted_dates[i] - accepted_dates[i-1]).days
            if delta > 3:
                trading_days_gap = delta - 2
                if trading_days_gap > max_gap:
                    max_gap = trading_days_gap
                    gap_start = accepted_dates[i-1]
        if max_gap > 5:
            msg = (
                f"{symbol}/{interval}: {max_gap} consecutive trading days "
                f"missing after {gap_start}"
            )
            logger.warning(msg)
            summary.add_audit_entry(str(gap_start), "alert",
                "max_consecutive_missing_days", msg)
