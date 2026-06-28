
"""
TradeAnalytics Data Quality Validator
======================================
Orchestrates validation of a batch of raw OHLCV records.
Uses RuleEngine with stream-specific configuration.

Usage:
    from src.bronze.validation.validator import DataQualityValidator
    from src.shared.config.config_loader import ConfigLoader

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

from src.shared.config.config_loader import ConfigNode, _find_repo_root
from src.bronze.validation.models import (
    ValidationSummary, ValidationOutcome
)
from src.bronze.validation.rule_engine import RuleEngine

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """
    Validates batches of raw OHLCV records against stream-specific rules.
    Use DataQualityValidator.for_stream() to create a stream-aware instance.
    """

    def __init__(
        self,
        engine: RuleEngine,
        stream_name: str = "daily",
        provider_nullable_fields: Optional[List[str]] = None,
    ):
        self._engine                   = engine
        self._stream_name              = stream_name
        self._provider_nullable_fields = set(provider_nullable_fields or [])

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
            rules_path = repo_root / "config" / "quality" / "data_quality_rules.yml"

        # Get stream config
        stream_cfg = getattr(config, stream_name, None)
        if stream_cfg is None:
            raise ValueError(
                f"Stream '{stream_name}' not found in config. "
                f"Available: check config/streams/*.yml"
            )

        # Extract validation settings from stream config
        validation_cfg = stream_cfg.get(f"{stream_name}.validation")

        thresholds             = {}
        additional_rules       = []
        disabled_rules         = []
        provider_nullable_fields: list = []

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

            # Extract provider-declared nullable fields — these are legitimately
            # absent for certain providers (e.g. IBKR does not supply adj_close)
            # and must be excluded from the data_completeness_pct calculation.
            nullable_raw = config.get(
                f"{stream_name}.validation.provider_nullable_fields",
                default=[]
            )
            if nullable_raw:
                provider_nullable_fields = list(nullable_raw)

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

        return cls(
            engine=engine,
            stream_name=stream_name,
            provider_nullable_fields=provider_nullable_fields,
        )

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
        ingestion_type: str = "scheduled",
        instrument_id: Optional[int] = None,
        ingested_by: Optional[str] = None,
    ) -> ValidationSummary:
        """
        Validate a batch of raw OHLCV records for one symbol + interval.

        Args:
            symbol:           Ticker symbol
            interval:         Data interval
            batch_id:         Unique batch identifier (stamped on every record)
            raw_records:      Raw dicts from provider
            pipeline_version: Git SHA (stamped on every record)
            ingestion_type:   From IngestionMode.ingestion_type — "backfill" |
                              "scheduled" | "amendment" (stamped on every record)
            instrument_id:    Permanent surrogate key from reference.instrument
                              (stamped on every record for Silver joins)
            ingested_by:      Provider name (e.g. "ibkr", "yahoo") — stamped for audit
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
                ingestion_type=ingestion_type,
                instrument_id=instrument_id,
                ingested_by=ingested_by,
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
        ingestion_type: str,
        summary: ValidationSummary,
        instrument_id: Optional[int] = None,
        ingested_by: Optional[str] = None,
    ) -> None:
        fired_results = self._engine.apply(record)
        rejections = [r for r in fired_results
                      if r.outcome == ValidationOutcome.REJECTED.value]
        flags      = [r for r in fired_results
                      if r.outcome == ValidationOutcome.FLAGGED.value]
        record_date = str(record.get("bar_date", "unknown"))

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
            enriched = self._enrich_record(
                record, [f.rule_name for f in flags], True,
                batch_id, pipeline_version, ingestion_type,
                instrument_id=instrument_id, ingested_by=ingested_by,
            )
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
                self._enrich_record(
                    record, [], False,
                    batch_id, pipeline_version, ingestion_type,
                    instrument_id=instrument_id, ingested_by=ingested_by,
                )
            )
            summary.total_passed += 1

    def _enrich_record(
        self,
        record: dict,
        flag_reasons: list,
        flag: bool,
        batch_id: str = "unknown",
        pipeline_version: str = "unknown",
        ingestion_type: str = "scheduled",
        instrument_id: Optional[int] = None,
        ingested_by: Optional[str] = None,
    ) -> dict:
        """
        Enrich a validated record with all audit and pipeline fields.

        This is the ONLY place audit fields are stamped onto records.
        Records flow as plain dicts through the pipeline — BronzeRecord
        is not instantiated during ingestion. All fields that BronzeRecord
        __post_init__ would set must be explicitly stamped here.

        Fields stamped:
          Group 7 (Data Quality): data_quality_flag, data_quality_reasons,
                                  data_completeness_pct
          Group 8 (Pipeline Audit): batch_id, pipeline_version, record_version,
                                    ingested_at, ingestion_type, is_amended,
                                    amendment_reason, supersedes_batch
          Group 4 (Session): session_open, session_close (None if provider
                              cannot supply)
        """
        enriched = record.copy()

        # ── Group 7: Data Quality ─────────────────────────────────────────
        enriched["data_quality_flag"]    = flag
        enriched["data_quality_reasons"] = json.dumps(flag_reasons)
        enriched["data_completeness_pct"] = self._compute_completeness(enriched)

        # ── Group 8: Pipeline Audit ───────────────────────────────────────
        enriched["batch_id"]          = batch_id
        enriched["pipeline_version"]  = pipeline_version
        enriched["record_version"]    = enriched.get("record_version", 1)
        enriched["ingested_at"]       = datetime.now(timezone.utc).isoformat()
        enriched["ingestion_type"]    = ingestion_type
        enriched["is_amended"]        = enriched.get("is_amended", False)
        enriched["amendment_reason"]  = enriched.get("amendment_reason", None)
        enriched["supersedes_batch"]  = enriched.get("supersedes_batch", None)
        enriched["ingested_by"]       = ingested_by or enriched.get("ingested_by", None)
        enriched["fetch_attempt_count"] = enriched.get("fetch_attempt_count", 1)
        enriched["instrument_id"]     = instrument_id

        # ── Group 4: Session Context (provider may not supply) ────────────
        enriched.setdefault("session_open",  None)
        enriched.setdefault("session_close", None)

        return enriched

    def _compute_completeness(self, record: dict) -> float:
        """
        Compute data completeness score (0–100).
        Core fields weighted 80%, optional fields 20%.
        Fields in provider_nullable_fields are excluded from the denominator —
        they are legitimately absent for certain providers (e.g. IBKR adj_close).
        """
        core_fields = [
            "open", "high", "low", "close", "volume",
            "adj_close", "prev_close", "vwap",
        ]
        optional_fields = [
            "trade_count", "pre_market_volume", "post_market_volume",
            "shares_outstanding", "exchange",
        ]
        # Drop provider-declared nullable fields from scoring
        nullable = self._provider_nullable_fields
        core_scored     = [f for f in core_fields if f not in nullable]
        optional_scored = [f for f in optional_fields if f not in nullable]

        core_present     = sum(1 for f in core_scored if record.get(f) is not None)
        optional_present = sum(1 for f in optional_scored if record.get(f) is not None)

        core_score     = (core_present / len(core_scored)) * 80 if core_scored else 80.0
        optional_score = (optional_present / len(optional_scored)) * 20 if optional_scored else 20.0
        return round(core_score + optional_score, 1)

    def _build_rejected_record(self, record, rule_name, reason, batch_id, pipeline_version):
        return {
            "symbol":           record.get("symbol", "unknown"),
            "bar_date":         str(record.get("bar_date", "unknown")),
            "bar_interval":         record.get("bar_interval", "unknown"),
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
            date_val = r.get("bar_date")
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

