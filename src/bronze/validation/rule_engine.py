"""
TradeAnalytics Data Quality Rule Engine
========================================
Loads rules from data_quality_rules.yml and applies them to raw OHLCV records.
Supports tiered rules (shared + stream-specific) and per-stream threshold overrides.

Rule loading order:
  1. shared rules — always applied to all streams
  2. stream additional_rules — from streams/*.yml validation.additional_rules
  3. stream disabled_rules — shared rules disabled for this stream

Threshold resolution:
  1. Stream config validation.thresholds (highest priority)
  2. Rule default threshold from data_quality_rules.yml (fallback)

Adding a new rule:
  1. Add definition to data_quality_rules.yml under shared or stream section
  2. Implement _rule_{name} function below
  3. Register in _RULE_REGISTRY
  4. Add threshold override in streams/*.yml if threshold differs per stream
  5. Add test in tests/ingestion/validation/test_rule_engine.py
"""

from __future__ import annotations

import logging
import yaml
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from src.bronze.validation.models import (
    RuleResult, RuleSeverity, RuleAction, ValidationOutcome
)

logger = logging.getLogger(__name__)

_FLOAT_TOLERANCE = 1e-9


# ── Rule functions ─────────────────────────────────────────────────────────────
# Each: (record: dict, rule_cfg: dict) -> Optional[str]
# Returns None if PASSES, error message string if FAILS

def _rule_high_gte_low(record: dict, cfg: dict) -> Optional[str]:
    high, low = record.get("high"), record.get("low")
    if high is None or low is None:
        return None
    if high < low - _FLOAT_TOLERANCE:
        return f"High ({high}) must be >= Low ({low})"
    return None

def _rule_high_gte_open(record: dict, cfg: dict) -> Optional[str]:
    high, open_ = record.get("high"), record.get("open")
    if high is None or open_ is None:
        return None
    if high < open_ - _FLOAT_TOLERANCE:
        return f"High ({high}) must be >= Open ({open_})"
    return None

def _rule_high_gte_close(record: dict, cfg: dict) -> Optional[str]:
    high, close = record.get("high"), record.get("close")
    if high is None or close is None:
        return None
    if high < close - _FLOAT_TOLERANCE:
        return f"High ({high}) must be >= Close ({close})"
    return None

def _rule_low_lte_open(record: dict, cfg: dict) -> Optional[str]:
    low, open_ = record.get("low"), record.get("open")
    if low is None or open_ is None:
        return None
    if low > open_ + _FLOAT_TOLERANCE:
        return f"Low ({low}) must be <= Open ({open_})"
    return None

def _rule_low_lte_close(record: dict, cfg: dict) -> Optional[str]:
    low, close = record.get("low"), record.get("close")
    if low is None or close is None:
        return None
    if low > close + _FLOAT_TOLERANCE:
        return f"Low ({low}) must be <= Close ({close})"
    return None

def _rule_volume_positive(record: dict, cfg: dict) -> Optional[str]:
    volume = record.get("volume")
    if volume is None:
        return None
    if volume <= 0:
        return f"Volume ({volume}) must be > 0"
    return None

def _rule_price_positive(record: dict, cfg: dict) -> Optional[str]:
    for field in ("open", "high", "low", "close"):
        value = record.get(field)
        if value is not None and value <= 0:
            return f"{field.capitalize()} price ({value}) must be > 0"
    return None

def _rule_no_weekend_dates(record: dict, cfg: dict) -> Optional[str]:
    date_val = record.get("bar_date")
    if date_val is None:
        return None
    try:
        if isinstance(date_val, str):
            from datetime import date
            date_val = date.fromisoformat(date_val)
        if date_val.weekday() in (5, 6):
            day_name = "Saturday" if date_val.weekday() == 5 else "Sunday"
            return f"Date {date_val} falls on a {day_name} — markets closed"
    except (ValueError, AttributeError):
        return f"Cannot parse date: {repr(date_val)}"
    return None

def _rule_price_change_pct_threshold(record: dict, cfg: dict) -> Optional[str]:
    prev_close = record.get("prev_close")
    close = record.get("close")
    threshold = cfg.get("threshold", 50.0)
    if prev_close is None or close is None or prev_close <= 0:
        return None
    change_pct = abs((close - prev_close) / prev_close) * 100
    if change_pct > threshold:
        return (
            f"Price change {change_pct:.1f}% exceeds threshold {threshold}% "
            f"(prev_close={prev_close}, close={close})"
        )
    return None

def _rule_no_null_ohlcv(record: dict, cfg: dict) -> Optional[str]:
    for field in ("open", "high", "low", "close", "volume"):
        if record.get(field) is None:
            return f"Required field '{field}' is null"
    return None

def _rule_max_consecutive_missing_days(record: dict, cfg: dict) -> Optional[str]:
    return None  # batch-level rule — handled in DataQualityValidator

def _rule_vwap_between_low_and_high(record: dict, cfg: dict) -> Optional[str]:
    vwap = record.get("vwap")
    low = record.get("low")
    high = record.get("high")
    if vwap is None or low is None or high is None:
        return None
    if vwap < low - _FLOAT_TOLERANCE or vwap > high + _FLOAT_TOLERANCE:
        return f"VWAP ({vwap}) must be between Low ({low}) and High ({high})"
    return None

def _rule_prev_close_positive(record: dict, cfg: dict) -> Optional[str]:
    prev_close = record.get("prev_close")
    if prev_close is None:
        return None
    if prev_close <= 0:
        return f"prev_close ({prev_close}) must be > 0"
    return None

def _rule_trade_count_positive(record: dict, cfg: dict) -> Optional[str]:
    trade_count = record.get("trade_count")
    if trade_count is None:
        return None
    if trade_count <= 0:
        return f"trade_count ({trade_count}) should be > 0 on active trading days"
    return None

def _rule_gap_threshold(record: dict, cfg: dict) -> Optional[str]:
    prev_close = record.get("prev_close")
    open_ = record.get("open")
    threshold = cfg.get("threshold", 15.0)
    if prev_close is None or open_ is None or prev_close <= 0:
        return None
    gap_pct = abs((open_ - prev_close) / prev_close) * 100
    if gap_pct > threshold:
        return (
            f"Gap {gap_pct:.1f}% exceeds threshold {threshold}% "
            f"(prev_close={prev_close}, open={open_})"
        )
    return None

def _rule_amended_record_detected(record: dict, cfg: dict) -> Optional[str]:
    if record.get("is_amended", False):
        return f"Record is an amendment (version {record.get('record_version', '?')})"
    return None

def _rule_fetch_duration_threshold(record: dict, cfg: dict) -> Optional[str]:
    duration = record.get("fetch_duration_ms")
    threshold = cfg.get("threshold", 30000)
    if duration is None:
        return None
    if duration > threshold:
        return f"API fetch took {duration}ms > threshold {threshold}ms"
    return None

def _rule_pre_market_volume_non_negative(record: dict, cfg: dict) -> Optional[str]:
    vol = record.get("pre_market_volume")
    if vol is None:
        return None
    if vol < 0:
        return f"pre_market_volume ({vol}) cannot be negative"
    return None

def _rule_corporate_action_consistency(record: dict, cfg: dict) -> Optional[str]:
    """Corporate action flags must be internally consistent."""
    split_factor       = record.get("split_factor")
    dividend_amount    = record.get("dividend_amount", 0.0)
    is_ex_div          = record.get("is_ex_dividend_date", False)
    is_ex_split        = record.get("is_ex_split_date", False)
    has_corp_action    = record.get("has_corporate_action", False)

    issues = []

    # is_ex_split_date=True but split_factor is None
    if is_ex_split and split_factor is None:
        issues.append("is_ex_split_date=True but split_factor is None")

    # is_ex_dividend_date=True but dividend_amount=0
    if is_ex_div and (dividend_amount is None or dividend_amount <= 0):
        issues.append("is_ex_dividend_date=True but dividend_amount=0")

    # has_corporate_action=True but no action fields set
    if has_corp_action and split_factor is None and (dividend_amount or 0) <= 0:
        issues.append("has_corporate_action=True but no split_factor or dividend_amount")

    if issues:
        return f"Corporate action inconsistency: {'; '.join(issues)}"
    return None

# Placeholder rules for future stream implementations
def _rule_bar_within_market_hours(record: dict, cfg: dict) -> Optional[str]:
    return None  # Phase 3 implementation

def _rule_consistent_bar_spacing(record: dict, cfg: dict) -> Optional[str]:
    return None  # Phase 3 implementation

def _rule_consecutive_zero_volume(record: dict, cfg: dict) -> Optional[str]:
    return None  # Phase 5 implementation

def _rule_tick_price_spike(record: dict, cfg: dict) -> Optional[str]:
    return None  # Phase 5 implementation


# ── Rule registry ──────────────────────────────────────────────────────────────

_RULE_REGISTRY: Dict[str, Callable] = {
    "high_gte_low":                     _rule_high_gte_low,
    "high_gte_open":                    _rule_high_gte_open,
    "high_gte_close":                   _rule_high_gte_close,
    "low_lte_open":                     _rule_low_lte_open,
    "low_lte_close":                    _rule_low_lte_close,
    "volume_positive":                  _rule_volume_positive,
    "price_positive":                   _rule_price_positive,
    "no_weekend_dates":                 _rule_no_weekend_dates,
    "price_change_pct_threshold":       _rule_price_change_pct_threshold,
    "no_null_ohlcv":                    _rule_no_null_ohlcv,
    "max_consecutive_missing_days":     _rule_max_consecutive_missing_days,
    "vwap_between_low_and_high":        _rule_vwap_between_low_and_high,
    "prev_close_positive":              _rule_prev_close_positive,
    "trade_count_positive":             _rule_trade_count_positive,
    "gap_threshold":                    _rule_gap_threshold,
    "amended_record_detected":          _rule_amended_record_detected,
    "fetch_duration_threshold":         _rule_fetch_duration_threshold,
    "pre_market_volume_non_negative":   _rule_pre_market_volume_non_negative,
    "corporate_action_consistency":     _rule_corporate_action_consistency,
    "bar_within_market_hours":          _rule_bar_within_market_hours,
    "consistent_bar_spacing":           _rule_consistent_bar_spacing,
    "consecutive_zero_volume":          _rule_consecutive_zero_volume,
    "tick_price_spike":                 _rule_tick_price_spike,
}


# ── Rule Engine ────────────────────────────────────────────────────────────────

class RuleEngine:
    """
    Loads and applies data quality rules to OHLCV records.

    Supports tiered rule loading:
      shared rules    — always applied
      stream rules    — additional rules from stream config
      disabled rules  — shared rules skipped for this stream

    Supports per-stream threshold overrides:
      stream config validation.thresholds overrides rule defaults
    """

    def __init__(
        self,
        rules_path: Path,
        stream_name: str = "daily",
        stream_thresholds: Optional[Dict[str, Any]] = None,
        additional_rules: Optional[List[dict]] = None,
        disabled_rules: Optional[List[str]] = None,
    ):
        """
        Args:
            rules_path:        Path to data_quality_rules.yml
            stream_name:       Which stream (daily/intraday/tick)
            stream_thresholds: Threshold overrides from streams/*.yml
            additional_rules:  Extra rules from stream config
            disabled_rules:    Shared rules to skip for this stream
        """
        if not rules_path.exists():
            raise FileNotFoundError(
                f"Data quality rules file not found: {rules_path}"
            )

        with open(rules_path) as f:
            raw = yaml.safe_load(f)

        quality_rules = raw.get("quality_rules", {})

        # Load shared rules — always applied
        shared_rules: List[dict] = quality_rules.get("shared", [])

        # Apply stream threshold overrides to shared rules
        self._thresholds = stream_thresholds or {}
        if self._thresholds:
            shared_rules = self._apply_thresholds(shared_rules)

        # Apply disabled rules filter
        disabled: Set[str] = set(disabled_rules or [])
        shared_rules = [r for r in shared_rules if r["rule"] not in disabled]

        # Add stream-specific additional rules
        extra_rules = additional_rules or []
        if self._thresholds:
            extra_rules = self._apply_thresholds(extra_rules)

        self._rules: List[dict] = shared_rules + extra_rules

        # Validate all rules have implementations
        unknown = [r["rule"] for r in self._rules if r["rule"] not in _RULE_REGISTRY]
        if unknown:
            raise ValueError(
                f"Rules in config but not implemented: {unknown}. "
                f"Add to _RULE_REGISTRY in rule_engine.py."
            )

        logger.info(
            f"RuleEngine [{stream_name}]: "
            f"{len(shared_rules)} shared + {len(extra_rules)} stream-specific rules, "
            f"{len(disabled)} disabled"
        )

    def _apply_thresholds(self, rules: List[dict]) -> List[dict]:
        """Override rule thresholds with stream-specific values."""
        result = []
        for rule_cfg in rules:
            rule_name = rule_cfg["rule"]
            if rule_name in self._thresholds:
                # Create a copy with overridden threshold
                override = rule_cfg.copy()
                override["threshold"] = self._thresholds[rule_name]
                result.append(override)
            else:
                result.append(rule_cfg)
        return result

    @property
    def rule_count(self) -> int:
        return len(self._rules)

    @property
    def rule_names(self) -> List[str]:
        return [r["rule"] for r in self._rules]

    def apply(self, record: dict) -> List[RuleResult]:
        """
        Apply all rules to a single record.
        Returns list of RuleResult for rules that FIRED (failed).
        Empty list means all rules passed.
        """
        fired_results = []

        for rule_cfg in self._rules:
            rule_name = rule_cfg["rule"]
            severity  = rule_cfg.get("severity", RuleSeverity.WARNING.value)
            action    = rule_cfg.get("action", RuleAction.FLAG_RECORD.value)

            if action == RuleAction.ALERT.value:
                continue  # batch-level — skip per-record

            rule_fn = _RULE_REGISTRY[rule_name]

            try:
                error_msg = rule_fn(record, rule_cfg)
            except Exception as e:
                logger.error(
                    f"Rule '{rule_name}' raised exception: {e}. "
                    f"symbol={record.get('symbol')}, bar_date={record.get('bar_date')}"
                )
                error_msg = f"Rule evaluation error: {e}"
                severity  = RuleSeverity.WARNING.value
                action    = RuleAction.FLAG_RECORD.value

            if error_msg is not None:
                outcome = (
                    ValidationOutcome.REJECTED.value
                    if severity == RuleSeverity.CRITICAL.value
                    else ValidationOutcome.FLAGGED.value
                )
                fired_results.append(RuleResult(
                    rule_name=rule_name,
                    outcome=outcome,
                    severity=severity,
                    action=action,
                    message=error_msg,
                ))

        return fired_results
