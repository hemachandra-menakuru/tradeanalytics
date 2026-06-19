"""
TradeAnalytics Data Quality Rule Engine
========================================
Reads rules from config/data_quality_rules.yml and applies them
to raw OHLCV records. Returns RuleResult per rule per record.

Design principles:
  - Rules are data (YAML) — adding a new rule needs no code change
  - Each rule is a pure function: (record, rule_config) → RuleResult
  - Rule functions registered in _RULE_REGISTRY dict
  - Unknown rules in YAML raise at startup — fail fast
  - All rule functions receive the full raw dict — no assumptions about types

Adding a new rule:
  1. Add the rule definition to config/data_quality_rules.yml
  2. Add a function named _rule_{rule_name} below
  3. Register it in _RULE_REGISTRY at bottom of file
  4. Add a test in tests/ingestion/validation/test_rule_engine.py
"""

from __future__ import annotations

import logging
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.ingestion.validation.models import (
    RuleResult, RuleSeverity, RuleAction, ValidationOutcome
)

logger = logging.getLogger(__name__)

# Tolerance for float comparisons (avoid false failures from float precision)
_FLOAT_TOLERANCE = 1e-9


# ── Rule functions ─────────────────────────────────────────────────────────────
# Each function signature: (record: dict, rule_cfg: dict) -> Optional[str]
# Returns: None if rule PASSES, error message string if rule FAILS

def _rule_high_gte_low(record: dict, cfg: dict) -> Optional[str]:
    high, low = record.get("high"), record.get("low")
    if high is None or low is None:
        return None  # null check handled by no_null_ohlcv rule
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
    date_val = record.get("date")
    if date_val is None:
        return None
    try:
        if isinstance(date_val, str):
            from datetime import date
            date_val = date.fromisoformat(date_val)
        # weekday(): 5=Saturday, 6=Sunday
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
            f"(prev_close={prev_close}, close={close}) — review for split/error"
        )
    return None


def _rule_no_null_ohlcv(record: dict, cfg: dict) -> Optional[str]:
    for field in ("open", "high", "low", "close", "volume"):
        if record.get(field) is None:
            return f"Required field '{field}' is null"
    return None


def _rule_max_consecutive_missing_days(record: dict, cfg: dict) -> Optional[str]:
    # This is a batch-level rule — not applicable per-record
    # Handled separately in DataQualityValidator.check_continuity()
    return None


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
            f"(prev_close={prev_close}, open={open_}) — review for earnings/split"
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
        return (
            f"API fetch took {duration}ms > threshold {threshold}ms "
            f"— possible data quality issue"
        )
    return None


def _rule_pre_market_volume_non_negative(record: dict, cfg: dict) -> Optional[str]:
    vol = record.get("pre_market_volume")
    if vol is None:
        return None
    if vol < 0:
        return f"pre_market_volume ({vol}) cannot be negative"
    return None


# ── Rule registry ──────────────────────────────────────────────────────────────
# Maps rule name (from YAML) to rule function
# Add new rules here after implementing the function above

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
}


# ── Rule Engine ────────────────────────────────────────────────────────────────

class RuleEngine:
    """
    Loads rules from data_quality_rules.yml and applies them to records.
    Validates at startup that all rule names in YAML have implementations.

    Usage:
        engine = RuleEngine(rules_path)
        results = engine.apply(record_dict)
        # results is a list of RuleResult — one per rule that fired
    """

    def __init__(self, rules_path: Path):
        """
        Load and validate rules from YAML file.
        Raises at startup if any rule in YAML has no implementation.
        """
        if not rules_path.exists():
            raise FileNotFoundError(
                f"Data quality rules file not found: {rules_path}"
            )

        with open(rules_path) as f:
            raw = yaml.safe_load(f)

        self._rules: List[dict] = raw.get("quality_rules", {}).get("ohlcv", [])

        # Validate all rules have implementations — fail fast at startup
        unknown_rules = [
            r["rule"] for r in self._rules
            if r["rule"] not in _RULE_REGISTRY
        ]
        if unknown_rules:
            raise ValueError(
                f"Rules defined in YAML but not implemented: {unknown_rules}. "
                f"Add implementations to rule_engine.py and register in _RULE_REGISTRY."
            )

        logger.info(f"RuleEngine loaded {len(self._rules)} rules from {rules_path.name}")

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
        Rules that pass are not included — only failures matter.

        Args:
            record: Raw dict from provider (before BronzeRecord construction)

        Returns:
            List of RuleResult — empty list means all rules passed
        """
        fired_results = []

        for rule_cfg in self._rules:
            rule_name = rule_cfg["rule"]
            severity  = rule_cfg.get("severity", RuleSeverity.WARNING.value)
            action    = rule_cfg.get("action", RuleAction.FLAG_RECORD.value)

            # Skip alert-only rules — handled at batch level
            if action == RuleAction.ALERT.value:
                continue

            rule_fn = _RULE_REGISTRY[rule_name]

            try:
                error_msg = rule_fn(record, rule_cfg)
            except Exception as e:
                # Rule function itself crashed — log and treat as warning
                logger.error(
                    f"Rule '{rule_name}' raised exception: {e}. "
                    f"Record: symbol={record.get('symbol')}, "
                    f"date={record.get('date')}"
                )
                error_msg = f"Rule evaluation error: {e}"
                severity  = RuleSeverity.WARNING.value
                action    = RuleAction.FLAG_RECORD.value

            if error_msg is not None:
                # Rule fired — determine outcome from severity
                if severity == RuleSeverity.CRITICAL.value:
                    outcome = ValidationOutcome.REJECTED.value
                else:
                    outcome = ValidationOutcome.FLAGGED.value

                fired_results.append(RuleResult(
                    rule_name=rule_name,
                    outcome=outcome,
                    severity=severity,
                    action=action,
                    message=error_msg,
                ))

        return fired_results
