"""
TradeAnalytics Data Quality Validation Models
=============================================
Result objects returned by the RuleEngine and DataQualityValidator.
Keeps validation concerns separate from record models.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional


class ValidationOutcome(str, Enum):
    """Outcome for a single rule check against a single record."""
    PASSED   = "passed"    # rule check passed — no issue
    FLAGGED  = "flagged"   # warning — record kept but marked
    REJECTED = "rejected"  # critical — record must not enter Bronze


class RuleSeverity(str, Enum):
    """Severity level of a quality rule."""
    CRITICAL = "critical"  # failure → reject record
    WARNING  = "warning"   # failure → flag record


class RuleAction(str, Enum):
    """What to do when a rule fails."""
    REJECT_RECORD = "reject_record"
    FLAG_RECORD   = "flag_record"
    ALERT         = "alert"


@dataclass
class RuleResult:
    """
    Result of applying one rule to one record.
    Produced by RuleEngine, consumed by DataQualityValidator.
    """
    rule_name:    str
    outcome:      str                   # ValidationOutcome value
    severity:     str                   # RuleSeverity value
    action:       str                   # RuleAction value
    message:      Optional[str] = None  # human-readable explanation
    field_name:   Optional[str] = None  # which field triggered the rule
    field_value:  Optional[str] = None  # what value was found


@dataclass
class ValidationSummary:
    """
    Summary of validating a batch of records for one symbol.
    Returned by DataQualityValidator.validate_batch().

    clean_records:    write to bronze.market_data_{daily|intraday|tick}
    flagged_records:  write to bronze.market_data_{daily|intraday|tick}
                      with data_quality_flag=True
    rejected_records: write to bronze.market_data_rejected
    """
    symbol:             str
    interval:           str
    batch_id:           str
    validated_at:       str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # Record counts
    total_input:        int = 0
    total_passed:       int = 0
    total_flagged:      int = 0
    total_rejected:     int = 0

    # Records ready for Delta write
    clean_records:      List[dict] = field(default_factory=list)
    flagged_records:    List[dict] = field(default_factory=list)
    rejected_records:   List[dict] = field(default_factory=list)

    # Rule hit counts for monitoring
    rejection_reasons:  Dict[str, int] = field(default_factory=dict)
    flag_reasons:       Dict[str, int] = field(default_factory=dict)

    # Audit log entries
    audit_log:          List[dict] = field(default_factory=list)

    @property
    def pass_rate_pct(self) -> float:
        """Percentage of records that passed without issues."""
        if self.total_input == 0:
            return 100.0
        return round((self.total_passed / self.total_input) * 100, 1)

    @property
    def rejection_rate_pct(self) -> float:
        """Percentage of records rejected."""
        if self.total_input == 0:
            return 0.0
        return round((self.total_rejected / self.total_input) * 100, 1)

    @property
    def has_issues(self) -> bool:
        """True if any records were flagged or rejected."""
        return self.total_flagged > 0 or self.total_rejected > 0

    @property
    def writable_records(self) -> List[dict]:
        """All records that should be written to the main Bronze table."""
        return self.clean_records + self.flagged_records

    def add_audit_entry(
        self,
        record_date: str,
        outcome: str,
        rule_name: str,
        message: str,
    ) -> None:
        """Add an entry to the audit log."""
        self.audit_log.append({
            "symbol":       self.symbol,
            "bar_date":     record_date,
            "bar_interval":     self.interval,
            "outcome":      outcome,
            "rule":         rule_name,
            "message":      message,
            "logged_at":    datetime.now(timezone.utc).isoformat(),
        })

    def to_summary_dict(self) -> dict:
        """Compact summary dict for logging — no record data."""
        return {
            "symbol":               self.symbol,
            "bar_interval":             self.interval,
            "batch_id":             self.batch_id,
            "validated_at":         self.validated_at,
            "total_input":          self.total_input,
            "total_passed":         self.total_passed,
            "total_flagged":        self.total_flagged,
            "total_rejected":       self.total_rejected,
            "pass_rate_pct":        self.pass_rate_pct,
            "rejection_rate_pct":   self.rejection_rate_pct,
            "rejection_reasons":    self.rejection_reasons,
            "flag_reasons":         self.flag_reasons,
        }

    def __repr__(self) -> str:
        return (
            f"ValidationSummary("
            f"symbol={self.symbol}, "
            f"interval={self.interval}, "
            f"input={self.total_input}, "
            f"passed={self.total_passed}, "
            f"flagged={self.total_flagged}, "
            f"rejected={self.total_rejected}, "
            f"pass_rate={self.pass_rate_pct}%"
            f")"
        )
