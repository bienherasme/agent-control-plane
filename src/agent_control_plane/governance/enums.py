"""Closed governance enums."""

from __future__ import annotations

from enum import StrEnum


class GovernanceStatus(StrEnum):
    """The three-valued outcome of evaluating one policy, or a whole report, against facts.

    INDETERMINATE is distinct from PASS: missing observability data must never present as
    compliance. It is also distinct from execution failure, which is a separate concept the
    domain layer already tracks through ExecutionStatus and ExecutionFailure.
    """

    PASS = "pass"
    VIOLATION = "violation"
    INDETERMINATE = "indeterminate"


class PolicyViolationCode(StrEnum):
    """Stable, control-plane-owned identifiers for the kind of violation observed."""

    CAPABILITY_BOUNDARY = "capability_boundary"
    MISSING_REQUIRED_APPROVAL = "missing_required_approval"
    MODEL_USAGE_BUDGET_EXCEEDED = "model_usage_budget_exceeded"
