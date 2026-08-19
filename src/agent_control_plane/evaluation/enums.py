"""Closed evaluation and regression-comparison enums."""

from __future__ import annotations

from enum import StrEnum


class EvaluationStatus(StrEnum):
    """The three-valued outcome of checking one expectation, or a whole report, against facts.

    This is a distinct concept from GovernanceStatus even though the names read alike:
    governance asks whether configured policy was violated, evaluation asks whether declared
    expected behavior was observed. INDETERMINATE covers both missing observability and an
    execution that has not yet reached a state where the expectation can be decided.
    """

    PASS = "pass"
    FAIL = "fail"
    INDETERMINATE = "indeterminate"


class ExpectationChange(StrEnum):
    """How a single expectation's status moved between a baseline and a candidate run.

    Derived only from the PASS > INDETERMINATE > FAIL desirability ordering, never from
    counts, usage, or duration deltas.
    """

    UNCHANGED = "unchanged"
    REGRESSED = "regressed"
    IMPROVED = "improved"


class RegressionStatus(StrEnum):
    """The aggregate classification across every expectation transition in a comparison."""

    UNCHANGED = "unchanged"
    REGRESSION = "regression"
    IMPROVEMENT = "improvement"
    MIXED = "mixed"
