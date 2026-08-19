"""Deterministic execution evaluation against declared expectations, and regression comparison."""

from agent_control_plane.evaluation.comparison import (
    ExecutionComparison,
    ExecutionEvaluationRun,
    ExpectationComparison,
    IncompatibleExecutionComparisonError,
    aggregate_regression_status,
    compare_execution_runs,
)
from agent_control_plane.evaluation.engine import evaluate_execution
from agent_control_plane.evaluation.enums import (
    EvaluationStatus,
    ExpectationChange,
    RegressionStatus,
)
from agent_control_plane.evaluation.models import (
    CapabilityOccurrenceExpectation,
    EvaluationExpectation,
    EvaluationExpectationSet,
    EvaluationObservation,
    EvaluationReport,
    ExecutionOutcomeExpectation,
    ExecutionStatusExpectation,
    HumanInteractionExpectation,
    StepOccurrenceExpectation,
    aggregate_evaluation_status,
)

__all__ = [
    "CapabilityOccurrenceExpectation",
    "EvaluationExpectation",
    "EvaluationExpectationSet",
    "EvaluationObservation",
    "EvaluationReport",
    "EvaluationStatus",
    "ExecutionComparison",
    "ExecutionEvaluationRun",
    "ExecutionOutcomeExpectation",
    "ExecutionStatusExpectation",
    "ExpectationChange",
    "ExpectationComparison",
    "HumanInteractionExpectation",
    "IncompatibleExecutionComparisonError",
    "RegressionStatus",
    "StepOccurrenceExpectation",
    "aggregate_evaluation_status",
    "aggregate_regression_status",
    "compare_execution_runs",
    "evaluate_execution",
]
