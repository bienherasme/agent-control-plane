"""Descriptive baseline/candidate execution comparison plus expectation-only regression status.

Regression classification comes exclusively from how each expectation's evaluation status moved
between the two runs, using the PASS > INDETERMINATE > FAIL desirability ordering. It is not
inferred from step counts, usage, or any other descriptive delta: those are reported alongside
the classification for a human to read, not folded into it. A candidate is never declared
"better" or "worse" beyond what the declared expectations actually establish.

Governance plays no part here. ExecutionComparison never contains a GovernanceReport or a
governance status, and comparing two runs never triggers a governance evaluation.
"""

from __future__ import annotations

from collections.abc import Iterable

from pydantic import BaseModel, ConfigDict, model_validator

from agent_control_plane.domain.enums import ExecutionStatus, StepKind
from agent_control_plane.domain.models import ExecutionRecord, NonBlankStr
from agent_control_plane.evaluation.enums import (
    EvaluationStatus,
    ExpectationChange,
    RegressionStatus,
)
from agent_control_plane.evaluation.models import EvaluationReport

_STATUS_DESIRABILITY: dict[EvaluationStatus, int] = {
    EvaluationStatus.FAIL: 0,
    EvaluationStatus.INDETERMINATE: 1,
    EvaluationStatus.PASS: 2,
}


class IncompatibleExecutionComparisonError(Exception):
    """Raised when a baseline and candidate run cannot be meaningfully compared."""


class _ComparisonModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionEvaluationRun(_ComparisonModel):
    """An execution snapshot paired with the evaluation report computed for it.

    A comparison caller already has both of these on hand from project_execution() and
    evaluate_execution(); bundling them avoids passing four loose, easily transposed arguments.
    """

    execution: ExecutionRecord
    evaluation: EvaluationReport

    @model_validator(mode="after")
    def _validate_matching_execution_id(self) -> ExecutionEvaluationRun:
        if self.execution.execution_id != self.evaluation.execution_id:
            raise ValueError("execution and evaluation must reference the same execution_id")
        return self


class ExpectationComparison(_ComparisonModel):
    """How one expectation's evaluation status moved from baseline to candidate."""

    expectation_id: NonBlankStr
    baseline_status: EvaluationStatus
    candidate_status: EvaluationStatus
    change: ExpectationChange


def aggregate_regression_status(changes: Iterable[ExpectationChange]) -> RegressionStatus:
    """The shared regression rollup: both directions of movement outrank pure stability."""

    collected = list(changes)
    has_regression = any(change is ExpectationChange.REGRESSED for change in collected)
    has_improvement = any(change is ExpectationChange.IMPROVED for change in collected)
    if has_regression and has_improvement:
        return RegressionStatus.MIXED
    if has_regression:
        return RegressionStatus.REGRESSION
    if has_improvement:
        return RegressionStatus.IMPROVEMENT
    return RegressionStatus.UNCHANGED


class ExecutionComparison(_ComparisonModel):
    """A deterministic, descriptive comparison of two executions of the same workflow."""

    system_id: NonBlankStr
    workflow_name: NonBlankStr
    baseline_execution_id: NonBlankStr
    candidate_execution_id: NonBlankStr

    regression_status: RegressionStatus
    expectation_comparisons: tuple[ExpectationComparison, ...] = ()

    baseline_status: ExecutionStatus
    candidate_status: ExecutionStatus
    status_changed: bool

    baseline_outcome: str | None
    candidate_outcome: str | None
    outcome_changed: bool

    step_count_delta: int
    failed_step_count_delta: int
    model_step_count_delta: int
    capability_step_count_delta: int
    human_step_count_delta: int
    decision_step_count_delta: int

    input_units_delta: int | None
    output_units_delta: int | None

    @model_validator(mode="after")
    def _validate_regression_status(self) -> ExecutionComparison:
        expected = aggregate_regression_status(
            comparison.change for comparison in self.expectation_comparisons
        )
        if self.regression_status is not expected:
            raise ValueError(
                f"regression_status {self.regression_status.value!r} does not match the "
                f"aggregate of its expectation comparisons ({expected.value!r})"
            )
        return self


def _classify_transition(
    baseline: EvaluationStatus, candidate: EvaluationStatus
) -> ExpectationChange:
    if baseline is candidate:
        return ExpectationChange.UNCHANGED
    if _STATUS_DESIRABILITY[candidate] > _STATUS_DESIRABILITY[baseline]:
        return ExpectationChange.IMPROVED
    return ExpectationChange.REGRESSED


def _count_by_kind(execution: ExecutionRecord, kind: StepKind) -> int:
    return sum(1 for step in execution.steps if step.kind is kind)


def _count_by_status(execution: ExecutionRecord, status: ExecutionStatus) -> int:
    return sum(1 for step in execution.steps if step.status is status)


def _known_model_input_total(execution: ExecutionRecord) -> int | None:
    total = 0
    for step in execution.steps:
        if step.kind is not StepKind.MODEL or step.model_invocation is None:
            continue
        if step.model_invocation.input_units is None:
            return None
        total += step.model_invocation.input_units
    return total


def _known_model_output_total(execution: ExecutionRecord) -> int | None:
    total = 0
    for step in execution.steps:
        if step.kind is not StepKind.MODEL or step.model_invocation is None:
            continue
        if step.model_invocation.output_units is None:
            return None
        total += step.model_invocation.output_units
    return total


def _usage_delta(baseline_total: int | None, candidate_total: int | None) -> int | None:
    if baseline_total is None or candidate_total is None:
        return None
    return candidate_total - baseline_total


def compare_execution_runs(
    baseline: ExecutionEvaluationRun, candidate: ExecutionEvaluationRun
) -> ExecutionComparison:
    baseline_execution = baseline.execution
    candidate_execution = candidate.execution

    if baseline.evaluation.expectation_set_id != candidate.evaluation.expectation_set_id:
        raise IncompatibleExecutionComparisonError(
            "baseline and candidate evaluations use different expectation_set_id values"
        )
    if baseline_execution.system_id != candidate_execution.system_id:
        raise IncompatibleExecutionComparisonError(
            "baseline and candidate executions belong to different systems"
        )
    if baseline_execution.workflow_name != candidate_execution.workflow_name:
        raise IncompatibleExecutionComparisonError(
            "baseline and candidate executions belong to different workflows"
        )

    baseline_ids = tuple(o.expectation_id for o in baseline.evaluation.observations)
    candidate_ids = tuple(o.expectation_id for o in candidate.evaluation.observations)
    if baseline_ids != candidate_ids:
        raise IncompatibleExecutionComparisonError(
            "baseline and candidate evaluation reports cover different expectations"
        )

    baseline_by_id = {o.expectation_id: o for o in baseline.evaluation.observations}
    candidate_by_id = {o.expectation_id: o for o in candidate.evaluation.observations}

    expectation_comparisons = tuple(
        ExpectationComparison(
            expectation_id=expectation_id,
            baseline_status=baseline_by_id[expectation_id].status,
            candidate_status=candidate_by_id[expectation_id].status,
            change=_classify_transition(
                baseline_by_id[expectation_id].status, candidate_by_id[expectation_id].status
            ),
        )
        for expectation_id in baseline_ids
    )
    regression_status = aggregate_regression_status(
        comparison.change for comparison in expectation_comparisons
    )

    baseline_outcome = baseline_execution.outcome.outcome if baseline_execution.outcome else None
    candidate_outcome = (
        candidate_execution.outcome.outcome if candidate_execution.outcome else None
    )

    return ExecutionComparison(
        system_id=baseline_execution.system_id,
        workflow_name=baseline_execution.workflow_name,
        baseline_execution_id=baseline_execution.execution_id,
        candidate_execution_id=candidate_execution.execution_id,
        regression_status=regression_status,
        expectation_comparisons=expectation_comparisons,
        baseline_status=baseline_execution.status,
        candidate_status=candidate_execution.status,
        status_changed=baseline_execution.status != candidate_execution.status,
        baseline_outcome=baseline_outcome,
        candidate_outcome=candidate_outcome,
        outcome_changed=baseline_outcome != candidate_outcome,
        step_count_delta=len(candidate_execution.steps) - len(baseline_execution.steps),
        failed_step_count_delta=(
            _count_by_status(candidate_execution, ExecutionStatus.FAILED)
            - _count_by_status(baseline_execution, ExecutionStatus.FAILED)
        ),
        model_step_count_delta=(
            _count_by_kind(candidate_execution, StepKind.MODEL)
            - _count_by_kind(baseline_execution, StepKind.MODEL)
        ),
        capability_step_count_delta=(
            _count_by_kind(candidate_execution, StepKind.CAPABILITY)
            - _count_by_kind(baseline_execution, StepKind.CAPABILITY)
        ),
        human_step_count_delta=(
            _count_by_kind(candidate_execution, StepKind.HUMAN)
            - _count_by_kind(baseline_execution, StepKind.HUMAN)
        ),
        decision_step_count_delta=(
            _count_by_kind(candidate_execution, StepKind.DECISION)
            - _count_by_kind(baseline_execution, StepKind.DECISION)
        ),
        input_units_delta=_usage_delta(
            _known_model_input_total(baseline_execution),
            _known_model_input_total(candidate_execution),
        ),
        output_units_delta=_usage_delta(
            _known_model_output_total(baseline_execution),
            _known_model_output_total(candidate_execution),
        ),
    )
