"""Deterministic evaluation of an execution against declared behavioral expectations.

evaluate_execution is pure: the same event stream and the same expectation set always produce
the same report. It is entirely independent of governance: it never calls evaluate_governance,
never reads a GovernanceReport, and never copies a governance status into an evaluation result.

An expectation whose count or outcome cannot yet be decided while the execution is still
RUNNING is INDETERMINATE rather than FAIL, unless the observed facts already make the
expectation impossible to satisfy (a max bound already exceeded, or a terminal execution with
an unmet minimum) -- a known failure always outranks future uncertainty.
"""

from __future__ import annotations

from agent_control_plane.domain.enums import ExecutionStatus, StepKind
from agent_control_plane.domain.models import ExecutionRecord, ExecutionStep
from agent_control_plane.evaluation.enums import EvaluationStatus
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
from agent_control_plane.events.projection import project_execution
from agent_control_plane.events.stream import ExecutionEventStream


def evaluate_execution(
    stream: ExecutionEventStream, expectation_set: EvaluationExpectationSet
) -> EvaluationReport:
    record = project_execution(stream)

    observations = tuple(
        _evaluate_expectation(expectation, record) for expectation in expectation_set.expectations
    )

    status = aggregate_evaluation_status(observation.status for observation in observations)

    return EvaluationReport(
        execution_id=record.execution_id,
        expectation_set_id=expectation_set.expectation_set_id,
        status=status,
        observations=observations,
    )


def _evaluate_expectation(
    expectation: EvaluationExpectation, record: ExecutionRecord
) -> EvaluationObservation:
    if isinstance(expectation, ExecutionStatusExpectation):
        return _evaluate_execution_status(expectation, record)
    if isinstance(expectation, ExecutionOutcomeExpectation):
        return _evaluate_execution_outcome(expectation, record)
    if isinstance(expectation, StepOccurrenceExpectation):
        return _evaluate_step_occurrence(expectation, record)
    if isinstance(expectation, CapabilityOccurrenceExpectation):
        return _evaluate_capability_occurrence(expectation, record)
    return _evaluate_human_interaction(expectation, record)


def _evaluate_execution_status(
    expectation: ExecutionStatusExpectation, record: ExecutionRecord
) -> EvaluationObservation:
    if record.status in expectation.acceptable_statuses:
        return EvaluationObservation(
            expectation_id=expectation.expectation_id,
            status=EvaluationStatus.PASS,
            detail=f"execution status {record.status.value!r} is acceptable",
        )
    return EvaluationObservation(
        expectation_id=expectation.expectation_id,
        status=EvaluationStatus.FAIL,
        detail=f"execution status {record.status.value!r} is not among the accepted statuses",
    )


def _evaluate_execution_outcome(
    expectation: ExecutionOutcomeExpectation, record: ExecutionRecord
) -> EvaluationObservation:
    if record.outcome is None:
        if record.status is ExecutionStatus.RUNNING:
            return EvaluationObservation(
                expectation_id=expectation.expectation_id,
                status=EvaluationStatus.INDETERMINATE,
                detail="execution is still running and has not reported an outcome yet",
            )
        return EvaluationObservation(
            expectation_id=expectation.expectation_id,
            status=EvaluationStatus.FAIL,
            detail="execution is terminal but reported no outcome",
        )
    if record.outcome.outcome in expectation.acceptable_outcomes:
        return EvaluationObservation(
            expectation_id=expectation.expectation_id,
            status=EvaluationStatus.PASS,
            detail=f"execution outcome {record.outcome.outcome!r} matched the expectation",
        )
    return EvaluationObservation(
        expectation_id=expectation.expectation_id,
        status=EvaluationStatus.FAIL,
        detail=f"execution outcome {record.outcome.outcome!r} did not match the expectation",
    )


def _evaluate_occurrence(
    expectation_id: str,
    matching_step_ids: list[str],
    min_occurrences: int,
    max_occurrences: int | None,
    is_running: bool,
    subject: str,
) -> EvaluationObservation:
    count = len(matching_step_ids)

    if max_occurrences is not None and count > max_occurrences:
        return EvaluationObservation(
            expectation_id=expectation_id,
            status=EvaluationStatus.FAIL,
            detail=f"observed {count} {subject}; expected at most {max_occurrences}",
            related_step_ids=tuple(matching_step_ids),
        )

    if count < min_occurrences:
        if is_running:
            return EvaluationObservation(
                expectation_id=expectation_id,
                status=EvaluationStatus.INDETERMINATE,
                detail=(
                    f"observed {count} {subject} so far; expected at least "
                    f"{min_occurrences} and the execution is still running"
                ),
                related_step_ids=tuple(matching_step_ids),
            )
        return EvaluationObservation(
            expectation_id=expectation_id,
            status=EvaluationStatus.FAIL,
            detail=f"observed {count} {subject}; expected at least {min_occurrences}",
            related_step_ids=tuple(matching_step_ids),
        )

    return EvaluationObservation(
        expectation_id=expectation_id,
        status=EvaluationStatus.PASS,
        detail=f"observed {count} {subject}, satisfying the configured bounds",
        related_step_ids=tuple(matching_step_ids),
    )


def _step_matches(
    step: ExecutionStep,
    kind: StepKind | None,
    name: str | None,
    statuses: tuple[ExecutionStatus, ...],
) -> bool:
    if kind is not None and step.kind is not kind:
        return False
    if name is not None and step.name != name:
        return False
    if statuses and step.status not in statuses:
        return False
    return True


def _evaluate_step_occurrence(
    expectation: StepOccurrenceExpectation, record: ExecutionRecord
) -> EvaluationObservation:
    matching_step_ids = [
        step.step_id
        for step in record.steps
        if _step_matches(step, expectation.kind, expectation.name, expectation.statuses)
    ]
    return _evaluate_occurrence(
        expectation.expectation_id,
        matching_step_ids,
        expectation.min_occurrences,
        expectation.max_occurrences,
        record.status is ExecutionStatus.RUNNING,
        "matching steps",
    )


def _evaluate_capability_occurrence(
    expectation: CapabilityOccurrenceExpectation, record: ExecutionRecord
) -> EvaluationObservation:
    matching_step_ids: list[str] = []
    for step in record.steps:
        if step.kind is not StepKind.CAPABILITY or step.capability_invocation is None:
            continue
        invocation = step.capability_invocation
        if expectation.capability is not None and invocation.capability != expectation.capability:
            continue
        if expectation.operation is not None and invocation.operation != expectation.operation:
            continue
        if expectation.modes and invocation.mode not in expectation.modes:
            continue
        matching_step_ids.append(step.step_id)

    return _evaluate_occurrence(
        expectation.expectation_id,
        matching_step_ids,
        expectation.min_occurrences,
        expectation.max_occurrences,
        record.status is ExecutionStatus.RUNNING,
        "matching capability invocations",
    )


def _evaluate_human_interaction(
    expectation: HumanInteractionExpectation, record: ExecutionRecord
) -> EvaluationObservation:
    matching_step_ids: list[str] = []
    for step in record.steps:
        if step.kind is not StepKind.HUMAN or step.status is not ExecutionStatus.COMPLETED:
            continue
        interaction = step.human_interaction
        if interaction is None:
            continue
        if interaction.interaction_type != expectation.interaction_type:
            continue
        if interaction.outcome not in expectation.accepted_outcomes:
            continue
        matching_step_ids.append(step.step_id)

    return _evaluate_occurrence(
        expectation.expectation_id,
        matching_step_ids,
        expectation.min_occurrences,
        expectation.max_occurrences,
        record.status is ExecutionStatus.RUNNING,
        "qualifying human interactions",
    )
