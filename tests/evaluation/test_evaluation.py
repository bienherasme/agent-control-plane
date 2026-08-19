from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from agent_control_plane.domain import (
    CapabilityInvocation,
    CapabilityMode,
    ExecutionFailure,
    ExecutionOutcome,
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStep,
    HumanInteraction,
    ModelInvocation,
    StepKind,
)
from agent_control_plane.evaluation import (
    CapabilityOccurrenceExpectation,
    EvaluationExpectationSet,
    EvaluationObservation,
    EvaluationReport,
    EvaluationStatus,
    ExecutionEvaluationRun,
    ExecutionOutcomeExpectation,
    ExecutionStatusExpectation,
    ExpectationChange,
    HumanInteractionExpectation,
    IncompatibleExecutionComparisonError,
    RegressionStatus,
    StepOccurrenceExpectation,
    aggregate_evaluation_status,
    compare_execution_runs,
    evaluate_execution,
)
from agent_control_plane.events import (
    ExecutionCompletedEvent,
    ExecutionEvent,
    ExecutionEventStream,
    ExecutionFailedEvent,
    ExecutionStartedEvent,
    StepCompletedEvent,
    StepFailedEvent,
    StepStartedEvent,
    append_execution_event,
    project_execution,
)
from agent_control_plane.governance import (
    CapabilityBoundaryPolicy,
    GovernancePolicySet,
    GovernanceStatus,
    evaluate_governance,
)

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def started_event(**overrides: object) -> ExecutionStartedEvent:
    defaults: dict[str, object] = {
        "event_id": "evt-start",
        "execution_id": "exec-1",
        "sequence": 1,
        "occurred_at": at(0),
        "system_id": "incident-commander",
        "workflow_name": "incident-analysis",
    }
    defaults.update(overrides)
    return ExecutionStartedEvent(**defaults)


def step_started(**overrides: object) -> StepStartedEvent:
    defaults: dict[str, object] = {
        "event_id": "evt-step-start",
        "execution_id": "exec-1",
        "sequence": 2,
        "occurred_at": at(1),
        "step_id": "step-1",
        "parent_step_id": None,
        "kind": StepKind.WORKFLOW,
        "name": "run workflow",
    }
    defaults.update(overrides)
    return StepStartedEvent(**defaults)


def build_stream(*events: ExecutionEvent, execution_id: str = "exec-1") -> ExecutionEventStream:
    stream = ExecutionEventStream(execution_id=execution_id)
    for event in events:
        stream = append_execution_event(stream, event)
    return stream


def test_execution_status_expectation_pass_and_fail() -> None:
    stream = build_stream(started_event())

    accept_running = ExecutionStatusExpectation(
        expectation_id="e1", description="running is fine",
        acceptable_statuses=(ExecutionStatus.RUNNING,),
    )
    reject_running = ExecutionStatusExpectation(
        expectation_id="e2", description="must be completed",
        acceptable_statuses=(ExecutionStatus.COMPLETED,),
    )

    report = evaluate_execution(
        stream,
        EvaluationExpectationSet(
            expectation_set_id="es", expectations=(accept_running, reject_running)
        ),
    )

    assert report.observations[0].status is EvaluationStatus.PASS
    assert report.observations[1].status is EvaluationStatus.FAIL


def test_execution_outcome_expectation_transitions() -> None:
    expectation = ExecutionOutcomeExpectation(
        expectation_id="e1", description="approve or approve with conditions",
        acceptable_outcomes=("approved", "approved_with_conditions"),
    )
    expectation_set = EvaluationExpectationSet(expectation_set_id="es", expectations=(expectation,))

    running_report = evaluate_execution(build_stream(started_event()), expectation_set)
    assert running_report.observations[0].status is EvaluationStatus.INDETERMINATE

    matching_stream = build_stream(
        started_event(),
        ExecutionCompletedEvent(
            event_id="e-done", execution_id="exec-1", sequence=2, occurred_at=at(1),
            outcome=ExecutionOutcome(outcome="approved"),
        ),
    )
    matching_report = evaluate_execution(matching_stream, expectation_set)
    assert matching_report.observations[0].status is EvaluationStatus.PASS

    wrong_stream = build_stream(
        started_event(),
        ExecutionCompletedEvent(
            event_id="e-done", execution_id="exec-1", sequence=2, occurred_at=at(1),
            outcome=ExecutionOutcome(outcome="request_changes"),
        ),
    )
    wrong_report = evaluate_execution(wrong_stream, expectation_set)
    assert wrong_report.observations[0].status is EvaluationStatus.FAIL

    failed_stream = build_stream(
        started_event(),
        ExecutionFailedEvent(
            event_id="e-fail", execution_id="exec-1", sequence=2, occurred_at=at(1),
            failure=ExecutionFailure(category="timeout", detail="no response"),
        ),
    )
    failed_report = evaluate_execution(failed_stream, expectation_set)
    assert failed_report.observations[0].status is EvaluationStatus.FAIL


def test_step_occurrence_expectation_bounds_and_running_semantics() -> None:
    terminal_stream = build_stream(
        started_event(),
        step_started(event_id="e-a", sequence=2, step_id="a"),
        step_started(event_id="e-b", sequence=3, step_id="b"),
        StepCompletedEvent(
            event_id="e-a-done", execution_id="exec-1", sequence=4, occurred_at=at(2), step_id="a"
        ),
        StepCompletedEvent(
            event_id="e-b-done", execution_id="exec-1", sequence=5, occurred_at=at(2), step_id="b"
        ),
        ExecutionCompletedEvent(
            event_id="e-done", execution_id="exec-1", sequence=6, occurred_at=at(3),
            outcome=ExecutionOutcome(outcome="done"),
        ),
    )
    within_bounds = StepOccurrenceExpectation(
        expectation_id="e1", description="two to three workflow steps",
        kind=StepKind.WORKFLOW, min_occurrences=2, max_occurrences=3,
    )
    below_min = StepOccurrenceExpectation(
        expectation_id="e2", description="at least three workflow steps",
        kind=StepKind.WORKFLOW, min_occurrences=3,
    )
    above_max = StepOccurrenceExpectation(
        expectation_id="e3", description="at most one workflow step",
        kind=StepKind.WORKFLOW, min_occurrences=0, max_occurrences=1,
    )
    terminal_report = evaluate_execution(
        terminal_stream,
        EvaluationExpectationSet(
            expectation_set_id="es", expectations=(within_bounds, below_min, above_max)
        ),
    )
    assert terminal_report.observations[0].status is EvaluationStatus.PASS
    assert terminal_report.observations[1].status is EvaluationStatus.FAIL
    assert terminal_report.observations[2].status is EvaluationStatus.FAIL

    running_below_min_report = evaluate_execution(
        build_stream(started_event(), step_started(event_id="e-a", sequence=2, step_id="a")),
        EvaluationExpectationSet(expectation_set_id="es", expectations=(below_min,)),
    )
    assert running_below_min_report.observations[0].status is EvaluationStatus.INDETERMINATE

    running_above_max_report = evaluate_execution(
        build_stream(
            started_event(),
            step_started(event_id="e-a", sequence=2, step_id="a"),
            step_started(event_id="e-b", sequence=3, step_id="b"),
        ),
        EvaluationExpectationSet(expectation_set_id="es", expectations=(above_max,)),
    )
    assert running_above_max_report.observations[0].status is EvaluationStatus.FAIL


def test_capability_occurrence_expectation_matching() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="a", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
        step_started(
            event_id="e-2", sequence=3, step_id="b", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="cloud-api", operation="restart_service", mode=CapabilityMode.EXECUTE
            ),
        ),
        StepCompletedEvent(
            event_id="e-a-done", execution_id="exec-1", sequence=4, occurred_at=at(2), step_id="a"
        ),
        StepCompletedEvent(
            event_id="e-b-done", execution_id="exec-1", sequence=5, occurred_at=at(2), step_id="b"
        ),
        ExecutionCompletedEvent(
            event_id="e-done", execution_id="exec-1", sequence=6, occurred_at=at(3),
            outcome=ExecutionOutcome(outcome="done"),
        ),
    )

    wildcard_expectation = CapabilityOccurrenceExpectation(
        expectation_id="e1", description="at least two capability calls", min_occurrences=2
    )
    exact_expectation = CapabilityOccurrenceExpectation(
        expectation_id="e2", description="exactly one execute-mode cloud-api restart",
        capability="cloud-api", operation="restart_service", modes=(CapabilityMode.EXECUTE,),
        min_occurrences=1, max_occurrences=1,
    )
    non_matching_expectation = CapabilityOccurrenceExpectation(
        expectation_id="e3", description="no slack calls expected", capability="slack",
        min_occurrences=0, max_occurrences=0,
    )

    report = evaluate_execution(
        stream,
        EvaluationExpectationSet(
            expectation_set_id="es",
            expectations=(wildcard_expectation, exact_expectation, non_matching_expectation),
        ),
    )
    assert report.observations[0].status is EvaluationStatus.PASS
    assert report.observations[1].status is EvaluationStatus.PASS
    assert report.observations[2].status is EvaluationStatus.PASS


def test_human_interaction_expectation_counts_only_completed_accepted() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="approved", kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(interaction_type="approval"),
        ),
        StepCompletedEvent(
            event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="approved",
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        ),
        step_started(
            event_id="e-3", sequence=4, step_id="rejected", kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(interaction_type="approval"),
        ),
        StepCompletedEvent(
            event_id="e-4", execution_id="exec-1", sequence=5, occurred_at=at(4),
            step_id="rejected",
            human_interaction=HumanInteraction(interaction_type="approval", outcome="rejected"),
        ),
        step_started(
            event_id="e-5", sequence=6, step_id="failed", kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(interaction_type="approval"),
        ),
        StepFailedEvent(
            event_id="e-6", execution_id="exec-1", sequence=7, occurred_at=at(6),
            step_id="failed", failure=ExecutionFailure(category="timeout", detail="unavailable"),
        ),
        ExecutionCompletedEvent(
            event_id="e-done", execution_id="exec-1", sequence=8, occurred_at=at(7),
            outcome=ExecutionOutcome(outcome="done"),
        ),
    )

    expectation = HumanInteractionExpectation(
        expectation_id="e1", description="exactly one approval",
        interaction_type="approval", accepted_outcomes=("approved",),
        min_occurrences=1, max_occurrences=1,
    )
    report = evaluate_execution(
        stream, EvaluationExpectationSet(expectation_set_id="es", expectations=(expectation,))
    )
    observation = report.observations[0]
    assert observation.status is EvaluationStatus.PASS
    assert observation.related_step_ids == ("approved",)


def test_expectation_set_order_full_coverage_and_empty_set_pass() -> None:
    stream = build_stream(started_event())

    status_expectation = ExecutionStatusExpectation(
        expectation_id="e-status", description="running is fine",
        acceptable_statuses=(ExecutionStatus.RUNNING,),
    )
    outcome_expectation = ExecutionOutcomeExpectation(
        expectation_id="e-outcome", description="approved", acceptable_outcomes=("approved",)
    )
    report = evaluate_execution(
        stream,
        EvaluationExpectationSet(
            expectation_set_id="es", expectations=(outcome_expectation, status_expectation)
        ),
    )
    assert [observation.expectation_id for observation in report.observations] == [
        "e-outcome", "e-status",
    ]

    empty_report = evaluate_execution(
        stream, EvaluationExpectationSet(expectation_set_id="es-empty")
    )
    assert empty_report.status is EvaluationStatus.PASS
    assert empty_report.observations == ()


def test_deterministic_repeated_evaluation_equality() -> None:
    stream = build_stream(started_event(), step_started(event_id="e-1", sequence=2, step_id="a"))
    expectation_set = EvaluationExpectationSet(
        expectation_set_id="es",
        expectations=(
            StepOccurrenceExpectation(
                expectation_id="e1", description="at least one step", min_occurrences=1
            ),
        ),
    )

    report_a = evaluate_execution(stream, expectation_set)
    report_b = evaluate_execution(stream, expectation_set)

    assert report_a == report_b


def test_comparison_rejects_incompatible_runs() -> None:
    expectation_set = EvaluationExpectationSet(
        expectation_set_id="es",
        expectations=(
            ExecutionStatusExpectation(
                expectation_id="e1", description="running",
                acceptable_statuses=(ExecutionStatus.RUNNING,),
            ),
        ),
    )
    baseline_stream = build_stream(started_event())
    baseline_run = ExecutionEvaluationRun(
        execution=project_execution(baseline_stream),
        evaluation=evaluate_execution(baseline_stream, expectation_set),
    )

    other_expectation_set = EvaluationExpectationSet(
        expectation_set_id="es-other", expectations=expectation_set.expectations
    )
    same_workflow_stream = build_stream(
        started_event(event_id="e-start-2", execution_id="exec-2"), execution_id="exec-2"
    )
    same_workflow_run = ExecutionEvaluationRun(
        execution=project_execution(same_workflow_stream),
        evaluation=evaluate_execution(same_workflow_stream, expectation_set),
    )
    diff_expectation_set_run = ExecutionEvaluationRun(
        execution=project_execution(same_workflow_stream),
        evaluation=evaluate_execution(same_workflow_stream, other_expectation_set),
    )
    with pytest.raises(IncompatibleExecutionComparisonError, match="expectation_set_id"):
        compare_execution_runs(baseline_run, diff_expectation_set_run)

    diff_system_stream = build_stream(
        started_event(event_id="e-start-3", execution_id="exec-3", system_id="other-system"),
        execution_id="exec-3",
    )
    diff_system_run = ExecutionEvaluationRun(
        execution=project_execution(diff_system_stream),
        evaluation=evaluate_execution(diff_system_stream, expectation_set),
    )
    with pytest.raises(IncompatibleExecutionComparisonError, match="systems"):
        compare_execution_runs(baseline_run, diff_system_run)

    diff_workflow_stream = build_stream(
        started_event(event_id="e-start-4", execution_id="exec-4", workflow_name="other-workflow"),
        execution_id="exec-4",
    )
    diff_workflow_run = ExecutionEvaluationRun(
        execution=project_execution(diff_workflow_stream),
        evaluation=evaluate_execution(diff_workflow_stream, expectation_set),
    )
    with pytest.raises(IncompatibleExecutionComparisonError, match="workflows"):
        compare_execution_runs(baseline_run, diff_workflow_run)

    mismatched_coverage_report = EvaluationReport(
        execution_id=same_workflow_run.execution.execution_id,
        expectation_set_id="es",
        status=EvaluationStatus.PASS,
        observations=(
            EvaluationObservation(
                expectation_id="e-different", status=EvaluationStatus.PASS, detail="ok"
            ),
        ),
    )
    mismatched_coverage_run = ExecutionEvaluationRun(
        execution=same_workflow_run.execution, evaluation=mismatched_coverage_report
    )
    with pytest.raises(IncompatibleExecutionComparisonError, match="different expectations"):
        compare_execution_runs(baseline_run, mismatched_coverage_run)


def test_expectation_transition_and_aggregate_regression_classification() -> None:
    def make_run(statuses: dict[str, EvaluationStatus]) -> ExecutionEvaluationRun:
        execution = ExecutionRecord(
            execution_id="exec-x", system_id="sys", workflow_name="wf",
            status=ExecutionStatus.RUNNING, started_at=at(0),
        )
        observations = tuple(
            EvaluationObservation(expectation_id=expectation_id, status=status, detail="observed")
            for expectation_id, status in statuses.items()
        )
        report = EvaluationReport(
            execution_id="exec-x", expectation_set_id="es",
            status=aggregate_evaluation_status(o.status for o in observations),
            observations=observations,
        )
        return ExecutionEvaluationRun(execution=execution, evaluation=report)

    unchanged = compare_execution_runs(
        make_run({"a": EvaluationStatus.PASS}), make_run({"a": EvaluationStatus.PASS})
    )
    assert unchanged.regression_status is RegressionStatus.UNCHANGED

    regression = compare_execution_runs(
        make_run({"a": EvaluationStatus.PASS}), make_run({"a": EvaluationStatus.FAIL})
    )
    assert regression.regression_status is RegressionStatus.REGRESSION

    improvement = compare_execution_runs(
        make_run({"a": EvaluationStatus.FAIL}), make_run({"a": EvaluationStatus.PASS})
    )
    assert improvement.regression_status is RegressionStatus.IMPROVEMENT

    mixed_baseline = make_run(
        {
            "a": EvaluationStatus.PASS,
            "b": EvaluationStatus.PASS,
            "c": EvaluationStatus.INDETERMINATE,
            "d": EvaluationStatus.FAIL,
            "e": EvaluationStatus.INDETERMINATE,
            "f": EvaluationStatus.FAIL,
            "g": EvaluationStatus.PASS,
        }
    )
    mixed_candidate = make_run(
        {
            "a": EvaluationStatus.FAIL,
            "b": EvaluationStatus.INDETERMINATE,
            "c": EvaluationStatus.FAIL,
            "d": EvaluationStatus.PASS,
            "e": EvaluationStatus.PASS,
            "f": EvaluationStatus.INDETERMINATE,
            "g": EvaluationStatus.PASS,
        }
    )
    mixed = compare_execution_runs(mixed_baseline, mixed_candidate)
    assert mixed.regression_status is RegressionStatus.MIXED
    changes_by_id = {c.expectation_id: c.change for c in mixed.expectation_comparisons}
    assert changes_by_id == {
        "a": ExpectationChange.REGRESSED,
        "b": ExpectationChange.REGRESSED,
        "c": ExpectationChange.REGRESSED,
        "d": ExpectationChange.IMPROVED,
        "e": ExpectationChange.IMPROVED,
        "f": ExpectationChange.IMPROVED,
        "g": ExpectationChange.UNCHANGED,
    }


def test_descriptive_deltas_and_usage_delta_missing_data() -> None:
    def model_step(
        step_id: str, input_units: int | None, output_units: int | None
    ) -> ExecutionStep:
        return ExecutionStep(
            step_id=step_id, kind=StepKind.MODEL, name="call", status=ExecutionStatus.COMPLETED,
            started_at=at(1), completed_at=at(2),
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review",
                input_units=input_units, output_units=output_units,
            ),
        )

    baseline_execution = ExecutionRecord(
        execution_id="exec-baseline", system_id="sys", workflow_name="wf",
        status=ExecutionStatus.COMPLETED, started_at=at(0), completed_at=at(10),
        steps=(model_step("m1", 100, 50),), outcome=ExecutionOutcome(outcome="done"),
    )
    candidate_execution = ExecutionRecord(
        execution_id="exec-candidate", system_id="sys", workflow_name="wf",
        status=ExecutionStatus.COMPLETED, started_at=at(0), completed_at=at(10),
        steps=(model_step("m1", 150, 50), model_step("m2", None, 30)),
        outcome=ExecutionOutcome(outcome="done"),
    )

    baseline_run = ExecutionEvaluationRun(
        execution=baseline_execution,
        evaluation=EvaluationReport(
            execution_id="exec-baseline", expectation_set_id="es", status=EvaluationStatus.PASS
        ),
    )
    candidate_run = ExecutionEvaluationRun(
        execution=candidate_execution,
        evaluation=EvaluationReport(
            execution_id="exec-candidate", expectation_set_id="es", status=EvaluationStatus.PASS
        ),
    )

    comparison = compare_execution_runs(baseline_run, candidate_run)

    assert comparison.step_count_delta == 1
    assert comparison.model_step_count_delta == 1
    assert comparison.capability_step_count_delta == 0
    assert comparison.human_step_count_delta == 0
    assert comparison.decision_step_count_delta == 0
    assert comparison.failed_step_count_delta == 0
    assert comparison.output_units_delta == 30
    assert comparison.input_units_delta is None


def test_governance_and_evaluation_independence() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="restart", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="cloud-api", operation="restart_service", mode=CapabilityMode.EXECUTE
            ),
        ),
    )

    governance_report = evaluate_governance(
        stream,
        GovernancePolicySet(
            policy_set_id="ps",
            policies=(
                CapabilityBoundaryPolicy(
                    policy_id="deny-execute", description="deny execute",
                    denied_modes=(CapabilityMode.EXECUTE,),
                ),
            ),
        ),
    )
    assert governance_report.status is GovernanceStatus.VIOLATION

    evaluation_report = evaluate_execution(
        stream,
        EvaluationExpectationSet(
            expectation_set_id="es",
            expectations=(
                CapabilityOccurrenceExpectation(
                    expectation_id="e1", description="exactly one execute capability call",
                    modes=(CapabilityMode.EXECUTE,), min_occurrences=1, max_occurrences=1,
                ),
            ),
        ),
    )
    assert evaluation_report.status is EvaluationStatus.PASS


def test_evaluation_does_not_mutate_snapshot() -> None:
    stream = build_stream(started_event(), step_started(event_id="e-1", sequence=2, step_id="a"))
    record_before = project_execution(stream)

    evaluate_execution(
        stream,
        EvaluationExpectationSet(
            expectation_set_id="es",
            expectations=(
                StepOccurrenceExpectation(
                    expectation_id="e1", description="at least one step", min_occurrences=1
                ),
            ),
        ),
    )

    record_after = project_execution(stream)
    assert record_before == record_after


def test_evaluation_and_comparison_match_despite_observed_whitespace() -> None:
    stream = build_stream(
        started_event(system_id=" incident-commander ", workflow_name=" incident-analysis "),
        step_started(event_id="e-1", sequence=2, step_id="a", name=" supervisor-review "),
        StepCompletedEvent(
            event_id="e-1-done", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="a",
        ),
        ExecutionCompletedEvent(
            event_id="e-done", execution_id="exec-1", sequence=4, occurred_at=at(3),
            outcome=ExecutionOutcome(outcome=" approved "),
        ),
    )

    expectation_set = EvaluationExpectationSet(
        expectation_set_id="es",
        expectations=(
            ExecutionOutcomeExpectation(
                expectation_id="e-outcome", description="approved",
                acceptable_outcomes=("approved",),
            ),
            StepOccurrenceExpectation(
                expectation_id="e-step", description="supervisor review ran",
                name="supervisor-review", min_occurrences=1,
            ),
        ),
    )
    report = evaluate_execution(stream, expectation_set)
    assert report.status is EvaluationStatus.PASS
    assert report.observations[0].status is EvaluationStatus.PASS
    assert report.observations[1].status is EvaluationStatus.PASS

    baseline_run = ExecutionEvaluationRun(execution=project_execution(stream), evaluation=report)

    canonical_stream = build_stream(
        started_event(
            event_id="e-start-2", execution_id="exec-2",
            system_id="incident-commander", workflow_name="incident-analysis",
        ),
        execution_id="exec-2",
    )
    candidate_run = ExecutionEvaluationRun(
        execution=project_execution(canonical_stream),
        evaluation=evaluate_execution(canonical_stream, expectation_set),
    )

    comparison = compare_execution_runs(baseline_run, candidate_run)
    assert comparison.system_id == "incident-commander"
    assert comparison.workflow_name == "incident-analysis"
