from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from agent_control_plane.domain import (
    CapabilityInvocation,
    CapabilityMode,
    DecisionRecord,
    ExecutionFailure,
    HumanInteraction,
    ModelInvocation,
    StepKind,
)
from agent_control_plane.events import (
    ExecutionEvent,
    ExecutionEventStream,
    ExecutionStartedEvent,
    StepCancelledEvent,
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
    HumanApprovalRequirementPolicy,
    ModelUsageBudgetPolicy,
    aggregate_governance_status,
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


def build_stream(*events: ExecutionEvent) -> ExecutionEventStream:
    stream = ExecutionEventStream(execution_id="exec-1")
    for event in events:
        stream = append_execution_event(stream, event)
    return stream


def test_capability_boundary_wildcard_and_exact_matching() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-a", sequence=2, step_id="a", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
        step_started(
            event_id="e-b", sequence=3, step_id="b", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="slack", operation="post_message", mode=CapabilityMode.READ
            ),
        ),
        step_started(
            event_id="e-c", sequence=4, step_id="c", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="delete_issue", mode=CapabilityMode.WRITE
            ),
        ),
    )

    wildcard_policy = CapabilityBoundaryPolicy(
        policy_id="deny-write", description="deny all writes", denied_modes=(CapabilityMode.WRITE,)
    )
    scoped_policy = CapabilityBoundaryPolicy(
        policy_id="deny-jira-create",
        description="deny jira create_issue writes",
        denied_modes=(CapabilityMode.WRITE, CapabilityMode.EXECUTE),
        capabilities=("jira",),
        operations=("create_issue",),
    )

    wildcard_report = evaluate_governance(
        stream, GovernancePolicySet(policy_set_id="ps-1", policies=(wildcard_policy,))
    )
    scoped_report = evaluate_governance(
        stream, GovernancePolicySet(policy_set_id="ps-2", policies=(scoped_policy,))
    )

    wildcard_eval = wildcard_report.evaluations[0]
    assert wildcard_eval.status is GovernanceStatus.VIOLATION
    assert {v.related_step_ids[0] for v in wildcard_eval.violations} == {"a", "c"}

    scoped_eval = scoped_report.evaluations[0]
    assert scoped_eval.status is GovernanceStatus.VIOLATION
    assert [v.related_step_ids[0] for v in scoped_eval.violations] == ["a"]


def test_capability_violation_regardless_of_final_lifecycle_status() -> None:
    def capability(step_id: str, sequence: int) -> StepStartedEvent:
        return step_started(
            event_id=f"e-{step_id}", sequence=sequence, step_id=step_id, kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="cloud-api", operation="restart_service", mode=CapabilityMode.EXECUTE
            ),
        )

    stream = build_stream(
        started_event(),
        capability("running", 2),
        capability("completed", 3),
        StepCompletedEvent(
            event_id="e-completed-done", execution_id="exec-1", sequence=4, occurred_at=at(3),
            step_id="completed",
        ),
        capability("failed", 5),
        StepFailedEvent(
            event_id="e-failed-done", execution_id="exec-1", sequence=6, occurred_at=at(5),
            step_id="failed", failure=ExecutionFailure(category="timeout", detail="no response"),
        ),
        capability("cancelled", 7),
        StepCancelledEvent(
            event_id="e-cancelled-done", execution_id="exec-1", sequence=8, occurred_at=at(7),
            step_id="cancelled",
        ),
    )

    policy = CapabilityBoundaryPolicy(
        policy_id="deny-execute", description="deny execute mode",
        denied_modes=(CapabilityMode.EXECUTE,),
    )
    report = evaluate_governance(
        stream, GovernancePolicySet(policy_set_id="ps", policies=(policy,))
    )

    evaluation = report.evaluations[0]
    assert evaluation.status is GovernanceStatus.VIOLATION
    assert [v.related_step_ids[0] for v in evaluation.violations] == [
        "running", "completed", "failed", "cancelled",
    ]


def test_human_approval_ordering_pass_and_violation() -> None:
    policy = HumanApprovalRequirementPolicy(
        policy_id="jira-write-approval",
        description="require approval before jira writes",
        capability_modes=(CapabilityMode.WRITE, CapabilityMode.EXECUTE),
        capabilities=("jira",),
        interaction_type="approval",
        accepted_outcomes=("approved",),
    )
    policy_set = GovernancePolicySet(policy_set_id="ps", policies=(policy,))

    passing_stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="human", kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(interaction_type="approval"),
        ),
        StepCompletedEvent(
            event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(2), step_id="human",
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        ),
        step_started(
            event_id="e-3", sequence=4, step_id="jira", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
    )
    passing_report = evaluate_governance(passing_stream, policy_set)
    assert passing_report.evaluations[0].status is GovernanceStatus.PASS

    violating_stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="jira", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
        step_started(
            event_id="e-2", sequence=3, step_id="human", kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(interaction_type="approval"),
        ),
        StepCompletedEvent(
            event_id="e-3", execution_id="exec-1", sequence=4, occurred_at=at(3), step_id="human",
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        ),
    )
    violating_report = evaluate_governance(violating_stream, policy_set)
    violating_eval = violating_report.evaluations[0]
    assert violating_eval.status is GovernanceStatus.VIOLATION
    assert violating_eval.violations[0].related_step_ids == ("jira",)


def test_decision_step_does_not_satisfy_human_approval_policy() -> None:
    policy = HumanApprovalRequirementPolicy(
        policy_id="jira-write-approval",
        description="require approval before jira writes",
        capability_modes=(CapabilityMode.WRITE,),
        capabilities=("jira",),
        interaction_type="approval",
        accepted_outcomes=("approved",),
    )
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="decision", kind=StepKind.DECISION,
            decision=DecisionRecord(),
        ),
        StepCompletedEvent(
            event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="decision", decision=DecisionRecord(decision="approved"),
        ),
        step_started(
            event_id="e-3", sequence=4, step_id="jira", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
    )
    report = evaluate_governance(
        stream, GovernancePolicySet(policy_set_id="ps", policies=(policy,))
    )
    assert report.evaluations[0].status is GovernanceStatus.VIOLATION


def test_approval_uses_logical_sequence_not_occurred_at() -> None:
    policy = HumanApprovalRequirementPolicy(
        policy_id="approval", description="require approval",
        capability_modes=(CapabilityMode.WRITE,),
        interaction_type="approval", accepted_outcomes=("approved",),
    )
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="human", kind=StepKind.HUMAN,
            occurred_at=at(1),
            human_interaction=HumanInteraction(interaction_type="approval"),
        ),
        StepCompletedEvent(
            event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(100),
            step_id="human",
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        ),
        step_started(
            event_id="e-3", sequence=4, step_id="capability", kind=StepKind.CAPABILITY,
            occurred_at=at(50),
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
    )
    report = evaluate_governance(
        stream, GovernancePolicySet(policy_set_id="ps", policies=(policy,))
    )
    assert report.evaluations[0].status is GovernanceStatus.PASS


def test_model_usage_budget_pass_violation_and_indeterminate() -> None:
    def model_step(
        step_id: str, sequence: int, input_units: int | None, output_units: int | None
    ) -> StepStartedEvent:
        return step_started(
            event_id=f"e-{step_id}", sequence=sequence, step_id=step_id, kind=StepKind.MODEL,
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review",
                input_units=input_units, output_units=output_units,
            ),
        )

    policy = ModelUsageBudgetPolicy(
        policy_id="budget", description="cap total usage", max_total_units=1000
    )
    policy_set = GovernancePolicySet(policy_set_id="ps", policies=(policy,))

    passing_report = evaluate_governance(
        build_stream(started_event(), model_step("a", 2, 100, 100), model_step("b", 3, 200, 100)),
        policy_set,
    )
    assert passing_report.evaluations[0].status is GovernanceStatus.PASS

    violating_report = evaluate_governance(
        build_stream(started_event(), model_step("a", 2, 400, 300), model_step("b", 3, 200, 200)),
        policy_set,
    )
    violating_eval = violating_report.evaluations[0]
    assert violating_eval.status is GovernanceStatus.VIOLATION
    assert set(violating_eval.violations[0].related_step_ids) == {"a", "b"}

    indeterminate_report = evaluate_governance(
        build_stream(started_event(), model_step("a", 2, 400, None)), policy_set
    )
    indeterminate_eval = indeterminate_report.evaluations[0]
    assert indeterminate_eval.status is GovernanceStatus.INDETERMINATE
    assert indeterminate_eval.detail is not None


def test_model_usage_budget_no_matching_invocations_is_pass() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="model", kind=StepKind.MODEL,
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review"
            ),
        ),
    )
    policy = ModelUsageBudgetPolicy(
        policy_id="budget", description="cap openai usage", provider="openai",
        max_total_units=1000,
    )
    report = evaluate_governance(
        stream, GovernancePolicySet(policy_set_id="ps", policies=(policy,))
    )
    assert report.evaluations[0].status is GovernanceStatus.PASS


def test_policy_set_ordering_and_full_coverage() -> None:
    stream = build_stream(started_event())

    capability_policy = CapabilityBoundaryPolicy(
        policy_id="p-capability", description="deny writes", denied_modes=(CapabilityMode.WRITE,)
    )
    approval_policy = HumanApprovalRequirementPolicy(
        policy_id="p-approval", description="require approval",
        capability_modes=(CapabilityMode.WRITE,),
        interaction_type="approval", accepted_outcomes=("approved",),
    )
    budget_policy = ModelUsageBudgetPolicy(
        policy_id="p-budget", description="cap usage", max_total_units=1000
    )
    policy_set = GovernancePolicySet(
        policy_set_id="ps", policies=(budget_policy, capability_policy, approval_policy)
    )

    report = evaluate_governance(stream, policy_set)

    assert [evaluation.policy_id for evaluation in report.evaluations] == [
        "p-budget", "p-capability", "p-approval",
    ]
    assert all(evaluation.status is GovernanceStatus.PASS for evaluation in report.evaluations)


def test_status_aggregation_precedence_and_empty_is_pass() -> None:
    assert aggregate_governance_status([]) is GovernanceStatus.PASS
    assert (
        aggregate_governance_status([GovernanceStatus.PASS, GovernanceStatus.INDETERMINATE])
        is GovernanceStatus.INDETERMINATE
    )
    assert (
        aggregate_governance_status(
            [GovernanceStatus.PASS, GovernanceStatus.INDETERMINATE, GovernanceStatus.VIOLATION]
        )
        is GovernanceStatus.VIOLATION
    )

    empty_report = evaluate_governance(
        build_stream(started_event()), GovernancePolicySet(policy_set_id="ps-empty")
    )
    assert empty_report.status is GovernanceStatus.PASS
    assert empty_report.evaluations == ()


def test_deterministic_repeated_evaluation_equality() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="jira", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
    )
    policy_set = GovernancePolicySet(
        policy_set_id="ps",
        policies=(
            CapabilityBoundaryPolicy(
                policy_id="p", description="deny writes", denied_modes=(CapabilityMode.WRITE,)
            ),
        ),
    )

    report_a = evaluate_governance(stream, policy_set)
    report_b = evaluate_governance(stream, policy_set)

    assert report_a == report_b


def test_duplicate_policy_ids_rejected() -> None:
    with pytest.raises(ValidationError, match="unique"):
        GovernancePolicySet(
            policy_set_id="ps",
            policies=(
                CapabilityBoundaryPolicy(
                    policy_id="dup", description="a", denied_modes=(CapabilityMode.WRITE,)
                ),
                CapabilityBoundaryPolicy(
                    policy_id="dup", description="b", denied_modes=(CapabilityMode.EXECUTE,)
                ),
            ),
        )


def test_governance_evaluation_does_not_mutate_snapshot() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="jira", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
    )
    record_before = project_execution(stream)

    policy_set = GovernancePolicySet(
        policy_set_id="ps",
        policies=(
            CapabilityBoundaryPolicy(
                policy_id="p", description="deny writes", denied_modes=(CapabilityMode.WRITE,)
            ),
        ),
    )
    evaluate_governance(stream, policy_set)

    record_after = project_execution(stream)
    assert record_before == record_after


def test_governance_matching_normalizes_observed_whitespace() -> None:
    capability_policy = CapabilityBoundaryPolicy(
        policy_id="deny-jira-create",
        description="deny jira create_issue writes",
        denied_modes=(CapabilityMode.WRITE,),
        capabilities=("jira",),
        operations=("create_issue",),
    )
    capability_stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="a", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability=" jira ", operation="create_issue ", mode=CapabilityMode.WRITE
            ),
        ),
    )
    capability_report = evaluate_governance(
        capability_stream, GovernancePolicySet(policy_set_id="ps-1", policies=(capability_policy,))
    )
    assert capability_report.evaluations[0].status is GovernanceStatus.VIOLATION

    budget_policy = ModelUsageBudgetPolicy(
        policy_id="budget", description="cap openai usage",
        provider="openai", model="gpt-x", operation="review",
        max_total_units=50,
    )
    budget_stream = build_stream(
        started_event(),
        step_started(
            event_id="e-2", sequence=2, step_id="m", kind=StepKind.MODEL,
            model_invocation=ModelInvocation(
                provider=" openai ", model=" gpt-x ", operation=" review ",
                input_units=100, output_units=100,
            ),
        ),
    )
    budget_report = evaluate_governance(
        budget_stream, GovernancePolicySet(policy_set_id="ps-2", policies=(budget_policy,))
    )
    budget_eval = budget_report.evaluations[0]
    assert budget_eval.status is GovernanceStatus.VIOLATION
    assert budget_eval.violations[0].related_step_ids == ("m",)
