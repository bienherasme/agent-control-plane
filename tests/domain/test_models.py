from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from agent_control_plane.domain import (
    CapabilityInvocation,
    CapabilityMode,
    DecisionRecord,
    ExecutionFailure,
    ExecutionOutcome,
    ExecutionRecord,
    ExecutionStatus,
    ExecutionStep,
    HumanInteraction,
    ModelInvocation,
    StepKind,
)

EXECUTION_START = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
EXECUTION_END = EXECUTION_START + timedelta(minutes=10)


def make_step(**overrides: object) -> ExecutionStep:
    defaults: dict[str, object] = {
        "step_id": "step-1",
        "parent_step_id": None,
        "kind": StepKind.WORKFLOW,
        "name": "run workflow",
        "status": ExecutionStatus.COMPLETED,
        "started_at": EXECUTION_START,
        "completed_at": EXECUTION_END,
    }
    defaults.update(overrides)
    return ExecutionStep(**defaults)


def make_execution(**overrides: object) -> ExecutionRecord:
    defaults: dict[str, object] = {
        "execution_id": "exec-1",
        "system_id": "incident-commander",
        "workflow_name": "incident-analysis",
        "status": ExecutionStatus.COMPLETED,
        "started_at": EXECUTION_START,
        "completed_at": EXECUTION_END,
        "outcome": ExecutionOutcome(outcome="completed"),
    }
    defaults.update(overrides)
    return ExecutionRecord(**defaults)


def test_completed_execution_with_multiple_step_kinds() -> None:
    workflow_step = make_step(step_id="workflow", kind=StepKind.WORKFLOW)
    model_step = make_step(
        step_id="model",
        parent_step_id="workflow",
        kind=StepKind.MODEL,
        model_invocation=ModelInvocation(
            provider="anthropic",
            model="claude-sonnet-5",
            operation="hypothesis-generation",
            input_units=120,
            output_units=45,
        ),
    )
    capability_step = make_step(
        step_id="capability",
        parent_step_id="workflow",
        kind=StepKind.CAPABILITY,
        capability_invocation=CapabilityInvocation(
            capability="jira",
            operation="create_issue",
            target="INC-4821",
            mode=CapabilityMode.WRITE,
        ),
    )
    human_step = make_step(
        step_id="human",
        parent_step_id="workflow",
        kind=StepKind.HUMAN,
        human_interaction=HumanInteraction(
            interaction_type="approval",
            outcome="approved",
            actor_reference="reviewer-42",
        ),
    )
    decision_step = make_step(
        step_id="decision",
        parent_step_id="workflow",
        kind=StepKind.DECISION,
        decision=DecisionRecord(decision="escalate", rationale_reference="doc-91"),
    )

    execution = make_execution(
        steps=(workflow_step, model_step, capability_step, human_step, decision_step)
    )

    assert len(execution.steps) == 5
    assert execution.outcome is not None
    assert execution.outcome.outcome == "completed"


def test_step_detail_must_match_kind() -> None:
    with pytest.raises(ValidationError, match="requires model_invocation"):
        make_step(kind=StepKind.MODEL)

    with pytest.raises(ValidationError, match="must not set model_invocation"):
        make_step(
            kind=StepKind.WORKFLOW,
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review"
            ),
        )


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        make_execution(started_at=datetime(2026, 1, 1, 12, 0))

    with pytest.raises(ValidationError, match="timezone-aware"):
        make_step(started_at=datetime(2026, 1, 1, 12, 0))


def test_running_and_terminal_completed_at_rules() -> None:
    with pytest.raises(ValidationError, match="must not have completed_at"):
        make_execution(status=ExecutionStatus.RUNNING, completed_at=EXECUTION_END, outcome=None)

    with pytest.raises(ValidationError, match="requires completed_at"):
        make_execution(completed_at=None)

    with pytest.raises(ValidationError, match="must not have completed_at"):
        make_step(status=ExecutionStatus.RUNNING, completed_at=EXECUTION_END)

    with pytest.raises(ValidationError, match="requires completed_at"):
        make_step(completed_at=None)


def test_completed_execution_requires_outcome() -> None:
    with pytest.raises(ValidationError, match="requires an outcome"):
        make_execution(outcome=None)

    with pytest.raises(ValidationError, match="must not have an outcome"):
        make_execution(
            status=ExecutionStatus.RUNNING,
            completed_at=None,
            outcome=ExecutionOutcome(outcome="completed"),
        )


def test_failed_requires_failure() -> None:
    with pytest.raises(ValidationError, match="requires a failure"):
        make_execution(status=ExecutionStatus.FAILED, outcome=None)

    with pytest.raises(ValidationError, match="requires a failure"):
        make_step(status=ExecutionStatus.FAILED)


def test_duplicate_step_ids_rejected() -> None:
    with pytest.raises(ValidationError, match="unique"):
        make_execution(
            steps=(
                make_step(step_id="dup"),
                make_step(step_id="dup", name="second"),
            )
        )


def test_invalid_parent_references_rejected() -> None:
    with pytest.raises(ValidationError, match="unknown parent"):
        make_execution(steps=(make_step(step_id="a", parent_step_id="missing"),))

    with pytest.raises(ValidationError, match="own parent"):
        make_step(step_id="a", parent_step_id="a")

    with pytest.raises(ValidationError, match="cycle"):
        make_execution(
            steps=(
                make_step(step_id="a", parent_step_id="b"),
                make_step(step_id="b", parent_step_id="a"),
            )
        )


def test_step_timing_outside_execution_rejected() -> None:
    with pytest.raises(ValidationError, match="starts before"):
        make_execution(steps=(make_step(started_at=EXECUTION_START - timedelta(minutes=1)),))

    with pytest.raises(ValidationError, match="completes after"):
        make_execution(
            steps=(make_step(completed_at=EXECUTION_END + timedelta(minutes=1)),)
        )


def test_failed_child_step_does_not_fail_execution() -> None:
    failed_step = make_step(
        step_id="capability",
        kind=StepKind.CAPABILITY,
        status=ExecutionStatus.FAILED,
        failure=ExecutionFailure(category="timeout", detail="upstream did not respond"),
        capability_invocation=CapabilityInvocation(
            capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
        ),
    )

    execution = make_execution(steps=(failed_step,))

    assert execution.status is ExecutionStatus.COMPLETED
    assert execution.steps[0].status is ExecutionStatus.FAILED


def test_running_human_and_decision_steps_allow_pending_result() -> None:
    running_human = make_step(
        kind=StepKind.HUMAN,
        status=ExecutionStatus.RUNNING,
        completed_at=None,
        human_interaction=HumanInteraction(
            interaction_type="approval", actor_reference="reviewer-1"
        ),
    )
    assert running_human.human_interaction is not None
    assert running_human.human_interaction.outcome is None

    running_decision = make_step(
        kind=StepKind.DECISION,
        status=ExecutionStatus.RUNNING,
        completed_at=None,
        decision=DecisionRecord(rationale_reference="doc-1"),
    )
    assert running_decision.decision is not None
    assert running_decision.decision.decision is None

    with pytest.raises(ValidationError, match="must not have human_interaction.outcome"):
        make_step(
            kind=StepKind.HUMAN,
            status=ExecutionStatus.RUNNING,
            completed_at=None,
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        )

    with pytest.raises(ValidationError, match="must not have decision.decision"):
        make_step(
            kind=StepKind.DECISION,
            status=ExecutionStatus.RUNNING,
            completed_at=None,
            decision=DecisionRecord(decision="escalate"),
        )


def test_completed_requires_result_while_failed_and_cancelled_forbid_it() -> None:
    with pytest.raises(ValidationError, match="completed human step requires"):
        make_step(
            kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(interaction_type="approval"),
        )

    with pytest.raises(ValidationError, match="completed decision step requires"):
        make_step(kind=StepKind.DECISION, decision=DecisionRecord())

    with pytest.raises(ValidationError, match="must not have human_interaction.outcome"):
        make_step(
            kind=StepKind.HUMAN,
            status=ExecutionStatus.FAILED,
            failure=ExecutionFailure(category="timeout", detail="reviewer unavailable"),
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        )

    with pytest.raises(ValidationError, match="must not have decision.decision"):
        make_step(
            kind=StepKind.DECISION,
            status=ExecutionStatus.CANCELLED,
            decision=DecisionRecord(decision="escalate"),
        )


def test_governance_relevant_identifiers_are_normalized() -> None:
    model_invocation = ModelInvocation(
        provider=" anthropic ", model=" claude-sonnet-5 ", operation=" review "
    )
    assert model_invocation.provider == "anthropic"
    assert model_invocation.model == "claude-sonnet-5"
    assert model_invocation.operation == "review"

    capability_invocation = CapabilityInvocation(
        capability=" jira ", operation=" create_issue ", mode=CapabilityMode.WRITE
    )
    assert capability_invocation.capability == "jira"
    assert capability_invocation.operation == "create_issue"

    human_interaction = HumanInteraction(interaction_type=" approval ")
    assert human_interaction.interaction_type == "approval"

    with pytest.raises(ValidationError, match="blank"):
        ModelInvocation(provider="   ", model="claude-sonnet-5", operation="review")

    with pytest.raises(ValidationError, match="blank"):
        CapabilityInvocation(capability="jira", operation="   ", mode=CapabilityMode.WRITE)

    with pytest.raises(ValidationError, match="blank"):
        HumanInteraction(interaction_type="   ")


def test_evaluation_relevant_identifiers_are_normalized() -> None:
    outcome = ExecutionOutcome(outcome=" approved ")
    assert outcome.outcome == "approved"

    step = make_step(name=" supervisor-review ")
    assert step.name == "supervisor-review"

    execution = make_execution(
        system_id=" architecture-review-board ", workflow_name=" architecture-review "
    )
    assert execution.system_id == "architecture-review-board"
    assert execution.workflow_name == "architecture-review"

    with pytest.raises(ValidationError, match="blank"):
        ExecutionOutcome(outcome="   ")

    with pytest.raises(ValidationError, match="blank"):
        make_step(name="   ")

    with pytest.raises(ValidationError, match="blank"):
        make_execution(system_id="   ")
