from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from agent_control_plane.domain import (
    CapabilityInvocation,
    CapabilityMode,
    DecisionRecord,
    ExecutionFailure,
    ExecutionOutcome,
    ExecutionStatus,
    HumanInteraction,
    ModelInvocation,
    StepKind,
)
from agent_control_plane.events import (
    ExecutionCompletedEvent,
    ExecutionEvent,
    ExecutionEventConflictError,
    ExecutionEventStream,
    ExecutionProjectionError,
    ExecutionStartedEvent,
    StepCancelledEvent,
    StepCompletedEvent,
    StepFailedEvent,
    StepStartedEvent,
    append_execution_event,
    project_execution,
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


def test_execution_started_projects_running_snapshot() -> None:
    stream = build_stream(started_event())
    record = project_execution(stream)

    assert record.execution_id == "exec-1"
    assert record.system_id == "incident-commander"
    assert record.workflow_name == "incident-analysis"
    assert record.status is ExecutionStatus.RUNNING
    assert record.started_at == at(0)
    assert record.completed_at is None
    assert record.steps == ()
    assert record.outcome is None


def test_mixed_step_kinds_project_in_start_order_and_is_deterministic() -> None:
    stream = build_stream(
        started_event(),
        step_started(event_id="e-workflow", sequence=2, step_id="workflow"),
        step_started(
            event_id="e-model",
            sequence=3,
            step_id="model",
            parent_step_id="workflow",
            kind=StepKind.MODEL,
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review"
            ),
        ),
        step_started(
            event_id="e-capability",
            sequence=4,
            step_id="capability",
            parent_step_id="workflow",
            kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
        step_started(
            event_id="e-human",
            sequence=5,
            step_id="human",
            parent_step_id="workflow",
            kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(interaction_type="approval"),
        ),
        step_started(
            event_id="e-decision",
            sequence=6,
            step_id="decision",
            parent_step_id="workflow",
            kind=StepKind.DECISION,
            decision=DecisionRecord(),
        ),
    )

    record_a = project_execution(stream)
    record_b = project_execution(stream)

    assert [step.step_id for step in record_a.steps] == [
        "workflow",
        "model",
        "capability",
        "human",
        "decision",
    ]
    assert record_a == record_b


def test_duplicate_event_is_idempotent() -> None:
    stream0 = ExecutionEventStream(execution_id="exec-1")
    event = started_event()

    stream1 = append_execution_event(stream0, event)
    stream2 = append_execution_event(stream1, event)

    assert stream2 == stream1
    assert len(stream2.events) == len(stream1.events) == 1


def test_duplicate_event_id_with_different_content_conflicts() -> None:
    stream = build_stream(started_event())
    conflicting = step_started(event_id="evt-start", sequence=2)

    with pytest.raises(ExecutionEventConflictError, match="already appended"):
        append_execution_event(stream, conflicting)


def test_append_structural_conflicts_rejected() -> None:
    stream = build_stream(started_event())

    with pytest.raises(ExecutionEventConflictError, match="does not match stream"):
        append_execution_event(stream, step_started(execution_id="exec-2", sequence=2))

    with pytest.raises(ExecutionEventConflictError, match="expected 2"):
        append_execution_event(stream, step_started(event_id="evt-gap", sequence=3))

    with pytest.raises(ExecutionEventConflictError, match="expected 2"):
        append_execution_event(stream, step_started(event_id="evt-stale", sequence=1))

    stream_with_two = append_execution_event(
        stream, step_started(event_id="e-w", sequence=2, step_id="workflow")
    )

    with pytest.raises(ExecutionEventConflictError, match="expected 3"):
        append_execution_event(
            stream_with_two,
            step_started(event_id="e-different", sequence=2, step_id="other"),
        )

    closed_stream = append_execution_event(
        append_execution_event(
            stream_with_two,
            StepCompletedEvent(
                event_id="e-wc",
                execution_id="exec-1",
                sequence=3,
                occurred_at=at(2),
                step_id="workflow",
            ),
        ),
        ExecutionCompletedEvent(
            event_id="e-done",
            execution_id="exec-1",
            sequence=4,
            occurred_at=at(3),
            outcome=ExecutionOutcome(outcome="completed"),
        ),
    )

    with pytest.raises(ExecutionEventConflictError, match="already closed"):
        append_execution_event(
            closed_stream, step_started(event_id="e-late", sequence=5, step_id="late")
        )


def test_step_lifecycle_and_parent_transitions_rejected() -> None:
    with pytest.raises(ExecutionEventConflictError, match="unknown step"):
        build_stream(
            started_event(),
            StepCompletedEvent(
                event_id="e-c", execution_id="exec-1", sequence=2, occurred_at=at(1),
                step_id="ghost",
            ),
        )

    with pytest.raises(ExecutionEventConflictError, match="already started"):
        build_stream(
            started_event(),
            step_started(event_id="e-1", sequence=2, step_id="dup"),
            step_started(event_id="e-2", sequence=3, step_id="dup"),
        )

    with pytest.raises(ExecutionEventConflictError, match="no longer running"):
        build_stream(
            started_event(),
            step_started(event_id="e-1", sequence=2, step_id="s"),
            StepCompletedEvent(
                event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(2),
                step_id="s",
            ),
            StepFailedEvent(
                event_id="e-3", execution_id="exec-1", sequence=4, occurred_at=at(3),
                step_id="s", failure=ExecutionFailure(category="x", detail="y"),
            ),
        )

    with pytest.raises(ExecutionEventConflictError, match="has not started"):
        build_stream(
            started_event(),
            step_started(
                event_id="e-1", sequence=2, step_id="child", parent_step_id="missing-parent"
            ),
        )

    with pytest.raises(ExecutionEventConflictError, match="cannot start under a parent"):
        build_stream(
            started_event(),
            step_started(event_id="e-1", sequence=2, step_id="parent"),
            StepCompletedEvent(
                event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(2),
                step_id="parent",
            ),
            step_started(
                event_id="e-3", sequence=4, step_id="late-child", parent_step_id="parent"
            ),
        )


def test_terminal_transitions_blocked_by_running_children() -> None:
    with pytest.raises(ExecutionEventConflictError, match="descendant"):
        build_stream(
            started_event(),
            step_started(event_id="e-1", sequence=2, step_id="parent"),
            step_started(
                event_id="e-2", sequence=3, step_id="child", parent_step_id="parent"
            ),
            StepCompletedEvent(
                event_id="e-3", execution_id="exec-1", sequence=4, occurred_at=at(3),
                step_id="parent",
            ),
        )

    with pytest.raises(ExecutionEventConflictError, match="steps are running"):
        build_stream(
            started_event(),
            step_started(event_id="e-1", sequence=2, step_id="s"),
            ExecutionCompletedEvent(
                event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(2),
                outcome=ExecutionOutcome(outcome="completed"),
            ),
        )


def test_completion_merges_preserve_identity_and_reject_contradiction() -> None:
    model_events = [
        started_event(),
        step_started(
            event_id="e-model", sequence=2, step_id="model", kind=StepKind.MODEL,
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review",
                input_units=100,
            ),
        ),
    ]
    stream = append_execution_event(
        build_stream(*model_events),
        StepCompletedEvent(
            event_id="e-model-done", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="model",
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review",
                input_units=100, output_units=40,
            ),
        ),
    )
    model_step = project_execution(stream).steps[0]
    assert model_step.model_invocation is not None
    assert model_step.model_invocation.input_units == 100
    assert model_step.model_invocation.output_units == 40

    with pytest.raises(ExecutionEventConflictError, match="cannot change at completion"):
        append_execution_event(
            build_stream(*model_events),
            StepCompletedEvent(
                event_id="e-model-bad", execution_id="exec-1", sequence=3, occurred_at=at(2),
                step_id="model",
                model_invocation=ModelInvocation(
                    provider="anthropic", model="claude-sonnet-5", operation="review",
                    input_units=999,
                ),
            ),
        )

    human_stream = build_stream(
        started_event(),
        step_started(
            event_id="e-human", sequence=2, step_id="human", kind=StepKind.HUMAN,
            human_interaction=HumanInteraction(
                interaction_type="approval", actor_reference="reviewer-17"
            ),
        ),
    )
    completed_human = append_execution_event(
        human_stream,
        StepCompletedEvent(
            event_id="e-human-done", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="human",
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        ),
    )
    human_step = project_execution(completed_human).steps[0]
    assert human_step.human_interaction is not None
    assert human_step.human_interaction.outcome == "approved"
    assert human_step.human_interaction.actor_reference == "reviewer-17"

    with pytest.raises(ExecutionEventConflictError, match="cannot change at completion"):
        append_execution_event(
            human_stream,
            StepCompletedEvent(
                event_id="e-human-bad", execution_id="exec-1", sequence=3, occurred_at=at(2),
                step_id="human",
                human_interaction=HumanInteraction(
                    interaction_type="approval", outcome="approved",
                    actor_reference="someone-else",
                ),
            ),
        )

    decision_stream = build_stream(
        started_event(),
        step_started(
            event_id="e-decision", sequence=2, step_id="decision", kind=StepKind.DECISION,
            decision=DecisionRecord(rationale_reference="doc-1"),
        ),
    )
    completed_decision = append_execution_event(
        decision_stream,
        StepCompletedEvent(
            event_id="e-decision-done", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="decision",
            decision=DecisionRecord(decision="escalate"),
        ),
    )
    decision_step = project_execution(completed_decision).steps[0]
    assert decision_step.decision is not None
    assert decision_step.decision.decision == "escalate"
    assert decision_step.decision.rationale_reference == "doc-1"

    with pytest.raises(ExecutionEventConflictError, match="cannot change at completion"):
        append_execution_event(
            decision_stream,
            StepCompletedEvent(
                event_id="e-decision-bad", execution_id="exec-1", sequence=3, occurred_at=at(2),
                step_id="decision",
                decision=DecisionRecord(decision="escalate", rationale_reference="doc-2"),
            ),
        )


def test_empty_stream_projection_fails() -> None:
    with pytest.raises(ExecutionProjectionError, match="empty stream"):
        project_execution(ExecutionEventStream(execution_id="exec-1"))


def test_timestamp_contradictions_rejected_without_global_monotonicity() -> None:
    with pytest.raises(ExecutionEventConflictError, match="must not precede"):
        build_stream(
            started_event(),
            step_started(event_id="e-1", sequence=2, step_id="s", occurred_at=at(5)),
            StepCancelledEvent(
                event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(1),
                step_id="s",
            ),
        )

    stream = build_stream(
        started_event(),
        step_started(event_id="e-1", sequence=2, step_id="a", occurred_at=at(10)),
        step_started(event_id="e-2", sequence=3, step_id="b", occurred_at=at(5)),
    )
    record = project_execution(stream)
    assert record.steps[0].started_at == at(10)
    assert record.steps[1].started_at == at(5)


def test_failed_child_step_allows_execution_completion() -> None:
    stream = build_stream(
        started_event(),
        step_started(
            event_id="e-1", sequence=2, step_id="capability", kind=StepKind.CAPABILITY,
            capability_invocation=CapabilityInvocation(
                capability="jira", operation="create_issue", mode=CapabilityMode.WRITE
            ),
        ),
        StepFailedEvent(
            event_id="e-2", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="capability",
            failure=ExecutionFailure(category="timeout", detail="upstream did not respond"),
        ),
        ExecutionCompletedEvent(
            event_id="e-3", execution_id="exec-1", sequence=4, occurred_at=at(3),
            outcome=ExecutionOutcome(outcome="completed_with_warnings"),
        ),
    )

    record = project_execution(stream)

    assert record.status is ExecutionStatus.COMPLETED
    assert record.steps[0].status is ExecutionStatus.FAILED


def test_event_stream_round_trips_through_json() -> None:
    stream = build_stream(
        started_event(),
        step_started(event_id="e-1", sequence=2, step_id="s"),
    )

    restored = ExecutionEventStream.model_validate_json(stream.model_dump_json())

    assert restored == stream
    assert isinstance(restored.events[0], ExecutionStartedEvent)
    assert isinstance(restored.events[1], StepStartedEvent)
