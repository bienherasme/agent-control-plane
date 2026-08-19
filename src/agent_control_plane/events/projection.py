"""Deterministic projection from an execution's event history into a snapshot.

project_execution folds events strictly in the sequence order the stream already guarantees;
it never sorts or reorders. It keeps only local, transient state for the duration of the call,
so the same stream always yields the same ExecutionRecord: no clock, randomness, or I/O
influences the result.

Every ExecutionStep and ExecutionRecord this module builds goes through the ordinary domain
constructor, so the domain's own invariants (timing, detail alignment, outcome/failure
consistency) apply here for free rather than being reimplemented.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import ValidationError

from agent_control_plane.domain.enums import ExecutionStatus, StepKind
from agent_control_plane.domain.models import (
    DecisionRecord,
    ExecutionFailure,
    ExecutionOutcome,
    ExecutionRecord,
    ExecutionStep,
    HumanInteraction,
    ModelInvocation,
)
from agent_control_plane.events.models import (
    ExecutionCancelledEvent,
    ExecutionCompletedEvent,
    ExecutionEvent,
    ExecutionFailedEvent,
    ExecutionStartedEvent,
    StepCancelledEvent,
    StepCompletedEvent,
    StepFailedEvent,
    StepStartedEvent,
)

if TYPE_CHECKING:
    from agent_control_plane.events.stream import ExecutionEventStream


class ExecutionProjectionError(Exception):
    """Raised when an event stream cannot be folded into a valid ExecutionRecord."""


def project_execution(stream: ExecutionEventStream) -> ExecutionRecord:
    if not stream.events:
        raise ExecutionProjectionError("an empty stream has no execution snapshot yet")
    return _fold(stream.events)


def _fold(events: tuple[ExecutionEvent, ...]) -> ExecutionRecord:
    first = events[0]
    if not isinstance(first, ExecutionStartedEvent):
        raise ExecutionProjectionError("the first event of an execution must start it")

    steps: dict[str, ExecutionStep] = {}
    status: ExecutionStatus = ExecutionStatus.RUNNING
    outcome: ExecutionOutcome | None = None
    failure: ExecutionFailure | None = None
    completed_at: datetime | None = None

    for event in events[1:]:
        if isinstance(event, StepStartedEvent):
            _apply_step_started(event, steps)
        elif isinstance(event, StepCompletedEvent):
            _apply_step_completed(event, steps)
        elif isinstance(event, StepFailedEvent):
            _apply_step_failed(event, steps)
        elif isinstance(event, StepCancelledEvent):
            _apply_step_cancelled(event, steps)
        elif isinstance(event, ExecutionCompletedEvent):
            _require_no_running_steps(steps)
            status, outcome, failure, completed_at = (
                ExecutionStatus.COMPLETED,
                event.outcome,
                None,
                event.occurred_at,
            )
        elif isinstance(event, ExecutionFailedEvent):
            _require_no_running_steps(steps)
            status, outcome, failure, completed_at = (
                ExecutionStatus.FAILED,
                event.outcome,
                event.failure,
                event.occurred_at,
            )
        elif isinstance(event, ExecutionCancelledEvent):
            _require_no_running_steps(steps)
            status, outcome, failure, completed_at = (
                ExecutionStatus.CANCELLED,
                event.outcome,
                None,
                event.occurred_at,
            )
        else:
            raise ExecutionProjectionError("an execution may only start once")

    try:
        return ExecutionRecord(
            execution_id=first.execution_id,
            system_id=first.system_id,
            workflow_name=first.workflow_name,
            correlation_id=first.correlation_id,
            status=status,
            started_at=first.occurred_at,
            completed_at=completed_at,
            steps=tuple(steps.values()),
            outcome=outcome,
            failure=failure,
        )
    except ValidationError as exc:
        raise ExecutionProjectionError(str(exc)) from exc


def _apply_step_started(event: StepStartedEvent, steps: dict[str, ExecutionStep]) -> None:
    if event.step_id in steps:
        raise ExecutionProjectionError(f"step {event.step_id!r} has already started")

    if event.parent_step_id is not None:
        parent = steps.get(event.parent_step_id)
        if parent is None:
            raise ExecutionProjectionError(
                f"step {event.step_id!r} references a parent that has not started"
            )
        if parent.status is not ExecutionStatus.RUNNING:
            raise ExecutionProjectionError(
                f"step {event.step_id!r} cannot start under a parent that is not running"
            )

    try:
        step = ExecutionStep(
            step_id=event.step_id,
            parent_step_id=event.parent_step_id,
            kind=event.kind,
            name=event.name,
            status=ExecutionStatus.RUNNING,
            started_at=event.occurred_at,
            completed_at=None,
            model_invocation=event.model_invocation,
            capability_invocation=event.capability_invocation,
            human_interaction=event.human_interaction,
            decision=event.decision,
            failure=None,
        )
    except ValidationError as exc:
        raise ExecutionProjectionError(str(exc)) from exc

    steps[event.step_id] = step


def _require_running_step(step_id: str, steps: dict[str, ExecutionStep]) -> ExecutionStep:
    existing = steps.get(step_id)
    if existing is None:
        raise ExecutionProjectionError(f"terminal event for unknown step {step_id!r}")
    if existing.status is not ExecutionStatus.RUNNING:
        raise ExecutionProjectionError(f"step {step_id!r} is no longer running")
    return existing


def _require_no_running_descendant(step_id: str, steps: dict[str, ExecutionStep]) -> None:
    for candidate in steps.values():
        if candidate.status is not ExecutionStatus.RUNNING:
            continue
        ancestor: ExecutionStep | None = candidate
        while ancestor is not None and ancestor.parent_step_id is not None:
            if ancestor.parent_step_id == step_id:
                raise ExecutionProjectionError(
                    f"step {step_id!r} cannot terminate while descendant "
                    f"{candidate.step_id!r} is running"
                )
            ancestor = steps.get(ancestor.parent_step_id)


def _require_no_running_steps(steps: dict[str, ExecutionStep]) -> None:
    running = sorted(
        step_id for step_id, step in steps.items() if step.status is ExecutionStatus.RUNNING
    )
    if running:
        raise ExecutionProjectionError(
            f"execution cannot terminate while steps are running: {running!r}"
        )


def _replace_step(
    existing: ExecutionStep,
    *,
    status: ExecutionStatus,
    completed_at: datetime,
    failure: ExecutionFailure | None = None,
    model_invocation: ModelInvocation | None = None,
    human_interaction: HumanInteraction | None = None,
    decision: DecisionRecord | None = None,
) -> ExecutionStep:
    try:
        return ExecutionStep(
            step_id=existing.step_id,
            parent_step_id=existing.parent_step_id,
            kind=existing.kind,
            name=existing.name,
            status=status,
            started_at=existing.started_at,
            completed_at=completed_at,
            model_invocation=model_invocation or existing.model_invocation,
            capability_invocation=existing.capability_invocation,
            human_interaction=human_interaction or existing.human_interaction,
            decision=decision or existing.decision,
            failure=failure or existing.failure,
        )
    except ValidationError as exc:
        raise ExecutionProjectionError(str(exc)) from exc


def _apply_step_completed(event: StepCompletedEvent, steps: dict[str, ExecutionStep]) -> None:
    existing = _require_running_step(event.step_id, steps)
    _require_no_running_descendant(event.step_id, steps)

    merged_model_invocation: ModelInvocation | None = None
    merged_human_interaction: HumanInteraction | None = None
    merged_decision: DecisionRecord | None = None

    if existing.kind is StepKind.MODEL:
        if event.human_interaction is not None or event.decision is not None:
            raise ExecutionProjectionError(
                f"step {event.step_id!r} completion must not carry human/decision detail"
            )
        if existing.model_invocation is None:
            raise ExecutionProjectionError(f"step {event.step_id!r} is missing its invocation")
        merged_model_invocation = _merge_model_invocation(
            existing.model_invocation, event.model_invocation
        )
    elif existing.kind is StepKind.HUMAN:
        if event.model_invocation is not None or event.decision is not None:
            raise ExecutionProjectionError(
                f"step {event.step_id!r} completion must not carry model/decision detail"
            )
        if event.human_interaction is None:
            raise ExecutionProjectionError(
                f"step {event.step_id!r} completion requires human_interaction"
            )
        if existing.human_interaction is None:
            raise ExecutionProjectionError(f"step {event.step_id!r} is missing its interaction")
        merged_human_interaction = _merge_human_interaction(
            existing.human_interaction, event.human_interaction
        )
    elif existing.kind is StepKind.DECISION:
        if event.model_invocation is not None or event.human_interaction is not None:
            raise ExecutionProjectionError(
                f"step {event.step_id!r} completion must not carry model/human detail"
            )
        if event.decision is None:
            raise ExecutionProjectionError(f"step {event.step_id!r} completion requires decision")
        if existing.decision is None:
            raise ExecutionProjectionError(f"step {event.step_id!r} is missing its decision")
        merged_decision = _merge_decision(existing.decision, event.decision)
    else:
        if (
            event.model_invocation is not None
            or event.human_interaction is not None
            or event.decision is not None
        ):
            raise ExecutionProjectionError(
                f"a {existing.kind.value} step completion must not carry completion detail"
            )

    steps[event.step_id] = _replace_step(
        existing,
        status=ExecutionStatus.COMPLETED,
        completed_at=event.occurred_at,
        model_invocation=merged_model_invocation,
        human_interaction=merged_human_interaction,
        decision=merged_decision,
    )


def _apply_step_failed(event: StepFailedEvent, steps: dict[str, ExecutionStep]) -> None:
    existing = _require_running_step(event.step_id, steps)
    _require_no_running_descendant(event.step_id, steps)
    steps[event.step_id] = _replace_step(
        existing,
        status=ExecutionStatus.FAILED,
        completed_at=event.occurred_at,
        failure=event.failure,
    )


def _apply_step_cancelled(event: StepCancelledEvent, steps: dict[str, ExecutionStep]) -> None:
    existing = _require_running_step(event.step_id, steps)
    _require_no_running_descendant(event.step_id, steps)
    steps[event.step_id] = _replace_step(
        existing,
        status=ExecutionStatus.CANCELLED,
        completed_at=event.occurred_at,
        failure=None,
    )


def _merge_optional_int(existing: int | None, incoming: int | None, field: str) -> int | None:
    if existing is None:
        return incoming
    if incoming is None:
        return existing
    if existing != incoming:
        raise ExecutionProjectionError(f"{field} cannot change at completion")
    return existing


def _merge_optional_str(existing: str | None, incoming: str | None, field: str) -> str | None:
    if existing is None:
        return incoming
    if incoming is None:
        return existing
    if existing != incoming:
        raise ExecutionProjectionError(f"{field} cannot change at completion")
    return existing


def _merge_model_invocation(
    existing: ModelInvocation, incoming: ModelInvocation | None
) -> ModelInvocation:
    if incoming is None:
        return existing
    if (
        incoming.provider != existing.provider
        or incoming.model != existing.model
        or incoming.operation != existing.operation
    ):
        raise ExecutionProjectionError("model invocation identity cannot change at completion")
    return ModelInvocation(
        provider=existing.provider,
        model=existing.model,
        operation=existing.operation,
        input_units=_merge_optional_int(
            existing.input_units, incoming.input_units, "input_units"
        ),
        output_units=_merge_optional_int(
            existing.output_units, incoming.output_units, "output_units"
        ),
    )


def _merge_human_interaction(
    existing: HumanInteraction, incoming: HumanInteraction
) -> HumanInteraction:
    if incoming.interaction_type != existing.interaction_type:
        raise ExecutionProjectionError("interaction_type cannot change at completion")
    return HumanInteraction(
        interaction_type=existing.interaction_type,
        outcome=incoming.outcome,
        actor_reference=_merge_optional_str(
            existing.actor_reference, incoming.actor_reference, "actor_reference"
        ),
    )


def _merge_decision(existing: DecisionRecord, incoming: DecisionRecord) -> DecisionRecord:
    return DecisionRecord(
        decision=incoming.decision,
        rationale_reference=_merge_optional_str(
            existing.rationale_reference, incoming.rationale_reference, "rationale_reference"
        ),
    )
