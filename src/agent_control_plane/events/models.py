"""Typed execution events: the append-only facts an external producer emits.

Events are transition facts, not snapshot replacements. A StepCompletedEvent, for example,
carries only the detail that becomes known at completion, not a full copy of the step. The
projector in agent_control_plane.events.projection is what turns an ordered event stream into
an ExecutionRecord snapshot; these models do not know how to build one themselves.

event_type is a fixed literal per class rather than a plain string field, both so an
unrestricted string can never sneak into serialized history and so pydantic can use it as a
discriminator when parsing a stream back out of JSON.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from agent_control_plane.domain.enums import StepKind
from agent_control_plane.domain.models import (
    STEP_DETAIL_FIELDS,
    STEP_KIND_DETAIL_FIELD,
    CapabilityInvocation,
    DecisionRecord,
    ExecutionFailure,
    ExecutionOutcome,
    HumanInteraction,
    ModelInvocation,
    NonBlankStr,
    OptionalNonBlankStr,
)


def _require_positive(value: int) -> int:
    if value < 1:
        raise ValueError("must be >= 1")
    return value


PositiveInt = Annotated[int, AfterValidator(_require_positive)]


class _EventModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _EventEnvelope(_EventModel):
    """Fields every execution event carries.

    `sequence` is the control plane's own logical ordering within one execution, scoped by the
    caller; it is what append_execution_event uses for ordering. `occurred_at` is producer wall
    clock time and never drives ordering, only the lifecycle timestamps it eventually becomes.
    """

    event_id: NonBlankStr
    execution_id: NonBlankStr
    sequence: PositiveInt
    occurred_at: datetime

    @model_validator(mode="after")
    def _validate_occurred_at(self) -> _EventEnvelope:
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        return self


class ExecutionStartedEvent(_EventEnvelope):
    """The first event of an execution. No other event may precede it."""

    event_type: Literal["execution_started"] = "execution_started"
    system_id: NonBlankStr
    workflow_name: NonBlankStr
    correlation_id: OptionalNonBlankStr = None


class StepStartedEvent(_EventEnvelope):
    """Establishes a step in RUNNING state.

    Detail alignment mirrors ExecutionStep: exactly the field a given StepKind requires may be
    set, and nothing else. A started HUMAN or DECISION detail must not already carry a final
    result, since the event only means the step has begun.
    """

    event_type: Literal["step_started"] = "step_started"
    step_id: NonBlankStr
    parent_step_id: NonBlankStr | None = None
    kind: StepKind
    name: NonBlankStr
    model_invocation: ModelInvocation | None = None
    capability_invocation: CapabilityInvocation | None = None
    human_interaction: HumanInteraction | None = None
    decision: DecisionRecord | None = None

    @model_validator(mode="after")
    def _validate_not_self_parent(self) -> StepStartedEvent:
        if self.parent_step_id == self.step_id:
            raise ValueError("a step cannot be its own parent")
        return self

    @model_validator(mode="after")
    def _validate_detail_alignment(self) -> StepStartedEvent:
        required_field = STEP_KIND_DETAIL_FIELD.get(self.kind)
        for field_name in STEP_DETAIL_FIELDS:
            value = getattr(self, field_name)
            if field_name == required_field:
                if value is None:
                    raise ValueError(f"a {self.kind.value} step start requires {field_name}")
            elif value is not None:
                raise ValueError(f"a {self.kind.value} step start must not set {field_name}")
        return self

    @model_validator(mode="after")
    def _validate_no_premature_result(self) -> StepStartedEvent:
        if self.human_interaction is not None and self.human_interaction.outcome is not None:
            raise ValueError("a step start must not carry a human_interaction outcome")
        if self.decision is not None and self.decision.decision is not None:
            raise ValueError("a step start must not carry a final decision")
        return self


class StepCompletedEvent(_EventEnvelope):
    """The final detail a step's completion reveals, not a full step snapshot.

    Whether a given detail field is even legal here depends on the step's existing kind, which
    this event does not know by itself. That alignment is enforced during projection, where the
    step's kind is available.
    """

    event_type: Literal["step_completed"] = "step_completed"
    step_id: NonBlankStr
    model_invocation: ModelInvocation | None = None
    human_interaction: HumanInteraction | None = None
    decision: DecisionRecord | None = None

    @model_validator(mode="after")
    def _validate_result_is_final(self) -> StepCompletedEvent:
        if self.human_interaction is not None and self.human_interaction.outcome is None:
            raise ValueError("a step completion human_interaction must supply outcome")
        if self.decision is not None and self.decision.decision is None:
            raise ValueError("a step completion decision must supply decision")
        return self


class StepFailedEvent(_EventEnvelope):
    """A step failing. HUMAN/DECISION results and MODEL usage are not fabricated here."""

    event_type: Literal["step_failed"] = "step_failed"
    step_id: NonBlankStr
    failure: ExecutionFailure


class StepCancelledEvent(_EventEnvelope):
    """A step being cancelled. Cancellation is a lifecycle state, not a failure."""

    event_type: Literal["step_cancelled"] = "step_cancelled"
    step_id: NonBlankStr


class ExecutionCompletedEvent(_EventEnvelope):
    event_type: Literal["execution_completed"] = "execution_completed"
    outcome: ExecutionOutcome


class ExecutionFailedEvent(_EventEnvelope):
    event_type: Literal["execution_failed"] = "execution_failed"
    failure: ExecutionFailure
    outcome: ExecutionOutcome | None = None


class ExecutionCancelledEvent(_EventEnvelope):
    event_type: Literal["execution_cancelled"] = "execution_cancelled"
    outcome: ExecutionOutcome | None = None


ExecutionEvent = Annotated[
    ExecutionStartedEvent
    | StepStartedEvent
    | StepCompletedEvent
    | StepFailedEvent
    | StepCancelledEvent
    | ExecutionCompletedEvent
    | ExecutionFailedEvent
    | ExecutionCancelledEvent,
    Field(discriminator="event_type"),
]
