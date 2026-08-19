"""Execution domain model.

An ExecutionRecord is a validated snapshot of one execution, not the ingestion event format
a later version will define. Steps are stored as a flat tuple with parent references rather
than nested child objects: this keeps ingestion append-only, serialization stable, and
querying simple, and it avoids recursive mutation entirely once combined with immutability.

Every model here is frozen and rejects unknown fields. These are point-in-time records, not
mutable objects a caller edits in place.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, model_validator

from agent_control_plane.domain.enums import CapabilityMode, ExecutionStatus, StepKind


def _require_non_blank(value: str) -> str:
    if not value.strip():
        raise ValueError("must not be blank")
    return value


def _require_non_negative_if_present(value: int | None) -> int | None:
    if value is not None and value < 0:
        raise ValueError("must be >= 0")
    return value


def _strip_and_require_non_blank_if_present(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        raise ValueError("must not be blank")
    return stripped


def _require_normalized_identifier(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("must not be blank")
    return stripped


NonBlankStr = Annotated[str, AfterValidator(_require_non_blank)]
NonNegativeOptionalInt = Annotated[int | None, AfterValidator(_require_non_negative_if_present)]
OptionalNonBlankStr = Annotated[str | None, AfterValidator(_strip_and_require_non_blank_if_present)]

# For identifiers exact-matched by governance and evaluation (provider/model/operation,
# capability/operation, interaction_type, outcome, step name, system_id, workflow_name).
# Strips incidental leading/trailing whitespace only; case and internal content stay
# producer-owned so exact matching remains exact.
NormalizedIdentifierStr = Annotated[str, AfterValidator(_require_normalized_identifier)]


class _DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionOutcome(_DomainModel):
    """The observed system's own domain outcome, distinct from lifecycle status.

    `outcome` is an open, application-owned identifier (for example approved,
    request_changes, escalated). The control plane deliberately does not define a universal
    outcome enum, since different observed systems have different domain outcomes.
    """

    outcome: NormalizedIdentifierStr
    detail: str | None = None


class ModelInvocation(_DomainModel):
    """Identity and usage of a single model call, without its content.

    Raw prompts and responses are intentionally excluded from the core domain: the control
    plane observes execution metadata, not private model content. `input_units` and
    `output_units` are named generically because not every provider calls usage units tokens.
    """

    provider: NormalizedIdentifierStr
    model: NormalizedIdentifierStr
    operation: NormalizedIdentifierStr
    input_units: NonNegativeOptionalInt = None
    output_units: NonNegativeOptionalInt = None


class CapabilityInvocation(_DomainModel):
    """Use of an external capability (an API, integration, or tool), without its payload."""

    capability: NormalizedIdentifierStr
    operation: NormalizedIdentifierStr
    target: str | None = None
    mode: CapabilityMode


class HumanInteraction(_DomainModel):
    """A point where a human approves, reviews, or confirms something in the execution.

    `outcome` is None while the interaction is still open: the step's own ExecutionStatus
    carries lifecycle state, and this model should not fabricate a result before one exists.
    `actor_reference` is an opaque caller-owned reference, not personal identity data. The
    control plane does not store names, emails, or profile information.
    """

    interaction_type: NormalizedIdentifierStr
    outcome: OptionalNonBlankStr = None
    actor_reference: str | None = None


class DecisionRecord(_DomainModel):
    """A discrete decision made during execution, without its full reasoning.

    `decision` is None until the decision is actually reached; a step that is still RUNNING,
    or one that failed or was cancelled before deciding, must not carry a final value here.
    `rationale_reference` may point to an external, auditable artifact later; the reasoning
    text itself is not stored in the core domain.
    """

    decision: OptionalNonBlankStr = None
    rationale_reference: str | None = None


class ExecutionFailure(_DomainModel):
    """A sanitized, producer-owned description of a failure.

    No traceback, exception object, or raw provider response: failure detail is text the
    producing system has already decided is safe to share with the control plane.
    """

    category: NonBlankStr
    detail: NonBlankStr
    retryable: bool | None = None


# Shared with the event layer, which enforces the same alignment rule for StepStartedEvent
# before a step even exists as an ExecutionStep.
STEP_KIND_DETAIL_FIELD: dict[StepKind, str] = {
    StepKind.MODEL: "model_invocation",
    StepKind.CAPABILITY: "capability_invocation",
    StepKind.HUMAN: "human_interaction",
    StepKind.DECISION: "decision",
}
STEP_DETAIL_FIELDS = ("model_invocation", "capability_invocation", "human_interaction", "decision")


class ExecutionStep(_DomainModel):
    """One step within an execution.

    Steps form a hierarchy through `parent_step_id` rather than nested child objects, so a
    step never embeds its own children. `parent_step_id` is validated against the full step
    set at the ExecutionRecord level, since a step cannot know its siblings on its own.
    """

    step_id: NonBlankStr
    parent_step_id: NonBlankStr | None = None
    kind: StepKind
    name: NormalizedIdentifierStr
    status: ExecutionStatus
    started_at: datetime
    completed_at: datetime | None = None
    model_invocation: ModelInvocation | None = None
    capability_invocation: CapabilityInvocation | None = None
    human_interaction: HumanInteraction | None = None
    decision: DecisionRecord | None = None
    failure: ExecutionFailure | None = None

    @model_validator(mode="after")
    def _validate_not_self_parent(self) -> ExecutionStep:
        if self.parent_step_id == self.step_id:
            raise ValueError("a step cannot be its own parent")
        return self

    @model_validator(mode="after")
    def _validate_detail_alignment(self) -> ExecutionStep:
        required_field = STEP_KIND_DETAIL_FIELD.get(self.kind)
        for field_name in STEP_DETAIL_FIELDS:
            value = getattr(self, field_name)
            if field_name == required_field:
                if value is None:
                    raise ValueError(f"a {self.kind.value} step requires {field_name}")
            elif value is not None:
                raise ValueError(f"a {self.kind.value} step must not set {field_name}")
        return self

    @model_validator(mode="after")
    def _validate_timing(self) -> ExecutionStep:
        if self.started_at.tzinfo is None:
            raise ValueError("started_at must be timezone-aware")
        if self.completed_at is not None and self.completed_at.tzinfo is None:
            raise ValueError("completed_at must be timezone-aware")
        if self.status is ExecutionStatus.RUNNING:
            if self.completed_at is not None:
                raise ValueError("a running step must not have completed_at")
        elif self.completed_at is None:
            raise ValueError(f"a {self.status.value} step requires completed_at")
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        return self

    @model_validator(mode="after")
    def _validate_failure(self) -> ExecutionStep:
        if self.status is ExecutionStatus.FAILED and self.failure is None:
            raise ValueError("a failed step requires a failure")
        if self.status is not ExecutionStatus.FAILED and self.failure is not None:
            raise ValueError("failure is only valid for a failed step")
        return self

    @model_validator(mode="after")
    def _validate_human_and_decision_results(self) -> ExecutionStep:
        # A result only exists once the step has actually completed. Failing or
        # cancelling a human review or decision must not fabricate an outcome it never
        # reached, and a running one has not reached one yet either.
        if self.kind is StepKind.HUMAN:
            if self.human_interaction is None:
                raise ValueError("a human step requires human_interaction")
            outcome = self.human_interaction.outcome
            if self.status is ExecutionStatus.COMPLETED and outcome is None:
                raise ValueError("a completed human step requires human_interaction.outcome")
            if self.status is not ExecutionStatus.COMPLETED and outcome is not None:
                raise ValueError(
                    f"a {self.status.value} human step must not have human_interaction.outcome"
                )
        if self.kind is StepKind.DECISION:
            if self.decision is None:
                raise ValueError("a decision step requires decision")
            decision_value = self.decision.decision
            if self.status is ExecutionStatus.COMPLETED and decision_value is None:
                raise ValueError("a completed decision step requires decision.decision")
            if self.status is not ExecutionStatus.COMPLETED and decision_value is not None:
                raise ValueError(
                    f"a {self.status.value} decision step must not have decision.decision"
                )
        return self


class ExecutionRecord(_DomainModel):
    """A validated snapshot of one execution and its steps.

    A failed step does not mechanically make the execution FAILED, and vice versa: the
    control plane records observed state rather than inventing workflow semantics. An
    observed workflow may degrade gracefully around a failed capability call or reviewer, and
    only the producing system knows whether that counts as an overall failure.
    """

    execution_id: NonBlankStr
    system_id: NormalizedIdentifierStr
    workflow_name: NormalizedIdentifierStr
    correlation_id: NonBlankStr | None = None
    status: ExecutionStatus
    started_at: datetime
    completed_at: datetime | None = None
    steps: tuple[ExecutionStep, ...] = ()
    outcome: ExecutionOutcome | None = None
    failure: ExecutionFailure | None = None

    @model_validator(mode="after")
    def _validate_timing(self) -> ExecutionRecord:
        if self.started_at.tzinfo is None:
            raise ValueError("started_at must be timezone-aware")
        if self.completed_at is not None and self.completed_at.tzinfo is None:
            raise ValueError("completed_at must be timezone-aware")
        if self.status is ExecutionStatus.RUNNING:
            if self.completed_at is not None:
                raise ValueError("a running execution must not have completed_at")
        elif self.completed_at is None:
            raise ValueError(f"a {self.status.value} execution requires completed_at")
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        return self

    @model_validator(mode="after")
    def _validate_outcome(self) -> ExecutionRecord:
        if self.status is ExecutionStatus.RUNNING and self.outcome is not None:
            raise ValueError("a running execution must not have an outcome")
        if self.status is ExecutionStatus.COMPLETED and self.outcome is None:
            raise ValueError("a completed execution requires an outcome")
        return self

    @model_validator(mode="after")
    def _validate_failure(self) -> ExecutionRecord:
        if self.status is ExecutionStatus.FAILED and self.failure is None:
            raise ValueError("a failed execution requires a failure")
        if self.status is not ExecutionStatus.FAILED and self.failure is not None:
            raise ValueError("failure is only valid for a failed execution")
        return self

    @model_validator(mode="after")
    def _validate_steps(self) -> ExecutionRecord:
        step_ids = [step.step_id for step in self.steps]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("step_id values must be unique within an execution")

        step_by_id = {step.step_id: step for step in self.steps}
        for step in self.steps:
            if step.parent_step_id is not None and step.parent_step_id not in step_by_id:
                raise ValueError(
                    f"step {step.step_id!r} references unknown parent "
                    f"{step.parent_step_id!r}"
                )

        # Steps may arrive in any order, so cycle detection cannot assume a parent
        # appears before its child in the list.
        for step in self.steps:
            visited = {step.step_id}
            current = step
            while current.parent_step_id is not None:
                parent_id = current.parent_step_id
                if parent_id in visited:
                    raise ValueError(f"parent cycle detected at step {step.step_id!r}")
                visited.add(parent_id)
                current = step_by_id[parent_id]

        for step in self.steps:
            if step.started_at < self.started_at:
                raise ValueError(
                    f"step {step.step_id!r} starts before the execution it belongs to"
                )
            if (
                self.completed_at is not None
                and step.completed_at is not None
                and step.completed_at > self.completed_at
            ):
                raise ValueError(
                    f"step {step.step_id!r} completes after the execution it belongs to"
                )
        return self
