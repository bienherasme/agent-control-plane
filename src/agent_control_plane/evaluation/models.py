"""Evaluation domain: typed expectations, their observations, and aggregate reports.

Evaluation is a separate concern from governance. Governance asks whether configured policy was
violated by observed behavior; evaluation asks whether observed behavior matched a declared
scenario expectation. The two never call into each other and an evaluation report never embeds
or derives from a governance status.

Expectations are trusted control-plane configuration, exactly like governance policies. They
operate only on typed execution facts: statuses, outcome identity, step/capability identity and
counts, and human interaction outcome. Nothing here inspects free-text fields such as
ExecutionOutcome.detail, ExecutionFailure.detail, or a DecisionRecord's rationale reference.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from agent_control_plane.domain.enums import CapabilityMode, ExecutionStatus, StepKind
from agent_control_plane.domain.models import (
    NonBlankStr,
    NonNegativeOptionalInt,
    OptionalNonBlankStr,
)
from agent_control_plane.evaluation.enums import EvaluationStatus


def _require_non_negative(value: int) -> int:
    if value < 0:
        raise ValueError("must be >= 0")
    return value


def _strip_required(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("must not be blank")
    return stripped


def _normalize_required_strings(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = []
    for value in values:
        stripped = value.strip()
        if not stripped:
            raise ValueError("values must not be blank")
        normalized.append(stripped)
    if not normalized:
        raise ValueError("must not be empty")
    if len(set(normalized)) != len(normalized):
        raise ValueError("values must be unique")
    return tuple(normalized)


def _require_unique_non_empty_statuses(
    values: tuple[ExecutionStatus, ...],
) -> tuple[ExecutionStatus, ...]:
    if not values:
        raise ValueError("must not be empty")
    if len(set(values)) != len(values):
        raise ValueError("must be unique")
    return values


NonNegativeInt = Annotated[int, AfterValidator(_require_non_negative)]
NormalizedRequiredStr = Annotated[str, AfterValidator(_strip_required)]
RequiredNormalizedStrings = Annotated[tuple[str, ...], AfterValidator(_normalize_required_strings)]
AcceptableStatuses = Annotated[
    tuple[ExecutionStatus, ...], AfterValidator(_require_unique_non_empty_statuses)
]


class _EvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionStatusExpectation(_EvaluationModel):
    """The execution's lifecycle status must be one of a declared set.

    Evaluates RUNNING executions honestly: if RUNNING is itself an accepted status this passes
    immediately, and no terminal state is special-cased.
    """

    expectation_type: Literal["execution_status"] = "execution_status"
    expectation_id: NonBlankStr
    description: NonBlankStr
    acceptable_statuses: AcceptableStatuses


class ExecutionOutcomeExpectation(_EvaluationModel):
    """The execution's declared domain outcome must be one of a declared set.

    An outcome is never inferred from lifecycle status. A RUNNING execution with no outcome yet
    is INDETERMINATE; a terminal execution with no outcome is a FAIL, since the domain already
    allows FAILED/CANCELLED executions to carry none and that means the expected outcome simply
    was not observed.
    """

    expectation_type: Literal["execution_outcome"] = "execution_outcome"
    expectation_id: NonBlankStr
    description: NonBlankStr
    acceptable_outcomes: RequiredNormalizedStrings


class _OccurrenceExpectation(_EvaluationModel):
    """Shared occurrence-count bounds for step/capability/human-interaction expectations."""

    min_occurrences: NonNegativeInt = 1
    max_occurrences: NonNegativeOptionalInt = None

    @model_validator(mode="after")
    def _validate_occurrence_bounds(self) -> _OccurrenceExpectation:
        if self.max_occurrences is not None and self.max_occurrences < self.min_occurrences:
            raise ValueError("max_occurrences must be >= min_occurrences")
        return self


class StepOccurrenceExpectation(_OccurrenceExpectation):
    """A count bound over steps matching an optional kind/name/status filter.

    kind is a wildcard when None, otherwise exact. name is a wildcard when None, otherwise an
    exact match after stripping the configured value. statuses is a wildcard when empty,
    otherwise exact status membership. There is no fuzzy or partial name matching.
    """

    expectation_type: Literal["step_occurrence"] = "step_occurrence"
    expectation_id: NonBlankStr
    description: NonBlankStr
    kind: StepKind | None = None
    name: OptionalNonBlankStr = None
    statuses: tuple[ExecutionStatus, ...] = ()


class CapabilityOccurrenceExpectation(_OccurrenceExpectation):
    """A count bound over CAPABILITY steps matching an optional capability/operation/mode filter.

    Counts from the fact that a matching capability step was observed to start, regardless of
    its final lifecycle status, exactly like CapabilityBoundaryPolicy in governance. This is
    evaluation, not governance: matching a mode here says nothing about whether that mode is
    permitted by policy.
    """

    expectation_type: Literal["capability_occurrence"] = "capability_occurrence"
    expectation_id: NonBlankStr
    description: NonBlankStr
    capability: OptionalNonBlankStr = None
    operation: OptionalNonBlankStr = None
    modes: tuple[CapabilityMode, ...] = ()


class HumanInteractionExpectation(_OccurrenceExpectation):
    """A count bound over HUMAN steps that completed with one of the accepted outcomes.

    Only a COMPLETED human step with a matching interaction_type and an accepted outcome
    counts. A step that is still RUNNING, or one that FAILED or was CANCELLED, never counts,
    since none of those represent a reached, accepted outcome.
    """

    expectation_type: Literal["human_interaction"] = "human_interaction"
    expectation_id: NonBlankStr
    description: NonBlankStr
    interaction_type: NormalizedRequiredStr
    accepted_outcomes: RequiredNormalizedStrings


EvaluationExpectation = Annotated[
    ExecutionStatusExpectation
    | ExecutionOutcomeExpectation
    | StepOccurrenceExpectation
    | CapabilityOccurrenceExpectation
    | HumanInteractionExpectation,
    Field(discriminator="expectation_type"),
]


class EvaluationExpectationSet(_EvaluationModel):
    """An ordered, immutable expectation collection. Evaluation and reporting follow this order.

    An empty expectation set is valid and evaluates to PASS with zero observations.
    """

    expectation_set_id: NonBlankStr
    expectations: tuple[EvaluationExpectation, ...] = ()

    @model_validator(mode="after")
    def _validate_unique_expectation_ids(self) -> EvaluationExpectationSet:
        expectation_ids = [expectation.expectation_id for expectation in self.expectations]
        if len(set(expectation_ids)) != len(expectation_ids):
            raise ValueError("expectation_id values must be unique within an expectation set")
        return self


class EvaluationObservation(_EvaluationModel):
    """The result of checking one expectation against one execution's observed facts."""

    expectation_id: NonBlankStr
    status: EvaluationStatus
    detail: NonBlankStr
    related_step_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _validate_unique_related_steps(self) -> EvaluationObservation:
        if len(set(self.related_step_ids)) != len(self.related_step_ids):
            raise ValueError("related_step_ids must be unique")
        return self


def aggregate_evaluation_status(statuses: Iterable[EvaluationStatus]) -> EvaluationStatus:
    """The shared FAIL/INDETERMINATE/PASS precedence rule.

    A definite failure always outranks uncertainty, and uncertainty always outranks a clean
    pass. Categorical, not a vote or a score.
    """

    collected = list(statuses)
    if any(status is EvaluationStatus.FAIL for status in collected):
        return EvaluationStatus.FAIL
    if any(status is EvaluationStatus.INDETERMINATE for status in collected):
        return EvaluationStatus.INDETERMINATE
    return EvaluationStatus.PASS


class EvaluationReport(_EvaluationModel):
    """The full evaluation outcome for one execution against one expectation set."""

    execution_id: NonBlankStr
    expectation_set_id: NonBlankStr
    status: EvaluationStatus
    observations: tuple[EvaluationObservation, ...] = ()

    @model_validator(mode="after")
    def _validate_report(self) -> EvaluationReport:
        expectation_ids = [observation.expectation_id for observation in self.observations]
        if len(set(expectation_ids)) != len(expectation_ids):
            raise ValueError("observation expectation_id values must be unique")

        expected_status = aggregate_evaluation_status(
            observation.status for observation in self.observations
        )
        if self.status is not expected_status:
            raise ValueError(
                f"report status {self.status.value!r} does not match the aggregate of its "
                f"observations ({expected_status.value!r})"
            )
        return self
