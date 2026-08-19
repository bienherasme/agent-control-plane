"""Governance domain: typed policies, their evaluation results, and aggregate reports.

Governance in v0.1.0 is observational only. A VIOLATION means the observed execution history
violated a configured policy, not that Agent Control Plane intervened in or blocked anything.
Nothing here executes workflow actions, calls a model, or mutates an event stream.

Policy definitions are trusted control-plane configuration; observed execution data is never
evaluated as if it were a rule. Every policy operates on typed facts only, never on
free-text fields such as ExecutionOutcome.detail, ExecutionFailure.detail, or a DecisionRecord's
rationale reference, so evaluation stays deterministic and inspectable.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from agent_control_plane.domain.enums import CapabilityMode
from agent_control_plane.domain.models import (
    NonBlankStr,
    NonNegativeOptionalInt,
    OptionalNonBlankStr,
)
from agent_control_plane.governance.enums import GovernanceStatus, PolicyViolationCode


def _normalize_filter_strings(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = []
    for value in values:
        stripped = value.strip()
        if not stripped:
            raise ValueError("filter values must not be blank")
        normalized.append(stripped)
    if len(set(normalized)) != len(normalized):
        raise ValueError("filter values must be unique")
    return tuple(normalized)


def _normalize_required_strings(values: tuple[str, ...]) -> tuple[str, ...]:
    normalized = _normalize_filter_strings(values)
    if not normalized:
        raise ValueError("must not be empty")
    return normalized


def _require_unique_modes(values: tuple[CapabilityMode, ...]) -> tuple[CapabilityMode, ...]:
    if not values:
        raise ValueError("must not be empty")
    if len(set(values)) != len(values):
        raise ValueError("must be unique")
    return values


FilterStrings = Annotated[tuple[str, ...], AfterValidator(_normalize_filter_strings)]
RequiredStrings = Annotated[tuple[str, ...], AfterValidator(_normalize_required_strings)]
CapabilityModes = Annotated[tuple[CapabilityMode, ...], AfterValidator(_require_unique_modes)]


class _GovernanceModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CapabilityBoundaryPolicy(_GovernanceModel):
    """Flags capability activity that falls inside a denied mode and scope combination.

    Evaluated from the fact that a matching capability step started; a denied write is a denied
    write whether it later completes, fails, is cancelled, or is still running. An empty
    capabilities/operations tuple is a wildcard, otherwise the match is exact.
    """

    policy_type: Literal["capability_boundary"] = "capability_boundary"
    policy_id: NonBlankStr
    description: NonBlankStr
    denied_modes: CapabilityModes
    capabilities: FilterStrings = ()
    operations: FilterStrings = ()


class HumanApprovalRequirementPolicy(_GovernanceModel):
    """Requires a completed human approval, by logical sequence, before matching capability use.

    Ordering is decided by ExecutionEventStream sequence, never by occurred_at. One qualifying
    earlier approval may satisfy any number of later matching capability invocations in the same
    execution; there is no approval consumption, per-target binding, or expiry in v0.1.0.
    """

    policy_type: Literal["human_approval_requirement"] = "human_approval_requirement"
    policy_id: NonBlankStr
    description: NonBlankStr
    capability_modes: CapabilityModes
    capabilities: FilterStrings = ()
    operations: FilterStrings = ()
    interaction_type: NonBlankStr
    accepted_outcomes: RequiredStrings


class ModelUsageBudgetPolicy(_GovernanceModel):
    """An execution-level usage ceiling aggregated across all matching MODEL steps.

    provider/model/operation are wildcards when unset, otherwise an exact match. Lifecycle
    status never excludes an invocation: a failed or cancelled model call may still have
    consumed usage, and the snapshot's recorded usage is trusted as-is.
    """

    policy_type: Literal["model_usage_budget"] = "model_usage_budget"
    policy_id: NonBlankStr
    description: NonBlankStr
    provider: OptionalNonBlankStr = None
    model: OptionalNonBlankStr = None
    operation: OptionalNonBlankStr = None
    max_input_units: NonNegativeOptionalInt = None
    max_output_units: NonNegativeOptionalInt = None
    max_total_units: NonNegativeOptionalInt = None

    @model_validator(mode="after")
    def _validate_at_least_one_bound(self) -> ModelUsageBudgetPolicy:
        if (
            self.max_input_units is None
            and self.max_output_units is None
            and self.max_total_units is None
        ):
            raise ValueError("a model usage budget requires at least one configured bound")
        return self


GovernancePolicy = Annotated[
    CapabilityBoundaryPolicy | HumanApprovalRequirementPolicy | ModelUsageBudgetPolicy,
    Field(discriminator="policy_type"),
]


class GovernancePolicySet(_GovernanceModel):
    """An ordered, immutable policy collection. Evaluation and reporting follow this order.

    An empty policy set is valid: it represents a control plane with governance currently
    disabled for that evaluation, and evaluating it yields PASS with zero evaluations.
    """

    policy_set_id: NonBlankStr
    policies: tuple[GovernancePolicy, ...] = ()

    @model_validator(mode="after")
    def _validate_unique_policy_ids(self) -> GovernancePolicySet:
        policy_ids = [policy.policy_id for policy in self.policies]
        if len(set(policy_ids)) != len(policy_ids):
            raise ValueError("policy_id values must be unique within a policy set")
        return self


class PolicyViolation(_GovernanceModel):
    """One concrete, categorical fact: specific observed activity violated a specific policy."""

    policy_id: NonBlankStr
    code: PolicyViolationCode
    detail: NonBlankStr
    related_step_ids: tuple[str, ...] = ()
    related_event_ids: tuple[str, ...] = ()


class PolicyEvaluation(_GovernanceModel):
    """The result of evaluating one policy against one execution's observed facts.

    A policy never fabricates a violation merely to explain missing telemetry: INDETERMINATE
    carries an explanatory detail instead of empty or invented violations.
    """

    policy_id: NonBlankStr
    status: GovernanceStatus
    violations: tuple[PolicyViolation, ...] = ()
    detail: OptionalNonBlankStr = None

    @model_validator(mode="after")
    def _validate_status_consistency(self) -> PolicyEvaluation:
        if self.status is GovernanceStatus.PASS:
            if self.violations:
                raise ValueError("a passing evaluation must not carry violations")
            if self.detail is not None:
                raise ValueError("a passing evaluation must not carry detail")
        elif self.status is GovernanceStatus.VIOLATION:
            if not self.violations:
                raise ValueError("a violating evaluation requires at least one violation")
            for violation in self.violations:
                if violation.policy_id != self.policy_id:
                    raise ValueError(
                        "every violation must reference this evaluation's policy_id"
                    )
        else:
            if self.violations:
                raise ValueError("an indeterminate evaluation must not carry violations")
            if self.detail is None:
                raise ValueError("an indeterminate evaluation requires detail")
        return self


def aggregate_governance_status(statuses: Iterable[GovernanceStatus]) -> GovernanceStatus:
    """The shared PASS/VIOLATION/INDETERMINATE precedence rule.

    A definite violation always outranks uncertainty, and uncertainty always outranks a clean
    pass. This is categorical, not a vote or a score: one violation anywhere decides the
    outcome regardless of how many other checks passed.
    """

    collected = list(statuses)
    if any(status is GovernanceStatus.VIOLATION for status in collected):
        return GovernanceStatus.VIOLATION
    if any(status is GovernanceStatus.INDETERMINATE for status in collected):
        return GovernanceStatus.INDETERMINATE
    return GovernanceStatus.PASS


class GovernanceReport(_GovernanceModel):
    """The full governance outcome for one execution against one policy set."""

    execution_id: NonBlankStr
    policy_set_id: NonBlankStr
    status: GovernanceStatus
    evaluations: tuple[PolicyEvaluation, ...] = ()

    @model_validator(mode="after")
    def _validate_report(self) -> GovernanceReport:
        policy_ids = [evaluation.policy_id for evaluation in self.evaluations]
        if len(set(policy_ids)) != len(policy_ids):
            raise ValueError("evaluation policy_id values must be unique")

        expected_status = aggregate_governance_status(
            evaluation.status for evaluation in self.evaluations
        )
        if self.status is not expected_status:
            raise ValueError(
                f"report status {self.status.value!r} does not match the aggregate of its "
                f"evaluations ({expected_status.value!r})"
            )
        return self
