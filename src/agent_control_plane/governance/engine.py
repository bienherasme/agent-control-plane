"""Deterministic governance evaluation over already-accepted execution history.

evaluate_governance is pure: the same event stream and the same policy set always produce the
same report. It never mutates the stream, calls a model, or performs I/O. Event ingestion
(append_execution_event) stays entirely unaware of governance on purpose, so the same stored
execution can be re-evaluated against any number of policy sets, including ones written after
the execution happened, without touching its history.

Approval ordering is decided entirely from ExecutionEventStream sequence numbers, never from
occurred_at. Producer clocks can disagree with each other and with the control plane; sequence
is the control plane's own logical ordering and is what every governance check trusts.
"""

from __future__ import annotations

from agent_control_plane.domain.enums import ExecutionStatus, StepKind
from agent_control_plane.domain.models import ExecutionRecord
from agent_control_plane.events.models import StepCompletedEvent, StepStartedEvent
from agent_control_plane.events.projection import project_execution
from agent_control_plane.events.stream import ExecutionEventStream
from agent_control_plane.governance.enums import GovernanceStatus, PolicyViolationCode
from agent_control_plane.governance.matching import capability_invocation_matches
from agent_control_plane.governance.models import (
    CapabilityBoundaryPolicy,
    GovernancePolicy,
    GovernancePolicySet,
    GovernanceReport,
    HumanApprovalRequirementPolicy,
    ModelUsageBudgetPolicy,
    PolicyEvaluation,
    PolicyViolation,
    aggregate_governance_status,
)


def evaluate_governance(
    stream: ExecutionEventStream, policy_set: GovernancePolicySet
) -> GovernanceReport:
    record = project_execution(stream)

    started_events = {
        event.step_id: event for event in stream.events if isinstance(event, StepStartedEvent)
    }
    completed_events = {
        event.step_id: event for event in stream.events if isinstance(event, StepCompletedEvent)
    }

    evaluations = tuple(
        _evaluate_policy(policy, record, started_events, completed_events)
        for policy in policy_set.policies
    )

    status = aggregate_governance_status(evaluation.status for evaluation in evaluations)

    return GovernanceReport(
        execution_id=record.execution_id,
        policy_set_id=policy_set.policy_set_id,
        status=status,
        evaluations=evaluations,
    )


def _evaluate_policy(
    policy: GovernancePolicy,
    record: ExecutionRecord,
    started_events: dict[str, StepStartedEvent],
    completed_events: dict[str, StepCompletedEvent],
) -> PolicyEvaluation:
    if isinstance(policy, CapabilityBoundaryPolicy):
        return _evaluate_capability_boundary(policy, record, started_events)
    if isinstance(policy, HumanApprovalRequirementPolicy):
        return _evaluate_human_approval(policy, record, started_events, completed_events)
    return _evaluate_model_usage_budget(policy, record)


def _evaluate_capability_boundary(
    policy: CapabilityBoundaryPolicy,
    record: ExecutionRecord,
    started_events: dict[str, StepStartedEvent],
) -> PolicyEvaluation:
    violations = []
    for step in record.steps:
        if step.kind is not StepKind.CAPABILITY or step.capability_invocation is None:
            continue
        invocation = step.capability_invocation
        if invocation.mode not in policy.denied_modes:
            continue
        if not capability_invocation_matches(invocation, policy.capabilities, policy.operations):
            continue

        started_event = started_events.get(step.step_id)
        related_event_ids = (started_event.event_id,) if started_event is not None else ()
        violations.append(
            PolicyViolation(
                policy_id=policy.policy_id,
                code=PolicyViolationCode.CAPABILITY_BOUNDARY,
                detail=(
                    f"capability {invocation.capability!r} operation "
                    f"{invocation.operation!r} used denied mode {invocation.mode.value!r}"
                ),
                related_step_ids=(step.step_id,),
                related_event_ids=related_event_ids,
            )
        )

    if not violations:
        return PolicyEvaluation(policy_id=policy.policy_id, status=GovernanceStatus.PASS)
    return PolicyEvaluation(
        policy_id=policy.policy_id, status=GovernanceStatus.VIOLATION, violations=tuple(violations)
    )


def _evaluate_human_approval(
    policy: HumanApprovalRequirementPolicy,
    record: ExecutionRecord,
    started_events: dict[str, StepStartedEvent],
    completed_events: dict[str, StepCompletedEvent],
) -> PolicyEvaluation:
    # A capability invocation only needs one qualifying approval earlier in logical sequence
    # anywhere in the execution; approvals are not consumed or bound to a specific target.
    qualifying_sequences = []
    for step in record.steps:
        if step.kind is not StepKind.HUMAN or step.status is not ExecutionStatus.COMPLETED:
            continue
        interaction = step.human_interaction
        if interaction is None:
            continue
        if interaction.interaction_type != policy.interaction_type:
            continue
        if interaction.outcome not in policy.accepted_outcomes:
            continue
        completion_event = completed_events.get(step.step_id)
        if completion_event is None:
            continue
        qualifying_sequences.append(completion_event.sequence)

    earliest_qualifying_sequence = min(qualifying_sequences, default=None)

    violations = []
    for step in record.steps:
        if step.kind is not StepKind.CAPABILITY or step.capability_invocation is None:
            continue
        invocation = step.capability_invocation
        if invocation.mode not in policy.capability_modes:
            continue
        if not capability_invocation_matches(invocation, policy.capabilities, policy.operations):
            continue

        started_event = started_events.get(step.step_id)
        if started_event is None:
            continue
        if (
            earliest_qualifying_sequence is not None
            and earliest_qualifying_sequence < started_event.sequence
        ):
            continue

        violations.append(
            PolicyViolation(
                policy_id=policy.policy_id,
                code=PolicyViolationCode.MISSING_REQUIRED_APPROVAL,
                detail=(
                    f"capability {invocation.capability!r} operation "
                    f"{invocation.operation!r} started without a prior completed "
                    f"{policy.interaction_type!r} approval"
                ),
                related_step_ids=(step.step_id,),
                related_event_ids=(started_event.event_id,),
            )
        )

    if not violations:
        return PolicyEvaluation(policy_id=policy.policy_id, status=GovernanceStatus.PASS)
    return PolicyEvaluation(
        policy_id=policy.policy_id, status=GovernanceStatus.VIOLATION, violations=tuple(violations)
    )


def _evaluate_usage_dimension(values: list[int | None], limit: int) -> GovernanceStatus:
    known_sum = sum(value for value in values if value is not None)
    if known_sum > limit:
        return GovernanceStatus.VIOLATION
    if all(value is not None for value in values):
        return GovernanceStatus.PASS
    return GovernanceStatus.INDETERMINATE


def _evaluate_model_usage_budget(
    policy: ModelUsageBudgetPolicy, record: ExecutionRecord
) -> PolicyEvaluation:
    matching_step_ids: list[str] = []
    input_values: list[int | None] = []
    output_values: list[int | None] = []
    total_values: list[int | None] = []

    for step in record.steps:
        if step.kind is not StepKind.MODEL or step.model_invocation is None:
            continue
        invocation = step.model_invocation
        if policy.provider is not None and invocation.provider != policy.provider:
            continue
        if policy.model is not None and invocation.model != policy.model:
            continue
        if policy.operation is not None and invocation.operation != policy.operation:
            continue

        matching_step_ids.append(step.step_id)
        input_values.append(invocation.input_units)
        output_values.append(invocation.output_units)
        if invocation.input_units is None or invocation.output_units is None:
            total_values.append(None)
        else:
            total_values.append(invocation.input_units + invocation.output_units)

    if not matching_step_ids:
        return PolicyEvaluation(policy_id=policy.policy_id, status=GovernanceStatus.PASS)

    dimension_statuses = []
    if policy.max_input_units is not None:
        dimension_statuses.append(_evaluate_usage_dimension(input_values, policy.max_input_units))
    if policy.max_output_units is not None:
        dimension_statuses.append(
            _evaluate_usage_dimension(output_values, policy.max_output_units)
        )
    if policy.max_total_units is not None:
        dimension_statuses.append(_evaluate_usage_dimension(total_values, policy.max_total_units))

    status = aggregate_governance_status(dimension_statuses)

    if status is GovernanceStatus.VIOLATION:
        return PolicyEvaluation(
            policy_id=policy.policy_id,
            status=status,
            violations=(
                PolicyViolation(
                    policy_id=policy.policy_id,
                    code=PolicyViolationCode.MODEL_USAGE_BUDGET_EXCEEDED,
                    detail="observed model usage exceeds the configured budget",
                    related_step_ids=tuple(matching_step_ids),
                ),
            ),
        )
    if status is GovernanceStatus.INDETERMINATE:
        return PolicyEvaluation(
            policy_id=policy.policy_id,
            status=status,
            detail="model usage data required to evaluate this budget is incomplete",
        )
    return PolicyEvaluation(policy_id=policy.policy_id, status=GovernanceStatus.PASS)
