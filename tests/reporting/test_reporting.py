from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from agent_control_plane.domain import (
    CapabilityInvocation,
    CapabilityMode,
    ExecutionFailure,
    ExecutionOutcome,
    ExecutionStatus,
    StepKind,
)
from agent_control_plane.evaluation import (
    CapabilityOccurrenceExpectation,
    EvaluationExpectationSet,
    EvaluationStatus,
)
from agent_control_plane.events import (
    ExecutionCancelledEvent,
    ExecutionCompletedEvent,
    ExecutionFailedEvent,
    ExecutionStartedEvent,
    StepCompletedEvent,
    StepStartedEvent,
)
from agent_control_plane.governance import (
    CapabilityBoundaryPolicy,
    GovernancePolicySet,
    GovernanceStatus,
)
from agent_control_plane.reporting import ExecutionQuery, ExecutionQueryService
from agent_control_plane.storage import SQLiteExecutionEventStore

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def seed(
    store: SQLiteExecutionEventStore,
    execution_id: str,
    *,
    system_id: str,
    workflow_name: str,
    started_at: datetime,
    status: ExecutionStatus,
    capability: bool = False,
) -> None:
    store.append(
        ExecutionStartedEvent(
            event_id=f"{execution_id}-start", execution_id=execution_id, sequence=1,
            occurred_at=started_at, system_id=system_id, workflow_name=workflow_name,
        )
    )
    sequence = 2

    if capability:
        store.append(
            StepStartedEvent(
                event_id=f"{execution_id}-cap-start", execution_id=execution_id,
                sequence=sequence, occurred_at=started_at + timedelta(seconds=1),
                step_id="cap", kind=StepKind.CAPABILITY, name="call capability",
                capability_invocation=CapabilityInvocation(
                    capability="cloud-api", operation="restart_service",
                    mode=CapabilityMode.EXECUTE,
                ),
            )
        )
        sequence += 1

    if status is ExecutionStatus.RUNNING:
        return

    if capability:
        store.append(
            StepCompletedEvent(
                event_id=f"{execution_id}-cap-done", execution_id=execution_id,
                sequence=sequence, occurred_at=started_at + timedelta(seconds=2),
                step_id="cap",
            )
        )
        sequence += 1

    completed_at = started_at + timedelta(seconds=10)
    if status is ExecutionStatus.COMPLETED:
        store.append(
            ExecutionCompletedEvent(
                event_id=f"{execution_id}-done", execution_id=execution_id, sequence=sequence,
                occurred_at=completed_at, outcome=ExecutionOutcome(outcome="done"),
            )
        )
    elif status is ExecutionStatus.FAILED:
        store.append(
            ExecutionFailedEvent(
                event_id=f"{execution_id}-failed", execution_id=execution_id, sequence=sequence,
                occurred_at=completed_at,
                failure=ExecutionFailure(category="timeout", detail="no response"),
            )
        )
    elif status is ExecutionStatus.CANCELLED:
        store.append(
            ExecutionCancelledEvent(
                event_id=f"{execution_id}-cancelled", execution_id=execution_id,
                sequence=sequence, occurred_at=completed_at,
            )
        )


def test_get_execution_missing_returns_none_and_persisted_returns_snapshot(
    tmp_path: Path,
) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    service = ExecutionQueryService(store)

    assert service.get_execution("missing") is None

    seed(
        store, "exec-1", system_id="sys", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )

    record = service.get_execution("exec-1")
    assert record is not None
    assert record.execution_id == "exec-1"
    assert record.status is ExecutionStatus.COMPLETED


def test_query_executions_filters_orders_and_limits(tmp_path: Path) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    service = ExecutionQueryService(store)

    seed(
        store, "exec-a", system_id="sys-a", workflow_name="wf-1", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )
    seed(
        store, "exec-b", system_id="sys-a", workflow_name="wf-1", started_at=at(100),
        status=ExecutionStatus.RUNNING,
    )
    seed(
        store, "exec-c", system_id="sys-b", workflow_name="wf-2", started_at=at(50),
        status=ExecutionStatus.COMPLETED,
    )

    all_sys_a = service.query_executions(ExecutionQuery(system_id="sys-a"))
    assert [summary.execution_id for summary in all_sys_a] == ["exec-b", "exec-a"]

    running_only = service.query_executions(
        ExecutionQuery(statuses=(ExecutionStatus.RUNNING,))
    )
    assert [summary.execution_id for summary in running_only] == ["exec-b"]

    bounded = service.query_executions(ExecutionQuery(started_from=at(40), started_until=at(60)))
    assert [summary.execution_id for summary in bounded] == ["exec-c"]

    limited = service.query_executions(ExecutionQuery(limit=1))
    assert len(limited) == 1
    assert limited[0].execution_id == "exec-b"


def test_build_report_governance_and_evaluation_are_independent(tmp_path: Path) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    service = ExecutionQueryService(store)

    seed(
        store, "exec-1", system_id="sys", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED, capability=True,
    )

    policy_set = GovernancePolicySet(
        policy_set_id="ps",
        policies=(
            CapabilityBoundaryPolicy(
                policy_id="deny-execute", description="deny execute",
                denied_modes=(CapabilityMode.EXECUTE,),
            ),
        ),
    )
    expectation_set = EvaluationExpectationSet(
        expectation_set_id="es",
        expectations=(
            CapabilityOccurrenceExpectation(
                expectation_id="e1", description="exactly one execute call",
                modes=(CapabilityMode.EXECUTE,), min_occurrences=1, max_occurrences=1,
            ),
        ),
    )

    report = service.build_report("exec-1", policy_set=policy_set, expectation_set=expectation_set)

    assert report is not None
    assert report.governance is not None
    assert report.evaluation is not None
    assert report.governance.status is GovernanceStatus.VIOLATION
    assert report.evaluation.status is EvaluationStatus.PASS


def test_build_report_without_config_returns_snapshot_only(tmp_path: Path) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    service = ExecutionQueryService(store)

    assert service.build_report("missing") is None

    seed(
        store, "exec-1", system_id="sys", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )

    report = service.build_report("exec-1")
    assert report is not None
    assert report.governance is None
    assert report.evaluation is None
    assert report.execution.execution_id == "exec-1"


def test_reporting_does_not_mutate_persisted_events(tmp_path: Path) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    service = ExecutionQueryService(store)

    seed(
        store, "exec-1", system_id="sys", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )

    stream_before = store.load_stream("exec-1")

    service.get_execution("exec-1")
    service.query_executions(ExecutionQuery())
    service.build_report("exec-1")

    stream_after = store.load_stream("exec-1")
    assert stream_after == stream_before
