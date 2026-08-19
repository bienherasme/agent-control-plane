"""Read/reporting application service over durable execution history.

ExecutionQueryService depends only on the provider-neutral ExecutionEventStore port and the
existing deterministic projection/governance/evaluation engines; it never knows SQLite or any
other concrete backend.

Filtering and sorting currently happen in application code after loading and projecting every
stored execution: durable event history is the source of truth and local scale is small in
v0.1.0, so a second denormalized query index would be premature. A future store implementation
can optimize this without changing the public query contract.

Reports are never persisted. Governance and evaluation results are deterministic views computed
fresh from durable event history plus caller-supplied trusted configuration each time
build_report runs; the same execution can be re-evaluated against a different policy or
expectation set later without touching stored history. Note that evaluate_governance and
evaluate_execution each project the execution internally, so a call that requests both results
projects the same stream three times in total (once here, once per engine); that duplication is
an accepted tradeoff for keeping this service from reimplementing either engine's logic.
"""

from __future__ import annotations

from agent_control_plane.domain.enums import ExecutionStatus, StepKind
from agent_control_plane.domain.models import ExecutionRecord
from agent_control_plane.evaluation import EvaluationExpectationSet, evaluate_execution
from agent_control_plane.events.projection import project_execution
from agent_control_plane.governance import GovernancePolicySet, evaluate_governance
from agent_control_plane.reporting.models import ExecutionQuery, ExecutionReport, ExecutionSummary
from agent_control_plane.storage.store import ExecutionEventStore


class ExecutionQueryService:
    def __init__(self, store: ExecutionEventStore) -> None:
        self._store = store

    def get_execution(self, execution_id: str) -> ExecutionRecord | None:
        stream = self._store.load_stream(execution_id)
        if not stream.events:
            return None
        return project_execution(stream)

    def query_executions(self, query: ExecutionQuery) -> tuple[ExecutionSummary, ...]:
        matching: list[ExecutionRecord] = []
        for execution_id in self._store.list_execution_ids():
            stream = self._store.load_stream(execution_id)
            if not stream.events:
                continue
            record = project_execution(stream)
            if _matches(record, query):
                matching.append(record)

        # Stable two-pass sort: execution_id ASC first, then started_at DESC, so ties in
        # started_at keep a deterministic execution_id ASC tie-break.
        matching.sort(key=lambda record: record.execution_id)
        matching.sort(key=lambda record: record.started_at, reverse=True)

        return tuple(_to_summary(record) for record in matching[: query.limit])

    def build_report(
        self,
        execution_id: str,
        *,
        policy_set: GovernancePolicySet | None = None,
        expectation_set: EvaluationExpectationSet | None = None,
    ) -> ExecutionReport | None:
        stream = self._store.load_stream(execution_id)
        if not stream.events:
            return None

        execution = project_execution(stream)
        governance = evaluate_governance(stream, policy_set) if policy_set is not None else None
        evaluation = (
            evaluate_execution(stream, expectation_set) if expectation_set is not None else None
        )

        return ExecutionReport(execution=execution, governance=governance, evaluation=evaluation)


def _matches(record: ExecutionRecord, query: ExecutionQuery) -> bool:
    if query.system_id is not None and record.system_id != query.system_id:
        return False
    if query.workflow_name is not None and record.workflow_name != query.workflow_name:
        return False
    if query.statuses and record.status not in query.statuses:
        return False
    if query.started_from is not None and record.started_at < query.started_from:
        return False
    if query.started_until is not None and record.started_at > query.started_until:
        return False
    return True


def _count_by_status(record: ExecutionRecord, status: ExecutionStatus) -> int:
    return sum(1 for step in record.steps if step.status is status)


def _count_by_kind(record: ExecutionRecord, kind: StepKind) -> int:
    return sum(1 for step in record.steps if step.kind is kind)


def _to_summary(record: ExecutionRecord) -> ExecutionSummary:
    return ExecutionSummary(
        execution_id=record.execution_id,
        system_id=record.system_id,
        workflow_name=record.workflow_name,
        status=record.status,
        started_at=record.started_at,
        completed_at=record.completed_at,
        outcome=record.outcome.outcome if record.outcome is not None else None,
        step_count=len(record.steps),
        failed_step_count=_count_by_status(record, ExecutionStatus.FAILED),
        model_step_count=_count_by_kind(record, StepKind.MODEL),
        capability_step_count=_count_by_kind(record, StepKind.CAPABILITY),
        human_step_count=_count_by_kind(record, StepKind.HUMAN),
        decision_step_count=_count_by_kind(record, StepKind.DECISION),
    )
