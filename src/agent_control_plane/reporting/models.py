"""Read-side models: queries over durable execution history and the reports built from them.

ExecutionReport combines a projected snapshot with optional governance and evaluation results
without merging them into one overall status. Governance and evaluation are independent
concerns; a report just presents both results side by side when the caller asks for them, and
never persists what it builds.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict, model_validator

from agent_control_plane.domain.enums import ExecutionStatus
from agent_control_plane.domain.models import ExecutionRecord, NonBlankStr, OptionalNonBlankStr
from agent_control_plane.evaluation import EvaluationReport
from agent_control_plane.governance import GovernanceReport


def _require_unique_statuses(values: tuple[ExecutionStatus, ...]) -> tuple[ExecutionStatus, ...]:
    if len(set(values)) != len(values):
        raise ValueError("statuses must be unique")
    return values


def _require_bounded_limit(value: int) -> int:
    if not 1 <= value <= 200:
        raise ValueError("limit must be between 1 and 200")
    return value


UniqueStatuses = Annotated[tuple[ExecutionStatus, ...], AfterValidator(_require_unique_statuses)]
BoundedLimit = Annotated[int, AfterValidator(_require_bounded_limit)]


class _ReportingModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ExecutionQuery(_ReportingModel):
    """A deterministic, exact-match filter over discovered executions.

    This model only declares what a caller wants filtered; ExecutionQueryService decides how
    the query actually executes. No arbitrary sort expression, text search, or field selector.
    """

    system_id: OptionalNonBlankStr = None
    workflow_name: OptionalNonBlankStr = None
    statuses: UniqueStatuses = ()
    started_from: datetime | None = None
    started_until: datetime | None = None
    limit: BoundedLimit = 50

    @model_validator(mode="after")
    def _validate_time_bounds(self) -> ExecutionQuery:
        if self.started_from is not None and self.started_from.tzinfo is None:
            raise ValueError("started_from must be timezone-aware")
        if self.started_until is not None and self.started_until.tzinfo is None:
            raise ValueError("started_until must be timezone-aware")
        if (
            self.started_from is not None
            and self.started_until is not None
            and self.started_from > self.started_until
        ):
            raise ValueError("started_from must be <= started_until")
        return self


class ExecutionSummary(_ReportingModel):
    """A compact, typed view of one execution for query results. No raw event payloads."""

    execution_id: NonBlankStr
    system_id: NonBlankStr
    workflow_name: NonBlankStr
    status: ExecutionStatus
    started_at: datetime
    completed_at: datetime | None
    outcome: str | None

    step_count: int
    failed_step_count: int
    model_step_count: int
    capability_step_count: int
    human_step_count: int
    decision_step_count: int


class ExecutionReport(_ReportingModel):
    """A snapshot combined with optional governance and evaluation results.

    Never an overall status: a report may legitimately show a governance VIOLATION alongside an
    evaluation PASS, and both facts must remain visible and unmerged.
    """

    execution: ExecutionRecord
    governance: GovernanceReport | None = None
    evaluation: EvaluationReport | None = None
