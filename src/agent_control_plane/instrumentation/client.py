"""Producer-facing instrumentation session and client.

InstrumentationSession is the caller's explicit, immutable view of one execution's locally
acknowledged event history. Every producer operation takes the current session and, on success,
returns a new session; the original is left untouched. There is no global current execution, no
contextvar, and no hidden client-side mutable state: ExecutionInstrumentationClient holds only a
sink reference and can be reused freely across any number of independent sessions.

Every operation follows the same shape: derive the next logical sequence from the session, build
the exact typed event from caller-supplied identity (event_id, occurred_at) and payload, validate
it locally with the existing append_execution_event so a locally impossible transition never
reaches the sink, deliver it, verify the receipt actually describes the event sent, and only then
return the advanced session. Nothing advances local state before the sink confirms acceptance.

event_id and occurred_at are always caller-supplied and never generated here. A producer that is
unsure whether a delivery succeeded must retry with the exact same event_id, occurred_at, and
payload; remote idempotency depends on exact event content, and a regenerated identity would
silently create a second, unrelated event instead of resolving the uncertain retry.

Optimistic concurrency: if two operations start from the same session value they will derive the
same next sequence, and the sink may accept one and reject the other. A caller that wants strictly
linear emission for one execution must serialize its own writes; nothing here takes a lock, since
a process-local lock could not make concurrent writers from other processes coordinate anyway.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict

from agent_control_plane.domain.enums import StepKind
from agent_control_plane.domain.models import (
    CapabilityInvocation,
    DecisionRecord,
    ExecutionFailure,
    ExecutionOutcome,
    HumanInteraction,
    ModelInvocation,
)
from agent_control_plane.events import (
    ExecutionCancelledEvent,
    ExecutionCompletedEvent,
    ExecutionEvent,
    ExecutionEventStream,
    ExecutionFailedEvent,
    ExecutionStartedEvent,
    StepCancelledEvent,
    StepCompletedEvent,
    StepFailedEvent,
    StepStartedEvent,
    append_execution_event,
)
from agent_control_plane.instrumentation.errors import InstrumentationReceiptError
from agent_control_plane.instrumentation.sink import InstrumentationSink

_TERMINAL_EXECUTION_EVENTS = (
    ExecutionCompletedEvent,
    ExecutionFailedEvent,
    ExecutionCancelledEvent,
)


class InstrumentationSession(BaseModel):
    """The producer's locally acknowledged event history for one execution.

    Immutable: every accepted operation returns a new session rather than mutating this one.
    This records what the producer has itself confirmed was accepted; it is not a claim that no
    other writer has advanced the same execution remotely, and it is not authoritative history.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    stream: ExecutionEventStream

    @classmethod
    def for_execution(cls, execution_id: str) -> InstrumentationSession:
        return cls(stream=ExecutionEventStream(execution_id=execution_id))

    @property
    def execution_id(self) -> str:
        return self.stream.execution_id

    @property
    def next_sequence(self) -> int:
        if not self.stream.events:
            return 1
        return self.stream.events[-1].sequence + 1

    @property
    def started(self) -> bool:
        return bool(self.stream.events)

    @property
    def terminal(self) -> bool:
        if not self.stream.events:
            return False
        return isinstance(self.stream.events[-1], _TERMINAL_EXECUTION_EVENTS)


class ExecutionInstrumentationClient:
    """Producer-facing operations for emitting execution events through a sink.

    Holds only the sink reference. All per-execution progress lives in the caller-held
    InstrumentationSession that each method takes and returns, so the same client instance is
    safe to use across any number of independent sessions at once.
    """

    def __init__(self, sink: InstrumentationSink) -> None:
        self._sink = sink

    async def _emit(
        self, session: InstrumentationSession, event: ExecutionEvent
    ) -> InstrumentationSession:
        candidate_stream = append_execution_event(session.stream, event)

        receipt = await self._sink.deliver(event)

        if (
            receipt.event_id != event.event_id
            or receipt.execution_id != event.execution_id
            or receipt.sequence != event.sequence
        ):
            raise InstrumentationReceiptError(
                f"sink receipt for event {event.event_id!r} does not describe the event sent"
            )

        return InstrumentationSession(stream=candidate_stream)

    async def start_execution(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        system_id: str,
        workflow_name: str,
        correlation_id: str | None = None,
    ) -> InstrumentationSession:
        event = ExecutionStartedEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            system_id=system_id,
            workflow_name=workflow_name,
            correlation_id=correlation_id,
        )
        return await self._emit(session, event)

    async def start_step(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        step_id: str,
        kind: StepKind,
        name: str,
        parent_step_id: str | None = None,
        model_invocation: ModelInvocation | None = None,
        capability_invocation: CapabilityInvocation | None = None,
        human_interaction: HumanInteraction | None = None,
        decision: DecisionRecord | None = None,
    ) -> InstrumentationSession:
        event = StepStartedEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            step_id=step_id,
            parent_step_id=parent_step_id,
            kind=kind,
            name=name,
            model_invocation=model_invocation,
            capability_invocation=capability_invocation,
            human_interaction=human_interaction,
            decision=decision,
        )
        return await self._emit(session, event)

    async def complete_step(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        step_id: str,
        model_invocation: ModelInvocation | None = None,
        human_interaction: HumanInteraction | None = None,
        decision: DecisionRecord | None = None,
    ) -> InstrumentationSession:
        event = StepCompletedEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            step_id=step_id,
            model_invocation=model_invocation,
            human_interaction=human_interaction,
            decision=decision,
        )
        return await self._emit(session, event)

    async def fail_step(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        step_id: str,
        failure: ExecutionFailure,
    ) -> InstrumentationSession:
        event = StepFailedEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            step_id=step_id,
            failure=failure,
        )
        return await self._emit(session, event)

    async def cancel_step(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        step_id: str,
    ) -> InstrumentationSession:
        event = StepCancelledEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            step_id=step_id,
        )
        return await self._emit(session, event)

    async def complete_execution(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        outcome: ExecutionOutcome,
    ) -> InstrumentationSession:
        event = ExecutionCompletedEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            outcome=outcome,
        )
        return await self._emit(session, event)

    async def fail_execution(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        failure: ExecutionFailure,
        outcome: ExecutionOutcome | None = None,
    ) -> InstrumentationSession:
        event = ExecutionFailedEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            failure=failure,
            outcome=outcome,
        )
        return await self._emit(session, event)

    async def cancel_execution(
        self,
        session: InstrumentationSession,
        *,
        event_id: str,
        occurred_at: datetime,
        outcome: ExecutionOutcome | None = None,
    ) -> InstrumentationSession:
        event = ExecutionCancelledEvent(
            event_id=event_id,
            execution_id=session.execution_id,
            sequence=session.next_sequence,
            occurred_at=occurred_at,
            outcome=outcome,
        )
        return await self._emit(session, event)
