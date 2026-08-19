from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import pytest

from agent_control_plane.domain import (
    DecisionRecord,
    ExecutionOutcome,
    HumanInteraction,
    ModelInvocation,
    StepKind,
)
from agent_control_plane.events import (
    ExecutionEvent,
    ExecutionEventConflictError,
    ExecutionEventStream,
    ExecutionStartedEvent,
    append_execution_event,
    project_execution,
)
from agent_control_plane.instrumentation import (
    DeliveryReceipt,
    DeliveryStatus,
    ExecutionInstrumentationClient,
    InstrumentationDeliveryError,
    InstrumentationReceiptError,
    InstrumentationSession,
)

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
_T = TypeVar("_T")


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def run(coro: Coroutine[Any, Any, _T]) -> _T:
    return asyncio.run(coro)


class FakeSink:
    """Backed by the real append_execution_event API, tracking a separate remote stream per
    execution. Simulates a transport adapter: it translates a remote-side
    ExecutionEventConflictError into InstrumentationDeliveryError, and can simulate a lost
    response after the remote side has already accepted an event.
    """

    def __init__(self) -> None:
        self.remote_streams: dict[str, ExecutionEventStream] = {}
        self.delivered_event_ids: list[str] = []
        self._drop_next = False

    def drop_next_response(self) -> None:
        self._drop_next = True

    async def deliver(self, event: ExecutionEvent) -> DeliveryReceipt:
        self.delivered_event_ids.append(event.event_id)
        remote_stream = self.remote_streams.get(
            event.execution_id, ExecutionEventStream(execution_id=event.execution_id)
        )

        try:
            new_remote_stream = append_execution_event(remote_stream, event)
        except ExecutionEventConflictError as exc:
            raise InstrumentationDeliveryError(str(exc)) from exc

        status = (
            DeliveryStatus.DUPLICATE
            if len(new_remote_stream.events) == len(remote_stream.events)
            else DeliveryStatus.ACCEPTED
        )
        self.remote_streams[event.execution_id] = new_remote_stream

        if self._drop_next:
            self._drop_next = False
            raise InstrumentationDeliveryError("simulated response loss")

        return DeliveryReceipt(
            status=status,
            event_id=event.event_id,
            execution_id=event.execution_id,
            sequence=event.sequence,
        )


class AlwaysFailSink:
    def __init__(self) -> None:
        self.call_count = 0

    async def deliver(self, event: ExecutionEvent) -> DeliveryReceipt:
        self.call_count += 1
        raise InstrumentationDeliveryError("simulated transport failure")


class WrongSequenceSink:
    def __init__(self) -> None:
        self.call_count = 0

    async def deliver(self, event: ExecutionEvent) -> DeliveryReceipt:
        self.call_count += 1
        return DeliveryReceipt(
            status=DeliveryStatus.ACCEPTED,
            event_id=event.event_id,
            execution_id=event.execution_id,
            sequence=event.sequence + 1,
        )


def test_fresh_session_and_full_lifecycle_derives_contiguous_sequence() -> None:
    session = InstrumentationSession.for_execution("exec-1")
    assert session.stream.events == ()
    assert session.next_sequence == 1
    assert session.started is False
    assert session.terminal is False

    client = ExecutionInstrumentationClient(FakeSink())

    async def run_lifecycle() -> InstrumentationSession:
        s = await client.start_execution(
            session, event_id="e-1", occurred_at=at(0),
            system_id="incident-commander", workflow_name="incident-analysis",
        )
        s = await client.start_step(
            s, event_id="e-2", occurred_at=at(1), step_id="step-1",
            kind=StepKind.WORKFLOW, name="run workflow",
        )
        s = await client.complete_step(s, event_id="e-3", occurred_at=at(2), step_id="step-1")
        return await client.complete_execution(
            s, event_id="e-4", occurred_at=at(3), outcome=ExecutionOutcome(outcome="done")
        )

    final_session = run(run_lifecycle())

    assert [event.sequence for event in final_session.stream.events] == [1, 2, 3, 4]
    assert [event.event_id for event in final_session.stream.events] == [
        "e-1", "e-2", "e-3", "e-4",
    ]
    assert final_session.stream.events[0].occurred_at == at(0)
    assert final_session.terminal is True
    assert session.stream.events == ()


def test_client_operates_on_independent_sessions() -> None:
    client = ExecutionInstrumentationClient(FakeSink())
    session_a = InstrumentationSession.for_execution("exec-a")
    session_b = InstrumentationSession.for_execution("exec-b")

    async def start_both() -> tuple[InstrumentationSession, InstrumentationSession]:
        a = await client.start_execution(
            session_a, event_id="e-a1", occurred_at=at(0), system_id="sys", workflow_name="wf"
        )
        b = await client.start_execution(
            session_b, event_id="e-b1", occurred_at=at(0), system_id="sys", workflow_name="wf"
        )
        return a, b

    a, b = run(start_both())

    assert a.execution_id == "exec-a"
    assert b.execution_id == "exec-b"
    assert a.next_sequence == 2
    assert b.next_sequence == 2
    assert session_a.next_sequence == 1
    assert session_b.next_sequence == 1


def test_delivery_failure_leaves_session_unchanged() -> None:
    sink = AlwaysFailSink()
    client = ExecutionInstrumentationClient(sink)
    session = InstrumentationSession.for_execution("exec-1")

    async def attempt() -> None:
        await client.start_execution(
            session, event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf"
        )

    with pytest.raises(InstrumentationDeliveryError):
        run(attempt())

    assert session.stream.events == ()
    assert sink.call_count == 1


def test_lost_acknowledgement_retry_and_changed_identity() -> None:
    sink = FakeSink()
    client = ExecutionInstrumentationClient(sink)

    async def start() -> InstrumentationSession:
        return await client.start_execution(
            InstrumentationSession.for_execution("exec-1"),
            event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf",
        )

    session = run(start())
    sink.drop_next_response()

    async def lost_ack_attempt() -> None:
        await client.start_step(
            session, event_id="e-2", occurred_at=at(1), step_id="step-1",
            kind=StepKind.WORKFLOW, name="run workflow",
        )

    with pytest.raises(InstrumentationDeliveryError):
        run(lost_ack_attempt())

    assert session.next_sequence == 2

    async def retry_same() -> InstrumentationSession:
        return await client.start_step(
            session, event_id="e-2", occurred_at=at(1), step_id="step-1",
            kind=StepKind.WORKFLOW, name="run workflow",
        )

    advanced_session = run(retry_same())
    assert advanced_session.next_sequence == 3
    assert advanced_session.stream.events[-1].event_id == "e-2"

    async def retry_changed() -> None:
        await client.start_step(
            session, event_id="e-2", occurred_at=at(999), step_id="step-1",
            kind=StepKind.WORKFLOW, name="run workflow",
        )

    with pytest.raises(InstrumentationDeliveryError):
        run(retry_changed())


def test_invalid_receipt_raises_and_does_not_advance_session() -> None:
    sink = WrongSequenceSink()
    client = ExecutionInstrumentationClient(sink)
    session = InstrumentationSession.for_execution("exec-1")

    async def attempt() -> None:
        await client.start_execution(
            session, event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf"
        )

    with pytest.raises(InstrumentationReceiptError):
        run(attempt())

    assert session.stream.events == ()
    assert sink.call_count == 1


def test_local_lifecycle_conflict_precedes_sink_call() -> None:
    sink = FakeSink()
    client = ExecutionInstrumentationClient(sink)

    async def start() -> InstrumentationSession:
        return await client.start_execution(
            InstrumentationSession.for_execution("exec-1"),
            event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf",
        )

    started_session = run(start())
    delivered_before = len(sink.delivered_event_ids)

    async def complete_unknown_step() -> None:
        await client.complete_step(
            started_session, event_id="e-2", occurred_at=at(1), step_id="ghost"
        )

    with pytest.raises(ExecutionEventConflictError):
        run(complete_unknown_step())

    assert len(sink.delivered_event_ids) == delivered_before


def test_terminal_session_rejects_further_emission() -> None:
    sink = FakeSink()
    client = ExecutionInstrumentationClient(sink)

    async def reach_terminal() -> InstrumentationSession:
        s = await client.start_execution(
            InstrumentationSession.for_execution("exec-1"),
            event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf",
        )
        return await client.complete_execution(
            s, event_id="e-2", occurred_at=at(1), outcome=ExecutionOutcome(outcome="done")
        )

    terminal_session = run(reach_terminal())
    assert terminal_session.terminal is True
    delivered_before = len(sink.delivered_event_ids)

    async def attempt_more() -> None:
        await client.start_step(
            terminal_session, event_id="e-3", occurred_at=at(2), step_id="late",
            kind=StepKind.WORKFLOW, name="too late",
        )

    with pytest.raises(ExecutionEventConflictError):
        run(attempt_more())

    assert len(sink.delivered_event_ids) == delivered_before


def test_session_recovered_from_existing_stream_continues_sequence() -> None:
    stream = ExecutionEventStream(execution_id="exec-1")
    stream = append_execution_event(
        stream,
        ExecutionStartedEvent(
            event_id="e-1", execution_id="exec-1", sequence=1, occurred_at=at(0),
            system_id="sys", workflow_name="wf",
        ),
    )
    recovered_session = InstrumentationSession(stream=stream)
    assert recovered_session.next_sequence == 2

    # The recovered session reflects history the remote side already accepted, so the fake
    # sink's own remote record must start from that same point for this test to be meaningful.
    sink = FakeSink()
    sink.remote_streams["exec-1"] = stream
    client = ExecutionInstrumentationClient(sink)

    async def continue_lifecycle() -> InstrumentationSession:
        return await client.start_step(
            recovered_session, event_id="e-2", occurred_at=at(1), step_id="step-1",
            kind=StepKind.WORKFLOW, name="run workflow",
        )

    advanced = run(continue_lifecycle())
    assert advanced.next_sequence == 3
    assert [event.sequence for event in advanced.stream.events] == [1, 2]


def test_typed_step_detail_passes_through_model_human_decision() -> None:
    client = ExecutionInstrumentationClient(FakeSink())

    async def run_steps() -> InstrumentationSession:
        s = await client.start_execution(
            InstrumentationSession.for_execution("exec-1"),
            event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf",
        )
        s = await client.start_step(
            s, event_id="e-2", occurred_at=at(1), step_id="model", kind=StepKind.MODEL,
            name="call model",
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review",
                input_units=100,
            ),
        )
        s = await client.complete_step(
            s, event_id="e-3", occurred_at=at(2), step_id="model",
            model_invocation=ModelInvocation(
                provider="anthropic", model="claude-sonnet-5", operation="review",
                input_units=100, output_units=40,
            ),
        )
        s = await client.start_step(
            s, event_id="e-4", occurred_at=at(3), step_id="human", kind=StepKind.HUMAN,
            name="approval", human_interaction=HumanInteraction(interaction_type="approval"),
        )
        s = await client.complete_step(
            s, event_id="e-5", occurred_at=at(4), step_id="human",
            human_interaction=HumanInteraction(interaction_type="approval", outcome="approved"),
        )
        s = await client.start_step(
            s, event_id="e-6", occurred_at=at(5), step_id="decision", kind=StepKind.DECISION,
            name="escalation call", decision=DecisionRecord(),
        )
        return await client.complete_step(
            s, event_id="e-7", occurred_at=at(6), step_id="decision",
            decision=DecisionRecord(decision="escalate"),
        )

    final_session = run(run_steps())
    record = project_execution(final_session.stream)

    model_step = next(step for step in record.steps if step.step_id == "model")
    human_step = next(step for step in record.steps if step.step_id == "human")
    decision_step = next(step for step in record.steps if step.step_id == "decision")

    assert model_step.model_invocation is not None
    assert model_step.model_invocation.output_units == 40
    assert human_step.human_interaction is not None
    assert human_step.human_interaction.outcome == "approved"
    assert decision_step.decision is not None
    assert decision_step.decision.decision == "escalate"


def test_deterministic_equal_sessions_from_equivalent_operations() -> None:
    async def build() -> InstrumentationSession:
        client = ExecutionInstrumentationClient(FakeSink())
        return await client.start_execution(
            InstrumentationSession.for_execution("exec-1"),
            event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf",
        )

    session_a = run(build())
    session_b = run(build())

    assert session_a == session_b
