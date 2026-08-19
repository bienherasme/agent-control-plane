from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar

import pytest

from agent_control_plane.domain import StepKind
from agent_control_plane.events import ExecutionStartedEvent, StepStartedEvent
from agent_control_plane.instrumentation import (
    DeliveryStatus,
    ExecutionInstrumentationClient,
    InstrumentationDeliveryError,
    InstrumentationSession,
    StoredInstrumentationSink,
)
from agent_control_plane.storage import SQLiteExecutionEventStore

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
_T = TypeVar("_T")


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def run(coro: Coroutine[Any, Any, _T]) -> _T:
    return asyncio.run(coro)


def started_event(**overrides: object) -> ExecutionStartedEvent:
    defaults: dict[str, object] = {
        "event_id": "evt-start",
        "execution_id": "exec-1",
        "sequence": 1,
        "occurred_at": at(0),
        "system_id": "sys",
        "workflow_name": "wf",
    }
    defaults.update(overrides)
    return ExecutionStartedEvent(**defaults)


def test_stored_sink_maps_accepted_and_duplicate_receipts(tmp_path: Path) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    sink = StoredInstrumentationSink(store)
    event = started_event()

    first_receipt = run(sink.deliver(event))
    assert first_receipt.status is DeliveryStatus.ACCEPTED
    assert first_receipt.event_id == event.event_id
    assert first_receipt.execution_id == event.execution_id
    assert first_receipt.sequence == event.sequence

    second_receipt = run(sink.deliver(event))
    assert second_receipt.status is DeliveryStatus.DUPLICATE

    assert len(store.load_stream("exec-1").events) == 1


def test_stored_sink_translates_store_errors_to_delivery_error(tmp_path: Path) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    sink = StoredInstrumentationSink(store)

    run(sink.deliver(started_event()))

    conflicting = StepStartedEvent(
        event_id="evt-bad", execution_id="exec-1", sequence=5, occurred_at=at(1),
        step_id="s", kind=StepKind.WORKFLOW, name="run workflow",
    )

    with pytest.raises(InstrumentationDeliveryError):
        run(sink.deliver(conflicting))

    assert len(store.load_stream("exec-1").events) == 1


def test_stored_sink_preserves_lost_ack_retry_with_real_store(tmp_path: Path) -> None:
    store = SQLiteExecutionEventStore(tmp_path / "events.db")
    sink = StoredInstrumentationSink(store)
    client = ExecutionInstrumentationClient(sink)

    session = run(
        client.start_execution(
            InstrumentationSession.for_execution("exec-1"),
            event_id="e-1", occurred_at=at(0), system_id="sys", workflow_name="wf",
        )
    )

    lost_event = StepStartedEvent(
        event_id="e-2", execution_id="exec-1", sequence=2, occurred_at=at(1),
        step_id="step-1", kind=StepKind.WORKFLOW, name="run workflow",
    )
    # Simulate the remote side having durably accepted this event while the producer's local
    # session never learned about it, e.g. it crashed before recording the acknowledgement.
    store.append(lost_event)
    assert session.next_sequence == 2

    advanced_session = run(
        client.start_step(
            session, event_id="e-2", occurred_at=at(1), step_id="step-1",
            kind=StepKind.WORKFLOW, name="run workflow",
        )
    )

    assert advanced_session.next_sequence == 3
    assert advanced_session.stream.events[-1].event_id == "e-2"
