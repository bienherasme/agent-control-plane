from __future__ import annotations

import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agent_control_plane.domain import ExecutionOutcome, StepKind
from agent_control_plane.events import (
    ExecutionCompletedEvent,
    ExecutionEventConflictError,
    ExecutionStartedEvent,
    StepCompletedEvent,
    StepStartedEvent,
    project_execution,
)
from agent_control_plane.storage import (
    ExecutionEventStoreError,
    SQLiteExecutionEventStore,
    StoreAppendResult,
    StoreAppendStatus,
)

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def make_store(tmp_path: Path) -> SQLiteExecutionEventStore:
    return SQLiteExecutionEventStore(tmp_path / "events.db")


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


def step_started(**overrides: object) -> StepStartedEvent:
    defaults: dict[str, object] = {
        "event_id": "evt-step",
        "execution_id": "exec-1",
        "sequence": 2,
        "occurred_at": at(1),
        "step_id": "step-1",
        "kind": StepKind.WORKFLOW,
        "name": "run workflow",
    }
    defaults.update(overrides)
    return StepStartedEvent(**defaults)


def test_schema_initializes_idempotently_and_enforces_supported_version(tmp_path: Path) -> None:
    db_path = tmp_path / "events.db"
    SQLiteExecutionEventStore(db_path)
    SQLiteExecutionEventStore(db_path)

    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA user_version = 99")
    connection.commit()
    connection.close()

    with pytest.raises(ExecutionEventStoreError):
        SQLiteExecutionEventStore(db_path)


def test_accepted_event_persists_and_load_stream_reconstructs_exactly(tmp_path: Path) -> None:
    store = make_store(tmp_path)

    result = store.append(started_event())
    assert result.status is StoreAppendStatus.ACCEPTED
    assert result.sequence == 1

    store.append(step_started())

    stream = store.load_stream("exec-1")
    assert [event.sequence for event in stream.events] == [1, 2]
    assert isinstance(stream.events[0], ExecutionStartedEvent)
    assert isinstance(stream.events[1], StepStartedEvent)

    record = project_execution(stream)
    assert record.execution_id == "exec-1"
    assert len(record.steps) == 1


def test_exact_duplicate_returns_duplicate_without_new_row(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    event = started_event()

    first = store.append(event)
    second = store.append(event)

    assert first.status is StoreAppendStatus.ACCEPTED
    assert second.status is StoreAppendStatus.DUPLICATE
    assert len(store.load_stream("exec-1").events) == 1


def test_conflicting_event_raises_and_rolls_back(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append(started_event())

    gap_event = step_started(event_id="evt-gap", sequence=3)
    with pytest.raises(ExecutionEventConflictError):
        store.append(gap_event)

    changed_event = started_event(system_id="different-system")
    with pytest.raises(ExecutionEventConflictError):
        store.append(changed_event)

    stream = store.load_stream("exec-1")
    assert len(stream.events) == 1
    assert stream.events[0].event_id == "evt-start"


def test_concurrent_writers_same_sequence_and_exact_duplicate(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.append(started_event())

    event_a = step_started(event_id="evt-a", sequence=2, step_id="a", name="step a")
    event_b = step_started(event_id="evt-b", sequence=2, step_id="b", name="step b")

    barrier = threading.Barrier(2)
    results: dict[str, object] = {}

    def attempt(key: str, event: StepStartedEvent) -> None:
        barrier.wait()
        try:
            results[key] = store.append(event)
        except ExecutionEventConflictError as exc:
            results[key] = exc

    threads = [
        threading.Thread(target=attempt, args=("a", event_a)),
        threading.Thread(target=attempt, args=("b", event_b)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    accepted = [r for r in results.values() if isinstance(r, StoreAppendResult)]
    conflicts = [r for r in results.values() if isinstance(r, ExecutionEventConflictError)]
    assert len(accepted) == 1
    assert len(conflicts) == 1

    stream = store.load_stream("exec-1")
    assert len(stream.events) == 2
    record = project_execution(stream)
    assert len(record.steps) == 1

    winning_step_id = record.steps[0].step_id
    dup_event = StepCompletedEvent(
        event_id="evt-complete", execution_id="exec-1", sequence=3, occurred_at=at(2),
        step_id=winning_step_id,
    )

    barrier2 = threading.Barrier(2)
    dup_results: dict[str, StoreAppendResult] = {}

    def attempt_dup(key: str) -> None:
        barrier2.wait()
        dup_results[key] = store.append(dup_event)

    dup_threads = [
        threading.Thread(target=attempt_dup, args=("1",)),
        threading.Thread(target=attempt_dup, args=("2",)),
    ]
    for thread in dup_threads:
        thread.start()
    for thread in dup_threads:
        thread.join()

    assert sorted(result.status.value for result in dup_results.values()) == [
        "accepted", "duplicate",
    ]
    assert len(store.load_stream("exec-1").events) == 3


def test_store_reopened_from_same_file_reconstructs_identical_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "events.db"
    store = SQLiteExecutionEventStore(db_path)
    store.append(started_event())
    store.append(step_started())
    store.append(
        StepCompletedEvent(
            event_id="evt-step-done", execution_id="exec-1", sequence=3, occurred_at=at(2),
            step_id="step-1",
        )
    )
    store.append(
        ExecutionCompletedEvent(
            event_id="evt-done", execution_id="exec-1", sequence=4, occurred_at=at(3),
            outcome=ExecutionOutcome(outcome="done"),
        )
    )
    stream_before = store.load_stream("exec-1")
    record_before = project_execution(stream_before)
    del store

    reopened = SQLiteExecutionEventStore(db_path)
    stream_after = reopened.load_stream("exec-1")
    record_after = project_execution(stream_after)

    assert stream_after == stream_before
    assert record_after == record_before
