"""Append-only per-execution event history.

ExecutionEventStream is immutable: appending never mutates an existing stream, it returns a
new one. This is not persistence; nothing here writes anywhere. It is the in-memory ordering
and conflict-detection contract a future persistence layer will sit behind.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from agent_control_plane.domain.models import NonBlankStr
from agent_control_plane.events.models import (
    ExecutionCancelledEvent,
    ExecutionCompletedEvent,
    ExecutionEvent,
    ExecutionFailedEvent,
    ExecutionStartedEvent,
)
from agent_control_plane.events.projection import ExecutionProjectionError, project_execution

_TERMINAL_EXECUTION_EVENTS = (
    ExecutionCompletedEvent,
    ExecutionFailedEvent,
    ExecutionCancelledEvent,
)


class ExecutionEventConflictError(Exception):
    """Raised when an incoming event cannot be appended to an execution's event stream."""


class ExecutionEventStream(BaseModel):
    """The ordered, immutable event history of exactly one execution.

    Normally built up through append_execution_event, one accepted event at a time, but this
    model validates its own shape independently of how it was constructed, since it may also
    arrive already assembled (from a future persistence layer, for instance).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    execution_id: NonBlankStr
    events: tuple[ExecutionEvent, ...] = ()

    @model_validator(mode="after")
    def _validate_events(self) -> ExecutionEventStream:
        if not self.events:
            return self

        seen_event_ids: set[str] = set()
        started_count = 0

        for index, event in enumerate(self.events):
            if event.execution_id != self.execution_id:
                raise ValueError(
                    f"event {event.event_id!r} belongs to execution "
                    f"{event.execution_id!r}, not {self.execution_id!r}"
                )
            if event.event_id in seen_event_ids:
                raise ValueError(f"duplicate event_id {event.event_id!r} in stream")
            seen_event_ids.add(event.event_id)

            expected_sequence = index + 1
            if event.sequence != expected_sequence:
                raise ValueError(
                    f"event {event.event_id!r} has sequence {event.sequence}, "
                    f"expected {expected_sequence}"
                )

            if isinstance(event, ExecutionStartedEvent):
                started_count += 1

        if not isinstance(self.events[0], ExecutionStartedEvent):
            raise ValueError("the first event in a stream must be ExecutionStartedEvent")
        if started_count > 1:
            raise ValueError("a stream may contain only one ExecutionStartedEvent")

        for event in self.events[:-1]:
            if isinstance(event, _TERMINAL_EXECUTION_EVENTS):
                raise ValueError("no event may follow a terminal execution event")

        return self


def append_execution_event(
    stream: ExecutionEventStream, event: ExecutionEvent
) -> ExecutionEventStream:
    """Append one event, enforcing idempotency, sequencing, and transition validity.

    An event whose event_id is already present is a no-op if its content is identical, and a
    conflict otherwise. A genuinely new event is only accepted once the resulting stream has
    been shown to project into a valid ExecutionRecord, so a caller never gets back a stream
    that later turns out to be unprojectable.
    """

    if event.execution_id != stream.execution_id:
        raise ExecutionEventConflictError(
            f"event execution_id {event.execution_id!r} does not match stream "
            f"execution_id {stream.execution_id!r}"
        )

    for existing_event in stream.events:
        if existing_event.event_id == event.event_id:
            if existing_event == event:
                return stream
            raise ExecutionEventConflictError(
                f"event_id {event.event_id!r} was already appended with different content"
            )

    if stream.events:
        last_event = stream.events[-1]
        if isinstance(last_event, _TERMINAL_EXECUTION_EVENTS):
            raise ExecutionEventConflictError(
                f"execution {stream.execution_id!r} is already closed by a terminal event"
            )
        expected_sequence = last_event.sequence + 1
    else:
        expected_sequence = 1

    if event.sequence != expected_sequence:
        raise ExecutionEventConflictError(
            f"event {event.event_id!r} has sequence {event.sequence}, "
            f"expected {expected_sequence}"
        )

    try:
        candidate = ExecutionEventStream(
            execution_id=stream.execution_id, events=(*stream.events, event)
        )
    except ValidationError as exc:
        raise ExecutionEventConflictError(str(exc)) from exc

    try:
        project_execution(candidate)
    except ExecutionProjectionError as exc:
        raise ExecutionEventConflictError(str(exc)) from exc

    return candidate
