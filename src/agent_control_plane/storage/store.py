"""Provider-neutral durable event-store port.

ExecutionEventStore is a Protocol: the SQLite adapter (and any future backend) satisfies it
structurally, without the rest of the system depending on a specific database. All methods are
synchronous, since a local reference store like SQLite has no real async requirement; the tiny
bridge to the async instrumentation sink contract lives in the stored sink adapter, not here.
"""

from __future__ import annotations

from typing import Protocol

from agent_control_plane.events import ExecutionEvent, ExecutionEventStream
from agent_control_plane.storage.models import StoreAppendResult


class ExecutionEventStore(Protocol):
    def append(self, event: ExecutionEvent) -> StoreAppendResult: ...

    def load_stream(self, execution_id: str) -> ExecutionEventStream: ...

    def list_execution_ids(self) -> tuple[str, ...]: ...
