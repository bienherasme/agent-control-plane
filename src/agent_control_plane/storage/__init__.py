"""Provider-neutral durable event-store port, plus the SQLite reference adapter.

Only agent_control_plane.storage.sqlite imports sqlite3. Domain, events, instrumentation
contracts, governance, and evaluation stay database-agnostic.
"""

from agent_control_plane.storage.models import (
    ExecutionEventStoreError,
    StoreAppendResult,
    StoreAppendStatus,
)
from agent_control_plane.storage.sqlite import SQLiteExecutionEventStore
from agent_control_plane.storage.store import ExecutionEventStore

__all__ = [
    "ExecutionEventStore",
    "ExecutionEventStoreError",
    "SQLiteExecutionEventStore",
    "StoreAppendResult",
    "StoreAppendStatus",
]
