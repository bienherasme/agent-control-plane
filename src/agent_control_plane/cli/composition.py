"""CLI composition root: the only place CLI code constructs a concrete store.

Kept separate from command logic so store construction happens lazily, exactly when a command
needs durable state, never as a side effect of parsing arguments or printing --help.
"""

from __future__ import annotations

from pathlib import Path

from agent_control_plane.cli.errors import CliOperationalError
from agent_control_plane.reporting import ExecutionQueryService
from agent_control_plane.storage import ExecutionEventStoreError, SQLiteExecutionEventStore


def open_store(db_path: str) -> SQLiteExecutionEventStore:
    try:
        return SQLiteExecutionEventStore(Path(db_path))
    except ExecutionEventStoreError as exc:
        raise CliOperationalError(str(exc)) from exc


def build_query_service(db_path: str) -> ExecutionQueryService:
    return ExecutionQueryService(open_store(db_path))
