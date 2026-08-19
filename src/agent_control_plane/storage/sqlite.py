"""Local reference implementation of ExecutionEventStore, backed by stdlib sqlite3.

The durable source of truth is the append-only event table, not a projected snapshot. Every
public read reconstructs an ExecutionEventStream from stored events and lets that model's own
invariants revalidate it; nothing here reimplements event lifecycle semantics.

Connections are short-lived and opened per call. append() runs inside one BEGIN IMMEDIATE
transaction that reads the current stream, validates the candidate through the real
append_execution_event, and inserts the new row, so a concurrent writer either serializes
behind this transaction or observes the row once it commits. There is no second, SQL-level
lifecycle validator: SQLite only stores what the event domain has already accepted.

This is a reference adapter for one local file. It gives transactional local durability and
writes serialized by SQLite's own locking; it makes no claim about distributed scale, and a
different ExecutionEventStore implementation can back the same provider-neutral port later.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from agent_control_plane.events import (
    ExecutionEvent,
    ExecutionEventConflictError,
    ExecutionEventStream,
    append_execution_event,
)
from agent_control_plane.storage.models import (
    ExecutionEventStoreError,
    StoreAppendResult,
    StoreAppendStatus,
)

_SCHEMA_VERSION = 1

_CREATE_TABLE_SQL = """
CREATE TABLE execution_events (
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (execution_id, sequence),
    UNIQUE (execution_id, event_id)
)
"""

_BUSY_TIMEOUT_SECONDS = 5.0

_EVENT_ADAPTER: TypeAdapter[ExecutionEvent] = TypeAdapter(ExecutionEvent)


class SQLiteExecutionEventStore:
    """SQLite-backed ExecutionEventStore. See module docstring for transaction design."""

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._initialize_schema()

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None puts pysqlite in full autocommit mode, so the explicit
        # BEGIN IMMEDIATE / COMMIT / ROLLBACK statements below are the only transaction
        # boundaries in effect; timeout is the busy-wait bound for a locked database.
        return sqlite3.connect(self._path, timeout=_BUSY_TIMEOUT_SECONDS, isolation_level=None)

    def _initialize_schema(self) -> None:
        connection = self._connect()
        try:
            row = connection.execute("PRAGMA user_version").fetchone()
            version = row[0]
            if version == 0:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(_CREATE_TABLE_SQL)
                # _SCHEMA_VERSION is a fixed internal constant, not user input; PRAGMA
                # statements do not accept bound parameters in sqlite3.
                connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
                connection.commit()
            elif version != _SCHEMA_VERSION:
                raise ExecutionEventStoreError(
                    f"unsupported execution event store schema version {version}"
                )
        finally:
            connection.close()

    def append(self, event: ExecutionEvent) -> StoreAppendResult:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            try:
                current_stream = self._read_stream(connection, event.execution_id)
                candidate_stream = append_execution_event(current_stream, event)

                if len(candidate_stream.events) == len(current_stream.events):
                    connection.commit()
                    return StoreAppendResult(
                        status=StoreAppendStatus.DUPLICATE,
                        event_id=event.event_id,
                        execution_id=event.execution_id,
                        sequence=event.sequence,
                    )

                connection.execute(
                    "INSERT INTO execution_events "
                    "(execution_id, sequence, event_id, event_type, event_json) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        event.execution_id,
                        event.sequence,
                        event.event_id,
                        event.event_type,
                        event.model_dump_json(),
                    ),
                )
                connection.commit()
                return StoreAppendResult(
                    status=StoreAppendStatus.ACCEPTED,
                    event_id=event.event_id,
                    execution_id=event.execution_id,
                    sequence=event.sequence,
                )
            except ExecutionEventConflictError:
                connection.rollback()
                raise
            except ExecutionEventStoreError:
                connection.rollback()
                raise
            except sqlite3.Error as exc:
                connection.rollback()
                raise ExecutionEventStoreError(f"append failed: {exc}") from exc
        finally:
            connection.close()

    def load_stream(self, execution_id: str) -> ExecutionEventStream:
        if not execution_id.strip():
            raise ValueError("execution_id must not be blank")
        connection = self._connect()
        try:
            return self._read_stream(connection, execution_id)
        except sqlite3.Error as exc:
            raise ExecutionEventStoreError(f"load_stream failed: {exc}") from exc
        finally:
            connection.close()

    def list_execution_ids(self) -> tuple[str, ...]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT DISTINCT execution_id FROM execution_events ORDER BY execution_id ASC"
            ).fetchall()
            return tuple(row[0] for row in rows)
        except sqlite3.Error as exc:
            raise ExecutionEventStoreError(f"list_execution_ids failed: {exc}") from exc
        finally:
            connection.close()

    def _read_stream(
        self, connection: sqlite3.Connection, execution_id: str
    ) -> ExecutionEventStream:
        try:
            rows = connection.execute(
                "SELECT event_json FROM execution_events "
                "WHERE execution_id = ? ORDER BY sequence ASC",
                (execution_id,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise ExecutionEventStoreError(f"read failed: {exc}") from exc
        return self._build_stream(execution_id, (row[0] for row in rows))

    def _build_stream(
        self, execution_id: str, event_json_rows: Iterable[str]
    ) -> ExecutionEventStream:
        try:
            events = tuple(_EVENT_ADAPTER.validate_json(row) for row in event_json_rows)
            return ExecutionEventStream(execution_id=execution_id, events=events)
        except ValidationError as exc:
            raise ExecutionEventStoreError(
                f"persisted event history for execution {execution_id!r} is corrupt"
            ) from exc
