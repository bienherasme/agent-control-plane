# Persistence and concurrency

## The abstraction

`ExecutionEventStore` (`storage.store`) is a provider-neutral `Protocol` with three synchronous
methods:

```
append(event) -> StoreAppendResult
load_stream(execution_id) -> ExecutionEventStream
list_execution_ids() -> tuple[str, ...]
```

Nothing above this port knows about SQLite, or any other concrete backend. A different
implementation (Postgres, an event-store product, cloud storage) can back the same port later
without changing `instrumentation`, `governance`, `evaluation`, `reporting`, or the CLI.

## The SQLite reference adapter

`SQLiteExecutionEventStore` uses stdlib `sqlite3` against one table, conceptually:

```
execution_events(
    execution_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (execution_id, sequence),
    UNIQUE (execution_id, event_id)
)
```

`PRAGMA user_version` tags the schema (`1` in v0.1.0); an unrecognized version raises
`ExecutionEventStoreError` on open rather than silently migrating.

## Transactional append

Each `append()` call runs inside one `BEGIN IMMEDIATE` transaction:

1. acquire the write lock immediately (not deferred to the first write)
2. read the current stream for that `execution_id` inside the transaction
3. validate the candidate through the real `append_execution_event` (the one lifecycle
   implementation; SQL never re-implements it)
4. if the event is an exact duplicate, commit with no row inserted, report `DUPLICATE`
5. otherwise insert the one new row and commit, report `ACCEPTED`
6. on any conflict or storage error, roll back and propagate the real error

Because the read, validation, and insert happen inside one transaction serialized by SQLite's
own locking, two connections racing to append at the same next sequence cannot both win: one
acquires the lock and completes its transaction; the other blocks until that commits, then reads
the now-updated stream and correctly fails its own append with a sequence conflict. The outcome
is exactly one `ACCEPTED` writer and one `ExecutionEventConflictError`, never two accepted rows
for the same sequence. If two connections race to append the exact same event, the outcome is
one `ACCEPTED` and one `DUPLICATE`, never two rows.

## Connections and timeout

Connections are short-lived, opened fresh per public call, with `isolation_level=None` (full
autocommit) so the explicit `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` statements are the only
transaction boundaries in effect. A five-second busy timeout lets a concurrent writer wait
briefly for a locked database instead of failing immediately; there is no manual retry loop
around that, SQLite's own lock wait is sufficient for this reference adapter.

## What is not persisted

Only raw events are ever written. `ExecutionRecord`, `GovernanceReport`, `EvaluationReport`, and
`ExecutionComparison` are all computed on demand and never stored; re-opening the database and
reloading a stream reconstructs an execution byte-for-byte identical to the one before the
database was closed, and a report built twice from the same history and the same configuration
is identical both times.

## Query tradeoff

`ExecutionQueryService.query_executions` currently loads and projects every stored execution,
filters and sorts in application code, then applies the limit. This is appropriate for local or
reference scale, where durable history is small enough to scan; it is not a claim about
performance at arbitrary distributed scale, and no denormalized query index exists yet.

## Producer-side optimistic concurrency

Two producer operations that start from the same `InstrumentationSession` value derive the same
next sequence. The store, not the client, decides which one is accepted; the other receives a
conflict. This is intentional optimistic concurrency: a process-local lock inside the client
would not solve anything for multiple writers across separate processes or machines, so none is
used. A caller that needs strictly linear emission for one execution is responsible for
serializing its own writes to that execution.
