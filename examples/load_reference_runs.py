"""Reference producer example: loads two deterministic executions through the instrumentation SDK.

Demonstrates the intended producer boundary: SQLiteExecutionEventStore, StoredInstrumentationSink,
ExecutionInstrumentationClient, InstrumentationSession. This is the same path any external system
would use to emit execution history; the script never touches SQLite directly and never imports a
specific producing system's package. system_id and workflow_name below are just open, producer-
chosen identifiers, exactly as any real integration would supply them.

Every event_id, step_id, and timestamp is fixed, and nothing here calls datetime.now() or
generates a random ID. Rerunning this script against the same database is safe and expected: each
run starts a fresh, empty InstrumentationSession, but every event it reconstructs is byte-for-byte
identical to what the store already accepted, so the store reports DUPLICATE for each one and the
script finishes exactly as it did the first time. That is not special-cased here; it falls
directly out of event idempotency.

Usage:

    python examples/load_reference_runs.py /tmp/control-plane.db
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime, timedelta

from agent_control_plane.domain import (
    CapabilityInvocation,
    CapabilityMode,
    DecisionRecord,
    ExecutionOutcome,
    ModelInvocation,
    StepKind,
)
from agent_control_plane.instrumentation import (
    ExecutionInstrumentationClient,
    InstrumentationSession,
    StoredInstrumentationSink,
)
from agent_control_plane.storage import SQLiteExecutionEventStore

SYSTEM_ID = "architecture-review-board"
WORKFLOW_NAME = "architecture-review"

_T0 = datetime(2026, 1, 1, 9, 0, tzinfo=UTC)

_MODEL_PROVIDER = "reference"
_MODEL_NAME = "structured-review-model"


def _at(minutes: int) -> datetime:
    return _T0 + timedelta(minutes=minutes)


async def _run_specialist_reviews(
    client: ExecutionInstrumentationClient,
    session: InstrumentationSession,
    execution_id: str,
    specialists: tuple[str, ...],
) -> InstrumentationSession:
    for index, specialist in enumerate(specialists, start=1):
        step_id = f"{execution_id}-specialist-{index}"
        session = await client.start_step(
            session,
            event_id=f"{step_id}-started",
            occurred_at=_at(index),
            step_id=step_id,
            kind=StepKind.MODEL,
            name=specialist,
            model_invocation=ModelInvocation(
                provider=_MODEL_PROVIDER, model=_MODEL_NAME, operation=specialist,
                input_units=400,
            ),
        )
        session = await client.complete_step(
            session,
            event_id=f"{step_id}-completed",
            occurred_at=_at(index) + timedelta(seconds=30),
            step_id=step_id,
            model_invocation=ModelInvocation(
                provider=_MODEL_PROVIDER, model=_MODEL_NAME, operation=specialist,
                input_units=400, output_units=180,
            ),
        )
    return session


async def _run_supervisor_decision(
    client: ExecutionInstrumentationClient,
    session: InstrumentationSession,
    execution_id: str,
    rationale_reference: str,
) -> InstrumentationSession:
    step_id = f"{execution_id}-decision"
    session = await client.start_step(
        session,
        event_id=f"{step_id}-started",
        occurred_at=_at(10),
        step_id=step_id,
        kind=StepKind.DECISION,
        name="supervisor-decision",
        decision=DecisionRecord(),
    )
    return await client.complete_step(
        session,
        event_id=f"{step_id}-completed",
        occurred_at=_at(11),
        step_id=step_id,
        decision=DecisionRecord(
            decision="request_changes", rationale_reference=rationale_reference
        ),
    )


async def load_baseline(client: ExecutionInstrumentationClient) -> None:
    execution_id = "arb-reference-baseline"
    session = InstrumentationSession.for_execution(execution_id)

    session = await client.start_execution(
        session, event_id=f"{execution_id}-started", occurred_at=_at(0),
        system_id=SYSTEM_ID, workflow_name=WORKFLOW_NAME,
    )

    specialists = (
        "api-design-review",
        "data-model-review",
        "security-review",
        "scalability-review",
        "operability-review",
    )
    session = await _run_specialist_reviews(client, session, execution_id, specialists)
    session = await _run_supervisor_decision(
        client, session, execution_id, "review-notes-baseline"
    )

    await client.complete_execution(
        session, event_id=f"{execution_id}-completed", occurred_at=_at(12),
        outcome=ExecutionOutcome(outcome="request_changes"),
    )


async def load_candidate(client: ExecutionInstrumentationClient) -> None:
    execution_id = "arb-reference-candidate"
    session = InstrumentationSession.for_execution(execution_id)

    session = await client.start_execution(
        session, event_id=f"{execution_id}-started", occurred_at=_at(0),
        system_id=SYSTEM_ID, workflow_name=WORKFLOW_NAME,
    )

    # One fewer specialist review than the baseline, and a deployment capability call the
    # baseline never makes: this is the intentional behavioral regression the reference
    # expectation/policy set in examples/ is designed to catch.
    specialists = (
        "api-design-review",
        "data-model-review",
        "security-review",
        "scalability-review",
    )
    session = await _run_specialist_reviews(client, session, execution_id, specialists)

    capability_step_id = f"{execution_id}-deploy"
    session = await client.start_step(
        session, event_id=f"{capability_step_id}-started", occurred_at=_at(8),
        step_id=capability_step_id, kind=StepKind.CAPABILITY, name="apply-deployment-change",
        capability_invocation=CapabilityInvocation(
            capability="deployment", operation="apply_change", mode=CapabilityMode.EXECUTE,
        ),
    )
    session = await client.complete_step(
        session, event_id=f"{capability_step_id}-completed",
        occurred_at=_at(8) + timedelta(seconds=45), step_id=capability_step_id,
    )

    session = await _run_supervisor_decision(
        client, session, execution_id, "review-notes-candidate"
    )

    await client.complete_execution(
        session, event_id=f"{execution_id}-completed", occurred_at=_at(12),
        outcome=ExecutionOutcome(outcome="request_changes"),
    )


async def _run(db_path: str) -> None:
    store = SQLiteExecutionEventStore(db_path)
    sink = StoredInstrumentationSink(store)
    client = ExecutionInstrumentationClient(sink)

    await load_baseline(client)
    await load_candidate(client)


def main() -> None:
    parser = argparse.ArgumentParser(description="load the reference architecture-review scenario")
    parser.add_argument("db_path", help="path to the SQLite control-plane database")
    args = parser.parse_args()

    asyncio.run(_run(args.db_path))

    print("loaded baseline=arb-reference-baseline candidate=arb-reference-candidate")


if __name__ == "__main__":
    main()
