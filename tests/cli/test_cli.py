from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agent_control_plane.cli.app import main
from agent_control_plane.domain import (
    CapabilityInvocation,
    CapabilityMode,
    ExecutionOutcome,
    ExecutionStatus,
    StepKind,
)
from agent_control_plane.evaluation import CapabilityOccurrenceExpectation, EvaluationExpectationSet
from agent_control_plane.events import (
    ExecutionCompletedEvent,
    ExecutionStartedEvent,
    StepCompletedEvent,
    StepStartedEvent,
)
from agent_control_plane.governance import CapabilityBoundaryPolicy, GovernancePolicySet
from agent_control_plane.storage import SQLiteExecutionEventStore

T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def at(seconds: int) -> datetime:
    return T0 + timedelta(seconds=seconds)


def seed(
    store: SQLiteExecutionEventStore,
    execution_id: str,
    *,
    system_id: str,
    workflow_name: str,
    started_at: datetime,
    status: ExecutionStatus,
    capability: bool = False,
) -> None:
    store.append(
        ExecutionStartedEvent(
            event_id=f"{execution_id}-start", execution_id=execution_id, sequence=1,
            occurred_at=started_at, system_id=system_id, workflow_name=workflow_name,
        )
    )
    sequence = 2

    if capability:
        store.append(
            StepStartedEvent(
                event_id=f"{execution_id}-cap-start", execution_id=execution_id,
                sequence=sequence, occurred_at=started_at + timedelta(seconds=1),
                step_id="cap", kind=StepKind.CAPABILITY, name="call capability",
                capability_invocation=CapabilityInvocation(
                    capability="cloud-api", operation="restart_service",
                    mode=CapabilityMode.EXECUTE,
                ),
            )
        )
        sequence += 1
        store.append(
            StepCompletedEvent(
                event_id=f"{execution_id}-cap-done", execution_id=execution_id,
                sequence=sequence, occurred_at=started_at + timedelta(seconds=2),
                step_id="cap",
            )
        )
        sequence += 1

    if status is ExecutionStatus.RUNNING:
        return

    store.append(
        ExecutionCompletedEvent(
            event_id=f"{execution_id}-done", execution_id=execution_id, sequence=sequence,
            occurred_at=started_at + timedelta(seconds=10),
            outcome=ExecutionOutcome(outcome="done"),
        )
    )


def test_help_works_and_creates_no_database(tmp_path: Path) -> None:
    console_script = Path(sys.executable).parent / "agent-control-plane"

    console_result = subprocess.run(
        [str(console_script), "--help"], cwd=tmp_path, capture_output=True, text=True
    )
    assert console_result.returncode == 0
    assert "usage:" in console_result.stdout

    module_result = subprocess.run(
        [sys.executable, "-m", "agent_control_plane", "--help"],
        cwd=tmp_path, capture_output=True, text=True,
    )
    assert module_result.returncode == 0
    assert "usage:" in module_result.stdout

    assert list(tmp_path.iterdir()) == []


def test_ingest_accepted_then_duplicate_and_rejects_invalid_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "events.db")
    event_path = tmp_path / "event.json"
    event_path.write_text(
        json.dumps(
            {
                "event_type": "execution_started",
                "event_id": "evt-1",
                "execution_id": "exec-1",
                "sequence": 1,
                "occurred_at": at(0).isoformat(),
                "system_id": "sys",
                "workflow_name": "wf",
            }
        )
    )

    assert main(["--db", db_path, "ingest", str(event_path), "--json"]) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["status"] == "accepted"

    assert main(["--db", db_path, "ingest", str(event_path), "--json"]) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["status"] == "duplicate"

    invalid_path = tmp_path / "invalid.json"
    invalid_path.write_text("{not valid json")
    assert main(["--db", db_path, "ingest", str(invalid_path)]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err != ""


def test_list_filters_order_and_json_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "events.db")
    store = SQLiteExecutionEventStore(db_path)
    seed(
        store, "exec-a", system_id="sys-a", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )
    seed(
        store, "exec-b", system_id="sys-a", workflow_name="wf", started_at=at(100),
        status=ExecutionStatus.COMPLETED,
    )
    seed(
        store, "exec-c", system_id="sys-b", workflow_name="wf", started_at=at(50),
        status=ExecutionStatus.COMPLETED,
    )

    assert main(["--db", db_path, "list", "--system-id", "sys-a", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert [item["execution_id"] for item in out] == ["exec-b", "exec-a"]

    assert main(["--db", db_path, "list", "--limit", "1"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 1
    assert lines[0].startswith("exec-b ")


def test_list_rejects_invalid_status_and_naive_datetime(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "events.db")
    SQLiteExecutionEventStore(db_path)

    assert main(["--db", db_path, "list", "--status", "bogus"]) == 2
    assert capsys.readouterr().err != ""

    assert main(["--db", db_path, "list", "--started-from", "2026-01-01T00:00:00"]) == 2
    assert capsys.readouterr().err != ""


def test_report_missing_execution_returns_not_found(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "events.db")
    SQLiteExecutionEventStore(db_path)

    assert main(["--db", db_path, "report", "missing-exec"]) == 3
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "not found" in captured.err


def test_report_governance_and_evaluation_independent_json_and_exit_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "events.db")
    store = SQLiteExecutionEventStore(db_path)
    seed(
        store, "exec-1", system_id="sys", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED, capability=True,
    )

    policy_path = tmp_path / "policies.json"
    policy_path.write_text(
        GovernancePolicySet(
            policy_set_id="ps",
            policies=(
                CapabilityBoundaryPolicy(
                    policy_id="deny-execute", description="deny execute",
                    denied_modes=(CapabilityMode.EXECUTE,),
                ),
            ),
        ).model_dump_json()
    )
    expectation_path = tmp_path / "expectations.json"
    expectation_path.write_text(
        EvaluationExpectationSet(
            expectation_set_id="es",
            expectations=(
                CapabilityOccurrenceExpectation(
                    expectation_id="e1", description="exactly one execute call",
                    modes=(CapabilityMode.EXECUTE,), min_occurrences=1, max_occurrences=1,
                ),
            ),
        ).model_dump_json()
    )

    exit_code = main(
        [
            "--db", db_path, "report", "exec-1",
            "--policy-set", str(policy_path), "--expectation-set", str(expectation_path),
            "--json",
        ]
    )
    assert exit_code == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    out = json.loads(captured.out)
    assert out["governance"]["status"] == "violation"
    assert out["evaluation"]["status"] == "pass"


def test_compare_regression_exits_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "events.db")
    store = SQLiteExecutionEventStore(db_path)
    seed(
        store, "exec-base", system_id="sys", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )
    seed(
        store, "exec-cand", system_id="sys", workflow_name="wf", started_at=at(100),
        status=ExecutionStatus.COMPLETED, capability=True,
    )

    expectation_path = tmp_path / "expectations.json"
    expectation_path.write_text(
        EvaluationExpectationSet(
            expectation_set_id="es",
            expectations=(
                CapabilityOccurrenceExpectation(
                    expectation_id="e1", description="no execute calls",
                    modes=(CapabilityMode.EXECUTE,), min_occurrences=0, max_occurrences=0,
                ),
            ),
        ).model_dump_json()
    )

    exit_code = main(
        [
            "--db", db_path, "compare", "exec-base", "exec-cand",
            "--expectation-set", str(expectation_path), "--json",
        ]
    )
    assert exit_code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["regression_status"] == "regression"


def test_compare_incompatible_returns_concise_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_path = str(tmp_path / "events.db")
    store = SQLiteExecutionEventStore(db_path)
    seed(
        store, "exec-a", system_id="sys-a", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )
    seed(
        store, "exec-b", system_id="sys-b", workflow_name="wf", started_at=at(0),
        status=ExecutionStatus.COMPLETED,
    )

    expectation_path = tmp_path / "expectations.json"
    expectation_path.write_text(EvaluationExpectationSet(expectation_set_id="es").model_dump_json())

    exit_code = main(
        ["--db", db_path, "compare", "exec-a", "exec-b", "--expectation-set", str(expectation_path)]
    )
    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err != ""


def test_required_arguments_enforced_by_argparse(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as missing_db:
        main(["list"])
    assert missing_db.value.code == 2

    db_path = str(tmp_path / "events.db")
    with pytest.raises(SystemExit) as missing_expectation_set:
        main(["--db", db_path, "compare", "a", "b"])
    assert missing_expectation_set.value.code == 2
