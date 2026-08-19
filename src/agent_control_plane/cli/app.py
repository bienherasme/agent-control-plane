"""Local CLI over the control-plane core.

A presentation layer only: every command delegates to the existing application services
(ExecutionEventStore, ExecutionQueryService, evaluate_execution, compare_execution_runs) rather
than reimplementing domain, governance, or evaluation behavior.

Argument parsing never touches the database. Store construction happens lazily inside each
command handler, so `--help` never creates a file.

ingest handles exactly one serialized event per invocation, never a batch: ExecutionEventStore
only guarantees atomicity for a single append, and accepting JSONL here would invite partial-
batch semantics the store does not actually provide.

A successfully generated report or comparison exits 0 even when governance found a VIOLATION or
evaluation found a FAIL/regression: those are observed product results, not CLI failures. Only
bad input, storage conflicts, and missing executions are process-level failures.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from agent_control_plane.cli.composition import build_query_service, open_store
from agent_control_plane.cli.errors import CliInputError, CliNotFoundError, CliOperationalError
from agent_control_plane.cli.output import print_error, print_json, print_json_list, print_line
from agent_control_plane.domain import ExecutionStatus
from agent_control_plane.evaluation import (
    EvaluationExpectationSet,
    ExecutionComparison,
    ExecutionEvaluationRun,
    IncompatibleExecutionComparisonError,
    compare_execution_runs,
    evaluate_execution,
)
from agent_control_plane.events import (
    ExecutionEvent,
    ExecutionEventConflictError,
    project_execution,
)
from agent_control_plane.governance import GovernancePolicySet
from agent_control_plane.reporting import ExecutionQuery, ExecutionReport, ExecutionSummary
from agent_control_plane.storage import ExecutionEventStoreError

EXIT_SUCCESS = 0
EXIT_OPERATIONAL_ERROR = 1
EXIT_INPUT_ERROR = 2
EXIT_NOT_FOUND = 3

_EVENT_ADAPTER: TypeAdapter[ExecutionEvent] = TypeAdapter(ExecutionEvent)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "ingest":
            return cmd_ingest(args)
        if args.command == "list":
            return cmd_list(args)
        if args.command == "report":
            return cmd_report(args)
        if args.command == "compare":
            return cmd_compare(args)
        raise AssertionError(f"unhandled command {args.command!r}")
    except CliInputError as exc:
        print_error(f"error: {exc}")
        return EXIT_INPUT_ERROR
    except CliNotFoundError as exc:
        print_error(f"error: {exc}")
        return EXIT_NOT_FOUND
    except CliOperationalError as exc:
        print_error(f"error: {exc}")
        return EXIT_OPERATIONAL_ERROR


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent-control-plane")
    parser.add_argument("--db", required=True, help="path to the SQLite control-plane database")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="append one serialized execution event")
    ingest_parser.add_argument(
        "event_file", help="path to a JSON ExecutionEvent file, or - to read from stdin"
    )
    ingest_parser.add_argument("--json", action="store_true", help="print machine-readable JSON")

    list_parser = subparsers.add_parser("list", help="query stored executions")
    list_parser.add_argument("--system-id")
    list_parser.add_argument("--workflow")
    list_parser.add_argument(
        "--status", action="append", default=[],
        help="repeatable; ExecutionStatus value (running, completed, failed, cancelled)",
    )
    list_parser.add_argument("--started-from", help="ISO-8601 timezone-aware datetime")
    list_parser.add_argument("--started-until", help="ISO-8601 timezone-aware datetime")
    list_parser.add_argument("--limit", type=int, default=50)
    list_parser.add_argument("--json", action="store_true", help="print machine-readable JSON")

    report_parser = subparsers.add_parser("report", help="build a governance/evaluation report")
    report_parser.add_argument("execution_id")
    report_parser.add_argument("--policy-set", help="path to a GovernancePolicySet JSON file")
    report_parser.add_argument(
        "--expectation-set", help="path to an EvaluationExpectationSet JSON file"
    )
    report_parser.add_argument("--json", action="store_true", help="print machine-readable JSON")

    compare_parser = subparsers.add_parser("compare", help="compare two executions")
    compare_parser.add_argument("baseline_execution_id")
    compare_parser.add_argument("candidate_execution_id")
    compare_parser.add_argument(
        "--expectation-set", required=True,
        help="path to an EvaluationExpectationSet JSON file (required)",
    )
    compare_parser.add_argument("--json", action="store_true", help="print machine-readable JSON")

    return parser


def cmd_ingest(args: argparse.Namespace) -> int:
    raw = _read_event_source(args.event_file)
    try:
        event = _EVENT_ADAPTER.validate_json(raw)
    except ValidationError as exc:
        raise CliInputError(f"invalid execution event: {exc}") from exc

    store = open_store(args.db)
    try:
        result = store.append(event)
    except (ExecutionEventConflictError, ExecutionEventStoreError) as exc:
        raise CliOperationalError(str(exc)) from exc

    if args.json:
        print_json(result)
    else:
        print_line(
            f"{result.status.value} execution={result.execution_id} "
            f"sequence={result.sequence} event={result.event_id}"
        )
    return EXIT_SUCCESS


def cmd_list(args: argparse.Namespace) -> int:
    query = _build_query(args)
    service = build_query_service(args.db)
    summaries = service.query_executions(query)

    if args.json:
        print_json_list(list(summaries))
    else:
        if not summaries:
            print_line("no executions found")
        for summary in summaries:
            print_line(_format_summary(summary))
    return EXIT_SUCCESS


def cmd_report(args: argparse.Namespace) -> int:
    policy_set = _load_policy_set(args.policy_set) if args.policy_set else None
    expectation_set = (
        _load_expectation_set(args.expectation_set) if args.expectation_set else None
    )

    service = build_query_service(args.db)
    report = service.build_report(
        args.execution_id, policy_set=policy_set, expectation_set=expectation_set
    )
    if report is None:
        raise CliNotFoundError(f"execution {args.execution_id!r} not found")

    if args.json:
        print_json(report)
    else:
        _print_report_human(report)
    return EXIT_SUCCESS


def cmd_compare(args: argparse.Namespace) -> int:
    expectation_set = _load_expectation_set(args.expectation_set)

    store = open_store(args.db)
    baseline_stream = store.load_stream(args.baseline_execution_id)
    candidate_stream = store.load_stream(args.candidate_execution_id)

    if not baseline_stream.events:
        raise CliNotFoundError(f"execution {args.baseline_execution_id!r} not found")
    if not candidate_stream.events:
        raise CliNotFoundError(f"execution {args.candidate_execution_id!r} not found")

    baseline_run = ExecutionEvaluationRun(
        execution=project_execution(baseline_stream),
        evaluation=evaluate_execution(baseline_stream, expectation_set),
    )
    candidate_run = ExecutionEvaluationRun(
        execution=project_execution(candidate_stream),
        evaluation=evaluate_execution(candidate_stream, expectation_set),
    )

    try:
        comparison = compare_execution_runs(baseline_run, candidate_run)
    except IncompatibleExecutionComparisonError as exc:
        raise CliOperationalError(str(exc)) from exc

    if args.json:
        print_json(comparison)
    else:
        _print_comparison_human(comparison)
    return EXIT_SUCCESS


def _read_event_source(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    try:
        return Path(path).read_text()
    except OSError as exc:
        raise CliInputError(f"cannot read {path!r}: {exc}") from exc


def _read_file(path: str) -> str:
    try:
        return Path(path).read_text()
    except OSError as exc:
        raise CliInputError(f"cannot read {path!r}: {exc}") from exc


def _load_policy_set(path: str) -> GovernancePolicySet:
    raw = _read_file(path)
    try:
        return GovernancePolicySet.model_validate_json(raw)
    except ValidationError as exc:
        raise CliInputError(f"invalid policy set: {exc}") from exc


def _load_expectation_set(path: str) -> EvaluationExpectationSet:
    raw = _read_file(path)
    try:
        return EvaluationExpectationSet.model_validate_json(raw)
    except ValidationError as exc:
        raise CliInputError(f"invalid expectation set: {exc}") from exc


def _build_query(args: argparse.Namespace) -> ExecutionQuery:
    statuses = tuple(_parse_status(value) for value in args.status) if args.status else ()
    started_from = _parse_datetime(args.started_from) if args.started_from is not None else None
    started_until = (
        _parse_datetime(args.started_until) if args.started_until is not None else None
    )

    try:
        return ExecutionQuery(
            system_id=args.system_id,
            workflow_name=args.workflow,
            statuses=statuses,
            started_from=started_from,
            started_until=started_until,
            limit=args.limit,
        )
    except ValidationError as exc:
        raise CliInputError(f"invalid query: {exc}") from exc


def _parse_status(value: str) -> ExecutionStatus:
    try:
        return ExecutionStatus(value)
    except ValueError as exc:
        valid = ", ".join(status.value for status in ExecutionStatus)
        raise CliInputError(f"invalid status {value!r}; expected one of: {valid}") from exc


def _parse_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise CliInputError(f"invalid datetime {value!r}: {exc}") from exc


def _format_summary(summary: ExecutionSummary) -> str:
    outcome = summary.outcome if summary.outcome is not None else "-"
    completed = summary.completed_at.isoformat() if summary.completed_at is not None else "-"
    return (
        f"{summary.execution_id} system={summary.system_id} workflow={summary.workflow_name} "
        f"status={summary.status.value} started={summary.started_at.isoformat()} "
        f"completed={completed} outcome={outcome} steps={summary.step_count} "
        f"failed_steps={summary.failed_step_count}"
    )


def _print_report_human(report: ExecutionReport) -> None:
    execution = report.execution
    print_line("execution:")
    print_line(f"  id: {execution.execution_id}")
    print_line(f"  system: {execution.system_id}")
    print_line(f"  workflow: {execution.workflow_name}")
    print_line(f"  status: {execution.status.value}")
    outcome = execution.outcome.outcome if execution.outcome is not None else "-"
    print_line(f"  outcome: {outcome}")
    print_line(f"  started: {execution.started_at.isoformat()}")
    completed = execution.completed_at.isoformat() if execution.completed_at is not None else "-"
    print_line(f"  completed: {completed}")
    print_line(f"  steps: {len(execution.steps)}")

    if report.governance is not None:
        governance = report.governance
        print_line("governance:")
        print_line(f"  status: {governance.status.value}")
        for evaluation in governance.evaluations:
            print_line(f"  policy {evaluation.policy_id}: {evaluation.status.value}")
            for violation in evaluation.violations:
                print_line(
                    f"    violation [{violation.code.value}] {violation.detail} "
                    f"steps={list(violation.related_step_ids)} "
                    f"events={list(violation.related_event_ids)}"
                )

    if report.evaluation is not None:
        evaluation_report = report.evaluation
        print_line("evaluation:")
        print_line(f"  status: {evaluation_report.status.value}")
        for observation in evaluation_report.observations:
            print_line(
                f"  expectation {observation.expectation_id}: {observation.status.value} "
                f"- {observation.detail}"
            )


def _print_comparison_human(comparison: ExecutionComparison) -> None:
    print_line(f"baseline: {comparison.baseline_execution_id}")
    print_line(f"candidate: {comparison.candidate_execution_id}")
    print_line(f"system: {comparison.system_id}")
    print_line(f"workflow: {comparison.workflow_name}")
    print_line(f"regression: {comparison.regression_status.value}")

    print_line("expectations:")
    for expectation_comparison in comparison.expectation_comparisons:
        print_line(
            f"  {expectation_comparison.expectation_id}: "
            f"{expectation_comparison.baseline_status.value} -> "
            f"{expectation_comparison.candidate_status.value} "
            f"({expectation_comparison.change.value})"
        )

    input_delta = (
        comparison.input_units_delta if comparison.input_units_delta is not None else "unknown"
    )
    output_delta = (
        comparison.output_units_delta if comparison.output_units_delta is not None else "unknown"
    )
    print_line("deltas:")
    print_line(f"  step_count: {comparison.step_count_delta}")
    print_line(f"  failed_step_count: {comparison.failed_step_count_delta}")
    print_line(f"  model_step_count: {comparison.model_step_count_delta}")
    print_line(f"  capability_step_count: {comparison.capability_step_count_delta}")
    print_line(f"  human_step_count: {comparison.human_step_count_delta}")
    print_line(f"  decision_step_count: {comparison.decision_step_count_delta}")
    print_line(f"  input_units: {input_delta}")
    print_line(f"  output_units: {output_delta}")
