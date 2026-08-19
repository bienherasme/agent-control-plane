from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from agent_control_plane.evaluation import EvaluationExpectationSet
from agent_control_plane.governance import GovernancePolicySet
from agent_control_plane.reporting import ExecutionQueryService
from agent_control_plane.storage import SQLiteExecutionEventStore

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_DIR = REPO_ROOT / "examples"


def test_reference_producer_idempotent_and_scenario_results(tmp_path: Path) -> None:
    db_path = tmp_path / "events.db"
    script = EXAMPLES_DIR / "load_reference_runs.py"

    first = subprocess.run(
        [sys.executable, str(script), str(db_path)], capture_output=True, text=True
    )
    assert first.returncode == 0
    assert first.stdout.strip() == (
        "loaded baseline=arb-reference-baseline candidate=arb-reference-candidate"
    )

    second = subprocess.run(
        [sys.executable, str(script), str(db_path)], capture_output=True, text=True
    )
    assert second.returncode == 0
    assert second.stdout == first.stdout

    store = SQLiteExecutionEventStore(db_path)
    policy_set = GovernancePolicySet.model_validate_json(
        (EXAMPLES_DIR / "policy_set.json").read_text()
    )
    expectation_set = EvaluationExpectationSet.model_validate_json(
        (EXAMPLES_DIR / "expectation_set.json").read_text()
    )
    service = ExecutionQueryService(store)

    baseline_report = service.build_report(
        "arb-reference-baseline", policy_set=policy_set, expectation_set=expectation_set
    )
    candidate_report = service.build_report(
        "arb-reference-candidate", policy_set=policy_set, expectation_set=expectation_set
    )

    assert baseline_report is not None
    assert baseline_report.governance is not None
    assert baseline_report.evaluation is not None
    assert baseline_report.governance.status.value == "pass"
    assert baseline_report.evaluation.status.value == "pass"

    assert candidate_report is not None
    assert candidate_report.governance is not None
    assert candidate_report.evaluation is not None
    assert candidate_report.governance.status.value == "violation"
    assert candidate_report.evaluation.status.value == "fail"


def test_reference_config_files_validate_through_domain_models() -> None:
    expectation_set = EvaluationExpectationSet.model_validate_json(
        (EXAMPLES_DIR / "expectation_set.json").read_text()
    )
    assert expectation_set.expectation_set_id == "arb-reference-expectations"
    assert len(expectation_set.expectations) == 5

    policy_set = GovernancePolicySet.model_validate_json(
        (EXAMPLES_DIR / "policy_set.json").read_text()
    )
    assert policy_set.policy_set_id == "arb-reference-policies"
    assert len(policy_set.policies) == 1
