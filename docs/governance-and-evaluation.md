# Governance and evaluation

These are two independent engines that read the same kind of execution facts and answer two
different questions. Neither calls the other, neither reads the other's result type, and neither
result is merged into a shared overall status.

## The two questions

Governance asks: was configured policy violated?

```
evaluate_governance(stream, policy_set) -> GovernanceReport
```

Evaluation asks: did this execution match declared expected behavior?

```
evaluate_execution(stream, expectation_set) -> EvaluationReport
```

## Statuses

```
GovernanceStatus:  PASS | VIOLATION | INDETERMINATE
EvaluationStatus:  PASS | FAIL      | INDETERMINATE
RegressionStatus:  UNCHANGED | REGRESSION | IMPROVEMENT | MIXED
```

`INDETERMINATE` in both engines means the same thing: required observability is missing, or the
execution has not yet reached a state where the question can be decided. It is never presented
as a pass. There is no shared numeric score, no voting, no weighting, and nothing in either
engine calls a model to make a judgment: every check is an exact, typed comparison.

## Why they can legitimately disagree

Consider one execution where a capability step calls a deployment API in `EXECUTE` mode:

- A `CapabilityBoundaryPolicy` denies `EXECUTE` capability activity globally. Governance sees
  the denied mode and reports `VIOLATION`.
- An `EvaluationExpectationSet` for this particular test scenario declares "exactly one EXECUTE
  capability call is expected here." Evaluation sees exactly one and reports `PASS`.

Both are correct and both are shown, unmerged, in the same `ExecutionReport`:

```json
{
  "execution": { "...": "..." },
  "governance": { "status": "violation", "...": "..." },
  "evaluation": { "status": "pass", "...": "..." }
}
```

A behavior can be exactly what a test scenario expected while still violating organizational
policy. Forcing agreement between the two would hide that distinction, so the architecture
deliberately does not attempt it.

## Regression is a third, later question

`compare_execution_runs(baseline, candidate)` asks a third question: what changed, relative to
an explicit baseline, in terms of the declared expectations? It classifies each expectation's
`EvaluationStatus` transition using a fixed desirability order, `PASS > INDETERMINATE > FAIL`,
and aggregates:

- only unchanged transitions: `UNCHANGED`
- one or more regressions, no improvements: `REGRESSION`
- one or more improvements, no regressions: `IMPROVEMENT`
- both: `MIXED`

Regression classification never reads `GovernanceReport` and never reads descriptive deltas
(step counts, usage): those are reported alongside the classification for a human to read, not
folded into it. A candidate is never automatically declared better or worse than a baseline
beyond what the declared expectations actually establish.
