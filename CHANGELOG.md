# Changelog

## 0.1.0

Initial release: a provider-neutral execution observability, governance, evaluation, and
regression control plane for AI-assisted software workflows.

### Execution domain

- Typed, immutable execution snapshots (`ExecutionRecord`, `ExecutionStep`) with a flat step
  hierarchy, closed lifecycle statuses, and typed detail per step kind (model invocation,
  capability invocation, human interaction, decision).
- No raw prompt or model response capture anywhere in the domain; only bounded invocation
  identity and usage counts.

### Event ingestion

- A discriminated `ExecutionEvent` union covering execution and step start/completion/failure/
  cancellation, with caller-supplied `event_id` and `occurred_at`.
- An append-only `ExecutionEventStream` with exact duplicate idempotency, sequence-gap and
  lifecycle-transition validation, and a guarantee that accepted history always projects.
- Deterministic `project_execution` folding.

### Producer instrumentation SDK

- `ExecutionInstrumentationClient` and an explicit, caller-held `InstrumentationSession`, with
  no contextvars, decorators, or hidden current-execution state.
- Session state advances only after sink acknowledgement, with exact lost-acknowledgement retry
  semantics.
- A transport-neutral `InstrumentationSink` protocol.

### Governance

- Deterministic, observational policy evaluation (`PASS` / `VIOLATION` / `INDETERMINATE`):
  capability boundary, human approval requirement (ordered by logical sequence, not wall clock),
  and model usage budget policies.

### Evaluation and regression comparison

- Deterministic behavioral evaluation against declared expectations (`PASS` / `FAIL` /
  `INDETERMINATE`): execution status, execution outcome, step occurrence, capability occurrence,
  and human interaction expectations.
- Baseline/candidate regression comparison (`UNCHANGED` / `REGRESSION` / `IMPROVEMENT` /
  `MIXED`), classified only from declared expectation transitions, alongside descriptive,
  non-judgmental execution deltas.

### Durable storage and reporting

- A provider-neutral `ExecutionEventStore` port and a SQLite reference adapter with transactional
  append and durable idempotency under concurrent writers.
- `ExecutionQueryService` for filtered execution queries and combined governance/evaluation
  reporting over durable history, with nothing derived ever persisted.

### CLI and reference scenario

- A local CLI (`ingest`, `list`, `report`, `compare`) with an explicit `--db` path and per-command
  `--json` output.
- A deterministic reference producer scenario (`examples/load_reference_runs.py`) demonstrating a
  passing baseline and a regressed, policy-violating candidate.
