# Project Scope

## Product responsibility

Agent Control Plane is a provider-neutral execution observability, governance, evaluation, and
regression control plane for AI-assisted software workflows.

It records typed execution events from external AI systems, reconstructs deterministic execution
state from that history, and provides a stable control plane for:

- execution trace representation
- bounded operational metadata
- capability/action visibility
- policy evaluation
- evaluation results
- governance decisions
- regression/comparison

It does not own the business workflow being observed. The observed system remains authoritative
for its own domain; the control plane observes and evaluates execution behavior without taking
ownership of it.

Examples of future producers: incident coordination systems, architecture review systems,
engineering assistants, and other bounded AI workflows. The control plane does not depend on any
of them.

## It is not

Agent Control Plane is not:

- an AI agent framework
- an LLM gateway
- a prompt management platform
- a chatbot
- an orchestration framework
- a generic workflow engine
- a model router
- a tracing UI
- an OpenTelemetry replacement
- a SIEM
- an autonomous governance agent
- a billing system
- an enterprise observability replacement
- an LLMOps platform
- a full distributed tracing system

## v0.1.0 scope

In scope for this version:

- typed, immutable execution snapshots
- append-only typed execution events, with deterministic projection into a snapshot
- exact event idempotency and safe producer retry
- hierarchical execution steps
- model invocation visibility
- external capability/action visibility
- human approval visibility
- explicit failure representation
- execution outcome
- explicit producer instrumentation boundary (session, client, sink contract)
- deterministic, observational governance policy evaluation over observed execution facts
- deterministic execution evaluation against declared expectations
- baseline/candidate execution regression comparison
- durable append-only local event storage with a SQLite reference adapter
- deterministic execution query/reporting over durable event history
- local CLI (ingest, list, report, compare)
- a deterministic reference producer example and scenario

Out of scope entirely:

- agent orchestration
- workflow execution
- prompt authoring or storage
- model routing
- raw prompt/response capture or tracing
- autonomous enforcement or remediation
- automatic deployment
- generic observability backend
- OpenTelemetry replacement
- distributed tracing backend
- HTTP service
- MCP server
- UI/dashboard
- multi-tenant authorization
- billing
- secrets management
- ticketing
- distributed database
- automatic baseline selection
- statistical benchmark or trend analysis

## Core architectural principle

The domain models execution semantics, not agent conversations. There is no `AgentMessage`,
`Conversation`, `ChatTurn`, `AgentMemory`, `Scratchpad`, `AssistantMessage`, or `UserMessage`
concept, and none should be added unless a concrete future requirement proves one necessary.

The control plane understands executions, steps, model invocations, external capability calls,
human approval, outcomes, and failures, without requiring conversational state.

## Events are the durable source of truth

`ExecutionRecord` is a validated execution snapshot. It is not persisted and it is not the
durable source of truth: the append-only `ExecutionEventStream` is. `project_execution` folds
that history deterministically into a snapshot on demand, and `GovernanceReport`,
`EvaluationReport`, and `ExecutionComparison` are all likewise derived, on demand, from event
history plus explicit trusted configuration. None of them are separately authoritative persisted
state.
