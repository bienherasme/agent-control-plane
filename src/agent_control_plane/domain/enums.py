"""Closed lifecycle and classification enums for the execution domain."""

from __future__ import annotations

from enum import StrEnum


class ExecutionStatus(StrEnum):
    """Lifecycle state shared by an execution and each of its steps.

    Intentionally closed to RUNNING/COMPLETED/FAILED/CANCELLED. Broader notions such as
    partial or degraded outcomes belong to a domain outcome or evaluation model, not to
    lifecycle state.
    """

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepKind(StrEnum):
    """The kind of activity a single execution step represents.

    There is no AGENT kind: the control plane models execution activities, not agents.
    """

    WORKFLOW = "workflow"
    MODEL = "model"
    CAPABILITY = "capability"
    HUMAN = "human"
    DECISION = "decision"


class CapabilityMode(StrEnum):
    """Governance-relevant classification of an external capability invocation.

    READ observes or retrieves external state. WRITE modifies external durable state.
    EXECUTE triggers an operational action not adequately represented as a data mutation.
    """

    READ = "read"
    WRITE = "write"
    EXECUTE = "execute"
