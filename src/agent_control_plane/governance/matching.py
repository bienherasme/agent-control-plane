"""Shared exact-match filtering for capability-scoped policies.

CapabilityBoundaryPolicy and HumanApprovalRequirementPolicy both scope themselves to specific
capabilities and operations with the same rule: an empty filter tuple is a wildcard, a
non-empty one requires an exact, case-sensitive match. There is no regex or glob matching.
"""

from __future__ import annotations

from agent_control_plane.domain.models import CapabilityInvocation


def capability_invocation_matches(
    invocation: CapabilityInvocation,
    capabilities: tuple[str, ...],
    operations: tuple[str, ...],
) -> bool:
    if capabilities and invocation.capability not in capabilities:
        return False
    if operations and invocation.operation not in operations:
        return False
    return True
