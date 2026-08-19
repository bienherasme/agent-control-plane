"""Narrow instrumentation errors.

These describe producer-side delivery and receipt-contract failures, not execution lifecycle
validity. A locally impossible transition still raises the existing
agent_control_plane.events.ExecutionEventConflictError, before a sink is ever called.
"""

from __future__ import annotations


class InstrumentationDeliveryError(Exception):
    """The sink did not establish successful acceptance of the event.

    The session the caller supplied remains unchanged. A caller that wants to retry should
    resubmit with the exact same event_id, occurred_at, and payload it already used, since
    accepted-event idempotency is based on exact content equality, not on caller intent.
    """


class InstrumentationReceiptError(Exception):
    """A sink returned an ACCEPTED/DUPLICATE receipt that does not describe the event sent.

    This is a sink/protocol contract failure, not a lifecycle failure. The session the caller
    supplied remains unchanged; the client never advances local state on an incoherent receipt.
    """
