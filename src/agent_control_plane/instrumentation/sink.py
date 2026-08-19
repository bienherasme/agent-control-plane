"""Transport-neutral output port for delivering execution events.

InstrumentationSink is a Protocol: any object with a matching async deliver method satisfies it
structurally, without subclassing. StoredInstrumentationSink is the one production adapter in
this repository; a networked transport (HTTP, queue) would be a separate adapter package.
"""

from __future__ import annotations

from typing import Protocol

from agent_control_plane.events import ExecutionEvent
from agent_control_plane.instrumentation.models import DeliveryReceipt


class InstrumentationSink(Protocol):
    async def deliver(self, event: ExecutionEvent) -> DeliveryReceipt: ...
