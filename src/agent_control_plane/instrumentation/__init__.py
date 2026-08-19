"""Producer instrumentation SDK: an explicit, transport-neutral event-emission boundary.

The core client/session/sink contract depends only on the domain and events layers. Governance
and evaluation are not imported anywhere in this package, and neither of them depends on it.
StoredInstrumentationSink is the one exception: it is a production adapter that bridges to the
storage port, so it (and only it) depends on agent_control_plane.storage.
"""

from agent_control_plane.instrumentation.client import (
    ExecutionInstrumentationClient,
    InstrumentationSession,
)
from agent_control_plane.instrumentation.errors import (
    InstrumentationDeliveryError,
    InstrumentationReceiptError,
)
from agent_control_plane.instrumentation.models import DeliveryReceipt, DeliveryStatus
from agent_control_plane.instrumentation.sink import InstrumentationSink
from agent_control_plane.instrumentation.stored_sink import StoredInstrumentationSink

__all__ = [
    "DeliveryReceipt",
    "DeliveryStatus",
    "ExecutionInstrumentationClient",
    "InstrumentationDeliveryError",
    "InstrumentationReceiptError",
    "InstrumentationSession",
    "InstrumentationSink",
    "StoredInstrumentationSink",
]
