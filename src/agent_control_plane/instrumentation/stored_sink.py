"""Production InstrumentationSink backed by a durable ExecutionEventStore.

This is the seam where the async producer-facing sink contract meets the synchronous storage
port. A local reference store like SQLite is small and fast enough that deliver() calls it
directly; there is no asyncio.to_thread indirection here, since nothing about this adapter
needs it.
"""

from __future__ import annotations

from agent_control_plane.events import ExecutionEvent, ExecutionEventConflictError
from agent_control_plane.instrumentation.errors import InstrumentationDeliveryError
from agent_control_plane.instrumentation.models import DeliveryReceipt, DeliveryStatus
from agent_control_plane.storage import ExecutionEventStore, ExecutionEventStoreError
from agent_control_plane.storage.models import StoreAppendStatus

_STATUS_MAP = {
    StoreAppendStatus.ACCEPTED: DeliveryStatus.ACCEPTED,
    StoreAppendStatus.DUPLICATE: DeliveryStatus.DUPLICATE,
}


class StoredInstrumentationSink:
    """Delivers events to a durable ExecutionEventStore through the InstrumentationSink contract.

    Producer code depends only on InstrumentationSink; it never sees the store or any
    storage-specific type. Expected store-side failures (event conflicts, persistence errors)
    are translated into InstrumentationDeliveryError; unexpected exceptions are not caught here.
    """

    def __init__(self, store: ExecutionEventStore) -> None:
        self._store = store

    async def deliver(self, event: ExecutionEvent) -> DeliveryReceipt:
        try:
            result = self._store.append(event)
        except (ExecutionEventConflictError, ExecutionEventStoreError) as exc:
            raise InstrumentationDeliveryError(str(exc)) from exc

        return DeliveryReceipt(
            status=_STATUS_MAP[result.status],
            event_id=result.event_id,
            execution_id=result.execution_id,
            sequence=result.sequence,
        )
