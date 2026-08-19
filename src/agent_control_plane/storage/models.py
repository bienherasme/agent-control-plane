"""Storage-layer append contract.

StoreAppendResult mirrors DeliveryReceipt in shape because the underlying semantics are the
same (accepted vs already-accepted), but it is a distinct type. Storage must not depend on the
instrumentation sink contract, and instrumentation must not depend on storage internals; the
stored sink adapter is what translates between the two.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict

from agent_control_plane.domain.models import NonBlankStr


def _require_positive(value: int) -> int:
    if value < 1:
        raise ValueError("must be >= 1")
    return value


PositiveInt = Annotated[int, AfterValidator(_require_positive)]


class StoreAppendStatus(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


class StoreAppendResult(BaseModel):
    """What the durable store did with one event, for a store's own callers to trust."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: StoreAppendStatus
    event_id: NonBlankStr
    execution_id: NonBlankStr
    sequence: PositiveInt


class ExecutionEventStoreError(Exception):
    """Raised for unexpected persistence failures: corrupt rows, unsupported schema, database
    errors. Expected execution-history conflicts remain ExecutionEventConflictError; this is
    reserved for failures outside the normal event lifecycle contract.
    """
