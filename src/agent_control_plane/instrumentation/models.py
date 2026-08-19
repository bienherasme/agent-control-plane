"""Transport-neutral delivery contract between the instrumentation client and a sink.

The sink Protocol and DeliveryReceipt here say nothing about how an event actually reaches the
control plane; that is a future transport adapter's job. This module only defines what the
client needs in order to trust a delivery outcome: whether the sink established acceptance, and
whether its receipt actually describes the event that was sent.
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


class DeliveryStatus(StrEnum):
    """What the sink is confirming about one exact event.

    Both values let the producer session advance: idempotent replay of an already-accepted
    event is a successful outcome, not a special case the client needs to branch on. Failure to
    establish acceptance is never represented here; it is InstrumentationDeliveryError instead.
    """

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


class DeliveryReceipt(BaseModel):
    """A sink's confirmation that one specific event was accepted, new or duplicate.

    A receipt is meaningless on its own: the client always cross-checks event_id,
    execution_id, and sequence against the event it actually sent before trusting it.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: DeliveryStatus
    event_id: NonBlankStr
    execution_id: NonBlankStr
    sequence: PositiveInt
