from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from uuid import uuid4


class MessageState(str, Enum):
    LOCAL_DELIVERED = "local_delivered"
    QUEUED_UPSTREAM = "queued_upstream"
    SYNCED = "synced"


@dataclass(slots=True)
class Message:
    message_uuid: str
    origin_edge_id: str
    sender: str
    recipient: str
    body: str
    state: MessageState

    @classmethod
    def create(
        cls,
        *,
        origin_edge_id: str,
        sender: str,
        recipient: str,
        body: str,
        state: MessageState,
    ) -> "Message":
        return cls(
            message_uuid=str(uuid4()),
            origin_edge_id=origin_edge_id,
            sender=sender,
            recipient=recipient,
            body=body,
            state=state,
        )
