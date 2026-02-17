from __future__ import annotations

import asyncio
import json
import logging

from edge_gateway.config import EdgeConfig
from edge_gateway.models import MessageState
from edge_gateway.storage import MessageStore

logger = logging.getLogger(__name__)


class UpstreamSync:
    def __init__(self, config: EdgeConfig, store: MessageStore) -> None:
        self.config = config
        self.store = store
        self._running = False

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            await self.sync_once()
            await asyncio.sleep(self.config.sync_interval_seconds)

    def stop(self) -> None:
        self._running = False

    async def sync_once(self) -> None:
        queued = self.store.queued_for_sync(limit=100)
        if not queued:
            return

        try:
            reader, writer = await asyncio.open_connection(
                self.config.upstream_host,
                self.config.upstream_port,
            )
        except OSError:
            logger.info("Upstream unavailable; keeping messages queued")
            return

        for message in queued:
            payload = {
                "type": "message",
                "message_uuid": message.message_uuid,
                "origin_edge_id": message.origin_edge_id,
                "sender": message.sender,
                "recipient": message.recipient,
                "body": message.body,
            }
            writer.write((json.dumps(payload) + "\n").encode("utf-8"))
            await writer.drain()
            ack_line = await reader.readline()
            if not ack_line:
                break
            ack = json.loads(ack_line.decode("utf-8"))
            if ack.get("status") == "ok":
                self.store.set_state(
                    message.message_uuid,
                    message.origin_edge_id,
                    MessageState.SYNCED,
                )

        writer.close()
        await writer.wait_closed()
