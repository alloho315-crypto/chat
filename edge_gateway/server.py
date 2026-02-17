from __future__ import annotations

import asyncio
import json
import logging

from edge_gateway.config import EdgeConfig
from edge_gateway.models import Message, MessageState
from edge_gateway.storage import MessageStore

logger = logging.getLogger(__name__)


class EdgeServer:
    def __init__(self, config: EdgeConfig, store: MessageStore) -> None:
        self.config = config
        self.store = store
        self.connected: dict[str, asyncio.StreamWriter] = {}

    async def start(self) -> None:
        server = await asyncio.start_server(
            self._handle_client,
            self.config.listen_host,
            self.config.listen_port,
        )
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
        logger.info("Edge server listening on %s", addrs)
        async with server:
            await server.serve_forever()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        current_jid: str | None = None
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                data = json.loads(line.decode("utf-8"))

                if data.get("type") == "bind":
                    current_jid = data["jid"]
                    self.connected[current_jid] = writer
                    writer.write(b'{"status":"bound"}\n')
                    await writer.drain()
                    continue

                sender = data["sender"]
                recipient = data["recipient"]
                body = data["body"]

                is_local = recipient.endswith("@edge.local")
                state = (
                    MessageState.LOCAL_DELIVERED
                    if is_local
                    else MessageState.QUEUED_UPSTREAM
                )

                message = Message.create(
                    origin_edge_id=self.config.edge_id,
                    sender=sender,
                    recipient=recipient,
                    body=body,
                    state=state,
                )
                self.store.insert_message(message)

                if is_local and recipient in self.connected:
                    self.connected[recipient].write(
                        (
                            json.dumps(
                                {
                                    "type": "message",
                                    "message_uuid": message.message_uuid,
                                    "sender": sender,
                                    "body": body,
                                }
                            )
                            + "\n"
                        ).encode("utf-8")
                    )
                    await self.connected[recipient].drain()

                ack = {
                    "status": "accepted",
                    "message_uuid": message.message_uuid,
                    "state": message.state.value,
                }
                writer.write((json.dumps(ack) + "\n").encode("utf-8"))
                await writer.drain()
        except (json.JSONDecodeError, KeyError):
            writer.write(b'{"status":"error","reason":"bad_request"}\n')
            await writer.drain()
        finally:
            if current_jid and self.connected.get(current_jid) is writer:
                del self.connected[current_jid]
            writer.close()
            await writer.wait_closed()
            logger.info("Client disconnected: %s", peer)
