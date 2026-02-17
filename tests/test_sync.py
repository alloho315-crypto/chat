import asyncio
import json

from edge_gateway.config import EdgeConfig
from edge_gateway.models import Message, MessageState
from edge_gateway.storage import MessageStore
from edge_gateway.sync import UpstreamSync


async def _upstream_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            payload = json.loads(line.decode("utf-8"))
            assert payload["type"] == "message"
            writer.write(b'{"status":"ok"}\n')
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def _run_sync_test(tmp_path):
    server = await asyncio.start_server(_upstream_handler, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    store = MessageStore(str(tmp_path / "test.db"))
    msg = Message.create(
        origin_edge_id="edge-1",
        sender="a@edge.local",
        recipient="b@core.local",
        body="queued",
        state=MessageState.QUEUED_UPSTREAM,
    )
    store.insert_message(msg)

    config = EdgeConfig(upstream_host=host, upstream_port=port)
    syncer = UpstreamSync(config, store)

    await syncer.sync_once()

    queued = store.queued_for_sync()
    assert queued == []

    server.close()
    await server.wait_closed()


def test_sync_moves_message_to_synced(tmp_path):
    asyncio.run(_run_sync_test(tmp_path))
