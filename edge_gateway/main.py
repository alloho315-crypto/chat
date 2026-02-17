from __future__ import annotations

import asyncio
import logging

from edge_gateway.config import EdgeConfig
from edge_gateway.server import EdgeServer
from edge_gateway.storage import MessageStore
from edge_gateway.sync import UpstreamSync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


async def run() -> None:
    config = EdgeConfig()
    store = MessageStore(config.db_path)
    server = EdgeServer(config, store)
    syncer = UpstreamSync(config, store)

    sync_task = asyncio.create_task(syncer.run_forever())
    try:
        await server.start()
    finally:
        syncer.stop()
        await sync_task


if __name__ == "__main__":
    asyncio.run(run())
