from dataclasses import dataclass


@dataclass(slots=True)
class EdgeConfig:
    edge_id: str = "edge-1"
    listen_host: str = "127.0.0.1"
    listen_port: int = 5223
    db_path: str = "edge_gateway.db"
    upstream_host: str = "127.0.0.1"
    upstream_port: int = 5290
    sync_interval_seconds: float = 2.0
