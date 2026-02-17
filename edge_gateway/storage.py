from __future__ import annotations

import sqlite3
from pathlib import Path

from edge_gateway.models import Message, MessageState


class MessageStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_uuid TEXT NOT NULL,
                    origin_edge_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    body TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(origin_edge_id, message_uuid)
                )
                """
            )

    def insert_message(self, msg: Message) -> bool:
        """Insert message. Returns False if duplicate by unique key."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO messages
                    (message_uuid, origin_edge_id, sender, recipient, body, state)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        msg.message_uuid,
                        msg.origin_edge_id,
                        msg.sender,
                        msg.recipient,
                        msg.body,
                        msg.state.value,
                    ),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def set_state(self, message_uuid: str, origin_edge_id: str, state: MessageState) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE messages
                SET state = ?
                WHERE message_uuid = ? AND origin_edge_id = ?
                """,
                (state.value, message_uuid, origin_edge_id),
            )

    def queued_for_sync(self, limit: int = 100) -> list[Message]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT message_uuid, origin_edge_id, sender, recipient, body, state
                FROM messages
                WHERE state = ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (MessageState.QUEUED_UPSTREAM.value, limit),
            ).fetchall()

        return [
            Message(
                message_uuid=row["message_uuid"],
                origin_edge_id=row["origin_edge_id"],
                sender=row["sender"],
                recipient=row["recipient"],
                body=row["body"],
                state=MessageState(row["state"]),
            )
            for row in rows
        ]
