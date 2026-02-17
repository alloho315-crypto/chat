from edge_gateway.models import Message, MessageState
from edge_gateway.storage import MessageStore


def test_insert_and_queue(tmp_path):
    store = MessageStore(str(tmp_path / "test.db"))

    msg = Message.create(
        origin_edge_id="edge-1",
        sender="alice@edge.local",
        recipient="bob@core.local",
        body="hello",
        state=MessageState.QUEUED_UPSTREAM,
    )

    assert store.insert_message(msg) is True
    queued = store.queued_for_sync()
    assert len(queued) == 1
    assert queued[0].message_uuid == msg.message_uuid


def test_dedup_unique_key(tmp_path):
    store = MessageStore(str(tmp_path / "test.db"))

    msg = Message(
        message_uuid="fixed-id",
        origin_edge_id="edge-1",
        sender="alice@edge.local",
        recipient="bob@core.local",
        body="hello",
        state=MessageState.QUEUED_UPSTREAM,
    )

    assert store.insert_message(msg) is True
    assert store.insert_message(msg) is False
