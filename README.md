# chat

Python MVP for an **Edge XMPP Server + Gateway** architecture.

## What this prototype includes

- Local edge server for client connections (JSON line protocol as a stand-in for XMPP stanzas).
- SQLite-backed message journal with states:
  - `local_delivered`
  - `queued_upstream`
  - `synced`
- Upstream sync worker with dedup via `(origin_edge_id, message_uuid)`.
- Offline-first behavior: local delivery continues while upstream is down.

> Note: This is an architecture MVP; it does not yet implement full XMPP stanza parsing.

## Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
python -m edge_gateway.main
```

Server listens on `127.0.0.1:5223` by default.

## Simple client message format

Each client sends one JSON object per line:

```json
{"sender":"alice@edge.local","recipient":"bob@edge.local","body":"hello"}
```

The server returns an ACK JSON line containing the generated `message_uuid` and state.

## Tests

```bash
pytest -q
```
