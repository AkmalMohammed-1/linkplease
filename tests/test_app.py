import importlib
import uuid
from pathlib import Path

from fastapi.testclient import TestClient


def make_client(monkeypatch):
    db_dir = Path.cwd() / "test_dbs"
    db_dir.mkdir(exist_ok=True)
    monkeypatch.setenv("DATABASE_URL", str(db_dir / f"{uuid.uuid4().hex}.db"))
    monkeypatch.setenv("PSEUDOGRAM_API_KEY", "secret")
    monkeypatch.setenv("VERIFY_SIGNATURES", "false")
    monkeypatch.setenv("RUN_WORKER", "false")
    import app

    importlib.reload(app)
    app.init_db()
    client = TestClient(app.app)
    return client, app


def test_rule_webhook_and_duplicate_stats(monkeypatch):
    client, _ = make_client(monkeypatch)

    response = client.post(
        "/rules",
        json={"keyword": "PRICE", "dm_message": "Here is the price list"},
    )
    assert response.status_code == 201
    rule = response.json()
    assert rule["keyword"] == "PRICE"

    event = {
        "event_id": "evt_1",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_1",
            "text": "price please",
            "from": {"user_id": "usr_1", "username": "a"},
        },
    }
    assert client.post("/webhook", json=event).status_code == 200

    event["event_id"] = "evt_2"
    event["data"]["comment_id"] = "cmt_2"
    assert client.post("/webhook", json=event).status_code == 200

    stats = client.get("/stats").json()
    assert stats["queued"] == 1
    assert stats["duplicates_blocked"] == 1


def test_comment_deleted_before_created_does_not_queue(monkeypatch):
    client, _ = make_client(monkeypatch)
    client.post("/rules", json={"keyword": "PRICE", "dm_message": "Here"})

    deleted = {
        "event_id": "evt_deleted",
        "event_type": "comment.deleted",
        "data": {"comment_id": "cmt_9"},
    }
    assert client.post("/webhook", json=deleted).status_code == 200

    created = {
        "event_id": "evt_created",
        "event_type": "comment.created",
        "data": {
            "comment_id": "cmt_9",
            "text": "PRICE",
            "from": {"user_id": "usr_9", "username": "b"},
        },
    }
    assert client.post("/webhook", json=created).status_code == 200
    assert client.get("/stats").json()["queued"] == 0
