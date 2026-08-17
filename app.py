import asyncio
import hashlib
import hmac
import json
import os
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


DATABASE_URL = os.getenv("DATABASE_URL", "linkplease.db")
PSEUDOGRAM_BASE_URL = os.getenv("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")
PSEUDOGRAM_API_KEY = os.getenv("PSEUDOGRAM_API_KEY", "")
VERIFY_SIGNATURES = os.getenv("VERIFY_SIGNATURES", "false").lower() in {"1", "true", "yes"}
RUN_WORKER = os.getenv("RUN_WORKER", "true").lower() not in {"0", "false", "no"}

MAX_SEND_ATTEMPTS = int(os.getenv("MAX_SEND_ATTEMPTS", "8"))
MAX_STATUS_ATTEMPTS = int(os.getenv("MAX_STATUS_ATTEMPTS", "20"))
WORKER_POLL_SECONDS = float(os.getenv("WORKER_POLL_SECONDS", "0.5"))
STATUS_POLL_SECONDS = float(os.getenv("STATUS_POLL_SECONDS", "5"))
SEND_RATE_LIMIT = int(os.getenv("SEND_RATE_LIMIT", "10"))
SEND_RATE_WINDOW_SECONDS = int(os.getenv("SEND_RATE_WINDOW_SECONDS", "60"))

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS rules (
    id TEXT PRIMARY KEY,
    keyword TEXT NOT NULL,
    keyword_normalized TEXT NOT NULL,
    dm_message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deleted_comments (
    comment_id TEXT PRIMARY KEY,
    deleted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    comment_id TEXT NOT NULL,
    message TEXT NOT NULL,
    status TEXT NOT NULL,
    send_attempts INTEGER NOT NULL DEFAULT 0,
    status_attempts INTEGER NOT NULL DEFAULT 0,
    dm_id TEXT,
    next_attempt_at REAL NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(rule_id, user_id),
    FOREIGN KEY(rule_id) REFERENCES rules(id)
);

CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def db_path() -> str:
    if DATABASE_URL == ":memory:":
        return DATABASE_URL
    return str(Path(DATABASE_URL))


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db() -> None:
    with connect_db() as conn:
        conn.executescript(SCHEMA)
        for name in ("duplicates_blocked",):
            conn.execute(
                "INSERT OR IGNORE INTO counters(name, value) VALUES (?, 0)",
                (name,),
            )
        conn.execute(
            """
            UPDATE deliveries
            SET status = 'queued', updated_at = ?, last_error = 'recovered after interrupted send'
            WHERE status = 'sending'
            """,
            (utc_now(),),
        )
        conn.execute(
            """
            UPDATE deliveries
            SET status = 'accepted', updated_at = ?, last_error = 'recovered after interrupted status check'
            WHERE status = 'checking'
            """,
            (utc_now(),),
        )


def increment_counter(conn: sqlite3.Connection, name: str, amount: int = 1) -> None:
    conn.execute(
        """
        INSERT INTO counters(name, value) VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET value = value + excluded.value
        """,
        (name, amount),
    )


class RuleCreate(BaseModel):
    keyword: str = Field(min_length=1)
    dm_message: str = Field(min_length=1)


class RuleOut(BaseModel):
    rule_id: str
    keyword: str
    dm_message: str


class PseudogramClient:
    def __init__(self) -> None:
        self.client = httpx.AsyncClient(
            base_url=PSEUDOGRAM_BASE_URL,
            timeout=httpx.Timeout(15, connect=5),
            headers={"X-API-Key": PSEUDOGRAM_API_KEY} if PSEUDOGRAM_API_KEY else {},
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def send_dm(
        self,
        *,
        recipient_user_id: str,
        message: str,
        comment_id: str,
        idempotency_key: str,
    ) -> tuple[str, dict[str, Any]]:
        response = await self.client.post(
            "/v1/dm/send",
            json={
                "recipient_user_id": recipient_user_id,
                "message": message,
                "comment_id": comment_id,
            },
            headers={"Idempotency-Key": idempotency_key},
        )
        if response.status_code == 202:
            body = response.json()
            return body["dm_id"], body
        raise httpx.HTTPStatusError("send failed", request=response.request, response=response)

    async def get_dm(self, dm_id: str) -> dict[str, Any]:
        response = await self.client.get(f"/v1/dm/{dm_id}")
        response.raise_for_status()
        return response.json()


class RateLimiter:
    def __init__(self, limit: int, window_seconds: int) -> None:
        self.limit = limit
        self.window_seconds = window_seconds
        self.sent_at: list[float] = []
        self.lock = asyncio.Lock()

    async def wait_for_slot(self) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                self.sent_at = [ts for ts in self.sent_at if now - ts < self.window_seconds]
                if len(self.sent_at) < self.limit:
                    self.sent_at.append(now)
                    return
                sleep_for = self.window_seconds - (now - self.sent_at[0]) + 0.05
            await asyncio.sleep(max(sleep_for, 0.1))


class DeliveryWorker:
    def __init__(self, client: PseudogramClient) -> None:
        self.client = client
        self.rate_limiter = RateLimiter(SEND_RATE_LIMIT, SEND_RATE_WINDOW_SECONDS)
        self.stop_event = asyncio.Event()
        self.task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self.task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self.stop_event.set()
        if self.task:
            await self.task

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                worked = await self.process_one()
                if not worked:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=WORKER_POLL_SECONDS)
            except asyncio.TimeoutError:
                pass
            except Exception:
                await asyncio.sleep(1)

    async def process_one(self) -> bool:
        row = claim_due_delivery()
        if not row:
            return False
        if row["status"] == "sending":
            await self.send(row)
        elif row["status"] == "checking":
            await self.check(row)
        return True

    async def send(self, row: sqlite3.Row) -> None:
        if row["send_attempts"] >= MAX_SEND_ATTEMPTS:
            mark_failed(row["id"], "send attempts exhausted")
            return

        await self.rate_limiter.wait_for_slot()
        attempt = row["send_attempts"] + 1
        idempotency_key = f"{row['id']}:send:{attempt}"
        try:
            dm_id, _ = await self.client.send_dm(
                recipient_user_id=row["user_id"],
                message=row["message"],
                comment_id=row["comment_id"],
                idempotency_key=idempotency_key,
            )
        except httpx.HTTPStatusError as exc:
            response = exc.response
            if response.status_code == 400:
                mark_failed(row["id"], response.text)
                return
            delay = retry_delay(attempt, response)
            reschedule(row["id"], "queued", "sending", delay, response.text)
            return
        except httpx.HTTPError as exc:
            delay = retry_delay(attempt)
            reschedule(row["id"], "queued", "sending", delay, str(exc))
            return

        schedule_status_check(row["id"], dm_id)

    async def check(self, row: sqlite3.Row) -> None:
        if not row["dm_id"]:
            reschedule(row["id"], "queued", "sending", 0, "missing dm_id")
            return
        if row["status_attempts"] >= MAX_STATUS_ATTEMPTS:
            mark_failed(row["id"], "status polling exhausted")
            return

        try:
            body = await self.client.get_dm(row["dm_id"])
        except httpx.HTTPError as exc:
            reschedule(row["id"], "accepted", "checking", STATUS_POLL_SECONDS, str(exc), bump_status=True)
            return

        status = body.get("status")
        if status == "delivered":
            mark_delivered(row["id"])
        elif status == "failed":
            reschedule(row["id"], "queued", "sending", retry_delay(row["send_attempts"] + 1), "dm failed later")
        else:
            reschedule(row["id"], "accepted", "checking", STATUS_POLL_SECONDS, f"status={status}", bump_status=True)


def retry_delay(attempt: int, response: httpx.Response | None = None) -> float:
    if response is not None and response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 1)
            except ValueError:
                pass
    return min(2**attempt, 60)


def claim_due_delivery() -> sqlite3.Row | None:
    now = time.time()
    with connect_db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT * FROM deliveries
            WHERE status IN ('queued', 'accepted') AND next_attempt_at <= ?
            ORDER BY next_attempt_at, created_at
            LIMIT 1
            """,
            (now,),
        ).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None

        new_status = "sending" if row["status"] == "queued" else "checking"
        conn.execute(
            """
            UPDATE deliveries
            SET status = ?, updated_at = ?,
                send_attempts = send_attempts + CASE WHEN ? = 'sending' THEN 1 ELSE 0 END
            WHERE id = ?
            """,
            (new_status, utc_now(), new_status, row["id"]),
        )
        claimed = conn.execute("SELECT * FROM deliveries WHERE id = ?", (row["id"],)).fetchone()
        conn.execute("COMMIT")
        return claimed


def reschedule(
    delivery_id: str,
    public_status: str,
    from_status: str,
    delay_seconds: float,
    last_error: str,
    *,
    bump_status: bool = False,
) -> None:
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE deliveries
            SET status = ?, next_attempt_at = ?, last_error = ?, updated_at = ?,
                status_attempts = status_attempts + ?
            WHERE id = ? AND status = ?
            """,
            (public_status, time.time() + delay_seconds, last_error[:1000], utc_now(), 1 if bump_status else 0, delivery_id, from_status),
        )


def schedule_status_check(delivery_id: str, dm_id: str) -> None:
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE deliveries
            SET status = 'accepted', dm_id = ?, next_attempt_at = ?, last_error = NULL, updated_at = ?
            WHERE id = ?
            """,
            (dm_id, time.time() + STATUS_POLL_SECONDS, utc_now(), delivery_id),
        )


def mark_delivered(delivery_id: str) -> None:
    with connect_db() as conn:
        conn.execute(
            "UPDATE deliveries SET status = 'delivered', updated_at = ? WHERE id = ?",
            (utc_now(), delivery_id),
        )


def mark_failed(delivery_id: str, reason: str) -> None:
    with connect_db() as conn:
        conn.execute(
            "UPDATE deliveries SET status = 'failed', last_error = ?, updated_at = ? WHERE id = ?",
            (reason[:1000], utc_now(), delivery_id),
        )


def verify_signature(raw_body: bytes, signature: str | None) -> bool:
    if not PSEUDOGRAM_API_KEY:
        return False
    if not signature or not signature.startswith("sha256="):
        return False
    expected = hmac.new(PSEUDOGRAM_API_KEY.encode(), raw_body, hashlib.sha256).hexdigest()
    provided = signature.removeprefix("sha256=")
    return hmac.compare_digest(expected, provided)


def handle_event(event: dict[str, Any]) -> None:
    event_id = event.get("event_id")
    event_type = event.get("event_type")
    data = event.get("data") or {}
    now = utc_now()

    with connect_db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        if event_id:
            existed = conn.execute(
                "SELECT 1 FROM processed_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if not existed:
                conn.execute(
                    "INSERT INTO processed_events(event_id, processed_at) VALUES (?, ?)",
                    (event_id, now),
                )

        comment_id = data.get("comment_id")
        if event_type == "comment.deleted" and comment_id:
            conn.execute(
                "INSERT OR IGNORE INTO deleted_comments(comment_id, deleted_at) VALUES (?, ?)",
                (comment_id, now),
            )
            conn.execute(
                """
                UPDATE deliveries
                SET status = 'cancelled', last_error = 'comment deleted before delivery', updated_at = ?
                WHERE comment_id = ? AND status IN ('queued', 'accepted')
                """,
                (now, comment_id),
            )
            conn.execute("COMMIT")
            return

        if event_type != "comment.created" or not comment_id:
            conn.execute("COMMIT")
            return

        deleted = conn.execute(
            "SELECT 1 FROM deleted_comments WHERE comment_id = ?",
            (comment_id,),
        ).fetchone()
        if deleted:
            conn.execute("COMMIT")
            return

        user = data.get("from") or {}
        user_id = user.get("user_id")
        text = (data.get("text") or "").casefold()
        if not user_id or not text:
            conn.execute("COMMIT")
            return

        rules = conn.execute("SELECT * FROM rules").fetchall()
        for rule in rules:
            if rule["keyword_normalized"] not in text:
                continue
            delivery_id = uuid.uuid4().hex
            try:
                conn.execute(
                    """
                    INSERT INTO deliveries(
                        id, rule_id, user_id, comment_id, message, status,
                        next_attempt_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?)
                    """,
                    (
                        delivery_id,
                        rule["id"],
                        user_id,
                        comment_id,
                        rule["dm_message"],
                        time.time(),
                        now,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                increment_counter(conn, "duplicates_blocked")
        conn.execute("COMMIT")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    client = PseudogramClient()
    worker = DeliveryWorker(client)
    app.state.worker = worker
    if RUN_WORKER:
        worker.start()
    try:
        yield
    finally:
        if RUN_WORKER:
            await worker.stop()
        await client.close()


app = FastAPI(lifespan=lifespan)


@app.post("/rules", response_model=RuleOut, status_code=201)
def create_rule(rule: RuleCreate) -> RuleOut:
    rule_id = f"rule_{uuid.uuid4().hex[:12]}"
    keyword = rule.keyword.strip()
    dm_message = rule.dm_message.strip()
    if not keyword or not dm_message:
        raise HTTPException(status_code=400, detail="keyword and dm_message are required")
    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO rules(id, keyword, keyword_normalized, dm_message, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (rule_id, keyword, keyword.casefold(), dm_message, utc_now()),
        )
    return RuleOut(rule_id=rule_id, keyword=keyword, dm_message=dm_message)


@app.post("/webhook")
async def webhook(
    request: Request,
    x_pseudogram_signature: str | None = Header(default=None),
) -> JSONResponse:
    raw_body = await request.body()
    if VERIFY_SIGNATURES and not verify_signature(raw_body, x_pseudogram_signature):
        raise HTTPException(status_code=401, detail="invalid signature")
    try:
        event = json.loads(raw_body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid json")
    handle_event(event)
    return JSONResponse({"ok": True})


@app.get("/stats")
def stats() -> dict[str, int]:
    with connect_db() as conn:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status IN ('queued', 'sending', 'accepted', 'checking') THEN 1 ELSE 0 END) AS queued
            FROM deliveries
            """
        ).fetchone()
        duplicate_row = conn.execute(
            "SELECT value FROM counters WHERE name = 'duplicates_blocked'"
        ).fetchone()
    return {
        "sent": int(row["sent"] or 0),
        "failed": int(row["failed"] or 0),
        "queued": int(row["queued"] or 0),
        "duplicates_blocked": int(duplicate_row["value"] if duplicate_row else 0),
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
