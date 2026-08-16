"""
Database layer for LinkPlease.
Uses SQLite with WAL mode for persistent, concurrent-safe storage.
All write operations are serialized via an asyncio.Lock to prevent
interleaving of multi-step DB transactions.
"""

import aiosqlite
import asyncio
import logging
import os
import uuid

logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "linkplease.db")


class Database:
    """Async SQLite database wrapper with all LinkPlease operations."""

    def __init__(self, path: str | None = None):
        self.path = path or DB_PATH
        self.db: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    # ──────────────────── Lifecycle ────────────────────

    async def init(self):
        """Initialize database connection and create tables."""
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute("PRAGMA synchronous=NORMAL")
        await self._create_tables()
        await self._recover_stale_sending()
        logger.info(f"Database initialized at {self.path}")

    async def _create_tables(self):
        await self.db.executescript("""
            CREATE TABLE IF NOT EXISTS rules (
                rule_id     TEXT PRIMARY KEY,
                keyword     TEXT NOT NULL,
                dm_message  TEXT NOT NULL,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS processed_events (
                event_id     TEXT PRIMARY KEY,
                event_type   TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS dm_dedup (
                user_id    TEXT NOT NULL,
                rule_id    TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, rule_id)
            );

            CREATE TABLE IF NOT EXISTS dm_queue (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                comment_id      TEXT    NOT NULL,
                user_id         TEXT    NOT NULL,
                username        TEXT    DEFAULT '',
                rule_id         TEXT    NOT NULL,
                dm_message      TEXT    NOT NULL,
                status          TEXT    DEFAULT 'queued'
                    CHECK(status IN (
                        'queued','sending','accepted',
                        'delivered','failed','cancelled'
                    )),
                dm_id           TEXT,
                retries         INTEGER DEFAULT 0,
                max_retries     INTEGER DEFAULT 5,
                next_retry_at   TIMESTAMP DEFAULT (datetime('now')),
                idempotency_key TEXT    UNIQUE,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_dm_queue_status
                ON dm_queue(status, next_retry_at);
            CREATE INDEX IF NOT EXISTS idx_dm_queue_comment
                ON dm_queue(comment_id);
            CREATE INDEX IF NOT EXISTS idx_dm_queue_accepted
                ON dm_queue(status) WHERE status = 'accepted';

            CREATE TABLE IF NOT EXISTS deleted_comments (
                comment_id TEXT PRIMARY KEY,
                deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS stats_counters (
                key   TEXT    PRIMARY KEY,
                value INTEGER DEFAULT 0
            );
        """)
        # Seed the duplicates_blocked counter
        await self.db.execute(
            "INSERT OR IGNORE INTO stats_counters (key, value) VALUES ('duplicates_blocked', 0)"
        )
        await self.db.commit()

    async def _recover_stale_sending(self):
        """
        On startup, reset any DMs stuck in 'sending' state from a previous crash.
        They were mid-flight when the process died — re-queue them.
        """
        async with self._write_lock:
            cursor = await self.db.execute(
                "UPDATE dm_queue SET status = 'queued', updated_at = datetime('now') "
                "WHERE status = 'sending'"
            )
            if cursor.rowcount > 0:
                logger.warning(
                    f"Recovered {cursor.rowcount} DM(s) stuck in 'sending' state"
                )
            await self.db.commit()

    async def close(self):
        """Close the database connection."""
        if self.db:
            await self.db.close()
            logger.info("Database connection closed")

    # ──────────────────── Events ────────────────────

    async def mark_event_processed(self, event_id: str, event_type: str) -> bool:
        """
        Record an event as processed.
        Returns True if the event is new, False if it's a duplicate.
        """
        async with self._write_lock:
            try:
                await self.db.execute(
                    "INSERT INTO processed_events (event_id, event_type) VALUES (?, ?)",
                    (event_id, event_type),
                )
                await self.db.commit()
                return True
            except aiosqlite.IntegrityError:
                return False

    # ──────────────────── Rules ────────────────────

    async def create_rule(self, keyword: str, dm_message: str) -> dict:
        """Create a new keyword-matching rule. Returns the created rule dict."""
        rule_id = f"rule_{uuid.uuid4().hex[:12]}"
        async with self._write_lock:
            await self.db.execute(
                "INSERT INTO rules (rule_id, keyword, dm_message) VALUES (?, ?, ?)",
                (rule_id, keyword, dm_message),
            )
            await self.db.commit()
        return {"rule_id": rule_id, "keyword": keyword, "dm_message": dm_message}

    async def get_all_rules(self) -> list[dict]:
        """Fetch all active rules."""
        cursor = await self.db.execute(
            "SELECT rule_id, keyword, dm_message FROM rules"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ──────────────────── Dedup ────────────────────

    async def check_and_set_dedup(
        self, user_id: str, rule_id: str, comment_id: str
    ) -> bool:
        """
        Attempt to claim the (user_id, rule_id) slot.
        Returns True  → first DM for this user+rule → should send.
        Returns False → duplicate → increments duplicates_blocked counter.
        """
        async with self._write_lock:
            cursor = await self.db.execute(
                "INSERT OR IGNORE INTO dm_dedup (user_id, rule_id, comment_id) "
                "VALUES (?, ?, ?)",
                (user_id, rule_id, comment_id),
            )
            if cursor.rowcount == 0:
                # Duplicate — user already received or is scheduled for this rule
                await self.db.execute(
                    "UPDATE stats_counters SET value = value + 1 "
                    "WHERE key = 'duplicates_blocked'"
                )
                await self.db.commit()
                return False
            await self.db.commit()
            return True

    # ──────────────────── DM Queue ────────────────────

    async def enqueue_dm(
        self,
        comment_id: str,
        user_id: str,
        username: str,
        rule_id: str,
        dm_message: str,
        idempotency_key: str,
    ) -> int | None:
        """
        Add a DM to the send queue.
        Returns the row ID, or None if the idempotency key already exists.
        """
        async with self._write_lock:
            try:
                cursor = await self.db.execute(
                    """INSERT INTO dm_queue
                       (comment_id, user_id, username, rule_id,
                        dm_message, idempotency_key)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (comment_id, user_id, username, rule_id,
                     dm_message, idempotency_key),
                )
                await self.db.commit()
                return cursor.lastrowid
            except aiosqlite.IntegrityError:
                # Idempotency key already exists — skip
                return None

    async def get_next_queued_dm(self) -> dict | None:
        """Fetch the next DM that is ready to be sent (oldest first)."""
        cursor = await self.db.execute(
            """SELECT id, comment_id, user_id, username, rule_id, dm_message,
                      retries, max_retries, idempotency_key
               FROM dm_queue
               WHERE status = 'queued'
                 AND next_retry_at <= datetime('now')
               ORDER BY next_retry_at ASC, id ASC
               LIMIT 1"""
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def mark_dm_sending(self, task_id: int):
        """Mark a DM as currently being sent (in-flight)."""
        async with self._write_lock:
            await self.db.execute(
                "UPDATE dm_queue SET status = 'sending', "
                "updated_at = datetime('now') WHERE id = ?",
                (task_id,),
            )
            await self.db.commit()

    async def mark_dm_accepted(self, task_id: int, api_dm_id: str):
        """Mark a DM as accepted by the API (202 response received)."""
        async with self._write_lock:
            await self.db.execute(
                "UPDATE dm_queue SET status = 'accepted', dm_id = ?, "
                "updated_at = datetime('now') WHERE id = ?",
                (api_dm_id, task_id),
            )
            await self.db.commit()

    async def mark_dm_delivered(self, task_id: int):
        """Mark a DM as confirmed delivered by the API."""
        async with self._write_lock:
            await self.db.execute(
                "UPDATE dm_queue SET status = 'delivered', "
                "updated_at = datetime('now') WHERE id = ?",
                (task_id,),
            )
            await self.db.commit()

    async def mark_dm_failed(self, task_id: int, retries: int):
        """Mark a DM as permanently failed (retries exhausted)."""
        async with self._write_lock:
            await self.db.execute(
                "UPDATE dm_queue SET status = 'failed', retries = ?, "
                "updated_at = datetime('now') WHERE id = ?",
                (retries, task_id),
            )
            await self.db.commit()

    async def requeue_dm(self, task_id: int, retries: int, backoff_seconds: int):
        """Re-queue a DM for retry after a backoff period."""
        async with self._write_lock:
            await self.db.execute(
                "UPDATE dm_queue "
                "SET status = 'queued', dm_id = NULL, retries = ?, "
                "    next_retry_at = datetime('now', '+' || ? || ' seconds'), "
                "    updated_at = datetime('now') "
                "WHERE id = ?",
                (retries, str(backoff_seconds), task_id),
            )
            await self.db.commit()

    async def get_accepted_dms(self, limit: int = 50) -> list[dict]:
        """Fetch DMs accepted by the API but not yet confirmed delivered/failed."""
        cursor = await self.db.execute(
            """SELECT id, dm_id, retries, max_retries, comment_id,
                      user_id, dm_message, idempotency_key
               FROM dm_queue
               WHERE status = 'accepted' AND dm_id IS NOT NULL
               ORDER BY updated_at ASC
               LIMIT ?""",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ──────────────────── Comment Deletion ────────────────────

    async def mark_comment_deleted(self, comment_id: str) -> int:
        """
        Record a comment as deleted and cancel any queued DMs for it.
        Returns the number of DMs cancelled.
        """
        async with self._write_lock:
            # Remember this comment was deleted (for out-of-order events)
            await self.db.execute(
                "INSERT OR IGNORE INTO deleted_comments (comment_id) VALUES (?)",
                (comment_id,),
            )
            # Cancel any unsent DMs for this comment
            cursor = await self.db.execute(
                "UPDATE dm_queue SET status = 'cancelled', "
                "updated_at = datetime('now') "
                "WHERE comment_id = ? AND status IN ('queued', 'sending')",
                (comment_id,),
            )
            cancelled = cursor.rowcount
            await self.db.commit()
            return cancelled

    async def is_comment_deleted(self, comment_id: str) -> bool:
        """Check if a comment has been recorded as deleted."""
        cursor = await self.db.execute(
            "SELECT 1 FROM deleted_comments WHERE comment_id = ?",
            (comment_id,),
        )
        return await cursor.fetchone() is not None

    # ──────────────────── Stats ────────────────────

    async def get_stats(self) -> dict:
        """
        Compute current DM processing statistics from the queue.
          sent              → status = 'delivered'
          failed            → status = 'failed'
          queued            → status IN ('queued', 'sending', 'accepted')
          duplicates_blocked → atomic counter
        """
        stats = {}

        cursor = await self.db.execute(
            "SELECT COUNT(*) AS cnt FROM dm_queue WHERE status = 'delivered'"
        )
        stats["sent"] = (await cursor.fetchone())["cnt"]

        cursor = await self.db.execute(
            "SELECT COUNT(*) AS cnt FROM dm_queue WHERE status = 'failed'"
        )
        stats["failed"] = (await cursor.fetchone())["cnt"]

        cursor = await self.db.execute(
            "SELECT COUNT(*) AS cnt FROM dm_queue "
            "WHERE status IN ('queued', 'sending', 'accepted')"
        )
        stats["queued"] = (await cursor.fetchone())["cnt"]

        cursor = await self.db.execute(
            "SELECT value FROM stats_counters WHERE key = 'duplicates_blocked'"
        )
        row = await cursor.fetchone()
        stats["duplicates_blocked"] = row["value"] if row else 0

        return stats
