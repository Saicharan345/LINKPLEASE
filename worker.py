"""
Background workers for LinkPlease.

DMSenderWorker
  Continuously picks queued DMs from the database and sends them
  via POST /v1/dm/send, respecting the 10-req/60s rate limit.

ReconciliationWorker
  Polls GET /v1/dm/{dm_id} for DMs the API accepted (202) to detect
  late failures (~15 % of accepted DMs). Re-queues failed ones.
"""

import asyncio
import logging
import os
import time

import httpx

logger = logging.getLogger(__name__)

def get_api_base() -> str:
    return os.getenv(
        "PSEUDOGRAM_API_BASE", "https://pseudogram-api.onrender.com"
    )


def get_api_key() -> str:
    return os.getenv("PSEUDOGRAM_API_KEY", "")



# ──────────────────────────────────────────────────────────
#  Sliding-window rate limiter
# ──────────────────────────────────────────────────────────

class SlidingWindowRateLimiter:
    """
    Enforces ≤ max_requests within any rolling window of window_seconds.
    We default to 9/60 s (one below the real 10/60 s limit) as a safety
    margin against timing drift between our clock and the API server's.
    """

    def __init__(
        self, max_requests: int = 9, window_seconds: float = 60.0
    ):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self):
        """Block until a request slot is available, then claim it."""
        while True:
            async with self._lock:
                now = time.monotonic()
                # Drop timestamps outside the current window
                self.timestamps = [
                    t for t in self.timestamps
                    if now - t < self.window_seconds
                ]
                if len(self.timestamps) < self.max_requests:
                    self.timestamps.append(now)
                    return  # slot acquired

                # All slots used — calculate wait
                wait = self.window_seconds - (now - self.timestamps[0]) + 0.5

            logger.debug(f"Rate limiter: waiting {wait:.1f}s for slot")
            await asyncio.sleep(wait)


# ──────────────────────────────────────────────────────────
#  DM Sender Worker
# ──────────────────────────────────────────────────────────

class DMSenderWorker:
    """Sends queued DMs one at a time, honouring the rate limit."""

    def __init__(self, db):
        self.db = db
        self._task: asyncio.Task | None = None
        self._running = False
        self.rate_limiter = SlidingWindowRateLimiter(
            max_requests=9, window_seconds=60.0
        )

    def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("DM sender worker started")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            logger.info("DM sender worker stopped")

    # ── main loop ──

    async def _run(self):
        async with httpx.AsyncClient(timeout=15.0) as client:
            while self._running:
                try:
                    await self._process_one(client)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error(f"Sender error: {exc}", exc_info=True)
                    await asyncio.sleep(1)

    async def _process_one(self, client: httpx.AsyncClient):
        """Pick the next queued DM, send it, handle the response."""
        dm = await self.db.get_next_queued_dm()
        if dm is None:
            await asyncio.sleep(0.3)  # nothing to do — back off
            return

        task_id = dm["id"]
        await self.db.mark_dm_sending(task_id)

        # ── wait for a rate-limit slot ──
        await self.rate_limiter.acquire()

        try:
            resp = await client.post(
                f"{get_api_base()}/v1/dm/send",
                json={
                    "recipient_user_id": dm["user_id"],
                    "message": dm["dm_message"],
                    "comment_id": dm["comment_id"],
                },
                headers={
                    "X-API-Key": get_api_key(),
                    "Idempotency-Key": dm["idempotency_key"],
                },
            )
            await self._handle_response(resp, dm)

        except httpx.RequestError as exc:
            logger.error(
                f"Network error sending DM for comment={dm['comment_id']}: {exc}"
            )
            await self._schedule_retry(dm)
        except Exception as exc:
            # Keep the job retryable instead of leaving it stuck in 'sending'
            # until process restart (e.g. malformed JSON on a 202 body).
            logger.error(
                f"Unexpected error sending DM for comment={dm['comment_id']}: {exc}",
                exc_info=True,
            )
            await self._schedule_retry(dm)

    # ── response routing ──

    async def _handle_response(self, resp: httpx.Response, dm: dict):
        task_id = dm["id"]

        if resp.status_code in (200, 202):
            try:
                data = resp.json()
            except Exception:
                logger.warning(
                    f"Unreadable API response for comment={dm['comment_id']}, scheduling retry"
                )
                await self._schedule_retry(dm)
                return
            api_dm_id = data.get("dm_id", "")
            api_status = data.get("status", "")
            
            if api_status == "delivered":
                await self.db.mark_dm_delivered(task_id)
                logger.info(
                    f"DM delivered directly: dm_id={api_dm_id} comment={dm['comment_id']}"
                )
            elif api_status == "failed":
                logger.warning(
                    f"DM failed directly: dm_id={api_dm_id} comment={dm['comment_id']}, scheduling retry"
                )
                await self._schedule_retry(dm)
            else:
                await self.db.mark_dm_accepted(task_id, api_dm_id)
                logger.info(
                    f"DM accepted: dm_id={api_dm_id} comment={dm['comment_id']}"
                )

        elif resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            await self.db.requeue_dm(task_id, dm["retries"], retry_after)
            logger.warning(f"Rate-limited by API — pausing {retry_after}s")
            # Pause the whole worker to respect the server's cooldown
            await asyncio.sleep(min(retry_after, 65))

        elif resp.status_code == 500:
            logger.warning(
                f"API 500 for comment={dm['comment_id']}, scheduling retry"
            )
            await self._schedule_retry(dm)

        elif resp.status_code == 400:
            # Malformed request — retrying will not help
            await self.db.mark_dm_failed(task_id, dm["max_retries"])
            logger.error(
                f"DM permanently failed (400 bad request): "
                f"comment={dm['comment_id']}  body={resp.text}"
            )

        else:
            logger.warning(
                f"Unexpected status {resp.status_code}: {resp.text}"
            )
            await self._schedule_retry(dm)

    # ── retry logic ──

    async def _schedule_retry(self, dm: dict):
        task_id = dm["id"]
        retries = dm["retries"] + 1

        if retries >= dm["max_retries"]:
            await self.db.mark_dm_failed(task_id, retries)
            logger.error(
                f"DM permanently failed after {retries} retries: "
                f"comment={dm['comment_id']}"
            )
        else:
            backoff = min(2 ** retries, 30)  # 2, 4, 8, 16, 30 s
            await self.db.requeue_dm(task_id, retries, backoff)
            logger.info(
                f"DM retry {retries}/{dm['max_retries']} in {backoff}s: "
                f"comment={dm['comment_id']}"
            )


# ──────────────────────────────────────────────────────────
#  Reconciliation Worker
# ──────────────────────────────────────────────────────────

class ReconciliationWorker:
    """
    Polls GET /v1/dm/{dm_id} for every accepted-but-unconfirmed DM.
    Marks delivered ones as 'delivered' and re-queues failed ones.
    Reads do NOT count against the rate limit.
    """

    def __init__(self, db):
        self.db = db
        self._task: asyncio.Task | None = None
        self._running = False

    def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info("Reconciliation worker started")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            logger.info("Reconciliation worker stopped")

    async def _run(self):
        # Give the sender a head-start before polling statuses
        await asyncio.sleep(5)

        async with httpx.AsyncClient(timeout=10.0) as client:
            while self._running:
                try:
                    await self._reconcile_batch(client)
                    await asyncio.sleep(3)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error(
                        f"Reconciliation error: {exc}", exc_info=True
                    )
                    await asyncio.sleep(5)

    async def _reconcile_batch(self, client: httpx.AsyncClient):
        dms = await self.db.get_accepted_dms(limit=50)
        if not dms:
            return

        for dm in dms:
            await self._check_one(client, dm)

    async def _check_one(self, client: httpx.AsyncClient, dm: dict):
        try:
            resp = await client.get(
                f"{get_api_base()}/v1/dm/{dm['dm_id']}",
                headers={"X-API-Key": get_api_key()},
            )
            if resp.status_code != 200:
                return

            status = resp.json().get("status")

            if status == "delivered":
                await self.db.mark_dm_delivered(dm["id"])
                logger.info(f"DM confirmed delivered: {dm['dm_id']}")

            elif status == "failed":
                retries = dm["retries"] + 1
                if retries >= dm["max_retries"]:
                    await self.db.mark_dm_failed(dm["id"], retries)
                    logger.error(
                        f"DM delivery failed permanently: {dm['dm_id']}"
                    )
                else:
                    backoff = min(2 ** retries, 30)
                    await self.db.requeue_dm(dm["id"], retries, backoff)
                    logger.info(
                        f"DM delivery failed — re-queued: {dm['dm_id']}  "
                        f"retry {retries}/{dm['max_retries']}"
                    )
            # status == "queued" at the API → still in-flight, check later

        except httpx.RequestError as exc:
            logger.error(f"Error polling DM {dm['dm_id']}: {exc}")
