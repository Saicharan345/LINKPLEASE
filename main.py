"""
LinkPlease — Instagram DM Automation Service
=============================================
Matches incoming Instagram comments against keyword rules and sends
DMs via the Pseudogram mock API, with deduplication, rate limiting,
webhook signature verification, and delivery reconciliation.

Endpoints
---------
POST /webhook   — receive comment events (return 200 fast)
POST /rules     — create keyword → DM rules
GET  /stats     — live processing statistics
"""

import hashlib
import hmac
import json
import logging
import os
import time
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from database import Database
from worker import DMSenderWorker, ReconciliationWorker

load_dotenv()

# ──────────────────── Logging ────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("linkplease")

# ──────────────────── Config ────────────────────

API_KEY = os.getenv("PSEUDOGRAM_API_KEY", "")

# ──────────────────── Globals ────────────────────

db = Database()
sender_worker: DMSenderWorker | None = None
reconciliation_worker: ReconciliationWorker | None = None

# In-memory rules cache — avoids a DB read on every webhook
_rules_cache: list[dict] = []
_rules_cache_time: float = 0.0
_RULES_CACHE_TTL = 2.0  # seconds


async def _get_cached_rules() -> list[dict]:
    """Return all rules, refreshing from DB at most every 2 s."""
    global _rules_cache, _rules_cache_time
    now = time.time()
    if now - _rules_cache_time > _RULES_CACHE_TTL:
        _rules_cache = await db.get_all_rules()
        _rules_cache_time = now
    return _rules_cache


def _invalidate_rules_cache():
    global _rules_cache_time
    _rules_cache_time = 0.0


# ──────────────────── App Lifecycle ────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start DB + background workers on boot, clean up on shutdown."""
    global sender_worker, reconciliation_worker

    await db.init()

    sender_worker = DMSenderWorker(db)
    reconciliation_worker = ReconciliationWorker(db)
    sender_worker.start()
    reconciliation_worker.start()

    logger.info("LinkPlease is running")
    yield

    sender_worker.stop()
    reconciliation_worker.stop()
    await db.close()
    logger.info("LinkPlease shut down")


app = FastAPI(
    title="LinkPlease",
    description="Instagram DM automation — keyword-triggered direct messages",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────── Helpers ────────────────────


def _verify_signature(body: bytes, signature_header: str) -> bool:
    """
    Part B — verify HMAC-SHA256 webhook signature.
    The API signs the raw body with our API key as the HMAC secret.
    Handles both `sha256=<hex>` and raw `<hex>` signature formats (case-insensitive).
    """
    if not API_KEY:
        return True  # no key configured — can't verify
    if not signature_header:
        return False

    computed_hex = hmac.new(
        API_KEY.encode(), body, hashlib.sha256
    ).hexdigest()

    # Try both formats: `sha256=<hex>` and raw `<hex>` (case-insensitive)
    sig_lower = signature_header.lower()
    expected_prefixed = ("sha256=" + computed_hex).lower()
    expected_raw = computed_hex.lower()

    if hmac.compare_digest(sig_lower, expected_prefixed):
        return True
    if hmac.compare_digest(sig_lower, expected_raw):
        return True

    logger.warning(
        f"Signature mismatch: received={signature_header[:20]}... "
        f"expected_prefixed={expected_prefixed[:20]}... "
        f"expected_raw={expected_raw[:20]}..."
    )
    return False


# In-memory webhook log buffer for remote debugging
_webhook_logs: list[dict] = []


def _log_debug_webhook(headers: dict, body: str, sig: str, verified: bool, status: int, error: str = None):
    global _webhook_logs
    _webhook_logs.append({
        "time": time.time(),
        "headers": headers,
        "body": body,
        "sig": sig,
        "verified": verified,
        "status": status,
        "error": error
    })
    # Keep last 100 entries
    if len(_webhook_logs) > 100:
        _webhook_logs.pop(0)


# ════════════════════════════════════════════════════════
#  POST /webhook
# ════════════════════════════════════════════════════════


@app.post("/webhook")
async def webhook(request: Request):
    """
    Receive a comment event from Pseudogram.
    Must return 200 within 5 seconds.
    All DM sending happens in background workers — this handler only
    does fast, synchronous DB writes (< 5 ms per event).
    """
    body = await request.body()
    headers_dict = dict(request.headers)
    body_str = ""
    try:
        body_str = body.decode("utf-8", errors="replace")
    except Exception:
        pass

    # ── Part B: signature verification ──
    sig = request.headers.get("X-PseudoGram-Signature", "")
    
    # If a signature is provided, verify it.
    verified = True
    if sig:
        verified = _verify_signature(body, sig)

    if sig and not verified:
        logger.warning("Webhook rejected — invalid signature")
        _log_debug_webhook(headers_dict, body_str, sig, False, 401, "invalid_signature")
        return JSONResponse(
            status_code=401, content={"error": "invalid_signature"}
        )

    # ── Parse payload ──
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        _log_debug_webhook(headers_dict, body_str, sig, verified, 400, "invalid_json")
        return JSONResponse(
            status_code=400, content={"error": "invalid_json"}
        )

    event_id = payload.get("event_id")
    event_type = payload.get("event_type")
    if not event_id or not event_type:
        _log_debug_webhook(headers_dict, body_str, sig, verified, 400, "missing_fields")
        return JSONResponse(
            status_code=400, content={"error": "missing_fields"}
        )

    # ── Event-level dedup (handles the ~8 % redelivery rate) ──
    is_new = await db.mark_event_processed(event_id, event_type)
    if not is_new:
        logger.debug(f"Duplicate event skipped: {event_id}")
        _log_debug_webhook(headers_dict, body_str, sig, verified, 200, "duplicate_event_skipped")
        return {"status": "ok", "detail": "duplicate_event"}

    # ── Route by event type ──
    data = payload.get("data", {})

    if event_type == "comment.created":
        await _handle_comment_created(data)
    elif event_type == "comment.deleted":
        await _handle_comment_deleted(data)
    else:
        logger.debug(f"Ignored event type: {event_type}")

    _log_debug_webhook(headers_dict, body_str, sig, verified, 200)
    return {"status": "ok"}


async def _handle_comment_created(data: dict):
    """Match comment text against rules; enqueue DMs for every match."""
    comment_id = data.get("comment_id", "")
    text = data.get("text", "")
    from_data = data.get("from", {})
    user_id = from_data.get("user_id", "")
    username = from_data.get("username", "")

    if not comment_id or not user_id:
        logger.warning(f"Incomplete comment data, skipping: {data}")
        return

    # Part C — handle out-of-order deletion
    if await db.is_comment_deleted(comment_id):
        logger.info(f"Comment {comment_id} was deleted before creation event arrived — skipped")
        return

    rules = await _get_cached_rules()
    text_lower = text.lower()

    for rule in rules:
        # Case-insensitive keyword match anywhere in the comment text
        if rule["keyword"].lower() not in text_lower:
            continue

        # User + Rule dedup — one DM per user per rule, ever
        is_new = await db.check_and_set_dedup(
            user_id, rule["rule_id"], comment_id
        )
        if not is_new:
            logger.debug(
                f"Duplicate blocked: user={user_id} rule={rule['rule_id']}"
            )
            continue

        # Enqueue for background sending
        idem_key = f"{comment_id}:{rule['rule_id']}"
        await db.enqueue_dm(
            comment_id=comment_id,
            user_id=user_id,
            username=username,
            rule_id=rule["rule_id"],
            dm_message=rule["dm_message"],
            idempotency_key=idem_key,
        )
        logger.info(
            f"Enqueued DM: user={user_id} rule={rule['rule_id']} "
            f"comment={comment_id}"
        )


async def _handle_comment_deleted(data: dict):
    """
    Part C — cancel queued DMs when a comment is deleted.
    Also stores the comment_id so a late-arriving comment.created
    for the same comment_id is skipped.
    """
    comment_id = data.get("comment_id", "")
    if not comment_id:
        return

    cancelled = await db.mark_comment_deleted(comment_id)
    logger.info(
        f"Comment {comment_id} deleted — cancelled {cancelled} queued DM(s)"
    )


# ════════════════════════════════════════════════════════
#  POST /rules
# ════════════════════════════════════════════════════════


@app.post("/rules", status_code=201)
async def create_rule(request: Request):
    """Create a keyword → DM-message rule."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400, content={"error": "invalid_json"}
        )

    keyword = body.get("keyword")
    dm_message = body.get("dm_message")

    if not keyword or not dm_message:
        return JSONResponse(
            status_code=400,
            content={"error": "keyword and dm_message are required"},
        )

    rule = await db.create_rule(keyword, dm_message)
    _invalidate_rules_cache()
    logger.info(f"Rule created: {rule['rule_id']} keyword='{keyword}'")
    return rule


# ════════════════════════════════════════════════════════
#  GET /stats
# ════════════════════════════════════════════════════════


@app.get("/stats")
async def get_stats():
    """
    Live DM processing statistics.
      sent              — DMs the mock API confirmed as delivered
      failed            — gave up after retries
      queued            — waiting to send or waiting on retry
      duplicates_blocked — DMs correctly not sent
    """
    return await db.get_stats()


# ════════════════════════════════════════════════════════
#  GET /health
# ════════════════════════════════════════════════════════


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "linkplease"}


# ════════════════════════════════════════════════════════
#  GET /rules
# ════════════════════════════════════════════════════════


@app.get("/rules")
async def list_rules():
    """List all active keyword rules."""
    rules = await db.get_all_rules()
    return {"rules": rules, "count": len(rules)}


# ════════════════════════════════════════════════════════
#  GET /debug-webhooks
# ════════════════════════════════════════════════════════


@app.get("/debug-webhooks")
async def get_debug_webhooks():
    """Expose the last 100 webhook requests for remote debugging."""
    return {"logs": _webhook_logs, "count": len(_webhook_logs)}


