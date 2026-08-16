# FAILURES.md — Known Failure Modes

Every system has failure modes. Here are the ones I know about.

---

## 1. Process restart loses in-memory rate-limiter state

The sliding-window rate limiter tracks request timestamps in memory. On restart (redeploy, crash, Render spin-down), the window resets to empty. If the process restarts right after sending 9 DMs, it immediately has 9 fresh slots and could send 9 more — briefly exceeding 10/60 s from the API's perspective. **Mitigation**: I use 9/60 s instead of 10/60 s, which shrinks the window but doesn't eliminate the risk entirely.

## 2. SQLite on Render's ephemeral filesystem

Render's free tier uses an ephemeral filesystem. A redeploy wipes the SQLite database: rules, queued DMs, dedup history, and stats are all lost. Any DM that was `queued` or `accepted` at the time of redeploy is permanently lost — no persistent job system survives the wipe. **Impact**: dedup resets too, so a user could receive a duplicate DM if events repeat across deploys. For production, this would need PostgreSQL or a persistent disk.

## 3. Narrow race window on user+rule dedup

Two webhook events for **different comments** by the **same user**, both matching the **same rule**, arriving within sub-millisecond timing can theoretically both pass the `INSERT OR IGNORE` dedup check. The `asyncio.Lock` makes this very unlikely in a single-process setup, but not impossible if uvicorn runs multiple workers. The Pseudogram API's `Idempotency-Key` is keyed on `comment_id:rule_id`, so it won't catch this either — the `comment_id`s are different. **Result**: the user gets two DMs for the same rule.

## 4. Accepted-but-failed DMs have a reconciliation delay

~15 % of DMs the API accepts (202) later fail. My reconciliation worker polls every 3 seconds with a 5-second startup delay. During that window, `/stats` reports those DMs as `queued` (not yet `sent` or `failed`). If the process restarts after a DM is marked `accepted` but before reconciliation runs, the DM stays as `accepted` in the DB — correctly recovered on next startup, but with delay. No DM is lost, but stats lag behind ground truth.

## 5. `comment.deleted` after DM already accepted or delivered

If a `comment.deleted` event arrives after the DM has already been accepted by the API (`status = 'accepted'` or `'delivered'`), I cannot un-send it. Only DMs still in `queued` or `sending` state are cancelled. This is inherent — you can't recall a delivered message.

## 6. Event-level dedup does not survive DB wipe

The `processed_events` table tracks which `event_id`s we've already handled. If the DB is wiped (see #2) and the API redelivers an old event, we'll process it again. Combined with the dedup table also being wiped, this could result in a duplicate DM send. In a production system, I'd use a durable database.

## 7. Single-sender bottleneck under sustained load

Only one DM sender coroutine runs. With a 9/60 s rate limit, the maximum throughput is 9 DMs per minute. For a 500-comment burst where, say, 250 match a rule, the queue takes ~28 minutes to drain. This is correct (the rate limit exists), but it means latency between comment and DM delivery is high under burst load. Adding more sender workers wouldn't help — the API rate limit is per-key, not per-connection.
