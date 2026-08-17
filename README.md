# LinkPlease

Instagram DM automation service. When someone comments a keyword on a creator's post, automatically DM them a message.

Built for the [LinkPlease Tech Intern Assignment](https://pseudogram-api.onrender.com).

## Parts Completed: A + B + C

| Part | Feature | Status |
|------|---------|--------|
| **A** | Keyword rules, comment matching, DM sending, dedup, retry | ✅ |
| **B** | Webhook signature verification, accurate live `/stats` | ✅ |
| **C** | Delivery reconciliation, `comment.deleted` handling, 500-event burst | ✅ |

## Architecture

```
Webhook → SQLite (event dedup + rule match + user/rule dedup + enqueue)
                          ↓
              Background DM Sender Worker  ←→  POST /v1/dm/send
                (rate-limited 9/60s)               (Pseudogram)
                          ↓
           Reconciliation Worker  ←→  GET /v1/dm/{dm_id}
            (polls delivery status)
```

- **FastAPI** — async web framework
- **SQLite (WAL mode)** — persistent queue + dedup + stats
- **httpx** — async HTTP client
- **asyncio** — background workers

## Quick Start

### 1. Get your API key

```bash
# Step 1: Apply
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H "Content-Type: application/json" \
  -d '{"name":"Your Name","email":"you@example.com","phone":"+91...","linkedin_url":"https://linkedin.com/in/you"}'

# Step 2: Get key
curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H "Content-Type: application/json" \
  -d '{"email":"you@example.com"}'
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env and paste your API key
```

### 3. Install & Run

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 4. Create a Rule

```bash
curl -X POST http://localhost:8000/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword":"PRICE","dm_message":"Here is the price list: ..."}'
```

### 5. Run Simulation

```bash
# Fire 500 comments at your webhook
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "X-API-Key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"webhook_url":"https://your-app.onrender.com/webhook","count":500,"duration_seconds":10}'

# Check your stats
curl http://localhost:8000/stats

# Compare against truth
curl https://pseudogram-api.onrender.com/v1/simulate/RUN_ID/truth \
  -H "X-API-Key: YOUR_KEY"
```

## Deploy to Render

1. Push to GitHub
2. Connect repo on [Render](https://render.com)
3. Set environment variable: `PSEUDOGRAM_API_KEY`
4. Deploy — Render uses the `render.yaml` blueprint automatically

## API Contract

### POST /webhook
Receives comment events. Returns `200` within 5 seconds after HMAC verification.

Signature header: `X-PseudoGram-Signature: sha256=<hex>`, where `<hex>` is HMAC-SHA256 of the **raw request body** using `PSEUDOGRAM_API_KEY` itself as the secret. Missing or invalid signatures are rejected with `401`.

### POST /rules
```json
// Request
{ "keyword": "PRICE", "dm_message": "Here's the price list: ..." }

// Response 201
{ "rule_id": "rule_a1b2c3d4e5f6", "keyword": "PRICE", "dm_message": "..." }
```

### GET /stats
```json
{
  "sent": 142,
  "failed": 3,
  "queued": 8,
  "duplicates_blocked": 57
}
```

## Design Decisions

| Decision | Why |
|---|---|
| SQLite over Postgres | Zero external deps, good enough for single-server grading |
| 9/60s internal rate limit | Safety margin below the real 10/60s to avoid 429s |
| `asyncio.Lock` on writes | Prevents interleaving of check-then-insert dedup operations |
| In-memory rules cache (2s TTL) | Avoids DB read on every webhook under burst load |
| `Idempotency-Key` on every DM send | Prevents duplicate sends at the API level even across retries |
| Crash recovery on startup | Resets `sending` → `queued` so no DM is silently lost |

## License

MIT
