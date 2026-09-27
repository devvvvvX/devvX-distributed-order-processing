# Phase 4 — Distributed Coordination with Redis

> **Architecture Decision Record**: Why and how we added Redis as a coordination layer.

---

## The Problem Statement

Phase 3 gave us reliable event processing, but left five coordination gaps that break under horizontal scaling:

| # | Problem | Phase 3 Behavior | Impact at Scale |
|---|---|---|---|
| 1 | No rate limiting | API accepts unlimited requests | One abuser takes down the platform |
| 2 | Concurrent status updates | Optimistic locking catches SOME races | Stale reads across instances → confusing errors |
| 3 | Hot-path queries hit DB | Every GET = DB roundtrip | Trending order gets 100 req/s → DB overwhelmed |
| 4 | Multiple outbox relays | All instances relay all events | 3x duplicate events (wasted work) |
| 5 | No driver assignment | No coordination for "who gets this order" | Two drivers accept the same order |

---

## Why Redis?

Redis is a **distributed data structure server** (not just a cache). It solves coordination problems that are impossible with PostgreSQL alone when multiple API instances are involved.

| Feature | PostgreSQL | Redis | Winner |
|---|---|---|---|
| Atomic counters | `UPDATE SET count = count + 1` (row lock) | `INCR key` (no lock needed) | Redis (10-100x faster) |
| Expiring data | Manual cleanup with cron jobs | Built-in TTL per key | Redis (zero maintenance) |
| Distributed locks | Advisory locks (per-connection) | `SET key NX PX ttl` (global) | Redis (cross-instance) |
| Request latency | 1-5ms per query | 0.1ms per command | Redis (10-50x faster) |
| Data durability | ACID transactions, WAL | In-memory, optional persistence | PostgreSQL |
| Complex queries | JOINs, aggregations, full-text | Key-value only | PostgreSQL |

**Use PostgreSQL for data you can't afford to lose.**
**Use Redis for ephemeral coordination data.**

---

## Architecture Overview

```
┌────────────────────────────────────────────────────────────────────┐
│                         CLIENT REQUEST                             │
└──────────────────────────────┬─────────────────────────────────────┘
                               │
                               ▼
┌────────────────────────────────────────────────────────────────────┐
│                     RATE LIMITER (Redis)                            │
│  INCR ratelimit:{clientId}:{window} → count > 30? → 429 REJECT    │
└──────────────────────────────┬─────────────────────────────────────┘
                               │ (passes rate limit)
                               ▼
┌──────────────────────── API ROUTES ────────────────────────────────┐
│                                                                    │
│  GET /orders/:id ──→ Cache check (Redis)                          │
│    HIT?  → return cached (0.1ms)                                  │
│    MISS? → query PostgreSQL → cache result → return               │
│                                                                    │
│  PATCH /orders/:id/status ──→ Acquire lock (Redis)                │
│    LOCKED?   → 409 Conflict                                        │
│    ACQUIRED? → update DB → invalidate cache → release lock        │
│                                                                    │
│  POST /orders/:id/assign ──→ SET NX (Redis)                      │
│    EXISTS?   → 409 Already Assigned                                │
│    SET?      → 201 Assigned                                        │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘

┌──────────────────── BACKGROUND (per instance) ────────────────────┐
│                                                                    │
│  Leader Election (Redis)                                           │
│    SET leader:outbox-relay {instanceId} NX PX 15000                │
│    → LEADER?   Start outbox relay                                  │
│    → FOLLOWER? Sleep (check every 5s)                              │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## Deep Dive: Rate Limiting

### The Algorithm: Fixed Window Counter

```
Key:   ratelimit:{clientId}:{windowStart}
Value: request count in this window
TTL:   window size (e.g., 60 seconds)

Request arrives:
  1. MULTI                          ← start atomic batch
  2.   INCR key                     ← increment (creates key=1 if new)
  3.   PEXPIRE key windowMs         ← set TTL (idempotent)
  4. EXEC                           ← execute atomically
  5. If count > limit → 429
```

### Why MULTI/EXEC?

Without atomic batching, a race condition could cause a key to exist without a TTL:

```
Thread A: INCR key → key created (no TTL!)
Thread B: INCR key
Thread A: PEXPIRE key → sets TTL
```

If Thread A crashes before PEXPIRE, the key persists forever (memory leak). MULTI/EXEC ensures both commands execute atomically.

### Fail-Open Design

If Redis is down, the rate limiter **allows all requests** (fails open). The alternative (fail closed = reject everything) means a Redis outage takes down your entire API. That's worse than no rate limiting.

```
try {
  const count = await redis.multi().incr(key).pexpire(key, ttl).exec();
  if (count > limit) return 429;
} catch (err) {
  // Redis is down — let the request through
  log.error('Rate limiter error — falling open');
}
```

### Response Headers

Every response includes rate limit headers:

```
X-RateLimit-Limit: 30          ← max requests per window
X-RateLimit-Remaining: 17      ← requests left in this window
X-RateLimit-Reset: 1717785660  ← Unix timestamp when window resets
Retry-After: 23                ← seconds until next window (only on 429)
```

Well-behaved API clients (like Stripe's SDK) read these headers and self-throttle.

---

## Deep Dive: Distributed Locking

### The Race Condition

```
Instance A: GET order (status=PENDING)     → reads PENDING
Instance B: GET order (status=PENDING)     → reads PENDING
Instance A: validates PENDING→CONFIRMED    → ✅ updates
Instance B: validates PENDING→PREPARING    → ❌ stale state!
```

Instance B read the order BEFORE Instance A updated it. B thinks the order is PENDING (stale), validates PENDING→PREPARING as legal, and tries to update. PostgreSQL's optimistic locking might catch this, but the error is confusing and wastes a DB roundtrip.

### The Fix: Redis Lock

```
Instance A: SET lock:order:123 tokenA NX PX 5000    → OK (acquired!)
Instance B: SET lock:order:123 tokenB NX PX 5000    → null (blocked)
Instance A: GET order → UPDATE → DEL lock:order:123 → done
Instance B: (retries or returns 409 Conflict)
```

### Lock Release: Why Lua?

Without Lua, releasing requires two commands:

```
token = GET lock:order:123     ← check if we still own it
if token == myToken:
  DEL lock:order:123           ← release
```

Between GET and DEL, another process could acquire the lock. We'd delete THEIR lock. The Lua script runs atomically:

```lua
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
```

### Lock TTL as Safety Net

If the lock holder crashes without releasing, the lock auto-expires after TTL. Trade-off:
- **Too short** (1s): Lock expires while still processing → two holders
- **Too long** (60s): Crashed holder blocks others for a minute
- **We use 5s**: Enough for a DB roundtrip, short enough for quick recovery

---

## Deep Dive: Caching (Cache-Aside Pattern)

### Read Path (Cache-Aside)

```
GET /orders/:id
  1. redis.GET("cache:order:123")
     → HIT?  Return cached JSON (skip DB)
     → MISS? Continue to step 2
  2. pool.query("SELECT * FROM orders WHERE id = $1")
  3. redis.SETEX("cache:order:123", 60, JSON.stringify(order))
  4. Return order
```

### Write Path (Cache Invalidation)

```
PATCH /orders/:id/status
  1. pool.query("UPDATE orders SET status = $1 ...")
  2. redis.DEL("cache:order:123")  ← invalidate!
  3. Next GET will fetch fresh data from DB
```

### Why Invalidate, Not Update?

**Option A (Invalidate — what we do):**
  Delete cache key → next read fetches fresh data.
  Worst case: one extra DB query.

**Option B (Update):**
  Write new value to cache after DB update.
  Problem: DB update succeeds but cache write fails → inconsistency.

Invalidation is simpler and safer. The TTL is the ultimate safety net.

### Cache Stampede

If 100 requests hit a cold cache simultaneously, all 100 miss and query the DB. This is a "cache stampede" or "thundering herd."

For Phase 4, we accept this risk (it's rare for orders). Solutions for Phase 6+:
- **Lock-based**: Only one request queries DB, others wait
- **Probabilistic early refresh**: Refresh cache before TTL expires
- **Request coalescing**: Batch concurrent misses into one DB query

---

## Deep Dive: Leader Election

### The Problem

Phase 3 runs the outbox relay in every API instance. With 3 instances, we get 3 relays polling the same outbox table and publishing the same events to Kafka. Consumer idempotency handles the duplicates, but it's wasteful.

### The Fix: Lease-Based Leadership

```
Every 5 seconds, each instance:

  SET leader:outbox-relay {instanceId} NX PX 15000
  ├── NX: only if key doesn't exist
  └── PX: auto-expire after 15 seconds

  Result:
    "OK"  → I am the leader. Start/continue relay. Renew lease.
    null  → Someone else is leader. Sleep.

Leader heartbeat (renew lease):
  Lua: if GET(key) == myId then PSETEX(key, 15000, myId)
  Result:
    "OK"  → Still leader. Continue.
    null  → Lost leadership (lease expired). Stop relay.
```

### Failure Modes

| Scenario | What Happens | Impact |
|---|---|---|
| Leader crashes | Lease expires in ≤15s → follower takes over | 15s gap: events accumulate in outbox, published on takeover |
| Network partition | Leader can't renew → loses leadership | Brief period where two relays might run (idempotency handles duplicates) |
| Redis crashes | No one can acquire lease → no relay runs | Events accumulate safely, published when Redis returns |
| Split brain | Two leaders briefly (near lease expiry) | Consumer idempotency handles any duplicates |

### Why Not ZooKeeper/etcd/Consul?

These provide **stronger** leader election (consensus-based) but are complex to operate. For our use case (outbox relay where duplicates are safe), Redis is sufficient. The worst case (brief duplicate publishing) is handled by consumer idempotency.

---

## Deep Dive: Order Assignment

### The Coordination Problem

When an order is READY_FOR_PICKUP, multiple drivers see it simultaneously. Only ONE should be assigned.

```
Driver A: POST /orders/123/assign { driverId: "driver-1" }
Driver B: POST /orders/123/assign { driverId: "driver-2" }
```

### The Fix: Redis SET NX

```
SET assignment:order:123 driver-1 NX PX 300000
├── NX:  only if key doesn't exist
├── PX:  auto-expire after 5 minutes (pickup window)
└── Result:
      "OK"  → Driver 1 wins! → 201 Created
      null  → Already assigned → 409 Conflict
```

One atomic command. No locks. No transactions. No race conditions.

### Auto-Expiring Assignments

If the driver doesn't pick up the order within 5 minutes, the Redis key expires and the order becomes available again. This prevents "ghost assignments" where a driver accepts but never shows up.

---

## Experiments to Try

### 1. Test Rate Limiting
```bash
# Send 35 requests in rapid succession
for i in $(seq 1 35); do
  echo "Request $i:"
  curl -s -o /dev/null -w "HTTP %{http_code}" \
    -X POST http://localhost:3000/api/v1/orders \
    -H "Content-Type: application/json" \
    -d '{"customerId":"rate-limit-test","customerName":"Test","customerEmail":"test@test.com","restaurantId":"r1","restaurantName":"R","deliveryAddress":"123 Main","items":[{"itemName":"Burger","quantity":1,"unitPrice":10}]}'
  echo ""
done
# Requests 1-30: HTTP 201 (or 409 for duplicates)
# Requests 31-35: HTTP 429 Too Many Requests
```

### 2. Test Distributed Lock (Concurrent Updates)
```bash
# Create an order first
ORDER_ID=$(curl -s -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: lock-test-001" \
  -d '{"customerId":"c1","customerName":"Lock Test","customerEmail":"t@t.com","restaurantId":"r1","restaurantName":"R","deliveryAddress":"123","items":[{"itemName":"B","quantity":1,"unitPrice":10}]}' | jq -r '.data.id')

# Send two concurrent status updates
curl -X PATCH "http://localhost:3000/api/v1/orders/$ORDER_ID/status" \
  -H "Content-Type: application/json" \
  -d '{"status":"CONFIRMED"}' &
curl -X PATCH "http://localhost:3000/api/v1/orders/$ORDER_ID/status" \
  -H "Content-Type: application/json" \
  -d '{"status":"CONFIRMED"}' &
wait
# One succeeds (200), one gets 409 Conflict
```

### 3. Test Cache (Observe Cache Hits)
```bash
# First request — cache MISS (queries DB)
curl -s http://localhost:3000/api/v1/orders/$ORDER_ID | jq .

# Check Redis — key should exist
docker compose exec redis redis-cli GET "op:cache:order:$ORDER_ID"

# Second request — cache HIT (no DB query)
curl -s http://localhost:3000/api/v1/orders/$ORDER_ID | jq .

# Check app logs — first request shows DB query, second doesn't
docker compose logs app | grep "Cache"
```

### 4. Test Leader Election
```bash
# Check which instance is leader
docker compose logs app | grep "LEADER"

# Check Redis
docker compose exec redis redis-cli GET "op:leader:outbox-relay"
```

### 5. Test Order Assignment (Driver Race)
```bash
# Assign two drivers simultaneously
curl -X POST "http://localhost:3000/api/v1/orders/$ORDER_ID/assign" \
  -H "Content-Type: application/json" \
  -d '{"driverId":"driver-001"}' &
curl -X POST "http://localhost:3000/api/v1/orders/$ORDER_ID/assign" \
  -H "Content-Type: application/json" \
  -d '{"driverId":"driver-002"}' &
wait
# One gets 201 (assigned!), other gets 409 (already assigned)

# Check current assignment
curl -s http://localhost:3000/api/v1/orders/$ORDER_ID/assignment | jq .

# Check Redis
docker compose exec redis redis-cli GET "op:assignment:order:$ORDER_ID"
```

### 6. Test Graceful Degradation (Kill Redis)
```bash
# Stop Redis
docker compose stop redis

# API still works! Just without coordination features
curl -s http://localhost:3000/health | jq .
# status: "degraded" (not "unhealthy")

# Orders still work (no caching, no rate limiting, no locks)
curl -s http://localhost:3000/api/v1/orders | jq .

# Start Redis back up
docker compose start redis
# Features automatically recover
```

---

## Key Production Patterns Introduced

| Pattern | Problem It Solves | Redis Command | Location |
|---|---|---|---|
| **Fixed Window Rate Limit** | API abuse | `MULTI/INCR/PEXPIRE/EXEC` | `src/middleware/rate-limiter.ts` |
| **Distributed Lock** | Concurrent update races | `SET NX PX` + Lua release | `src/redis/distributed-lock.ts` |
| **Cache-Aside** | DB overload on hot paths | `GET` / `SETEX` / `DEL` | `src/redis/cache.ts` |
| **Lease-Based Leader Election** | Duplicate outbox relays | `SET NX PX` + Lua renew | `src/redis/leader-election.ts` |
| **Atomic Assignment** | Double driver assignment | `SET NX PX` | `src/modules/orders/order-assignment.service.ts` |
| **Fail-Open Design** | Redis outage resilience | try/catch fallthrough | All Redis code |
| **Graceful Degradation** | Partial infrastructure failure | Three-state health check | `src/shared/health.ts` |
