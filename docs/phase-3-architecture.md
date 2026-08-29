# Phase 3 — Reliable Distributed Processing

> **Architecture Decision Record**: Why and how we made the event pipeline production-reliable.

---

## The Problem Statement

Phase 2 introduced Kafka for asynchronous event processing, reducing order creation latency from ~520ms to ~20ms. But it left **four critical failure modes** unhandled:

| # | Failure Mode | Phase 2 Behavior | Impact |
|---|---|---|---|
| 1 | Notification service fails | Event logged and skipped | Customer never gets email. Silent data loss. |
| 2 | Consumer crashes mid-processing | Event re-delivered by Kafka | Customer gets duplicate email. |
| 3 | Kafka down during event publish | Event silently dropped | Order exists but no notification event published. |
| 4 | Traffic spike overwhelms consumer | Single consumer bottlenecked | Consumer lag grows unbounded. Hours of notification delay. |

**Phase 3 fixes all four.**

---

## Solution Overview

```
┌────────────────────── WRITE PATH ──────────────────────┐
│                                                         │
│  OrderService.createOrder()                             │
│    BEGIN TRANSACTION                                    │
│      INSERT INTO orders (...)                           │
│      INSERT INTO outbox_events (...)  ← ATOMIC!        │
│    COMMIT                                               │
│                                                         │
│  OutboxRelay (background, every 1s)                     │
│    SELECT * FROM outbox_events WHERE published_at IS NULL│
│    FOR EACH: kafka.produce() → UPDATE published_at      │
│                                                         │
└─────────────────────────┬───────────────────────────────┘
                          │ Kafka
                          ▼
┌────────────────────── READ PATH ───────────────────────┐
│                                                         │
│  Consumer Group (3 instances, 3 partitions)             │
│                                                         │
│  For each event:                                        │
│    1. IDEMPOTENCY CHECK: processed_events table         │
│       → Already processed? SKIP                         │
│    2. PROCESS with retry (exponential backoff + jitter) │
│       → Success? Mark processed, commit offset          │
│       → All retries fail? Send to DLQ, commit offset    │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## Deep Dive: Transactional Outbox Pattern

### The Dual-Write Problem

In Phase 2, the OrderService did two writes to two different systems:

```
1. pool.query('INSERT INTO orders...')   → PostgreSQL  ✅
2. producer.send({ topic: 'order-events'... })  → Kafka  ❌ (can fail!)
```

If step 1 succeeds but step 2 fails (Kafka is down, network partition, timeout), the order exists but the event is lost. The customer placed an order but never gets a notification.

This is the **dual-write problem**: writing to two different systems without a distributed transaction.

### The Fix: Write to One System, Relay to the Other

```
1. BEGIN
     INSERT INTO orders (...)
     INSERT INTO outbox_events (...)     ← Same DB, same transaction!
   COMMIT                                ← ATOMIC: both or neither

2. OutboxRelay (background):
     Poll outbox_events WHERE published_at IS NULL
     Produce to Kafka
     Mark as published
```

By writing the event to the **same database** as the business data, we get atomicity for free (PostgreSQL ACID transactions). The OutboxRelay bridges the gap to Kafka asynchronously.

### Outbox Table Schema

```sql
CREATE TABLE outbox_events (
  id            UUID PRIMARY KEY,
  aggregate_id  VARCHAR(255) NOT NULL,  -- orderId
  event_type    VARCHAR(100) NOT NULL,  -- ORDER_CREATED, etc.
  topic         VARCHAR(255) NOT NULL,  -- Kafka topic name
  key           VARCHAR(255),           -- Partition key
  payload       JSONB NOT NULL,         -- Full event envelope
  headers       JSONB DEFAULT '{}',     -- Kafka headers
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at  TIMESTAMPTZ            -- NULL until relayed
);

-- Partial index: only indexes unpublished rows
CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at ASC)
  WHERE published_at IS NULL;
```

### Relay Failure Analysis

| Failure | Impact | Mitigation |
|---|---|---|
| Relay crashes after Kafka produce, before marking published | Event re-published next poll (duplicate in Kafka) | Consumer idempotency handles the duplicate |
| Relay crashes before Kafka produce | Event stays unpublished, retried next poll | No data loss |
| Kafka is down | Events accumulate in outbox table | Published when Kafka recovers |
| Relay falls behind (too many events) | Increase batch size or poll frequency | Or switch to CDC (Debezium) |

---

## Deep Dive: Idempotent Consumers

### Why Duplicates Happen

Kafka guarantees **at-least-once delivery**, meaning every event is delivered at least once, but potentially more. Duplicates happen in three scenarios:

1. **Consumer crash after processing, before offset commit**: The event was processed (email sent), but the offset wasn't committed. On restart, Kafka re-delivers from the last committed offset.

2. **Outbox relay re-publish**: The relay published to Kafka but crashed before marking the outbox row as published. Next poll re-publishes the same event.

3. **Consumer group rebalance**: During rebalance, a partition moves from Consumer A to Consumer B. If Consumer A was mid-processing, Consumer B starts from the last committed offset, re-processing the event.

### The Fix: processed_events Table

```sql
CREATE TABLE processed_events (
  event_id        VARCHAR(255) NOT NULL,
  consumer_group  VARCHAR(100) NOT NULL,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_id, consumer_group)
);
```

Before processing, check: `SELECT 1 FROM processed_events WHERE event_id = ? AND consumer_group = ?`
- Found → SKIP (already processed)
- Not found → process, then INSERT into processed_events

The INSERT happens in the **same DB transaction** as the business logic (creating the notification row). This guarantees:
- If business logic commits → event is marked as processed
- If business logic rolls back → event is NOT marked, will be retried

---

## Deep Dive: Retry with Exponential Backoff + Jitter

### Why Not Retry Immediately?

If the email service goes down and 100 consumers all retry immediately, they overwhelm the recovering service → it goes down again → infinite loop (cascading failure).

### Exponential Backoff

```
Attempt 0: wait 0-1000ms    (avg 500ms)
Attempt 1: wait 0-2000ms    (avg 1000ms)
Attempt 2: wait 0-4000ms    (avg 2000ms)
Attempt 3: wait 0-8000ms    (avg 4000ms)
```

Each retry doubles the maximum wait time, spreading load over a wider window.

### Full Jitter

Without jitter, all consumers compute the same backoff and retry simultaneously. Full jitter adds randomness:

```
delay = random(0, baseDelay × 2^attempt)
```

This spreads retries uniformly across the backoff window, preventing thundering herd.

### When to Give Up

After `CONSUMER_MAX_RETRIES` (default 3) failures, the event goes to the Dead Letter Queue. Some errors will never succeed no matter how many times you retry:
- Malformed event (bad JSON)
- Business logic violation (order doesn't exist)
- Schema incompatibility

---

## Deep Dive: Dead Letter Queue (DLQ)

### What Goes in the DLQ

Events that exhaust all retry attempts. The DLQ message preserves:
- Original event payload (unchanged)
- Error metadata in Kafka headers:
  - `dlq-original-topic`, `dlq-original-partition`, `dlq-original-offset`
  - `dlq-error-message`
  - `dlq-retry-count`
  - `dlq-failed-at`
  - `dlq-consumer-group`

### DLQ Operations

| Operation | How | When |
|---|---|---|
| **Inspect** | Kafka UI → `order-events-dlq` topic | Any time DLQ has messages |
| **Replay** | Publish original event back to `order-events` | After fixing root cause |
| **Purge** | Delete from DLQ topic | After investigation complete |
| **Alert** | Monitor DLQ consumer lag > 0 | Phase 6 (Observability) |

---

## Deep Dive: Consumer Groups

### How Partition Assignment Works

```
Topic: order-events (3 partitions)
Consumer Group: notification-service (3 consumers)

Assignment:
  Consumer-1 → Partition 0
  Consumer-2 → Partition 1
  Consumer-3 → Partition 2
```

Each partition is assigned to exactly ONE consumer in the group. This means:
- **Max parallelism** = min(consumers, partitions) = 3
- Events with the same key (orderId) always go to the same partition → same consumer → **per-order ordering preserved**

### Rebalancing

When a consumer joins or leaves the group, Kafka **rebalances**:

```
Consumer-2 crashes:
  Consumer-1 → Partition 0, Partition 1  (picks up orphan)
  Consumer-3 → Partition 2

Consumer-2 recovers:
  Consumer-1 → Partition 0
  Consumer-2 → Partition 1  (gets partition back)
  Consumer-3 → Partition 2
```

During rebalance:
- ALL consumers in the group **stop processing** briefly
- Uncommitted offsets are lost (events may be re-delivered)
- This is why idempotency is essential

### Experiment: Observe Rebalancing

```bash
# Watch consumer group assignments in real-time
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group notification-service

# Kill one consumer
docker compose stop notification-consumer-2

# Check assignments again — partition 1 moved to consumer 1 or 3
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group notification-service

# Bring it back
docker compose start notification-consumer-2

# Check again — partition 1 returned to consumer 2
```

---

## Experiments to Try

### 1. Test DLQ (Force Failures)
```bash
# Set 100% notification failure rate
docker compose stop notification-consumer-1 notification-consumer-2 notification-consumer-3

# Edit docker-compose.yml: NOTIFICATION_FAILURE_RATE: 1.0
# Restart consumers
docker compose up -d notification-consumer-1 notification-consumer-2 notification-consumer-3

# Create an order
curl -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: dlq-test-001" \
  -d '{"customerId":"cust-001","customerName":"DLQ Test","customerEmail":"test@test.com","restaurantId":"rest-001","restaurantName":"Test Restaurant","deliveryAddress":"123 Main St","items":[{"itemName":"Burger","quantity":1,"unitPrice":12.99}]}'

# Watch consumer logs — see retry attempts + DLQ
docker compose logs -f notification-consumer-1

# Check DLQ in Kafka UI
open http://localhost:8080
```

### 2. Test Outbox (Kafka Downtime)
```bash
# Stop Kafka
docker compose stop kafka

# Create an order (should still succeed — writes to outbox)
curl -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: outbox-test-001" \
  -d '{"customerId":"cust-001","customerName":"Outbox Test","customerEmail":"test@test.com","restaurantId":"rest-001","restaurantName":"Test Restaurant","deliveryAddress":"123 Main St","items":[{"itemName":"Burger","quantity":1,"unitPrice":12.99}]}'

# Check outbox table — event is there, published_at IS NULL
docker compose exec postgres psql -U orderplatform -d order_platform \
  -c "SELECT id, event_type, published_at FROM outbox_events;"

# Start Kafka back up
docker compose start kafka

# Wait a few seconds for relay to publish
# Check outbox again — published_at is now set
docker compose exec postgres psql -U orderplatform -d order_platform \
  -c "SELECT id, event_type, published_at FROM outbox_events;"
```

### 3. Test Idempotency (Duplicate Detection)
```bash
# Check notifications and processed events after creating orders
docker compose exec postgres psql -U orderplatform -d order_platform \
  -c "SELECT order_id, count(*) FROM notifications GROUP BY order_id HAVING count(*) > 1;"
# Should return 0 rows (no duplicates)

docker compose exec postgres psql -U orderplatform -d order_platform \
  -c "SELECT count(*) FROM processed_events;"
# Should match number of unique events processed
```

---

## Key Production Patterns Introduced

| Pattern | Problem It Solves | Location |
|---|---|---|
| **Transactional Outbox** | Dual-write data loss | `src/outbox/` |
| **Polling Publisher (Relay)** | Outbox → Kafka bridge | `src/outbox/outbox-relay.ts` |
| **Idempotent Consumer** | Duplicate event processing | `src/kafka/idempotency.ts` |
| **Exponential Backoff + Jitter** | Thundering herd on retry | `src/kafka/retry.ts` |
| **Dead Letter Queue** | Poison pills / permanent failures | `src/kafka/dlq-producer.ts` |
| **Consumer Groups** | Single consumer bottleneck | `docker-compose.yml` |
| **Partial Index** | Fast outbox polling at scale | `scripts/init-db.sql` |
