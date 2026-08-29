# Phase 2 — Event-Driven Architecture with Kafka

## Architecture Decision Record

**Date:** 2026-05-24
**Status:** Implemented
**Context:** Phase 1's synchronous notification bottleneck

---

## Problem Statement

In Phase 1, order creation took **~520ms** because the notification service was called synchronously:

```
POST /orders → DB write (20ms) → Notification send (500ms) → Response (520ms)
```

The customer waited 500ms for an email they wouldn't read for 10 minutes. Worse, if the email service was down, order creation could fail — even though the order was already saved.

## Decision

Introduce Apache Kafka as an event bus. The Order API publishes events to Kafka (2ms), and a separate notification consumer processes them asynchronously.

```
POST /orders → DB write (20ms) → Kafka publish (2ms) → Response (22ms)
                                        ↓ (async)
                              Notification Consumer → Email (500ms)
```

## Architecture — Phase 2

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                          │
│                                                             │
│  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌──────────────┐  │
│  │PostgreSQL│  │  Kafka   │  │Kafka UI │  │  Kafka Init  │  │
│  │ :5432    │  │ :9092    │  │ :8080   │  │  (one-shot)  │  │
│  └────┬─────┘  └────┬─────┘  └─────────┘  └──────────────┘  │
│       │              │                                       │
│  ┌────▼──────────────▼────┐   ┌────────────────────────────┐│
│  │   Order API (:3000)    │   │  Notification Consumer     ││
│  │   • HTTP endpoints     │   │  • Kafka consumer          ││
│  │   • Kafka producer     │   │  • Notification service    ││
│  └────────────────────────┘   └────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Key Concepts Introduced

### 1. Kafka Topics and Partitions

Topic `order-events` has **3 partitions**, keyed by `orderId`:

```
Partition 0: [order-aaa events] [order-ddd events] ...
Partition 1: [order-bbb events] [order-eee events] ...
Partition 2: [order-ccc events] [order-fff events] ...
```

**Why 3 partitions?** Each partition can have at most one consumer in a consumer group. 3 partitions = up to 3 parallel consumers in Phase 3.

**Why key by orderId?** All events for the same order go to the same partition, guaranteeing per-order ordering. `ORDER_CREATED` is always processed before `ORDER_STATUS_CHANGED` for the same order.

### 2. Producers

The Order API produces events after DB operations:
- `ORDER_CREATED` — when a new order is inserted
- `ORDER_STATUS_CHANGED` — when order status is updated
- `ORDER_CANCELLED` — when an order is cancelled

Events are self-contained — they carry all data needed for notification (customer name, email, order details). The consumer doesn't need to query the DB.

### 3. Consumers

The notification consumer:
- Joins consumer group `notification-service`
- Subscribes to `order-events` topic
- Processes messages one at a time (eachMessage)
- Uses **manual offset commits** (commit AFTER processing)
- Handles poison pills (malformed messages logged and skipped)

### 4. Eventual Consistency

```
T+0ms:   POST /orders received
T+22ms:  Order saved + event published → 201 response
T+22ms:  Customer can GET /orders/:id → order exists ✅
T+22ms:  Customer checks email → not yet ❌
T+522ms: Consumer processes event → email sent ✅
```

The system is eventually consistent. There's a brief window where the order exists but the notification hasn't been sent.

### 5. Event Schema (Envelope Pattern)

Every event has a consistent envelope:

```json
{
  "eventId": "uuid",
  "eventType": "ORDER_CREATED",
  "aggregateId": "order-uuid",
  "timestamp": "2026-05-24T...",
  "version": 1,
  "source": "order-api",
  "payload": { ... }
}
```

## Failure Modes

| Failure | Impact | Mitigation |
|---|---|---|
| Kafka down | Orders created, events not published | Log error, order succeeds |
| Consumer down | Events queue in Kafka | Consumer catches up on restart |
| Consumer crash mid-processing | Event re-delivered (at-least-once) | Phase 3: idempotent processing |
| Poison pill (bad message) | Logged and skipped | Phase 3: Dead Letter Queue |
| Consumer falls behind (lag) | Delayed notifications | Phase 6: lag monitoring alerts |

## What Changed from Phase 1

| File | Change |
|---|---|
| `src/modules/orders/order.service.ts` | Replaced `NotificationService.send()` with `EventProducer.publishOrderCreated()` |
| `src/index.ts` | Added Kafka producer initialization |
| `src/server.ts` | Accepts and passes `EventProducer` to routes |
| `src/shared/shutdown.ts` | Added Kafka producer disconnect |
| `src/kafka/*` | New — client, producer, consumer, events |
| `src/consumers/*` | New — notification consumer process |
| `docker-compose.yml` | Added Kafka, kafka-init, kafka-ui, notification-consumer |

## What's Next (Phase 3 Preview)

Phase 2 has intentional gaps:

1. **No idempotency in consumers** — if a message is re-delivered, the notification is sent twice
2. **No Dead Letter Queue** — failed messages are just logged
3. **No retry logic** — one attempt, then skip
4. **No consumer group scaling** — single consumer instance
5. **Dual-write risk** — DB commit and Kafka publish are not atomic

Phase 3 will address all of these with:
- Consumer idempotency (deduplication)
- DLQ pattern
- Exponential backoff retries
- Consumer group with multiple instances
- Transactional Outbox pattern

---

## Running Phase 2

```bash
# Start all services
docker compose up --build -d

# Check services
docker compose ps

# Health check
curl http://localhost:3000/health

# Create an order — should respond in ~20ms (not ~520ms!)
time curl -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: phase2-001" \
  -d '{
    "customerId": "cust-001",
    "customerName": "Alice",
    "customerEmail": "alice@example.com",
    "restaurantId": "rest-001",
    "restaurantName": "Burger Joint",
    "deliveryAddress": "123 Main St",
    "items": [{"itemName": "Burger", "quantity": 1, "unitPrice": 12.99}]
  }'

# Watch consumer process the event
docker compose logs -f notification-consumer

# Inspect events in Kafka UI
open http://localhost:8080

# Inspect notifications in DB
docker compose exec postgres psql -U orderplatform -d order_platform \
  -c "SELECT id, order_id, type, status FROM notifications;"
```

### Experiments

1. **Stop the consumer → create orders → restart consumer:**
   ```bash
   docker compose stop notification-consumer
   # Create several orders...
   docker compose start notification-consumer
   # Watch it process the backlog!
   ```

2. **Compare latency with Phase 1:**
   ```bash
   # Phase 2 — event-driven (should be ~20ms)
   time curl -s -X POST http://localhost:3000/api/v1/orders ...

   # Feel the difference from Phase 1's ~520ms!
   ```
