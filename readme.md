# Real-Time Distributed Order Processing Platform

> **Learning Path**: A hands-on distributed systems course built by evolving a production-grade system from a monolith to a fully distributed, event-driven architecture.

---

## Phase Roadmap

| Phase | Architecture | Key Concepts |
|---|---|---|
| **1 — Monolith** ✅ | Fastify + PostgreSQL | Request lifecycle, transactions, idempotency, graceful shutdown |
| **2 — Event-Driven** ✅ | + Kafka | Producers, consumers, topics, partitions, async processing |
| **3 — Distributed Processing** ✅ | + Consumer groups | Transactional outbox, idempotency, DLQs, retry, consumer scaling |
| **4 — Coordination** ✅ | + Redis | Rate limiting, distributed locks, caching, leader election, order assignment |
| **5 — Realtime** | + SSE | Streaming, backpressure, connection management |
| **6 — Observability** | + Prometheus/Grafana | Metrics, tracing, structured logging |
| **7 — Production** | + Kubernetes | Autoscaling, rolling deploys, resilience patterns |

---

## Phase 1 — The Monolith

### What You'll Learn in Phase 1

- Synchronous request lifecycle (HTTP → Service → Repository → DB → Response)
- PostgreSQL transactions and ACID guarantees
- Connection pooling (why it matters, how to size it)
- Idempotency keys (preventing duplicate orders from retries)
- Order state machines (valid vs invalid transitions)
- Graceful shutdown (why `npm start` → `node dist/index.js` matters)
- Health checks (liveness vs readiness probes)
- Structured logging with Pino
- **The monolith's breaking points** — the motivation for Phase 2

### Architecture (Phase 1)

```
Client → Fastify (HTTP) → OrderService → OrderRepository → PostgreSQL
                       ↘ NotificationService (SYNCHRONOUS — the bottleneck!)
```

> **Critical insight**: The notification service runs synchronously inside the order creation request. 500ms notification delay = 500ms slower order creation. This is the exact pain point that motivates Kafka in Phase 2.

---

## Phase 2 — Event-Driven Architecture with Kafka

### What You'll Learn in Phase 2

- **Kafka Brokers, Topics, and Partitions**: Core storage mechanics of a commit log.
- **Kafka Producers**: Message routing, serialization, keys, and acknowledgment (`acks`) strategies.
- **Kafka Consumers**: Polling loop, offset management, and offset committing.
- **Asynchronous Decoupling**: Offloading blocking non-core work (like email notifications) to background workers.
- **Event Schema Design**: Building self-contained event envelopes that scale with schema changes.
- **Eventual Consistency**: The order exists before the notification is sent.

### Architecture (Phase 2)

```
┌────────────────────────────────┐
│      ORDER API (Fastify)       │
│                                │
│ POST /orders ────────────┐     │
│                          │     │
│   DB: Insert order       │     │
│   Kafka: Publish event ──┼─────┼───────┐
│   Respond 201 (~20ms)    │     │       │
└──────────────────────────┘     │       │ order-events topic
                                 │       ▼
                                 │ ┌───────────┐
                                 │ │   KAFKA   │
                                 │ └─────┬─────┘
                                 │       │ (async poll)
┌────────────────────────────────┐       ▼
│     NOTIFICATION CONSUMER      │◀──────┘
│                                │
│   Consume ORDER_CREATED        │
│   Call NotificationService     │
│   Send Email (500ms delay)     │
│   DB: Update status to SENT    │
│   Commit Offset                │
└────────────────────────────────┘
```

---

## Phase 3 — Reliable Distributed Processing

### What You'll Learn in Phase 3

- **Transactional Outbox Pattern**: Eliminate the dual-write problem — order + event in one DB transaction.
- **Polling Publisher (Outbox Relay)**: Background process that bridges PostgreSQL → Kafka.
- **Idempotent Consumers**: Prevent duplicate processing with a `processed_events` table.
- **Exponential Backoff with Jitter**: Retry transient failures without thundering herd.
- **Dead Letter Queue (DLQ)**: Where permanently failing events go for investigation.
- **Consumer Groups & Rebalancing**: How Kafka distributes partitions across multiple consumers.

### Architecture (Phase 3)

```
┌────────────────────────────────────────────────────────────────────┐
│                   ORDER API (Fastify + Outbox Relay)                │
│                                                                    │
│  POST /orders ─────────────────────────────┐                       │
│                                            │                       │
│    BEGIN TRANSACTION                       │                       │
│      INSERT INTO orders (...)              │                       │
│      INSERT INTO outbox_events (...)       │ ← ATOMIC!            │
│    COMMIT                                  │                       │
│    Respond 201 (~20ms)                     │                       │
│                                            │                       │
│  ┌─────────────────────────────────────────▼──────────────────┐    │
│  │  Outbox Relay (polls every 1s)                             │    │
│  │  SELECT outbox WHERE published_at IS NULL                  │    │
│  │  → kafka.produce(event)                                    │    │
│  │  → UPDATE published_at = NOW()                             │    │
│  └─────────────────────────┬──────────────────────────────────┘    │
└────────────────────────────┼───────────────────────────────────────┘
                             │ produce
                             ▼
┌────────────────────────────────────────────────────────────────────┐
│                         KAFKA                                      │
│  order-events (3 partitions)     order-events-dlq (1 partition)   │
└──────────────┬─────────────────────────────────────────────────────┘
               │ consume (consumer group: notification-service)
               ▼
┌────────────────────────────────────────────────────────────────────┐
│  CONSUMER GROUP (3 instances → 1 partition each)                   │
│                                                                    │
│  For each event:                                                   │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ 1. Idempotency check (processed_events table)                │  │
│  │    → Already processed? SKIP                                 │  │
│  │ 2. Process with retry (3 attempts, exponential backoff)      │  │
│  │    → Success? Mark processed + commit offset                 │  │
│  │    → All fail? Publish to DLQ + commit offset                │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

### Quick Start

```bash
# 1. Setup environment variables
cp .env.example .env

# 2. Start services (includes 3 consumer replicas!)
docker compose up --build -d

# 3. Verify health
curl http://localhost:3000/health

# 4. Check consumer group — all 3 consumers with partition assignments
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group notification-service

# 5. Create an order and watch all 3 consumer logs
docker compose logs -f notification-consumer-1 notification-consumer-2 notification-consumer-3

time curl -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: phase3-test-001" \
  -d '{
    "customerId": "cust-001",
    "customerName": "Alice Johnson",
    "customerEmail": "alice@example.com",
    "customerPhone": "+15551234567",
    "restaurantId": "rest-001",
    "restaurantName": "The Burger Joint",
    "deliveryAddress": "123 Main Street, San Francisco, CA",
    "deliveryLat": 37.7749,
    "deliveryLng": -122.4194,
    "items": [
      { "itemName": "Classic Burger", "quantity": 2, "unitPrice": 12.99 },
      { "itemName": "Fries", "quantity": 2, "unitPrice": 4.99 }
    ]
  }'

# 6. Verify outbox was relayed
docker compose exec postgres psql -U orderplatform -d order_platform \
  -c "SELECT event_type, published_at FROM outbox_events ORDER BY created_at DESC LIMIT 5;"

# 7. Verify idempotency tracking
docker compose exec postgres psql -U orderplatform -d order_platform \
  -c "SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 5;"

# 8. Open Kafka UI — inspect topics, consumer group, DLQ
open http://localhost:8080
```

### API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/orders` | Create order (supports `Idempotency-Key` header) |
| `GET` | `/api/v1/orders` | List orders (`?page=1&limit=20&status=PENDING&customerId=x`) |
| `GET` | `/api/v1/orders/:id` | Get order by ID (cached via Redis — Phase 4) |
| `PATCH` | `/api/v1/orders/:id/status` | Update status (distributed lock — Phase 4) |
| `POST` | `/api/v1/orders/:id/cancel` | Cancel order (invalidates cache) |
| `GET` | `/api/v1/orders/:id/history` | Get status change history |
| `POST` | `/api/v1/orders/:id/assign` | Assign driver to order (Phase 4) |
| `GET` | `/api/v1/orders/:id/assignment` | Get current driver assignment (Phase 4) |
| `GET` | `/health` | Health check (DB + Redis status) |

### Stop the System

```bash
# Graceful shutdown (sends SIGTERM, drains connections/offsets)
docker compose stop

# Full cleanup (removes containers, networks, and volumes)
docker compose down -v
```

---

## Project Structure

```
order-platform/
├── src/
│   ├── index.ts                    # Entry point (startup + Redis + leader election)
│   ├── server.ts                   # Fastify bootstrap, plugins, rate limiter
│   ├── config/index.ts             # Typed config with Zod validation
│   ├── db/pool.ts                  # PostgreSQL connection pool
│   ├── redis/                      # Phase 4 — Redis Coordination Layer
│   │   ├── client.ts               # Redis client factory (singleton)
│   │   ├── cache.ts                # Cache-aside service (get/set/invalidate)
│   │   ├── distributed-lock.ts     # Lock/unlock with Lua scripts
│   │   └── leader-election.ts      # Lease-based leadership for outbox relay
│   ├── middleware/                  # Phase 4 — Request Middleware
│   │   └── rate-limiter.ts         # Fixed window counter (Redis INCR)
│   ├── outbox/                     # Phase 3 — Transactional Outbox
│   │   ├── outbox.repository.ts    # Insert/query outbox events
│   │   └── outbox-relay.ts         # Background poller → Kafka publisher
│   ├── kafka/                      # Kafka Infrastructure Layer
│   │   ├── client.ts               # KafkaJS client wrapper
│   │   ├── producer.ts             # Event producer (used by outbox relay)
│   │   ├── consumer.ts             # Reliable consumer (retry + idempotency + DLQ)
│   │   ├── events.ts               # Typed event envelopes & builders
│   │   ├── idempotency.ts          # Phase 3 — processed_events table guard
│   │   ├── retry.ts                # Phase 3 — Exponential backoff + jitter
│   │   └── dlq-producer.ts         # Phase 3 — Dead Letter Queue publisher
│   ├── consumers/                  # Background Consumer Services
│   │   ├── index.ts                # Consumer process entry point
│   │   └── notification-consumer.ts# Business handler for order notifications
│   ├── shared/
│   │   ├── logger.ts               # Pino structured logger
│   │   ├── errors.ts               # Custom error classes
│   │   ├── health.ts               # /health endpoint (DB + Redis)
│   │   └── shutdown.ts             # Graceful shutdown (+ Redis + leader election)
│   ├── modules/
│   │   ├── orders/
│   │   │   ├── order.types.ts      # Domain types + state machine
│   │   │   ├── order.schemas.ts    # Zod validation schemas
│   │   │   ├── order.repository.ts # Data access (with outbox callback)
│   │   │   ├── order.service.ts    # Business logic (outbox pattern)
│   │   │   ├── order.routes.ts     # HTTP handlers (cache + lock + assign)
│   │   │   └── order-assignment.service.ts  # Phase 4 — Driver assignment (Redis NX)
│   │   └── notifications/
│   │       ├── notification.types.ts
│   │       ├── notification.repository.ts
│   │       └── notification.service.ts
│   └── types/index.ts              # Shared TypeScript types
├── scripts/
│   ├── init-db.sql                 # PostgreSQL schema (+ outbox + processed_events)
│   └── load-test.sh                # Load test script
├── docs/
│   ├── phase-1-architecture.md     # Phase 1 design notes
│   ├── phase-2-architecture.md     # Phase 2 Kafka Architecture Decision Record
│   ├── phase-3-architecture.md     # Phase 3 Reliability Architecture Decision Record
│   └── phase-4-architecture.md     # Phase 4 Redis Coordination Decision Record
├── docker-compose.yml              # 9 services (+ Redis)
├── Dockerfile
└── .env.example
```

---

## Key Production Patterns Introduced

| Pattern | Phase | Location | Why It Matters |
|---|---|---|---|
| **Idempotency keys** | Phase 1 | `order.service.ts` | Prevents duplicate orders from retries |
| **State machine** | Phase 1 | `order.types.ts` | Prevents invalid status transitions |
| **Optimistic locking** | Phase 1 | `order.repository.ts` | Handles concurrent status updates safely |
| **Graceful shutdown** | Phase 1 | `shared/shutdown.ts` | Clean deploys without dropped requests |
| **Decoupled architecture**| Phase 2 | `notification-consumer.ts` | Isolates core database transactions from slow integrations |
| **Event schema versioning**| Phase 2 | `kafka/events.ts` | Safely evolve payload structures over time |
| **Message partition keying**| Phase 2 | `kafka/producer.ts` | Guarantees ordered message processing per entity |
| **Transactional outbox** | Phase 3 | `outbox/` | Eliminates dual-write data loss between DB and Kafka |
| **Outbox relay** | Phase 3 | `outbox/outbox-relay.ts` | Bridges PostgreSQL → Kafka asynchronously |
| **Idempotent consumer** | Phase 3 | `kafka/idempotency.ts` | Prevents duplicate processing on re-delivery |
| **Exponential backoff** | Phase 3 | `kafka/retry.ts` | Retries transient failures without thundering herd |
| **Dead Letter Queue** | Phase 3 | `kafka/dlq-producer.ts` | Captures permanently failing events for investigation |
| **Consumer groups** | Phase 3 | `docker-compose.yml` | Horizontal scaling of event processing |
| **Partial index** | Phase 3 | `init-db.sql` | Fast outbox polling regardless of table size |
| **Rate limiting** | Phase 4 | `middleware/rate-limiter.ts` | Protects API from abuse across all instances |
| **Distributed lock** | Phase 4 | `redis/distributed-lock.ts` | Prevents concurrent status update races |
| **Cache-aside** | Phase 4 | `redis/cache.ts` | Reduces DB load for hot-path queries (10-50x faster) |
| **Leader election** | Phase 4 | `redis/leader-election.ts` | Only one instance runs the outbox relay |
| **Atomic assignment** | Phase 4 | `order-assignment.service.ts` | Prevents double driver assignment |
| **Fail-open design** | Phase 4 | All Redis code | Redis outage doesn't break the API |
| **Graceful degradation** | Phase 4 | `shared/health.ts` | Three-state health: healthy/degraded/unhealthy |