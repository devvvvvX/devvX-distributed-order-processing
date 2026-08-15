# Real-Time Distributed Order Processing Platform

> **Learning Path**: A hands-on distributed systems course built by evolving a production-grade system from a monolith to a fully distributed, event-driven architecture.

---

## Phase Roadmap

| Phase | Architecture | Key Concepts |
|---|---|---|
| **1 — Monolith** ✅ | Fastify + PostgreSQL | Request lifecycle, transactions, idempotency, graceful shutdown |
| **2 — Event-Driven** ✅ | + Kafka | Producers, consumers, topics, partitions, async processing |
| **3 — Distributed Processing** | + Consumer groups | At-least-once delivery, idempotency, DLQs, event replay |
| **4 — Coordination** | + Redis | Distributed locks, leader election, race conditions |
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

### Quick Start

```bash
# 1. Setup environment variables
cp .env.example .env

# 2. Start services (includes Kafka, Postgres, App, Consumer, Kafka UI)
docker compose up --build -d

# 3. Verify health of the Order API
curl http://localhost:3000/health

# 4. Stream consumer logs in another terminal to watch events process in real-time
docker compose logs -f notification-consumer

# 5. Create an order and watch the logs (notice the ultra-fast ~20ms response time!)
time curl -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: phase2-test-001" \
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

# 6. Open Kafka UI to inspect topics, offsets, and consumer groups
open http://localhost:8080
```

### API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/orders` | Create order (supports `Idempotency-Key` header) |
| `GET` | `/api/v1/orders` | List orders (`?page=1&limit=20&status=PENDING&customerId=x`) |
| `GET` | `/api/v1/orders/:id` | Get order by ID |
| `PATCH` | `/api/v1/orders/:id/status` | Update order status (publishes event) |
| `POST` | `/api/v1/orders/:id/cancel` | Cancel order (publishes event) |
| `GET` | `/api/v1/orders/:id/history` | Get status change history |
| `GET` | `/health` | Health check + DB pool metrics |

### Feel the Decoupled Speed

```bash
# Make the script executable
chmod +x scripts/load-test.sh

# Run 20 concurrent requests — see average latency stay low (~20ms per order)!
# The database connection pool is no longer blocked waiting for external notification calls.
./scripts/load-test.sh 20 concurrent
```

### Inspect the Database

```bash
# Connect to PostgreSQL
docker compose exec postgres psql -U orderplatform -d order_platform

# Check the notification status — they should eventually update from PENDING to SENT!
SELECT order_id, type, status, retry_count, sent_at FROM notifications;
```

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
│   ├── index.ts                    # Entry point (startup sequence)
│   ├── server.ts                   # Fastify bootstrap, plugins, error handler
│   ├── config/index.ts             # Typed config with Zod validation
│   ├── db/pool.ts                  # PostgreSQL connection pool
│   ├── kafka/                      # NEW — Kafka Infrastructure Layer
│   │   ├── client.ts               # KafkaJS client wrapper
│   │   ├── producer.ts             # Event producer logic
│   │   ├── consumer.ts             # Base Kafka consumer runner
│   │   └── events.ts               # Typed event envelopes & builders
│   ├── consumers/                  # NEW — Background Consumer Services
│   │   ├── index.ts                # Consumer process entry point
│   │   └── notification-consumer.ts# Business handler for order notifications
│   ├── shared/
│   │   ├── logger.ts               # Pino structured logger
│   │   ├── errors.ts               # Custom error classes
│   │   ├── health.ts               # /health endpoint
│   │   └── shutdown.ts             # Graceful shutdown handler
│   ├── modules/
│   │   ├── orders/
│   │   │   ├── order.types.ts      # Domain types + state machine
│   │   │   ├── order.schemas.ts    # Zod validation schemas
│   │   │   ├── order.repository.ts # Data access layer (SQL)
│   │   │   ├── order.service.ts    # Business logic (modified to produce events)
│   │   │   └── order.routes.ts     # HTTP handlers
│   │   └── notifications/
│   │       ├── notification.types.ts
│   │       ├── notification.repository.ts
│   │       └── notification.service.ts  # Triggered asynchronously by consumer
│   └── types/index.ts              # Shared TypeScript types
├── scripts/
│   ├── init-db.sql                 # PostgreSQL schema (DDL)
│   └── load-test.sh                # Load test script
├── docs/
│   ├── phase-1-architecture.md     # Phase 1 design notes
│   └── phase-2-architecture.md     # Phase 2 Kafka Architecture Decision Record
├── docker-compose.yml
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
| **At-least-once processing**| Phase 2 | `kafka/consumer.ts` | Assures processing safety via manual offset committing |