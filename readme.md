# Real-Time Distributed Order Processing Platform

> **Learning Path**: A hands-on distributed systems course built by evolving a production-grade system from a monolith to a fully distributed, event-driven architecture.

---

## Phase Roadmap

| Phase | Architecture | Key Concepts |
|---|---|---|
| **1 — Monolith** ✅ | Fastify + PostgreSQL | Request lifecycle, transactions, idempotency, graceful shutdown |
| **2 — Event-Driven** | + Kafka | Producers, consumers, topics, partitions, async processing |
| **3 — Distributed Processing** | + Consumer groups | At-least-once delivery, idempotency, DLQs, event replay |
| **4 — Coordination** | + Redis | Distributed locks, leader election, race conditions |
| **5 — Realtime** | + SSE | Streaming, backpressure, connection management |
| **6 — Observability** | + Prometheus/Grafana | Metrics, tracing, structured logging |
| **7 — Production** | + Kubernetes | Autoscaling, rolling deploys, resilience patterns |

---

## Phase 1 — The Monolith

### What You'll Learn

- Synchronous request lifecycle (HTTP → Service → Repository → DB → Response)
- PostgreSQL transactions and ACID guarantees
- Connection pooling (why it matters, how to size it)
- Idempotency keys (preventing duplicate orders from retries)
- Order state machines (valid vs invalid transitions)
- Graceful shutdown (why `npm start` → `node dist/index.js` matters)
- Health checks (liveness vs readiness probes)
- Structured logging with Pino
- **The monolith's breaking points** — the motivation for Phase 2

### Architecture

```
Client → Fastify (HTTP) → OrderService → OrderRepository → PostgreSQL
                       ↘ NotificationService (SYNCHRONOUS — the bottleneck!)
```

> **Critical insight**: The notification service runs synchronously inside the order creation request. 500ms notification delay = 500ms slower order creation. This is the exact pain point that motivates Kafka in Phase 2.

### Quick Start

```bash
# 1. Clone and setup
cp .env.example .env

# 2. Start services
docker compose up --build -d

# 3. Verify health
curl http://localhost:3000/health

# 4. Create your first order
curl -X POST http://localhost:3000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: my-first-order-001" \
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
      { "itemName": "Fries", "quantity": 2, "unitPrice": 4.99 },
      { "itemName": "Coke", "quantity": 2, "unitPrice": 2.99 }
    ]
  }'
```

### API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/orders` | Create order (supports `Idempotency-Key` header) |
| `GET` | `/api/v1/orders` | List orders (`?page=1&limit=20&status=PENDING&customerId=x`) |
| `GET` | `/api/v1/orders/:id` | Get order by ID |
| `PATCH` | `/api/v1/orders/:id/status` | Update order status |
| `POST` | `/api/v1/orders/:id/cancel` | Cancel order |
| `GET` | `/api/v1/orders/:id/history` | Get status change history |
| `GET` | `/health` | Health check + DB pool metrics |

### Feel the Bottleneck

```bash
# Make the script executable
chmod +x scripts/load-test.sh

# 1. Baseline (fast): NOTIFICATION_DELAY_MS=0 in docker-compose.yml
./scripts/load-test.sh 10

# 2. Feel the bottleneck: NOTIFICATION_DELAY_MS=2000
./scripts/load-test.sh 10

# 3. Concurrent load (pool exhaustion): 20 concurrent requests
./scripts/load-test.sh 20 concurrent
```

### Inspect the Database

```bash
# Connect to PostgreSQL
docker compose exec postgres psql -U orderplatform -d order_platform

# See all orders
SELECT id, customer_name, status, grand_total, created_at FROM orders;

# See status history (audit trail)
SELECT * FROM order_status_history WHERE order_id = '<your-order-id>';

# See notification records
SELECT id, type, channel, status, retry_count FROM notifications;

# See connection pool activity
SELECT pid, application_name, state, query FROM pg_stat_activity;
```

### Stop the System

```bash
# Graceful shutdown (watches logs to confirm drain)
docker compose stop

# Full cleanup (removes volumes/data)
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
│   │   │   ├── order.service.ts    # Business logic
│   │   │   └── order.routes.ts     # HTTP handlers
│   │   └── notifications/
│   │       ├── notification.types.ts
│   │       ├── notification.repository.ts
│   │       └── notification.service.ts  # ← The intentional bottleneck
│   └── types/index.ts              # Shared TypeScript types
├── scripts/
│   ├── init-db.sql                 # PostgreSQL schema (DDL)
│   └── load-test.sh                # Bottleneck demonstration
├── docs/
│   └── phase-1-architecture.md    # Architecture decision record
├── docker-compose.yml
├── Dockerfile
└── .env.example
```

---

## Key Production Patterns Introduced

| Pattern | Location | Why It Matters |
|---|---|---|
| **Idempotency keys** | `order.service.ts` | Prevents duplicate orders from retries |
| **State machine** | `order.types.ts` | Prevents invalid status transitions |
| **Optimistic locking** | `order.repository.ts` | Handles concurrent status updates safely |
| **Repository pattern** | `*/repository.ts` | Each module becomes a service in Phase 2+ |
| **Connection pool monitoring** | `db/pool.ts` | Detect pool exhaustion before it causes outages |
| **Fail-fast config** | `config/index.ts` | Missing env var = crash at startup, not runtime |
| **Graceful shutdown** | `shared/shutdown.ts` | Clean deploys without dropped requests |
| **Health checks** | `shared/health.ts` | Infrastructure knows when to send traffic |