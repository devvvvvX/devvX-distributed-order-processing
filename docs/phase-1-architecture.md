# Phase 1 — Architecture Decision Record: The Monolith

## Overview

Phase 1 implements a **synchronous monolithic** order processing API. This is the starting point — the simplest architecture that works for a small-scale food delivery platform.

Everything runs in a single Node.js process. All communication is synchronous. All data lives in one PostgreSQL database. This is intentional.

## Architecture Diagram

```
                     ┌────────────────────────────────┐
                     │          CLIENTS                │
                     │   (Mobile App / Web / API)      │
                     └───────────────┬────────────────┘
                                     │ HTTP
                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                        MONOLITH                                 │
│                   (Single Node.js Process)                      │
│                                                                 │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │                    HTTP Layer (Fastify)                   │  │
│   │  POST /orders  GET /orders  PATCH /orders/:id/status     │  │
│   └────────────────────────┬─────────────────────────────────┘  │
│                            │                                    │
│   ┌────────────────────────▼─────────────────────────────────┐  │
│   │                   Service Layer                          │  │
│   │  ┌─────────────────┐      ┌──────────────────────────┐   │  │
│   │  │  OrderService   │─────▶│  NotificationService     │   │  │
│   │  │                 │      │  (SYNCHRONOUS!)          │   │  │
│   │  │  • Create order │      │  • Send email (simulated)│   │  │
│   │  │  • Update status│      │  • 500ms artificial delay│   │  │
│   │  │  • State machine│      │  • Configurable failures │   │  │
│   │  └────────┬────────┘      └──────────────┬───────────┘   │  │
│   │           │                              │               │  │
│   └───────────┼──────────────────────────────┼───────────────┘  │
│               │                              │                  │
│   ┌───────────▼──────────────────────────────▼───────────────┐  │
│   │                  Repository Layer                        │  │
│   │  ┌─────────────────┐      ┌──────────────────────────┐   │  │
│   │  │ OrderRepository │      │ NotificationRepository   │   │  │
│   │  │  • SQL queries  │      │  • Notification records  │   │  │
│   │  │  • Transactions │      │  • Status tracking       │   │  │
│   │  └────────┬────────┘      └──────────────┬───────────┘   │  │
│   │           │                              │               │  │
│   └───────────┼──────────────────────────────┼───────────────┘  │
│               │                              │                  │
│   ┌───────────▼──────────────────────────────▼───────────────┐  │
│   │              PostgreSQL Connection Pool (pg)             │  │
│   │              min: 2, max: 10 connections                 │  │
│   └───────────────────────────┬──────────────────────────────┘  │
│                               │                                 │
│   Cross-Cutting Concerns:     │                                 │
│   • Structured logging (Pino) │                                 │
│   • Health checks (/health)   │                                 │
│   • Graceful shutdown         │                                 │
│   • Error handling            │                                 │
│   • Config validation (Zod)   │                                 │
└───────────────────────────────┼─────────────────────────────────┘
                                │
                                ▼
                   ┌────────────────────────┐
                   │      PostgreSQL 15     │
                   │                        │
                   │  orders                │
                   │  order_items           │
                   │  order_status_history  │
                   │  notifications         │
                   └────────────────────────┘
```

## Why This Architecture Works (Initially)

| Factor | Assessment |
|---|---|
| **Simplicity** | One process, one database, one deploy. Easy to reason about, debug, and operate. |
| **Consistency** | Database transactions give us ACID guarantees. No eventual consistency headaches. |
| **Latency** | No network hops between services. Everything is in-process. |
| **Debugging** | Single log stream. Stack traces show the full call chain. |
| **Deployment** | One Docker image. One `docker compose up`. |
| **Team size** | For 1-3 developers, this is the right architecture. |

### Real-World Validation
Amazon started as a monolith. So did Uber, Airbnb, and Netflix. They evolved because they HAD to, not because microservices were trendy. If you're not hitting the limits below, you don't need a distributed system.

---

## Where This Architecture Breaks

### 1. Synchronous Notification Bottleneck

**The Problem:**
```
Order creation time = DB write (20ms) + Notification send (500ms) = 520ms
```

The customer waits 500ms longer than necessary because we're sending email inside the HTTP request handler.

**Why it gets worse:**
- Add SMS notification: +300ms → 820ms total
- Add push notification: +200ms → 1020ms total
- Email API has a bad day (timeout): +5000ms → 5020ms total
- Email API is down: Order creation FAILS even though the order was saved

**Real-world impact:**
- Every 100ms of latency reduces conversion by ~1% (Amazon study)
- 520ms vs 20ms = customers abandoning orders

### 2. Single Point of Failure

**The Problem:**
One process crash = zero requests served. No redundancy.

```
Process crashes → 100% downtime until restart
Restart takes ~5 seconds → 5 seconds of dropped requests
```

**In production:**
- The process WILL crash (OOM, uncaught exception, dependency bug)
- Deploys require restart → brief downtime per deploy
- No ability to do blue-green or canary deployments

### 3. Cannot Scale Independently

**The Problem:**
Order processing and notification sending scale together, even though they have different load profiles.

```
Black Friday scenario:
- 10x order volume → need 10 instances
- But notification sending is the bottleneck, not order writing
- You're paying for 10x compute when you only need 10x notification capacity
```

### 4. Connection Pool Exhaustion Under Load

**The Problem:**
With `pool_max=10` connections and notification delay of 500ms:
- Each order holds a connection for ~520ms
- Max throughput: 10 / 0.52 ≈ 19 orders/second
- Request #20 waits for a free connection → timeout → 500 error

**The insidious part:**
Adding more connections seems like a fix, but PostgreSQL can only handle ~100-300 concurrent connections before performance degrades. You can't pool your way out of this.

### 5. Tight Coupling

**The Problem:**
Notification logic is called directly from order creation. If we want to:
- Add a new notification channel (WhatsApp)
- Change notification priority based on order value
- A/B test notification content

...we have to modify and redeploy the entire monolith. One bad notification change can break order creation.

### 6. No Retry Mechanism

**The Problem:**
If a notification fails, it's gone. No retry. The customer never gets notified.

In the current architecture, our only options are:
- Retry inline (blocks the HTTP response even longer)
- Ignore the failure (customer never knows their order was placed)
- Build a polling job (complex, adds latency, doesn't scale)

---

## What Phase 2 Solves

Phase 2 introduces **Apache Kafka** to decouple order creation from notification processing:

```
BEFORE (Phase 1 — Synchronous):
  Client → Create Order → Send Notification → Respond to Client
  Latency: 520ms, Coupled, Fragile

AFTER (Phase 2 — Event-Driven):
  Client → Create Order → Publish Event → Respond to Client  (20ms)
  Background: Event → Notification Consumer → Send Email       (async)
  Latency: 20ms, Decoupled, Resilient
```

Key improvements:
1. **Order latency drops from 520ms to 20ms**
2. **Notification failures don't affect order creation**
3. **Kafka retries failed notifications automatically**
4. **Notification processing scales independently**
5. **New consumers can be added without touching order code**

But Kafka introduces new complexity:
- Eventual consistency (order exists but notification hasn't been sent yet)
- Event ordering (what if events arrive out of order?)
- Duplicate events (what if the same event is processed twice?)
- Operational overhead (Kafka cluster management)

These are the topics for Phase 2.

---

## Production Patterns Introduced in Phase 1

| Pattern | Purpose | Phase Evolution |
|---|---|---|
| Idempotency keys | Prevent duplicate orders from retries | Expanded in Phase 3 for Kafka consumers |
| State machine | Prevent invalid status transitions | Critical in Phase 2 when events arrive out-of-order |
| Optimistic concurrency | Handle concurrent status updates | Foundation for distributed locking in Phase 4 |
| Connection pooling | Efficient DB connection usage | Monitoring added in Phase 6 |
| Graceful shutdown | Clean deploys without data loss | Expanded for Kafka consumers in Phase 2 |
| Health checks | Infrastructure integration | Readiness/liveness split in Phase 6 |
| Structured logging | Production debugging | Distributed tracing in Phase 6 |
| Repository pattern | Separation of concerns | Each module becomes a service in Phase 2+ |
