-- ============================================================
-- Phase 1 — Database Schema: Order Platform
-- ============================================================
-- 🔍 LEARNING NOTE: This schema runs ONCE when PostgreSQL initializes.
-- In a real production system, you'd use a migration tool (Flyway, Knex, Prisma)
-- so schema changes are versioned, reversible, and auditable.
-- For learning purposes, a single init script is simpler.

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- ENUM TYPES
-- ============================================================
-- 🔍 LEARNING NOTE: Using PostgreSQL enums instead of plain VARCHAR
-- gives us database-level validation. You can't insert an invalid status.
-- Tradeoff: Adding new enum values requires ALTER TYPE, which can be
-- tricky in production with long-running transactions. Some teams
-- prefer VARCHAR + application-level validation for this reason.

CREATE TYPE order_status AS ENUM (
  'PENDING',
  'CONFIRMED',
  'PREPARING',
  'READY_FOR_PICKUP',
  'OUT_FOR_DELIVERY',
  'DELIVERED',
  'CANCELLED'
);

CREATE TYPE notification_type AS ENUM (
  'ORDER_CONFIRMED',
  'ORDER_STATUS_CHANGED',
  'ORDER_CANCELLED',
  'DELIVERY_UPDATE'
);

CREATE TYPE notification_channel AS ENUM (
  'EMAIL',
  'SMS',
  'PUSH'
);

CREATE TYPE notification_status AS ENUM (
  'PENDING',
  'SENT',
  'FAILED'
);

-- ============================================================
-- ORDERS TABLE
-- ============================================================
CREATE TABLE orders (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  
  -- Customer info (denormalized for simplicity in Phase 1)
  -- 🔍 LEARNING NOTE: In a real system, customer data lives in a
  -- separate customer service/table. We denormalize here because:
  -- 1. We need the data for notifications without extra JOINs
  -- 2. Customer info at order time might differ from current info
  --    (they may update their email later, but the order was placed with the old one)
  customer_id     VARCHAR(255) NOT NULL,
  customer_name   VARCHAR(255) NOT NULL,
  customer_email  VARCHAR(255) NOT NULL,
  customer_phone  VARCHAR(50),
  
  -- Restaurant info (also denormalized)
  restaurant_id   VARCHAR(255) NOT NULL,
  restaurant_name VARCHAR(255) NOT NULL,
  
  -- Order state
  status          order_status NOT NULL DEFAULT 'PENDING',
  
  -- Financial fields
  -- 🔍 LEARNING NOTE: NEVER use FLOAT for money. NUMERIC(precision, scale)
  -- stores exact decimal values. FLOAT would give you 19.99 → 19.989999999...
  -- Every fintech/commerce engineer learns this the hard way.
  total_amount    NUMERIC(12, 2) NOT NULL DEFAULT 0,
  delivery_fee    NUMERIC(12, 2) NOT NULL DEFAULT 0,
  tax_amount      NUMERIC(12, 2) NOT NULL DEFAULT 0,
  grand_total     NUMERIC(12, 2) NOT NULL DEFAULT 0,
  
  -- Delivery info
  delivery_address TEXT NOT NULL,
  delivery_lat     NUMERIC(10, 7),
  delivery_lng     NUMERIC(10, 7),
  
  -- Metadata
  notes           TEXT,
  
  -- 🔍 LEARNING NOTE: Idempotency key is CRITICAL even in a monolith.
  -- Scenario: Customer clicks "Place Order", network times out, they click again.
  -- Without idempotency key: two orders created, customer charged twice.
  -- With idempotency key: second request returns the existing order.
  -- We use a UNIQUE constraint so the DB enforces uniqueness atomically.
  -- This concept becomes even more important with Kafka in Phase 2-3.
  idempotency_key VARCHAR(255) UNIQUE,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- ORDER ITEMS TABLE
-- ============================================================
CREATE TABLE order_items (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id        UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  
  item_name       VARCHAR(255) NOT NULL,
  quantity        INTEGER NOT NULL CHECK (quantity > 0),
  unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
  total_price     NUMERIC(12, 2) NOT NULL CHECK (total_price >= 0),
  customizations  TEXT,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- ORDER STATUS HISTORY
-- ============================================================
-- 🔍 LEARNING NOTE: This is an append-only audit log.
-- In production, "why is this order stuck in PREPARING?" is a question
-- you WILL answer at 2 AM. Without status history, you're blind.
-- This pattern is also the foundation for event sourcing (Phase 2+).
CREATE TABLE order_status_history (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id        UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  
  from_status     order_status,  -- NULL for initial creation
  to_status       order_status NOT NULL,
  changed_by      VARCHAR(255) NOT NULL DEFAULT 'system',
  reason          TEXT,
  metadata        JSONB DEFAULT '{}',
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- NOTIFICATIONS TABLE
-- ============================================================
-- 🔍 LEARNING NOTE: We persist every notification attempt because:
-- 1. Support needs to answer "did the customer get notified?"
-- 2. We need to track failure rates for operational monitoring
-- 3. We might need to retry failed notifications (Phase 2 will do this better)
CREATE TABLE notifications (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id        UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  
  type            notification_type NOT NULL,
  channel         notification_channel NOT NULL,
  status          notification_status NOT NULL DEFAULT 'PENDING',
  
  recipient       VARCHAR(255) NOT NULL,
  subject         VARCHAR(500),
  content         TEXT NOT NULL,
  
  retry_count     INTEGER NOT NULL DEFAULT 0,
  failure_reason  TEXT,
  
  sent_at         TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- OUTBOX EVENTS TABLE (Phase 3 — Transactional Outbox Pattern)
-- ============================================================
-- 🔍 LEARNING NOTE: The Transactional Outbox solves the DUAL-WRITE problem.
--
-- The problem (Phase 2):
--   1. INSERT INTO orders → success
--   2. kafka.produce(event) → Kafka is DOWN → event LOST
--   The order exists but the notification event was never published.
--
-- The fix (Phase 3):
--   1. BEGIN TRANSACTION
--      INSERT INTO orders
--      INSERT INTO outbox_events    ← SAME transaction, SAME database!
--   2. COMMIT                       ← atomic: both succeed or both fail
--
-- A background "Outbox Relay" polls this table and publishes
-- unpublished events to Kafka. If the relay crashes between
-- publishing and marking as published, it re-publishes on the next poll.
-- This is safe because consumers are IDEMPOTENT (see processed_events below).
--
-- Key insight: We moved the Kafka write OUT of the critical path.
-- The order API only writes to Postgres. The relay handles Kafka asynchronously.
-- If Kafka is down for hours, events accumulate in the outbox and get
-- published when Kafka comes back. ZERO data loss.
CREATE TABLE outbox_events (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  aggregate_id  VARCHAR(255) NOT NULL,    -- orderId (for tracing)
  event_type    VARCHAR(100) NOT NULL,    -- ORDER_CREATED, ORDER_STATUS_CHANGED, etc.
  topic         VARCHAR(255) NOT NULL,    -- Target Kafka topic
  key           VARCHAR(255),             -- Kafka partition key (orderId)
  payload       JSONB NOT NULL,           -- Full serialized event envelope
  headers       JSONB DEFAULT '{}',       -- Kafka message headers
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at  TIMESTAMPTZ              -- NULL until successfully relayed to Kafka
);

-- ============================================================
-- PROCESSED EVENTS TABLE (Phase 3 — Consumer Idempotency)
-- ============================================================
-- 🔍 LEARNING NOTE: Kafka guarantees AT-LEAST-ONCE delivery.
-- "At least once" means the SAME event can be delivered MULTIPLE times:
--
-- Scenario 1 — Consumer crash after processing, before offset commit:
--   Consumer processes event (sends email) → crashes
--   Restarts → Kafka re-delivers the same event → duplicate email!
--
-- Scenario 2 — Outbox relay re-publishes (crash after Kafka, before DB update):
--   Relay publishes event to Kafka → crashes before marking published_at
--   Next poll re-publishes → Kafka has the event TWICE → duplicate!
--
-- Scenario 3 — Consumer group rebalance:
--   Consumer A is processing event → rebalance triggers
--   Partition moves to Consumer B → Consumer B reads same offset → duplicate!
--
-- The fix: track which events have been processed. Before processing,
-- check this table. If the event_id is found, SKIP it.
-- The INSERT into this table happens in the SAME transaction as the
-- business logic (e.g., creating the notification row).
CREATE TABLE processed_events (
  event_id        VARCHAR(255) NOT NULL,
  consumer_group  VARCHAR(100) NOT NULL,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_id, consumer_group)
);

-- ============================================================
-- INDEXES
-- ============================================================
-- 🔍 LEARNING NOTE: Indexes are not free. Each index:
-- 1. Speeds up reads on that column
-- 2. Slows down writes (must update the index too)
-- 3. Uses disk space
-- Only index columns you actually query by. We'll add more as needed.

-- Orders: commonly queried by customer and status
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX idx_orders_restaurant_id ON orders(restaurant_id);

-- Order items: always queried by order
CREATE INDEX idx_order_items_order_id ON order_items(order_id);

-- Status history: always queried by order, newest first
CREATE INDEX idx_order_status_history_order_id ON order_status_history(order_id, created_at DESC);

-- Notifications: queried by order and by status (for retry logic)
CREATE INDEX idx_notifications_order_id ON notifications(order_id);
CREATE INDEX idx_notifications_status ON notifications(status);

-- Outbox: the relay polls for unpublished events frequently.
-- 🔍 LEARNING NOTE: This is a PARTIAL INDEX — it only indexes rows
-- WHERE published_at IS NULL. As events get published, they leave
-- the index. This keeps the index tiny even if the table has millions
-- of historical rows. Partial indexes are a PostgreSQL superpower.
CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at ASC)
  WHERE published_at IS NULL;

-- Processed events: looked up by event_id (already covered by PK)
-- The composite PK (event_id, consumer_group) handles all our queries.

-- ============================================================
-- UPDATED_AT TRIGGER
-- ============================================================
-- 🔍 LEARNING NOTE: This trigger automatically updates the updated_at
-- column whenever a row is modified. Without this, you'd need to
-- remember to set updated_at in every UPDATE query.
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_orders_updated_at
  BEFORE UPDATE ON orders
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();