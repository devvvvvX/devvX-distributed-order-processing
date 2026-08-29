// ============================================================
// Idempotency Store — Preventing Duplicate Event Processing
// ============================================================
// 🔍 LEARNING NOTE: This is the consumer's defense against at-least-once delivery.
//
// AT-LEAST-ONCE means Kafka guarantees every event is delivered AT LEAST ONCE.
// But it ALSO means events might be delivered MORE THAN ONCE:
//
//   Consumer processes event → crashes before committing offset
//   Consumer restarts → Kafka re-delivers → consumer processes AGAIN → duplicate!
//
// The IdempotencyStore tracks which events have been processed.
// Before processing an event, the consumer checks:
//   "Have I seen this eventId before?"
//   YES → skip it (already processed)
//   NO  → process it, then record the eventId
//
// CRITICAL: The markAsProcessed INSERT must happen in the SAME database
// transaction as the business logic. This guarantees that:
// - If the business logic commits → the event is marked as processed
// - If the business logic rolls back → the event is NOT marked (will be re-tried)
//
// Without transactional marking, you could mark an event as processed
// but fail to execute the business logic → the event is silently lost.
//
// WHY PER-CONSUMER-GROUP?
// Different consumer groups process the same event independently.
// The notification-service group and a future analytics-service group
// both need to process ORDER_CREATED, but independently. The composite
// key (event_id, consumer_group) allows this.

import { Pool, PoolClient } from 'pg';

export class IdempotencyStore {
    constructor(private readonly pool: Pool) { }

    // ─────────────────────────────────────────────────
    // Check if an event has already been processed
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This is a simple SELECT check. It runs BEFORE
    // any business logic, so duplicate events are rejected early with
    // minimal overhead (one DB query).
    //
    // In ultra-high-throughput systems, you might use Redis instead of
    // Postgres for this check (faster, but less durable). For our
    // use case, Postgres is perfect — the data is already there, and
    // the processed_events table is tiny (one row per event).
    async hasBeenProcessed(
        eventId: string,
        consumerGroup: string
    ): Promise<boolean> {
        const result = await this.pool.query(
            `SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_group = $2`,
            [eventId, consumerGroup]
        );
        return result.rowCount !== null && result.rowCount > 0;
    }

    // ─────────────────────────────────────────────────
    // Mark an event as processed (within a transaction)
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This method accepts a PoolClient (not Pool)
    // so it can participate in the SAME transaction as the business logic.
    //
    // The caller's code looks like:
    //   BEGIN
    //   INSERT INTO notifications (...)      ← business logic
    //   INSERT INTO processed_events (...)   ← this method
    //   COMMIT
    //
    // If the notification INSERT fails → both roll back → event NOT marked
    // → Kafka re-delivers → consumer retries. This is correct behavior!
    //
    // ON CONFLICT DO NOTHING handles the edge case where a race condition
    // causes two consumers to check simultaneously — both see "not processed",
    // both try to INSERT, one wins, the other silently skips.
    async markAsProcessed(
        eventId: string,
        consumerGroup: string,
        client?: PoolClient
    ): Promise<void> {
        const queryable = client ?? this.pool;

        await queryable.query(
            `INSERT INTO processed_events (event_id, consumer_group)
             VALUES ($1, $2)
             ON CONFLICT (event_id, consumer_group) DO NOTHING`,
            [eventId, consumerGroup]
        );
    }
}