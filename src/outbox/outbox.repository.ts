// ============================================================
// Outbox Repository — Transactional Outbox Data Access
// ============================================================
// 🔍 LEARNING NOTE: The Outbox Repository has two distinct callers:
//
// 1. ORDER SERVICE (write path):
//    Calls insertWithClient() inside the same DB transaction as the
//    order INSERT. This guarantees atomicity: the outbox event either
//    exists with the order, or neither exists. No dual-write gap.
//
// 2. OUTBOX RELAY (read path):
//    Calls fetchUnpublished() to find events waiting to be sent to Kafka.
//    Calls markPublished() after successfully producing to Kafka.
//
// These two callers NEVER conflict because:
// - The write path only INSERTs
// - The read path only SELECTs and UPDATEs
// - They operate on different rows (new vs. already-committed)

import { Pool, PoolClient } from 'pg';

export interface OutboxEvent {
    id: string;
    aggregateId: string;
    eventType: string;
    topic: string;
    key: string | null;
    payload: Record<string, unknown>;
    headers: Record<string, string>;
    createdAt: Date;
    publishedAt: Date | null;
}

export interface InsertOutboxEvent {
    aggregateId: string;
    eventType: string;
    topic: string;
    key: string;
    payload: Record<string, unknown>;
    headers: Record<string, string>;
}

export class OutboxRepository {
    constructor(private readonly pool: Pool) { }

    // ─────────────────────────────────────────────────
    // Insert outbox event within an existing transaction
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This method takes a PoolClient, not the Pool.
    // A PoolClient represents a SINGLE database connection that may be
    // inside a transaction. By accepting it as a parameter, the caller
    // controls the transaction boundary.
    //
    // This is the critical difference from Phase 2's direct Kafka publish:
    //   Phase 2: INSERT order → COMMIT → kafka.produce() ← can fail independently!
    //   Phase 3: BEGIN → INSERT order → INSERT outbox → COMMIT ← all or nothing!
    async insertWithClient(
        client: PoolClient,
        event: InsertOutboxEvent
    ): Promise<void> {
        await client.query(
            `INSERT INTO outbox_events (aggregate_id, event_type, topic, key, payload, headers)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [
                event.aggregateId,
                event.eventType,
                event.topic,
                event.key,
                JSON.stringify(event.payload),
                JSON.stringify(event.headers),
            ]
        );
    }

    // ─────────────────────────────────────────────────
    // Fetch unpublished events for the relay
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: ORDER BY created_at ASC ensures FIFO ordering.
    // LIMIT prevents the relay from loading too many events into memory
    // during a backlog. The partial index (WHERE published_at IS NULL)
    // makes this query fast regardless of table size.
    //
    // FOR UPDATE SKIP LOCKED is a production pattern:
    // - FOR UPDATE: locks the selected rows so other transactions can't modify them
    // - SKIP LOCKED: if a row is already locked (by another relay instance),
    //   skip it instead of waiting. This enables safe parallel relays.
    //   We only have one relay, but this makes it safe to accidentally
    //   run two without causing duplicate publishes.
    async fetchUnpublished(limit: number): Promise<OutboxEvent[]> {
        const result = await this.pool.query<OutboxEventRow>(
            `SELECT * FROM outbox_events
             WHERE published_at IS NULL
             ORDER BY created_at ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED`,
            [limit]
        );

        return result.rows.map((row) => this.mapToOutboxEvent(row));
    }

    // ─────────────────────────────────────────────────
    // Mark an event as published
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This happens AFTER the Kafka produce succeeds.
    // If we crash between produce and this UPDATE, the event stays
    // unpublished and gets re-relayed on the next poll. The consumer's
    // idempotency guard handles the duplicate.
    async markPublished(id: string): Promise<void> {
        await this.pool.query(
            `UPDATE outbox_events SET published_at = NOW() WHERE id = $1`,
            [id]
        );
    }

    // ─────────────────────────────────────────────────
    // Mark multiple events as published (batch)
    // ─────────────────────────────────────────────────
    async markPublishedBatch(ids: string[]): Promise<void> {
        if (ids.length === 0) return;

        await this.pool.query(
            `UPDATE outbox_events SET published_at = NOW() WHERE id = ANY($1)`,
            [ids]
        );
    }

    private mapToOutboxEvent(row: OutboxEventRow): OutboxEvent {
        return {
            id: row.id,
            aggregateId: row.aggregate_id,
            eventType: row.event_type,
            topic: row.topic,
            key: row.key,
            payload: row.payload,
            headers: row.headers,
            createdAt: row.created_at,
            publishedAt: row.published_at
        }
    }
}

interface OutboxEventRow {
    id: string;
    aggregate_id: string;
    event_type: string;
    topic: string;
    key: string | null;
    payload: Record<string, unknown>;
    headers: Record<string, string>;
    created_at: Date;
    published_at: Date | null;
}