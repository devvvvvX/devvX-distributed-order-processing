// ============================================================
// Outbox Relay — Polling Publisher Pattern
// ============================================================
// 🔍 LEARNING NOTE: The Outbox Relay is the second half of the
// Transactional Outbox pattern. Here's the full picture:
//
// WRITE PATH (Order Service):
//   BEGIN → INSERT order → INSERT outbox_event → COMMIT
//   (Atomic — both writes succeed or both fail)
//
// READ PATH (This Relay):
//   Poll outbox_events WHERE published_at IS NULL
//   For each unpublished event:
//     1. Produce to Kafka
//     2. Mark as published in DB
//   Sleep for pollInterval, repeat
//
// WHY NOT USE KAFKA CONNECT OR CDC (Change Data Capture)?
// In production, you'd likely use Debezium (CDC) to stream the outbox
// table to Kafka — it's lower latency and more robust than polling.
// But polling is simpler to understand and sufficient for learning.
//
// THE RELAY RUNS IN-PROCESS with the Order API. Why?
// - One fewer container to manage
// - Shares the same DB pool (no extra connections)
// - If the API is down, no new orders → no new outbox events → relay isn't needed
// - If the API restarts, the relay catches up on any unpublished events
//
// FAILURE MODES:
// 1. Kafka produce succeeds, but markPublished fails (relay crashes):
//    → Event re-published on next poll → consumer idempotency handles it
// 2. Kafka produce fails:
//    → Event stays unpublished → retried on next poll
// 3. Relay falls behind (events accumulate faster than relay can publish):
//    → Increase batch size or poll frequency. Or switch to CDC.

import { Producer } from "kafkajs";
import { OutboxRepository, OutboxEvent } from "./outbox.repository";
import { config } from "../config";
import { createModuleLogger } from "../shared/logger";

const log = createModuleLogger('outbox-relay');

export class OutboxRelay {
    private intervalHandle: ReturnType<typeof setInterval> | null = null;
    private isRunning = false;
    private isProcessing = false;

    constructor(
        private readonly outboxRepo: OutboxRepository,
        private readonly producer: Producer
    ) { }

    // ─────────────────────────────────────────────────
    // Start the polling loop
    // ─────────────────────────────────────────────────
    start(): void {
        if (this.isRunning) {
            log.warn('Outbox relay is already running');
            return;
        }

        this.isRunning = true;

        // 🔍 LEARNING NOTE: We use setInterval instead of a recursive setTimeout.
        // The isProcessing guard prevents overlapping polls. If a poll takes
        // longer than the interval, the next tick is simply skipped.
        this.intervalHandle = setInterval(
            () => this.poll(),
            config.outboxPollIntervalMs
        );

        // Unref so the timer doesn't prevent Node.js from exiting during shutdown
        this.intervalHandle.unref();

        log.info(
            {
                pollIntervalMs: config.outboxPollIntervalMs,
                batchSize: config.outboxBatchSize
            },
            '✅ Outbox relay started'
        );

        // Run an initial poll immedidately (don't wait for the first interval)
        this.poll();
    }

    // ─────────────────────────────────────────────────
    // Stop the polling loop
    // ─────────────────────────────────────────────────
    async stop(): Promise<void> {
        if (!this.isRunning) return;

        this.isRunning = false;

        if (this.intervalHandle) {
            clearInterval(this.intervalHandle);
            this.intervalHandle = null;
        }

        // Wait for any in-progress poll to complete
        // 🔍 LEARNING NOTE: This is crucial for graceful shutdown.
        // If we stop while a poll is mid-way through producing to Kafka,
        // we might produce an event but not mark it as published.
        // The idempotent consumer handles this, but it's cleaner to wait.
        let waitAttempts = 0;
        while (this.isProcessing && waitAttempts < 10) {
            await new Promise((resolve) => setTimeout(resolve, 200));
            waitAttempts++;
        }

        log.info('✅ Outbox relay stopped');
    }

    // ─────────────────────────────────────────────────
    // Single poll cycle
    // ─────────────────────────────────────────────────
    async poll(): Promise<void> {
        // Guard against overlapping polls
        if (this.isProcessing || !this.isRunning) return;
        this.isProcessing = true;

        try {
            const events = await this.outboxRepo.fetchUnpublished(config.outboxBatchSize);

            if (events.length === 0) {
                return; // Nothing to relay
            }

            log.debug({ count: events.length }, `📦 Relaying ${events.length} outbox events`);

            let publishedCount = 0;
            for (const event of events) {
                try {
                    await this.relayEvent(event);
                    publishedCount++;
                } catch (err) {
                    // 🔍 LEARNING NOTE: We log and continue to the next event.
                    // The failed event will be retried on the next poll.
                    // We don't break out of the loop because other events might
                    // be destined for different topics/partitions and could succeed.
                    log.error(
                        {
                            err,
                            outboxId: event.id,
                            eventType: event.eventType,
                            aggregateId: event.aggregateId
                        },
                        `❌ Failed to relay outbox event — will retry next poll`
                    );
                }
            }

            if (publishedCount > 0) {
                log.info(
                    { published: publishedCount, total: events.length },
                    `📤 Relayed ${publishedCount}/${events.length} outbox events to Kafka`
                );
            }
        } catch (err) {
            // 🔍 LEARNING NOTE: This catches errors from fetchUnpublished itself
            // (e.g., DB connection error). The relay keeps running — it'll try
            // again on the next interval.
            log.error({ err }, "❌ Outbox relay poll failed");
        } finally {
            this.isProcessing = false;
        }
    }

    // ─────────────────────────────────────────────────
    // Relay a single event to Kafka
    // ─────────────────────────────────────────────────
    private async relayEvent(event: OutboxEvent): Promise<void> {
        // Convert stored headers object to Kafka headers format
        // 🔍 LEARNING NOTE: Kafka headers are Buffer values.
        // We stored them as a JSON object { key: value } in Postgres,
        // so we convert string values to Buffers here.
        const kafkaHeaders: Record<string, string> = {};
        if (event.headers && typeof event.headers === 'object') {
            for (const [key, value] of Object.entries(event.headers)) {
                kafkaHeaders[key] = String(value);
            }
        }

        // Produce to Kafka
        await this.producer.send({
            topic: event.topic,
            messages: [
                {
                    key: event.key ?? undefined,
                    value: JSON.stringify(event.payload),
                    headers: kafkaHeaders
                },
            ],
            acks: -1, // Wait for all in-sync replicas
        });

        // Mark as published
        // 🔍 LEARNING NOTE: If we crash here (after produce, before markPublished),
        // the event gets re-published on the next poll. This is fine because:
        // 1. Kafka deduplication (if idempotent producer is enabled)
        // 2. Consumer idempotency (processed_events table check)
        this.outboxRepo.markPublished(event.id);
    }
}