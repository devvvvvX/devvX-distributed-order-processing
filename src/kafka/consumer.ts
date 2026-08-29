// ============================================================
// Kafka Consumer — Reliable Event Processing (Phase 3)
// ============================================================
// 🔍 LEARNING NOTE: This is the Phase 3 evolution of the consumer.
// Phase 2 consumer was simple: process message → commit offset.
// If processing failed, we logged and skipped. Events could be lost.
//
// Phase 3 adds THREE layers of reliability:
//
// 1. IDEMPOTENCY (processed_events table):
//    Before processing, check if we've seen this eventId before.
//    This prevents duplicate processing when Kafka re-delivers events
//    (consumer crash, rebalance, outbox re-publish).
//
// 2. RETRY WITH BACKOFF (exponential + jitter):
//    If processing fails (e.g., email API timeout), retry with
//    increasing delays. Transient failures often self-heal.
//
// 3. DEAD LETTER QUEUE (DLQ):
//    If all retries fail, publish to a DLQ topic for investigation
//    instead of losing the event forever.
//
// PROCESSING FLOW (Phase 3):
//   1. Receive message from Kafka
//   2. Deserialize event envelope → extract eventId
//   3. Check processed_events: already processed? → SKIP
//   4. Call handler.handle() with retry wrapper
//      - Success → mark as processed, commit offset
//      - All retries fail → publish to DLQ, commit offset
//   5. Commit offset (event is either processed or in DLQ)
//
// This provides AT-LEAST-ONCE processing with IDEMPOTENT deduplication:
// - Events may be delivered more than once (at-least-once transport)
// - Idempotency prevents duplicate business effects
// - Retry + DLQ ensures failed events are durably preserved
// - Combined: effectively once-per-event within our DB transaction guarantees

import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { config } from '../config';
import { createModuleLogger } from '../shared/logger.js';
import { IdempotencyStore } from './idempotency.js';
import { DlqProducer } from './dlq-producer.js';
import { withRetry } from './retry.js';
import { deserializeEvent } from './events.js';

const log = createModuleLogger('kafka-consumer');

export interface MessageHandler {
    handle(payload: EachMessagePayload): Promise<void>;
}

export class KafkaConsumer {
    private consumer: Consumer;
    private connected = false;

    constructor(
        kafka: Kafka,
        private readonly idempotencyStore: IdempotencyStore,
        private readonly dlqProducer: DlqProducer
    ) {
        this.consumer = kafka.consumer({
            groupId: config.kafkaConsumerGroupId,

            // 🔍 LEARNING NOTE: Session timeout and heartbeat interval.
            //
            // Phase 3 CRITICAL INSIGHT — Consumer Groups and Rebalancing:
            //
            // When you run MULTIPLE consumers with the same groupId,
            // Kafka forms a CONSUMER GROUP. Kafka assigns each partition
            // to exactly ONE consumer in the group.
            //
            // With 3 partitions and 3 consumers:
            //   Consumer-1 → Partition 0
            //   Consumer-2 → Partition 1
            //   Consumer-3 → Partition 2
            //
            // If Consumer-2 dies (no heartbeat for sessionTimeout):
            //   Kafka triggers a REBALANCE:
            //   Consumer-1 → Partition 0, Partition 1  (picked up the orphan)
            //   Consumer-3 → Partition 2
            //
            // When Consumer-2 comes back:
            //   Another rebalance:
            //   Consumer-1 → Partition 0
            //   Consumer-2 → Partition 1  (back to normal)
            //   Consumer-3 → Partition 2
            //
            // REBALANCING IS EXPENSIVE:
            //   - All consumers stop processing during rebalance
            //   - All uncommitted progress is lost (messages re-delivered)
            //   - Can take several seconds
            //
            // That's why idempotency is essential — rebalancing CAUSES duplicates.
            sessionTimeout: 30000,   // 30 seconds
            heartbeatInterval: 3000, // 3 seconds

            maxWaitTimeInMs: 5000,

            retry: {
                initialRetryTime: 300,
                retries: 10,
                maxRetryTime: 30000,
            },
        });
    }

    // ─────────────────────────────────────────────────
    // Start consuming
    // ─────────────────────────────────────────────────
    async start(topic: string, handler: MessageHandler): Promise<void> {
        try {
            await this.consumer.connect();
            this.connected = true;

            log.info(
                { groupId: config.kafkaConsumerGroupId },
                '✅ Kafka consumer connected'
            );

            await this.consumer.subscribe({
                topic,
                fromBeginning: true
            });

            log.info({ topic }, `📥 Subscribed to topic: ${topic}`);

            await this.consumer.run({
                autoCommit: false,
                eachMessage: async (payload) => {
                    const { topic: msgTopic, partition, message } = payload;
                    const offset = message.offset;
                    const key = message.key?.toString();
                    const startTime = Date.now();

                    try {
                        // ─── Step 1: Extract eventId for idempotency ───
                        // 🔍 LEARNING NOTE: We try to get the eventId from the message
                        // headers first (fast, no deserialization needed), then fall
                        // back to deserializing the event body. Headers were set by
                        // the producer/outbox relay.
                        let eventId = message.headers?.['event-id']?.toString();

                        if (!eventId && message.value) {
                            try {
                                const event = deserializeEvent(message.value.toString());
                                eventId = event.eventId;
                            } catch (err) {
                                // Can't deserialize — we'll let the handler deal with it
                            }
                        }

                        // ─── Step 2: Idempotency check ───
                        if (eventId) {
                            const alreadyProcessed = await this.idempotencyStore.hasBeenProcessed(eventId, config.kafkaConsumerGroupId);
                            if (alreadyProcessed) {
                                log.debug(
                                    { eventId, partition, offset },
                                    `⏭️  Event already processed — skipping (idempotent)`
                                );

                                // Commit offset to advance past this duplicate
                                await this.consumer.commitOffsets([
                                    {
                                        topic: msgTopic,
                                        partition,
                                        offset: (BigInt(offset) + 1n).toString(),
                                    }
                                ])
                                return;
                            }
                        }

                        // ─── Step 3: Process with retry ───
                        log.debug(
                            { topic: msgTopic, partition, offset, key, eventId },
                            `📩 Processing message: partition=${partition} offset=${offset}`
                        );

                        const retryResult = await withRetry(
                            () => handler.handle(payload),
                            {
                                maxRetries: config.consumerMaxRetries,
                                baseDelayMs: config.consumerRetryBaseDelayMs
                            },
                            { topic: msgTopic, partition, offset, key, eventId }
                        );

                        if (retryResult.success) {
                            // ─── Step 4a: Mark as processed (success) ───
                            if (eventId) {
                                await this.idempotencyStore.markAsProcessed(eventId, config.kafkaConsumerGroupId);
                            }

                            // Commit offset
                            await this.consumer.commitOffsets([
                                {
                                    topic: msgTopic,
                                    partition,
                                    offset: (BigInt(offset) + 1n).toString(),
                                },
                            ]);

                            const latency = Date.now() - startTime;
                            log.debug(
                                {
                                    topic: msgTopic, partition, offset,
                                    latencyMs: latency, attempts: retryResult.attempts,
                                },
                                `✅ Message processed and committed (${latency}ms, ${retryResult.attempts} attempt(s))`
                            );
                        } else {
                            // ─── Step 4b: Send to DLQ (all retries exhausted) ───

                            const latency = Date.now() - startTime;
                            log.error(
                                {
                                    topic: msgTopic, partition, offset, key, eventId,
                                    latencyMs: latency,
                                    error: retryResult.lastError?.message,
                                },
                                `☠️  All retries exhausted — sending to DLQ`
                            );

                            await this.dlqProducer.sendToDlq({
                                originalTopic: msgTopic,
                                originalPartition: partition,
                                originalOffset: offset,
                                originalKey: key ?? null,
                                originalValue: message.value?.toString() ?? null,
                                errorMessage: retryResult.lastError?.message ?? 'Unknown error',
                                errorStack: retryResult.lastError?.stack,
                                retryCount: retryResult.attempts,
                                failedAt: new Date().toISOString(),
                                consumerGroup: config.kafkaConsumerGroupId
                            });

                            // Mark as processed (in DLQ) to prevent re-processing
                            if (eventId) {
                                await this.idempotencyStore.markAsProcessed(eventId, config.kafkaConsumerGroupId);
                            }

                            // Commit offset to move past the failed message
                            // 🔍 LEARNING NOTE: We commit the offset even for DLQ'd messages.
                            // The event is now in the DLQ — it's not lost. If we didn't
                            // commit, the consumer would re-process (and re-DLQ) this
                            // message on every restart, creating an infinite DLQ loop.
                            await this.consumer.commitOffsets([
                                {
                                    topic: msgTopic,
                                    partition,
                                    offset: (BigInt(offset) + 1n).toString(),
                                },
                            ]);

                        }
                    } catch (err) {
                        // 🔍 LEARNING NOTE: This outer catch handles unexpected errors
                        // (e.g., idempotency store is down, DLQ producer is down).
                        // These are infrastructure failures, not business failures.
                        // We log and commit to prevent infinite loops.
                        const latency = Date.now() - startTime;


                        log.error(
                            {
                                err,
                                topic: msgTopic,
                                partition,
                                offset,
                                key,
                                latencyMs: latency,
                            },
                            `❌ Unexpected error in consumer pipeline — skipping`
                        );

                        // Commit the offset to prevent infinite reprocessing
                        await this.consumer.commitOffsets([
                            {
                                topic: msgTopic,
                                partition,
                                offset: (BigInt(offset) + 1n).toString(),
                            }
                        ]);
                    }
                }
            })
        } catch (err) {
            log.error({ err }, '❌ Failed to start Kafka consumer');
            throw err;
        }
    }

    // ─────────────────────────────────────────────────
    // Graceful shutdown
    // ─────────────────────────────────────────────────
    async stop(): Promise<void> {
        if (!this.connected) return;

        try {
            await this.consumer.disconnect();
            this.connected = false;
            log.info('✅ Kafka consumer disconnected');
        } catch (err) {
            log.error({ err }, '❌ Error disconnecting Kafka consumer');
        }
    }
}