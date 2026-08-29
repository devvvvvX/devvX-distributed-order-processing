// ============================================================
// Dead Letter Queue (DLQ) Producer
// ============================================================
// 🔍 LEARNING NOTE: The Dead Letter Queue is where events go to die
// (or more accurately, to be investigated).
//
// When an event exhausts all retries, it means either:
// 1. The event itself is broken (malformed, missing fields, invalid data)
//    → No amount of retrying will fix it. Needs human investigation.
// 2. The downstream dependency has a sustained outage
//    → Retrying would just waste resources. Wait for recovery.
// 3. A bug in the consumer code
//    → Fix the code, then replay events from the DLQ.
//
// DLQ DESIGN:
// - Separate Kafka topic (order-events-dlq)
// - Same partition key as original (preserves ordering context)
// - Enriched with error metadata:
//   - Original topic, partition, offset
//   - Error message and stack trace
//   - Number of retries attempted
//   - Timestamp of failure
//
// DLQ OPERATIONS:
// In production, you'd have a DLQ consumer or admin tool that can:
// - Inspect failed events (via Kafka UI in our case)
// - Replay events back to the original topic after fixing the root cause
// - Purge events that are no longer relevant
// - Alert ops team when DLQ has messages (DLQ depth > 0 = problem)
//
// For now, we publish to the DLQ and inspect via Kafka UI.

import { Kafka, Producer } from 'kafkajs';
import { config } from '../config/index.js'
import { createModuleLogger } from '../shared/logger.js';

const log = createModuleLogger('dlq-producer');

export interface DlqMessage {
    originalTopic: string;
    originalPartition: number;
    originalOffset: string;
    originalKey: string | null;
    originalValue: string | null;
    errorMessage: string;
    errorStack?: string;
    retryCount: number;
    failedAt: string;
    consumerGroup: string;
}

export class DlqProducer {
    private producer: Producer;
    private connected = false;

    constructor(kafka: Kafka) {
        this.producer = kafka.producer({
            allowAutoTopicCreation: false,
        });
    }

    async connect(): Promise<void> {
        try {
            await this.producer.connect();
            this.connected = true;
            log.info({ topic: config.kafkaTopicDlq }, '✅ DLQ producer connected');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect DLQ producer');
        }
    }

    async disconnect(): Promise<void> {
        if (!this.connected) return;
        try {
            await this.producer.disconnect();
            this.connected = false;
            log.info('✅ DLQ producer disconnected');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect DLQ producer');
        }
    }

    // ─────────────────────────────────────────────────
    // Send a failed event to the DLQ
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: The DLQ message wraps the original event with
    // error context. The original event is preserved as-is in the value,
    // and the error metadata is in the headers.
    //
    // This way, when replaying from the DLQ, you can extract the
    // original value and re-publish it to the original topic.
    async sendToDlq(dlqMessage: DlqMessage): Promise<void> {
        if (!this.connected) {
            log.warn(
                { originalTopic: dlqMessage.originalTopic },
                '⚠️  DLQ producer not connected — failed event will be lost'
            );
            return;
        }

        try {
            // 🔍 LEARNING NOTE: We preserve the original message key so
            // DLQ messages for the same order end up in the same partition.
            // This makes DLQ investigation easier — all failures for a
            // given order are grouped together.
            await this.producer.send({
                topic: config.kafkaTopicDlq,
                messages: [
                    {
                        key: dlqMessage.originalKey ?? undefined,
                        // The value is the original event payload (unchanged)
                        value: dlqMessage.originalValue ?? '',
                        headers: {
                            'dlq-original-topic': dlqMessage.originalTopic,
                            'dlq-original-partition': String(dlqMessage.originalPartition),
                            'dlq-original-offset': dlqMessage.originalOffset,
                            'dlq-error-messasge': dlqMessage.errorMessage,
                            'dlq-retry-count': String(dlqMessage.retryCount),
                            'dlq-failed-at': dlqMessage.failedAt,
                            'dlq-consumer-group': dlqMessage.consumerGroup,
                        },
                    }
                ],
                acks: -1,
            });

            log.info({
                originalTopic: dlqMessage.originalTopic,
                originalPartition: dlqMessage.originalPartition,
                originalOffset: dlqMessage.originalOffset,
                retryCount: dlqMessage.retryCount,
                errorMessage: dlqMessage.errorMessage,
            },
                '☠️  Event sent to DLQ'
            );
        } catch (err) {
            // 🔍 LEARNING NOTE: If even the DLQ publish fails, we log it.
            // This is the absolute last resort. The event is effectively lost.
            // In production, this should trigger a critical alert.
            log.error(
                {
                    err,
                    originalTopic: dlqMessage.originalTopic,
                    originalOffset: dlqMessage.originalOffset
                },
                '❌ CRITICAL: Failed to send event to DLQ'
            );
        }
    }
}