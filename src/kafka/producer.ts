// ============================================================
// Kafka Producer — Publishing Events
// ============================================================
// 🔍 LEARNING NOTE: The producer is responsible for publishing events
// to Kafka topics. Key decisions:
//
// 1. PARTITION KEY: We use orderId as the key. Kafka hashes the key
//    to determine which partition the message goes to. All events for
//    the SAME order go to the SAME partition, guaranteeing per-order
//    ordering (ORDER_CREATED before ORDER_STATUS_CHANGED).
//
// 2. ACKS (acknowledgments):
//    - acks=0: Fire and forget. Producer doesn't wait for broker confirmation.
//              Fastest, but messages can be lost if the broker crashes.
//    - acks=1: Leader acknowledgment. Producer waits for the partition leader
//              to write the message. If the leader crashes before replication,
//              the message is lost.
//    - acks=-1 (all): All in-sync replicas acknowledge. Safest, but slowest.
//              Used for financial/critical data. With 1 broker (dev), this = acks=1.
//
// 3. IDEMPOTENT PRODUCER: KafkaJS supports enable.idempotence which
//    prevents duplicate messages from network retries. We'll enable this
//    in Phase 3 when we deep-dive idempotency.
//
// 4. BATCHING: The producer batches messages before sending.
//    For low-latency scenarios, you might want to reduce batch size.
//    For throughput, increase it. Default is fine for us.

import { Kafka, Producer, ProducerRecord } from 'kafkajs';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger.js';
import {
    OrderEvent,
    OrderCreatedPayload,
    OrderStatusChangedPayload,
    buildOrderCreatedEvent,
    buildOrderStatusChangedEvent,
    serializeEvent,
} from './events.js';

const log = createModuleLogger('kafka-producer');

export class EventProducer {
    private producer: Producer;
    private connected = false;

    constructor(kafka: Kafka) {
        this.producer = kafka.producer({
            // 🔍 LEARNING NOTE: allowAutoTopicCreation=false forces explicit
            // topic creation. In production, auto-created topics get default
            // settings (1 partition, no replication) which is almost never
            // what you want. Always create topics explicitly with proper
            // partition count and replication factor.
            allowAutoTopicCreation: false,

            // 🔍 LEARNING NOTE: Transaction timeout. If a producer transaction
            // takes longer than this, Kafka aborts it. We don't use transactions
            // in Phase 2 (that's Phase 3's exactly-once semantics), but setting
            // a sane timeout is good practice.
            transactionTimeout: 30000,
        })
    }

    // ─────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────

    async connect(): Promise<void> {
        try {
            await this.producer.connect();
            this.connected = true;
            log.info('✅ Kafka producer connected');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect Kafka producer');
            // Don't crash — the app can function without Kafka.
            // Events just won't be published until reconnection.
        }
    }

    async disconnect(): Promise<void> {
        try {
            // 🔍 LEARNING NOTE: disconnect() flushes any pending messages
            // in the producer's internal buffer before closing the connection.
            // This is critical for graceful shutdown — without it, the last
            // batch of messages might be lost.
            await this.producer.disconnect();
            this.connected = false;
            log.info('✅ Kafka producer disconnected');
        } catch (err) {
            log.error({ err }, '❌ Failed to disconnect Kafka producer');
        }
    }


    // ─────────────────────────────────────────────────
    // Publish events
    // ─────────────────────────────────────────────────

    async publishOrderCreated(order: OrderCreatedPayload['order']): Promise<void> {
        const event = buildOrderCreatedEvent(order);
        await this.publishEvent(event);
    }

    async publishOrderStatusChanged(data: OrderStatusChangedPayload): Promise<void> {
        const event = buildOrderStatusChangedEvent(data);
        await this.publishEvent(event);
    }

    // ─────────────────────────────────────────────────
    // Core publish method
    // ─────────────────────────────────────────────────
    private async publishEvent(event: OrderEvent): Promise<void> {
        if (!this.connected) {
            log.warn(
                { eventType: event.eventType, aggregateId: event.aggregateId },
                '⚠️  Kafka producer not connected — event will NOT be published'
            );
        }

        const topic = config.kafkaTopicOrderEvents;

        const record: ProducerRecord = {
            topic,
            messages: [
                {
                    // 🔍 LEARNING NOTE: The KEY determines which partition this
                    // message goes to. All events with the same key (orderId) go
                    // to the same partition. This guarantees per-order ordering.
                    //
                    // If we used null/random keys, messages would be round-robin'd
                    // across partitions. An ORDER_STATUS_CHANGED event could end
                    // up in a different partition than ORDER_CREATED, and a consumer
                    // might process them out of order.
                    key: event.aggregateId,


                    // 🔍 LEARNING NOTE: The VALUE is the serialized event.
                    // We use JSON for simplicity. In production at scale, you'd
                    // consider Avro or Protobuf for:
                    // - Smaller message sizes (binary encoding)
                    // - Schema registry (enforce schema evolution rules)
                    // - Backward/forward compatibility guarantees
                    value: serializeEvent(event),

                    // 🔍 LEARNING NOTE: Headers are metadata about the message.
                    // They're searchable in Kafka UI and useful for routing/filtering
                    // without deserializing the entire message body.
                    headers: {
                        'event-type': event.eventType,
                        'event-id': event.eventId,
                        'source': event.source,
                    }
                }
            ],

            // 🔍 LEARNING NOTE: acks=1 means the partition leader must
            // acknowledge the write. This is a good balance between
            // durability and latency for our use case.
            // With a single broker in dev, acks=-1 (all) is equivalent.
            acks: -1
        };

        const startTime = Date.now();

        try {
            const result = await this.producer.send(record);
            const latency = Date.now() - startTime;
            log.info(
                {
                    eventType: event.eventType,
                    eventId: event.eventId,
                    aggregateId: event.aggregateId,
                    topic,
                    partition: result[0]?.partition,
                    offset: result[0]?.offset,
                    latencyMs: latency,
                },
                `📤 Event published: ${event.eventType} (${latency}ms)`
            );
        } catch (err) {
            const latency = Date.now() - startTime;
            // 🔍 LEARNING NOTE: We log the error but DON'T throw.
            // The order was already saved to the DB. If Kafka publish fails:
            // - The order exists (customer is happy)
            // - The notification won't be sent (customer doesn't get email)
            // - We log it for operational visibility
            //
            // This is the "dual-write problem" in action. The DB write
            // succeeded but the Kafka write failed. In Phase 3, we'll
            // implement the Transactional Outbox pattern to eliminate
            // this failure window entirely.
            log.error(
                {
                    err,
                    eventType: event.eventType,
                    eventId: event.eventId,
                    aggregateId: event.aggregateId,
                    latencyMs: latency,
                },
                `❌ Failed to publish event: ${event.eventType}`
            );
        }
    }
}