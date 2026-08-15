// ============================================================
// Kafka Consumer — Processing Events
// ============================================================
// 🔍 LEARNING NOTE: Kafka consumers are fundamentally different from
// HTTP request handlers. Key differences:
//
// HTTP (Phase 1):
//   - Request arrives → process → respond → done
//   - Stateless: each request is independent
//   - If the server crashes, the client retries
//
// Kafka Consumer (Phase 2):
//   - Consumer POLLS for messages in a loop
//   - Consumer maintains STATE (offset = where am I in the log?)
//   - If the consumer crashes, it restarts from its last committed offset
//   - Messages are NOT deleted after consumption (unlike RabbitMQ)
//   - Multiple consumers in a GROUP share the work (Phase 3)
//
// OFFSET MANAGEMENT is the most critical concept:
//   - Each partition has an OFFSET counter (0, 1, 2, 3, ...)
//   - The consumer tracks "I've processed up to offset 42"
//   - On restart, it resumes from offset 42
//   - COMMIT = persist the current offset to Kafka
//   - If you commit BEFORE processing → message lost on crash
//   - If you commit AFTER processing → message re-processed on crash
//   - We commit AFTER → at-least-once delivery (safe default)


import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { config } from '../config';
import { createModuleLogger } from '../shared/logger';

const log = createModuleLogger('kafka-consumer');

export interface MessageHandler {
    handle(payload: EachMessagePayload): Promise<void>;
}

export class KafkaConsumer {
    private consumer: Consumer;
    private connected = false;

    constructor(kafka: Kafka) {
        this.consumer = kafka.consumer({
            groupId: config.kafkaConsumerGroupId,

            // 🔍 LEARNING NOTE: Session timeout and heartbeat interval.
            //
            // The consumer sends HEARTBEATS to Kafka to prove it's alive.
            // If Kafka doesn't receive a heartbeat within sessionTimeout,
            // it considers the consumer DEAD and reassigns its partitions
            // to other consumers in the group (REBALANCING — Phase 3 deep dive).
            //
            // Heartbeat interval should be < 1/3 of session timeout.
            // If your message processing takes longer than sessionTimeout,
            // Kafka thinks you're dead and rebalances — causing duplicate
            // processing. This is a common production issue.

            sessionTimeout: 30000,   // 30 seconds
            heartbeatInterval: 3000, // 3 seconds

            // 🔍 LEARNING NOTE: maxWaitTimeInMs controls how long the
            // consumer waits for new messages before returning an empty
            // poll response. Lower = more responsive, higher = fewer network calls.
            maxWaitTimeInMs: 5000,

            // 🔍 LEARNING NOTE: retry config for the consumer itself.
            // If the broker is temporarily unreachable, KafkaJS retries
            // with exponential backoff before giving up.
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

            // Subscribe to topic
            // 🔍 LEARNING NOTE: fromBeginning=true means "if this consumer
            // group has never consumed this topic before, start from offset 0"
            // (the oldest available message). If false, it starts from the
            // latest offset (only new messages).
            //
            // For the notification consumer, we want fromBeginning=true so
            // we don't miss any events that were published before the consumer
            // started for the first time.
            //
            // After the first run, the consumer group's committed offset
            // determines where to resume, regardless of this setting.

            await this.consumer.subscribe({
                topic,
                fromBeginning: true
            });

            log.info({ topic }, `📥 Subscribed to topic: ${topic}`);

            // Start consuming messages
            // 🔍 LEARNING NOTE: eachMessage processes ONE message at a time.
            // KafkaJS also offers eachBatch for higher throughput (process
            // multiple messages before committing). We use eachMessage because:
            // 1. Simpler error handling (one message at a time)
            // 2. Better for learning (see each message's lifecycle)
            // 3. Sufficient for our throughput needs
            //
            // autoCommit=false means WE control when offsets are committed.
            // This is critical for at-least-once delivery:
            // - Process message → commit offset → done
            // - If we crash between process and commit → message re-delivered ✅
            // - If we commit before processing → message lost on crash ❌

            await this.consumer.run({
                autoCommit: false,
                eachMessage: async (payload) => {
                    const { topic: msgTopic, partition, message } = payload;
                    const offset = message.offset;
                    const key = message.key?.toString();
                    const startTime = Date.now();

                    try {
                        log.debug(
                            { topic: msgTopic, partition, offset, key },
                            `📩 Processing message: partition=${partition} offset=${offset}`
                        );

                        // Delegate to the handler (notification consumer logic)
                        await handler.handle(payload);

                        // 🔍 LEARNING NOTE: Commit offset AFTER successful processing.
                        // The "+1" is because Kafka expects the NEXT offset to read,
                        // not the offset that was just processed.
                        await this.consumer.commitOffsets([
                            {
                                topic: msgTopic,
                                partition,
                                offset: (BigInt(offset) + 1n).toString(),
                            }
                        ]);

                        const latency = Date.now() - startTime;
                        log.debug(
                            { topic: msgTopic, partition, offset, latencyMs: latency },
                            `✅ Message processed and committed (${latency}ms)`
                        );
                    } catch (err) {
                        const latency = Date.now() - startTime;

                        // 🔍 LEARNING NOTE: We catch errors PER MESSAGE so one bad
                        // message doesn't crash the entire consumer. This is the
                        // "poison pill" defense.
                        //
                        // Without this catch, a single malformed event would crash
                        // the consumer, blocking ALL subsequent events in the partition.
                        //
                        // In Phase 3, we'll send failed messages to a Dead Letter Queue
                        // (DLQ) for manual investigation instead of just logging them.

                        log.error(
                            {
                                err,
                                topic: msgTopic,
                                partition,
                                offset,
                                key,
                                latencyMs: latency,
                            },
                            `❌ Error processing message — skipping`
                        );

                        // Commit the offset anyway to skip the bad message
                        // 🔍 LEARNING NOTE: This means the bad message is lost.
                        // In Phase 3, we'll publish it to a DLQ before skipping.
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
    // 🔍 LEARNING NOTE: Consumer shutdown is MORE complex than HTTP shutdown.
    //
    // Steps:
    // 1. Stop fetching new messages
    // 2. Wait for current message processing to complete
    // 3. Commit final offsets (so we don't re-process on restart)
    // 4. Disconnect from Kafka (triggers consumer group rebalance)
    //
    // If we skip step 3, the consumer restarts and re-processes messages
    // from the last committed offset → duplicate notifications.
    // If we skip step 4, Kafka doesn't know we've left the group until
    // sessionTimeout expires (30 seconds of partition limbo).
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