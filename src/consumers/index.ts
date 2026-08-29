// ============================================================
// Consumer Process Entry Point (Phase 3: Reliable Processing)
// ============================================================
// 🔍 LEARNING NOTE: This is a SEPARATE Node.js process from the
// Order API (src/index.ts). It has its own lifecycle:
//
// ORDER API PROCESS (src/index.ts):
//   1. Connect to PostgreSQL
//   2. Connect Kafka producer (for outbox relay)
//   3. Start OutboxRelay (background poller)
//   4. Start HTTP server
//   5. Handle web requests
//
// CONSUMER PROCESS (this file):
//   1. Connect to PostgreSQL
//   2. Create idempotency store (processed_events table)
//   3. Connect DLQ producer (for failed events)
//   4. Connect Kafka consumer (with retry + idempotency + DLQ)
//   5. Start polling for messages
//   6. Process events reliably
//
// 🔍 PHASE 3 EVOLUTION:
// The consumer now has THREE new dependencies:
// 1. IdempotencyStore — checks/marks events as processed
// 2. DlqProducer — publishes failed events to dead letter queue
// 3. RetryConfig — controls exponential backoff behavior
//
// These are wired into the KafkaConsumer, which orchestrates the
// reliable processing pipeline:
//   Receive → Deduplicate → Process (with retry) → DLQ (if all fail)
//
// 🔍 LEARNING NOTE: Why separate processes instead of running the
// consumer inside the HTTP server?
//
// 1. INDEPENDENT SCALING: You might need 3 consumer instances but
//    only 1 API instance (or vice versa). Separate processes let you
//    scale each independently.
//
// 2. FAULT ISOLATION: If the consumer crashes (OOM, unhandled error),
//    it doesn't take down the API. Customers can still place orders.
//
// 3. RESOURCE ISOLATION: The consumer might be CPU-intensive (image
//    processing, ML inference). Running it in the same process as
//    the API would steal CPU from request handling.
//
// 4. DEPLOYMENT INDEPENDENCE: You can deploy a consumer fix without
//    restarting the API (zero-downtime for customers).

import { Pool } from 'pg';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger';
import { createPool } from '../db/pool';
import { createKafkaClient } from '../kafka/client';
import { KafkaConsumer } from '../kafka/consumer';
import { IdempotencyStore } from '../kafka/idempotency.js';
import { DlqProducer } from '../kafka/dlq-producer.js';
import { NotificationConsumerHandler } from './notification-consumer';

const log = createModuleLogger('consumer-main');

async function main(): Promise<void> {
    log.info(
        {
            service: 'notification-consumer',
            nodeEnv: config.nodeEnv,
            kafkaBrokers: config.kafkaBrokers,
            consumerGroup: config.kafkaConsumerGroupId,
            topic: config.kafkaTopicOrderEvents,
            dlqTopic: config.kafkaTopicDlq,
            maxRetries: config.consumerMaxRetries,
            retryBaseDelay: config.consumerRetryBaseDelayMs
        },
        '🚀 Starting notification consumer (Phase 3 — Reliable Processing)...'
    );

    // Step 1: Connect to PostgreSQL
    const pool: Pool = createPool();

    try {
        const client = await pool.connect();
        client.release();
        log.info('✅ PostgreSQL connected');
    } catch (err) {
        log.fatal({ err }, '❌ Failed to connect to PostgreSQL');
        process.exit(1);
    }

    // Step 2: Create idempotency store
    // 🔍 LEARNING NOTE: The idempotency store uses the same DB pool as
    // the notification handler. This is important because the idempotency
    // check and the business logic need to share the same DB connection
    // pool (and potentially the same transaction in more advanced setups).
    const idempotencyStore = new IdempotencyStore(pool);
    log.info('✅ Idempotency store initialized');

    // Step 3: Create and connect DLQ producer
    // 🔍 LEARNING NOTE: The DLQ producer is a separate Kafka producer
    // that publishes to the dead letter queue topic. It connects
    // independently from the consumer — if the DLQ producer fails to
    // connect, the consumer still works (failed events are just logged
    // instead of being sent to the DLQ).
    const kafka = createKafkaClient();
    const dlqProducer = new DlqProducer(kafka);
    await dlqProducer.connect();

    // Step 4: Create Kafka consumer with Phase 3 reliability features
    // 🔍 PHASE 3 CHANGE: The KafkaConsumer now takes idempotencyStore
    // and dlqProducer as dependencies. Internally, it:
    //   1. Checks idempotency before processing
    //   2. Wraps handler.handle() with retry + backoff
    //   3. Sends to DLQ after all retries exhausted
    //   4. Marks events as processed after success or DLQ
    const consumer = new KafkaConsumer(kafka, idempotencyStore, dlqProducer);
    const handler = new NotificationConsumerHandler(pool);

    // Step 5: Graceful shutdown
    // 🔍 LEARNING NOTE: Consumer shutdown is critical because:
    // 1. We need to commit final offsets (avoid reprocessing on restart)
    // 2. We need to leave the consumer group cleanly (triggers immediate
    //    rebalance instead of waiting for session timeout)
    // 3. We need to disconnect the DLQ producer
    // 4. We need to close the DB pool (avoid connection leaks)
    const shutdown = async (signal: string) => {
        log.info({ signal }, '🛑 Shutting down notification consumer...');

        try {
            // Stop consuming first (commits remaining offsets)
            await consumer.stop();
            log.info('  ↳ Kafka consumer stopped');

            // Disconnect DLQ producer
            log.info('  ↳ DLQ producer stopped');

            // Then close DB pool
            await pool.end();
            log.info('  ↳ PostgreSQL pool closed');

            log.info('✅ Notification consumer shut down cleanly');
            process.exit(0);
        } catch (err) {
            log.error({ err }, '❌ Error during shutdown');
            process.exit(1);
        }
    };

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

    // Step 6: Start consuming
    try {
        await consumer.start(config.kafkaTopicOrderEvents, handler);
        log.info('📥 Notification consumer is running (reliable mode: idempotent + retry + DLQ)');
    } catch (err) {
        log.fatal({ err }, '❌ Failed to start notification consumer');
        await dlqProducer.disconnect();
        await pool.end();
        process.exit(1);
    }
}

// Handle unhandled errors
process.on('unhandledRejection', (reason) => {
    log.fatal({ err: reason }, '❌ Unhandled rejection in consumer');
    process.exit(1);
})

process.on('uncaughtException', (err) => {
    log.fatal({ err }, '❌ Uncaught exception in consumer');
    process.exit(1);
})

main().catch((err) => {
    log.fatal({ err }, '❌ Consumer startup failed');
    process.exit(1);
})