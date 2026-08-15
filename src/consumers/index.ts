// ============================================================
// Consumer Process Entry Point
// ============================================================
// 🔍 LEARNING NOTE: This is a SEPARATE Node.js process from the
// Order API (src/index.ts). It has its own lifecycle:
//
// ORDER API PROCESS (src/index.ts):
//   1. Connect to PostgreSQL
//   2. Connect Kafka producer
//   3. Start HTTP server
//   4. Handle web requests
//
// CONSUMER PROCESS (this file):
//   1. Connect to PostgreSQL
//   2. Connect Kafka consumer
//   3. Start polling for messages
//   4. Process events asynchronously
//
// In Docker Compose, these are separate containers running the same
// Docker image but with different CMD commands:
//   app:                    node dist/index.js       (HTTP server)
//   notification-consumer:  node dist/consumers/index.js (Kafka consumer)
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
        },
        '🚀 Starting notification consumer...'
    );

    // Step 1: Connect to PostgreSQL (notification table)
    const pool: Pool = createPool();

    try {
        const client = await pool.connect();
        client.release();
        log.info('✅ PostgreSQL connected');
    } catch (err) {
        log.fatal({ err }, '❌ Failed to connect to PostgreSQL');
        process.exit(1);
    }

    // Step 2: Create Kafka consumer
    const kafka = createKafkaClient();
    const consumer = new KafkaConsumer(kafka);
    const handler = new NotificationConsumerHandler(pool);

    // Step 3: Graceful shutdown
    // 🔍 LEARNING NOTE: Consumer shutdown is critical because:
    // 1. We need to commit final offsets (avoid reprocessing on restart)
    // 2. We need to leave the consumer group cleanly (avoid session timeout delay)
    // 3. We need to close the DB pool (avoid connection leaks)

    const shutdown = async (signal: string) => {
        log.info({ signal }, '🛑 Shutting down notification consumer...');

        try {
            // Stop consuming first (commits remaining offsets)
            await consumer.stop();
            log.info('  ↳ Kafka consumer stopped');

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

    // Step 4: Start consuming
    // 🔍 LEARNING NOTE: consumer.start() is a long-running operation.
    // It enters a poll loop that continues until the consumer is stopped.
    // This is why we don't await it in the same way as an HTTP server —
    // the consumer loop IS the main execution path.
    try {
        await consumer.start(config.kafkaTopicOrderEvents, handler);
        log.info('📥 Notification consumer is running and processing events');
    } catch (err) {
        log.fatal({ err }, '❌ Failed to start notification consumer');
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