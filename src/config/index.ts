// ============================================================
// Configuration — Typed, Validated, Fail-Fast
// ============================================================

import { z } from 'zod';
import dotenv from 'dotenv';

// Load .env file in (process.env.) development (no-op if file doesn't exist)
dotenv.config();

const configSchema = z.object({
    // Server
    nodeEnv: z.enum(['development', 'production', 'test']).default('development'),
    port: z.coerce.number().int().positive().default(3000),
    host: z.string().default('0.0.0.0'),

    // Database
    databaseUrl: z.url({ error: "DATABASE_URL must be a valid URL" }),

    // Connection Pool
    dbPoolMin: z.coerce.number().int().min(0).default(2),
    dbPoolMax: z.coerce.number().int().min(1).default(10),
    dbPoolIdleTimeoutMs: z.coerce.number().int().positive().default(30000),
    dbConnectionTimeoutMs: z.coerce.number().int().positive().default(5000),

    // Logging
    logLevel: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),

    // Notification simulation
    notificationDelayMs: z.coerce.number().int().min(0).default(500),
    notificationFailureRate: z.coerce.number().min(0).max(1).default(0),

    // Kafka
    // 🔍 LEARNING NOTE: Kafka config is optional — the Order API can
    // function without Kafka (events just won't be published). This
    // lets us develop and test the HTTP layer independently.
    kafkaBrokers: z.string().default(''), // comma-separated, e.g. "kafka:9092"
    kafkaClientId: z.string().default('order-platform'),
    kafkaConsumerGroupId: z.string().default('notification-service'),
    kafkaTopicOrderEvents: z.string().default('order-events'),

    // Kafka DLQ (Phase 3)
    // 🔍 LEARNING NOTE: Dead Letter Queue topic for events that exhaust
    // all retry attempts. Ops team monitors this topic for investigation.
    kafkaTopicDlq: z.string().default('order-events-dlq'),

    // Consumer retry (Phase 3)
    // 🔍 LEARNING NOTE: Exponential backoff with jitter prevents the
    // "thundering herd" problem where all consumers retry simultaneously
    // after a downstream outage, overwhelming the recovering service.
    consumerMaxRetries: z.coerce.number().int().min(0).default(3),
    consumerRetryBaseDelayMs: z.coerce.number().int().min(100).default(1000),

    // Outbox relay (Phase 3)
    // 🔍 LEARNING NOTE: The relay polls the outbox table at this interval.
    // Lower = faster event delivery, but more DB load.
    // Higher = less DB load, but events sit in outbox longer.
    // 1 second is a good balance for most use cases.
    outboxPollIntervalMs: z.coerce.number().int().min(100).default(1000),
    outboxBatchSize: z.coerce.number().int().min(1).default(50),

    // Graceful Shutdown
    shutdownTimeoutMs: z.coerce.number().int().positive().default(10000),
});

export type Config = z.infer<typeof configSchema>;

function loadConfig(): Config {
    const result = configSchema.safeParse({
        nodeEnv: process.env['NODE_ENV'],
        port: process.env['PORT'],
        host: process.env['HOST'],
        databaseUrl: process.env['DATABASE_URL'],
        dbPoolMin: process.env['DB_POOL_MIN'],
        dbPoolMax: process.env['DB_POOL_MAX'],
        dbPoolIdleTimeoutMs: process.env['DB_POOL_IDLE_TIMEOUT_MS'],
        dbConnectionTimeoutMs: process.env['DB_CONNECTION_TIMEOUT_MS'],
        logLevel: process.env['LOG_LEVEL'],
        notificationDelayMs: process.env['NOTIFICATION_DELAY_MS'],
        notificationFailureRate: process.env['NOTIFICATION_FAILURE_RATE'],
        kafkaBrokers: process.env['KAFKA_BROKERS'],
        kafkaClientId: process.env['KAFKA_CLIENT_ID'],
        kafkaConsumerGroupId: process.env['KAFKA_CONSUMER_GROUP_ID'],
        kafkaTopicOrderEvents: process.env['KAFKA_TOPIC_ORDER_EVENTS'],
        kafkaTopicDlq: process.env['KAFKA_TOPIC_DLQ'],
        consumerMaxRetries: process.env['CONSUMER_MAX_RETRIES'],
        consumerRetryBaseDelayMs: process.env['CONSUMER_RETRY_BASE_DELAY_MS'],
        outboxPollIntervalMs: process.env['OUTBOX_POLL_INTERVAL_MS'],
        outboxBatchSize: process.env['OUTBOX_BATCH_SIZE'],
        shutdownTimeoutMs: process.env['SHUTDOWN_TIMEOUT_MS'],
    });

    // Fail LOUD and CLEAR at startup
    if (!result.success) {
        console.error('❌ Invalid configuration');
        console.error(z.treeifyError(result.error));
        process.exit(1);
    }

    return result.data;
}

// Singleton — loaded once at startup
export const config = loadConfig();