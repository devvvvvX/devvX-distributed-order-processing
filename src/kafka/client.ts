// ============================================================
// Kafka Client Factory
// ============================================================
// 🔍 LEARNING NOTE: KafkaJS is the standard Kafka client for Node.js.
//
// Key concepts:
// - BROKER: A Kafka server. Messages are stored on brokers.
// - CLIENT: Your application's connection to the Kafka cluster.
// - CLIENT ID: Identifies your application in Kafka logs/monitoring.
//   When debugging "who's producing these events?", the client ID
//   shows up in Kafka's server logs.
//
// In production, you'd typically have:
// - 3+ brokers for fault tolerance (replication)
// - SSL/SASL authentication (don't let anyone publish to your topics)
// - Separate clients for producers and consumers
//
// For development, a single broker with no auth is fine.

import { Kafka, logLevel as KafkaLogLevel } from 'kafkajs';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger';

const log = createModuleLogger('kafka-client');

// 🔍 LEARNING NOTE: Map our log levels to KafkaJS log levels.
// KafkaJS has its own logging system. We bridge it to our Pino logger
// so all logs appear in the same stream with the same format.

const kafkaLogLevelMap: Record<string, number> = {
    fatal: KafkaLogLevel.NOTHING,
    error: KafkaLogLevel.ERROR,
    warn: KafkaLogLevel.WARN,
    info: KafkaLogLevel.INFO,
    debug: KafkaLogLevel.DEBUG,
    trace: KafkaLogLevel.DEBUG,
};

export function createKafkaClient(): Kafka {
    const brokers = config.kafkaBrokers
        .split(',')
        .map((b) => b.trim())
        .filter((b) => b.length > 0);


    if (brokers.length === 0) {
        log.warn('No Kafka brokers configured - Kafka features will be disabled');

        // Return a client with dummy broker - it will fail on connect,
        // which callers should handle gracefully.
        return new Kafka({
            clientId: config.kafkaClientId,
            brokers: ['localhost:9092'],
            logLevel: KafkaLogLevel.WARN
        })
    }


    log.info(
        { clientId: config.kafkaClientId, brokers },
        '📡 Creating Kafka client'
    );

    return new Kafka({
        clientId: config.kafkaClientId,
        brokers,

        // 🔍 LEARNING NOTE: Retry config for initial connection.
        // If Kafka isn't ready when the app starts (common in Docker),
        // KafkaJS retries with exponential backoff.
        retry: {
            initialRetryTime: 300,
            retries: 300,
            maxRetryTime: 30000
        },

        // Bridge KafkaJs logs to our Pino logger
        logLevel: kafkaLogLevelMap[config.logLevel] ?? kafkaLogLevelMap.WARN,
        logCreator: () => {
            return ({ log: logEntry }) => {
                const { message, ...extra } = logEntry;
                log.debug({ ...extra }, `kafka: ${message}`);
            }
        }
    });
}