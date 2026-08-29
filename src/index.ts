// ============================================================
// Entry Point — Application Startup (Phase 3: Transactional Outbox)
// ============================================================
// 🔍 LEARNING NOTE: This is the ONLY file that runs process-level code.
// It creates resources (DB pool, Kafka producer, outbox relay, server) and wires
// everything together.
//
// 🔍 PHASE 3 EVOLUTION:
// The Kafka producer is no longer injected into the OrderService.
// Instead, the OrderService writes events to the outbox table
// (atomically with order data), and the OutboxRelay polls the outbox
// and publishes events to Kafka in the background.
// 
// Startup order matters:
// 1. Load config (validates env vars — fails fast)
// 2. Create DB pool (doesn't connect yet — lazy initialization)
// 3. Connect Kafka producer (for outbox relay)
// 4. Start Outbox Relay (polls outbox → publishes to Kafka)
// 5. Build server (registers routes and plugins)
// 6. Start listening (begins accepting connections)
// 7. Register shutdown handlers
//
// KEY ARCHITECTURE CHANGE:
// The OrderService only writes to Postgres. It has ZERO Kafka awareness.
// The OutboxRelay bridges Postgres → Kafka asynchronously.
// If Kafka is down, events accumulate in the outbox and get published
// when Kafka comes back. The API latency is completely independent of Kafka.

import { config } from "./config/index.js";
import { createPool } from "./db/pool.js";
import { buildServer } from "./server.js";
import { setupGracefulShutdown } from "./shared/shutdown.js";
import { logger } from "./shared/logger.js";
import { createKafkaClient } from "./kafka/client.js";
import { EventProducer } from "./kafka/producer.js";
import { OutboxRepository } from "./outbox/outbox.repository.js";
import { OutboxRelay } from "./outbox/outbox-relay.js";

async function main(): Promise<void> {
  logger.info(
    {
      nodeEnv: config.nodeEnv,
      port: config.port,
      kafkaBrokers: config.kafkaBrokers || '(not configured)',
      notificationDelayMs: config.notificationDelayMs,
      notificationFailureRate: config.notificationFailureRate,
    },
    "🚀 Starting Order Platform...",
  );

  // Step 1: Create database connection pool
  const pool = createPool();

  // Step 2: Verify database connectivity before accepting traffic
  try {
    const result = await pool.query("SELECT NOW() as time");
    logger.info({ dbTime: result.rows[0] }, "✅ Database connection verified");
  } catch (err) {
    logger.fatal({ err }, "❌ Cannot connect to database — exiting");
    process.exit(1);
  }

  // Step 3: Connect Kafka producer (for outbox relay)
  // 🔍 PHASE 3 CHANGE: The producer is no longer injected into OrderService.
  // It's only used by the OutboxRelay to publish events from the outbox table.
  // If Kafka is down, the relay just can't publish — events stay in the outbox.
  let eventProducer: EventProducer | null = null;
  let outboxRelay: OutboxRelay | null = null;

  if (config.kafkaBrokers) {
    try {
      const kafka = createKafkaClient();
      eventProducer = new EventProducer(kafka);
      await eventProducer.connect();

      // Step 4: Start outbox relay
      // 🔍 LEARNING NOTE: The relay needs the RAW Kafka producer (not the
      // EventProducer wrapper) because it sends pre-serialized events.
      // We access the underlying producer through the EventProducer's
      // getProducer() method. But actually, the OutboxRelay uses its own
      // producer.send() — it just needs any Producer instance.
      //
      // For simplicity, we create a second producer for the relay.
      // In production, you might share the producer, but two producers
      // is fine and provides better fault isolation.
      const relayProducer = kafka.producer({ allowAutoTopicCreation: false });
      await relayProducer.connect();

      const outboxRepo = new OutboxRepository(pool);
      outboxRelay = new OutboxRelay(outboxRepo, relayProducer);
      outboxRelay.start();

      logger.info(
        {
          pollIntervalMs: config.outboxPollIntervalMs,
          batchSize: config.outboxBatchSize
        },
        '📤 Outbox relay started'
      );
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  Kafka/Outbox relay failed to start — events will accumulate in outbox'
      );
      // Don't exit — the API still works, events just won't be published
      eventProducer = null;
      outboxRelay = null;
    }
  } else {
    logger.info('ℹ️ Kafka not configured — outbox realy disabled');
  }

  // Step 5: Build and start HTTP server
  // 🔍 PHASE 3 CHANGE: buildServer no longer takes an EventProducer.
  // The OrderService writes to the outbox table directly — it has no
  // Kafka dependency. This is a significant simplification.
  const app = await buildServer(pool);

  try {
    await app.listen({ port: config.port, host: config.host });
    logger.info(
      {
        port: config.port,
        host: config.host,
      },
      `✅ Server listening on http://${config.host}:${config.port}`
    );
  } catch (err) {
    logger.fatal({ err }, "❌ Failed to start server");
    process.exit(1);
  }

  // Step 5: Register graceful shutdown handlers
  setupGracefulShutdown(app, pool, eventProducer, outboxRelay);

  // Log startup summary
  logger.info('─'.repeat(60));
  logger.info('📦 Order Platform — Phase 3 (Reliable Distributed Processing)');
  logger.info(`   Environment:  ${config.nodeEnv}`);
  logger.info(`   Port:         ${config.port}`);
  logger.info(`   Kafka:        ${config.kafkaBrokers || 'NOT CONFIGURED'}`);
  logger.info(`   Notification: ${config.notificationDelayMs}ms delay, ${config.notificationFailureRate * 100}% failure rate`);
  logger.info('');
  logger.info('   Endpoints:');
  logger.info('   POST   /api/v1/orders          — Create order');
  logger.info('   GET    /api/v1/orders           — List orders');
  logger.info('   GET    /api/v1/orders/:id       — Get order');
  logger.info('   PATCH  /api/v1/orders/:id/status — Update status');
  logger.info('   POST   /api/v1/orders/:id/cancel — Cancel order');
  logger.info('   GET    /api/v1/orders/:id/history — Status history');
  logger.info('   GET    /health                  — Health check');
  logger.info('');
  if (outboxRelay) {
    logger.info('   📤 Outbox relay:  ACTIVE (polling every ' + config.outboxPollIntervalMs + 'ms)');
    logger.info(`   📨 Publishing to: ${config.kafkaTopicOrderEvents}`);
    logger.info(`   ☠️  DLQ topic:     ${config.kafkaTopicDlq}`);
  } else {
    logger.info('   ⚠️  Outbox relay: INACTIVE (events accumulating in outbox table)');
  }
  logger.info('─'.repeat(60));
}

main().catch((err) => {
  logger.fatal({ err }, '❌ Unhandled error during startup');
  process.exit(1);
});
