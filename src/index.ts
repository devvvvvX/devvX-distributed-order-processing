// ============================================================
// Entry Point — Application Startup (Phase 2: Event-Driven)
// ============================================================
// 🔍 LEARNING NOTE: This is the ONLY file that runs process-level code.
// It creates resources (DB pool, Kafka producer, server) and wires
// everything together.
//
// Startup order matters:
// 1. Load config (validates env vars — fails fast)
// 2. Create DB pool (doesn't connect yet — lazy initialization)
// 3. Connect Kafka producer (if configured)
// 4. Build server (registers routes and plugins)
// 5. Start listening (begins accepting connections)
// 6. Register shutdown handlers
//
// 🔍 PHASE 2 EVOLUTION:
// Added Kafka producer initialization between DB and server startup.
// The producer is OPTIONAL — if Kafka is not configured, the app
// still works (orders are created, just no events published).
// This is graceful degradation: core function works, async features don't.

import { config } from "./config/index.js";
import { createPool } from "./db/pool.js";
import { buildServer } from "./server.js";
import { setupGracefulShutdown } from "./shared/shutdown.js";
import { logger } from "./shared/logger.js";
import { createKafkaClient } from "./kafka/client.js";
import { EventProducer } from "./kafka/producer.js";

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
  // 🔍 LEARNING NOTE: Don't start accepting HTTP requests until
  // you've verified the database is reachable. Otherwise:
  // - Health check passes (server is up)
  // - Load balancer sends traffic
  // - Every request fails with "connection refused"
  // - Customer-facing 500 errors
  try {
    const result = await pool.query("SELECT NOW() as time");
    logger.info({ dbTime: result.rows[0] }, "✅ Database connection verified");
  } catch (err) {
    logger.fatal({ err }, "❌ Cannot connect to database — exiting");
    process.exit(1);
  }

  // Step 3: Connect Kafka producer (if configured)
  // 🔍 LEARNING NOTE: Kafka connection is NOT required for startup.
  // If Kafka is unavailable, the app still starts and serves HTTP requests.
  // Orders will be created in the DB, but events won't be published.
  //
  // This is a critical production pattern: your API should not refuse to
  // start just because a downstream dependency (Kafka, Redis, etc.) is down.
  // Serve what you can, degrade gracefully, and let monitoring alert you.
  let eventProducer: EventProducer | null = null;

  if (config.kafkaBrokers) {
    try {
      const kafka = createKafkaClient();
      eventProducer = new EventProducer(kafka);
      await eventProducer.connect();
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  Kafka producer failed to connect — events will NOT be published'
      );

      // Don't exit — continue without Kafka
      eventProducer = null;
    }
  } else {
    logger.info('ℹ️ Kafka not configured — running without event publishing');
  }

  // Step 4: Build and start HTTP server
  const app = await buildServer(pool, eventProducer);

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
  setupGracefulShutdown(app, pool, eventProducer);

  // Log startup summary
  logger.info('─'.repeat(60));
  logger.info('📦 Order Platform — Phase 2 (Event-Driven)');
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
  if (eventProducer) {
    logger.info('   📤 Kafka producer: CONNECTED');
    logger.info(`   📨 Publishing to: ${config.kafkaTopicOrderEvents}`);
  } else {
    logger.info('   ⚠️  Kafka producer: DISCONNECTED (events not published)');
  }
}

main().catch((err) => {
  logger.fatal({ err }, '❌ Unhandled error during startup');
  process.exit(1);
});
