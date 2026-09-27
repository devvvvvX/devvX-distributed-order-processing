// ============================================================
// Entry Point — Application Startup (Phase 4: Redis Coordination)
// ============================================================
// 🔍 LEARNING NOTE: This is the ONLY file that runs process-level code.
// It creates resources (DB pool, Redis, Kafka, outbox relay, server) and wires
// everything together.
//
// 🔍 PHASE 4 EVOLUTION:
// Added Redis as coordination layer. Redis is OPTIONAL — the app
// gracefully degrades without it (no rate limiting, no caching,
// no distributed locks, no leader election).
// 
// Startup order matters:
// 1. Load config (validates env vars — fails fast)
// 2. Create DB pool (doesn't connect yet — lazy initialization)
// 3. Connect Redis (for rate limiting, caching, locks, leader election)
// 4. Connect Kafka producer (for outbox relay)
// 5. Start leader election (determines which instance runs the relay)
// 6. Build server (registers routes, rate limiter, and plugins)
// 7. Start listening (begins accepting connections)
// 8. Register shutdown handlers
//
// KEY PHASE 4 ARCHITECTURE CHANGE:
// The outbox relay is now controlled by LEADER ELECTION.
// Only the leader instance runs the relay — followers are idle.
// If the leader dies, a follower acquires the lease within 15 seconds
// and starts the relay. During the gap, events accumulate safely.

import { config } from "./config/index.js";
import { createPool } from "./db/pool.js";
import { buildServer } from "./server.js";
import { setupGracefulShutdown } from "./shared/shutdown.js";
import { logger } from "./shared/logger.js";
import { createKafkaClient } from "./kafka/client.js";
import { EventProducer } from "./kafka/producer.js";
import { OutboxRepository } from "./outbox/outbox.repository.js";
import { OutboxRelay } from "./outbox/outbox-relay.js";
import { createRedisClient } from "./redis/client.js";
import { LeaderElection } from './redis/leader-election.js';
import Redis from 'ioredis';

async function main(): Promise<void> {
  logger.info(
    {
      nodeEnv: config.nodeEnv,
      port: config.port,
      kafkaBrokers: config.kafkaBrokers || '(not configured)',
      redisUrl: config.redisUrl || '(not configured)',
      notificationDelayMs: config.notificationDelayMs,
      notificationFailureRate: config.notificationFailureRate,
    },
    "🚀 Starting Order Platform (Phase 4)...",
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

  // Step 3: Connect Redis (optional)
  // 🔍 PHASE 4 ADDITION: Redis provides:
  // - Rate limiting (protect API from abuse)
  // - Distributed locks (prevent concurrent status update races)
  // - Caching (reduce DB load for hot-path queries)
  // - Leader election (only one relay instance)
  // - Order assignment (atomic driver coordination)
  //
  // If Redis is not configured or connection fails, the app still works.
  // This is GRACEFUL DEGRADATION — a key production pattern.
  let redis: Redis | null = null;
  let leaderElection: LeaderElection | null = null;

  if (config.redisUrl) {
    try {
      redis = createRedisClient();
      // Verify connectivity with a PING
      // 🔍 LEARNING NOTE: Since enableOfflineQueue is false, ping() fails immediately
      // if the connection isn't fully established yet. We retry for a few seconds.
      let connected = false;
      for (let i = 0; i < 10; i++) {
        try {
          await redis.ping();
          connected = true;
          break;
        } catch (e) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }
      
      if (!connected) {
        throw new Error('Redis connection timeout during startup');
      }
      
      logger.info('✅ Redis connected');
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  Redis connection failed — running without Redis (degraded mode)'
      );
      redis = null;
    }
  } else {
    logger.info('ℹ️  Redis not configured — coordination features disabled');
  }

  // Step 4: Connect Kafka producer (for outbox relay)
  let eventProducer: EventProducer | null = null;
  let outboxRelay: OutboxRelay | null = null;
  let relayProducer: ReturnType<ReturnType<typeof createKafkaClient>['producer']> | null = null;

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
      relayProducer = kafka.producer({ allowAutoTopicCreation: false });
      await relayProducer.connect();

      const outboxRepo = new OutboxRepository(pool);
      outboxRelay = new OutboxRelay(outboxRepo, relayProducer);

      // Step 5: Leader election for outbox relay (Phase 4)
      // 🔍 PHASE 4 CHANGE: The relay is NO LONGER started immediately.
      // Instead, leader election controls which instance runs it.
      //
      // WITH REDIS: Only the leader instance runs the relay.
      //   Leader acquired → outboxRelay.start()
      //   Leadership lost → outboxRelay.stop()
      //
      // WITHOUT REDIS: Every instance runs the relay (Phase 3 behavior).
      //   Duplicates are handled by consumer idempotency.
      if (redis) {
        leaderElection = new LeaderElection(redis);
        leaderElection.start(
          // onBecomeLeader
          () => {
            logger.info('👑 This instance is now the LEADER — starting outbox relay');
            outboxRelay!.start();
          },

          // onLoseLeadership
          () => {
            logger.warn('🏳️  This instance LOST leadership — stopping outbox relay');
            outboxRelay!.stop();
          }
        );
      } else {
        // No Redis → start relay directly (Phase 3 behavior)
        outboxRelay.start();

        logger.info(
          {
            pollIntervalMs: config.outboxPollIntervalMs,
            batchSize: config.outboxBatchSize,
          },
          '📤 Outbox relay started (no leader election — all instances relay)'
        );
      }
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  Kafka/Outbox relay failed to start — events will accumulate in outbox'
      );
      eventProducer = null;
      outboxRelay = null;
    }
  } else {
    logger.info('ℹ️ Kafka not configured — outbox realy disabled');
  }

  // Step 6: Build and start HTTP server
  // 🔍 PHASE 4 CHANGE: buildServer now accepts Redis for rate limiting,
  // caching, distributed locks, and order assignment.
  const app = await buildServer(pool, redis);

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

  // Step 7: Register graceful shutdown handlers
  setupGracefulShutdown(app, pool, eventProducer, outboxRelay, leaderElection);

  // Log startup summary
  logger.info('─'.repeat(60));
  logger.info('📦 Order Platform — Phase 4 (Distributed Coordination)');
  logger.info(`   Environment:  ${config.nodeEnv}`);
  logger.info(`   Port:         ${config.port}`);
  logger.info(`   Kafka:        ${config.kafkaBrokers || 'NOT CONFIGURED'}`);
  logger.info(`   Redis:        ${config.redisUrl || 'NOT CONFIGURED'}`);
  logger.info('');
  logger.info('   Endpoints:');
  logger.info('   POST   /api/v1/orders             — Create order');
  logger.info('   GET    /api/v1/orders              — List orders');
  logger.info('   GET    /api/v1/orders/:id          — Get order (cached)');
  logger.info('   PATCH  /api/v1/orders/:id/status   — Update status (locked)');
  logger.info('   POST   /api/v1/orders/:id/cancel   — Cancel order');
  logger.info('   GET    /api/v1/orders/:id/history  — Status history');
  logger.info('   POST   /api/v1/orders/:id/assign   — Assign driver (Phase 4)');
  logger.info('   GET    /api/v1/orders/:id/assignment — Get assignment (Phase 4)');
  logger.info('   GET    /health                     — Health check');
  logger.info('');
  if (redis) {
    logger.info('   🔴 Redis:           CONNECTED');
    logger.info(`   🚦 Rate limiting:   ${config.rateLimitMaxRequests} req/${config.rateLimitWindowMs / 1000}s`);
    logger.info(`   📦 Cache TTL:       ${config.cacheOrderTtlSeconds}s`);
    logger.info(`   🔒 Distributed lock: ENABLED`);
    logger.info(`   🗳️  Leader election: ${leaderElection?.isLeader ? 'LEADER' : 'FOLLOWER'}`);
  } else {
    logger.info('   ⚠️  Redis:           NOT CONNECTED (degraded mode)');
    logger.info('   🚦 Rate limiting:   DISABLED');
    logger.info('   📦 Cache:           DISABLED');
    logger.info('   🔒 Distributed lock: DISABLED');
  }
  logger.info('');
  if (outboxRelay) {
    const relayStatus = leaderElection
      ? (leaderElection.isLeader ? 'ACTIVE (leader)' : 'STANDBY (follower)')
      : 'ACTIVE (no leader election)';
    logger.info(`   📤 Outbox relay:  ${relayStatus}`);
    logger.info(`   📨 Publishing to: ${config.kafkaTopicOrderEvents}`);
    logger.info(`   ☠️  DLQ topic:     ${config.kafkaTopicDlq}`);
  } else {
    logger.info('   ⚠️  Outbox relay: INACTIVE');
  }
  logger.info('─'.repeat(60));
}

main().catch((err) => {
  logger.fatal({ err }, '❌ Unhandled error during startup');
  process.exit(1);
});
