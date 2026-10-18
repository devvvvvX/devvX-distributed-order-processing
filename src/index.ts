// ============================================================
// Entry Point — Application Startup (Phase 5: + SSE Realtime)
// ============================================================
// 🔍 LEARNING NOTE: This is the ONLY file that runs process-level code.
// It creates resources (DB pool, Redis, Kafka, outbox relay, SSE, server) and wires
// everything together.
// 
// 🔍 PHASE 5 EVOLUTION:
// Added Server-Sent Events (SSE) for realtime order tracking.
// The SSE subsystem has THREE components:
//
// 1. SSEManager — holds all active SSE connections, routes events
//    to the correct connections, manages connection lifecycle
//
// 2. SSERedisSubscriber — listens to Redis Pub/Sub channels for
//    order updates, forwards them to SSEManager for fanout
//
// 3. SSE Routes — HTTP endpoint (GET /orders/:id/stream) that
//    opens SSE connections and registers them with SSEManager
//
// Data flow:
//   Kafka → Consumer → Redis PUBLISH → Redis SUBSCRIBE → SSEManager → Client
//
// Startup order matters:
// 1. Load config
// 2. Create DB pool + verify connectivity
// 3. Connect Redis (for rate limiting, caching, locks, leader election)
// 4. Connect Kafka producer (for outbox relay)
// 5. Start leader election (determines which instance runs the relay)
// 6. Create SSE infrastructure (mananger + Redis subscriber)
// 7. Build server (registers routes, rate limiter, SSE endpoint)
// 8. Start listening
// 9. Register shutdown handlers
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
import { SSEManager } from "./sse/sse-manager.js";
import { SSERedisSubscriber } from "./sse/redis-subscriber.js";
import { BackpressurePolicy } from "./sse/types.js";
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
    "🚀 Starting Order Platform (Phase 5)...",
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

  // Step 6: Create SSE infrastructure (Phase 5)
  // 🔍 PHASE 5 ADDITION: SSE is our realtime layer.
  // It requires Redis for cross-instance fanout (Pub/Sub).
  // Without Redis, SSE still works for single-instance deployments —
  // direct event publishing from the order service would work.
  // But cross-instance fanout requires Redis Pub/Sub.
  //
  // Components:
  //   SSEManager — connection registry + fanout
  //   SSERedisSubscriber — Redis Pub/Sub → SSEManager bridge
  //
  // The subscriber creates a DEDICATED Redis connection because
  // Redis requires separate connections for SUBSCRIBE mode.
  let sseManager: SSEManager | null = null;
  let sseSubscriber: SSERedisSubscriber | null = null;

  // SSE manager is always created (handles in-process connections)
  sseManager = new SSEManager({
    maxTotalConnections: config.sseMaxTotalConnections,
    maxConnectionsPerOrder: config.sseMaxConnectionsPerOrder,
    heartbeatIntervalMs: config.sseHeartbeatIntervalMs,
    clientRetryMs: config.sseClientRetryMs,
    backpressureConfig: {
      highWaterMark: config.sseBackpressureHighWaterMark,
      criticalMark: config.sseBackpressureCriticalMark,
      policy: BackpressurePolicy.DROP_NEWEST
    },
  });

  // Redis subscriber for cross-instance fanout
  if (redis && config.redisUrl) {
    try {
      sseSubscriber = new SSERedisSubscriber(
        config.redisUrl,
        sseManager,
        config.redisKeyPrefix
      );

      await sseSubscriber.connect();

      // Wire SSEManager callbacks → Redis subscriber
      // When the first SSE client connects for an order → subscribe to Redis channel
      // When the last SSE client disconnects → unsubscribe from Redis channel
      sseManager.setConnectionHandlers(
        (orderId) => { sseSubscriber!.subscribe(orderId); },
        (orderId) => { sseSubscriber!.unsubscribe(orderId); }
      );

      logger.info('✅ SSE Redis subscriber connected (cross-instance fanout enabled)');
    } catch (err) {
      logger.warn(
        { err },
        '⚠️  SSE Redis subscriber failed — SSE works for in-process events only'
      );
      sseSubscriber = null;
    }
  } else {
    logger.info('ℹ️  Redis not available — SSE cross-instance fanout disabled');
  }

  // Step 7: Build and start HTTP server
  const app = await buildServer(pool, redis, sseManager);

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

  // Step 8: Register graceful shutdown handlers
  setupGracefulShutdown(app, pool, eventProducer, outboxRelay, leaderElection, sseManager);

  // Log startup summary
  logger.info('─'.repeat(60));
  logger.info('📦 Order Platform — Phase 5 (Realtime SSE + Backpressure)');
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
  logger.info('   GET    /api/v1/orders/:id/history  — Status history');
  logger.info('   POST   /api/v1/orders/:id/assign   — Assign driver');
  logger.info('   GET    /api/v1/orders/:id/assignment — Get assignment');
  logger.info('   GET    /api/v1/orders/:id/stream   — SSE realtime stream (Phase 5)');
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
  // Phase 5: SSE status
  if (sseManager) {
    logger.info(`   📡 SSE:             ENABLED`);
    logger.info(`   📡 Max connections: ${config.sseMaxTotalConnections}`);
    logger.info(`   📡 Heartbeat:       ${config.sseHeartbeatIntervalMs}ms`);
    logger.info(`   📡 Redis fanout:    ${sseSubscriber?.connected ? 'ACTIVE' : 'DISABLED'}`);
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
