// ============================================================
// Graceful Shutdown (Phase 5: + SSE Connections)
// ============================================================
// 🔍 LEARNING NOTE: Graceful shutdown is one of the most important
// production patterns, and one of the most commonly overlooked.
//
// 🔍 PHASE 5 EVOLUTION:
// Added SSE connection draining and Redis Pub/Sub subscriber cleanup.
// 
// Shutdown order (reverse of startup):
//   1. SSE connections → drain all (send 'shutdown' event to clients)
//   2. HTTP server → stop accepting new connections
//   3. Leader election → release leadership (follower takes over relay)
//   4. Outbox relay → stop polling, flush in-progress batch
//   5. Kafka producer → flush pending events
//   6. SSE Redis subscriber → unsubscribe all, disconnect
//   7. Redis (main) → disconnect
//   8. DB pool → close connections
//
// The leader election MUST be stopped BEFORE the outbox relay,
// because releasing leadership triggers the relay stop callback.
// Redis MUST be disconnected AFTER leader election stops (it needs
// Redis to release the lease).
// 
// KEY PHASE 5 INSIGHT: SSE must be drained BEFORE closing the HTTP server.
// Why? Because closing the HTTP server immediately RST-closes all
// active connections. If we close HTTP first, SSE clients see an
// abrupt connection drop with no explanation. By draining first,
// clients receive a clean 'shutdown' event and can reconnect gracefully.

import { FastifyInstance } from "fastify";
import { Pool } from "pg";
import { createModuleLogger } from "./logger.js";
import { config } from "../config";
import { EventProducer } from "../kafka/producer.js";
import { OutboxRelay } from "../outbox/outbox-relay.js";
import { LeaderElection } from "../redis/leader-election.js";
import { disconnectRedis } from "../redis/client.js";
import { SSEManager } from "../sse/sse-manager.js";
import { SSERedisSubscriber } from "../sse/redis-subscriber.js";

const log = createModuleLogger("shutdown");

export function setupGracefulShutdown(
  app: FastifyInstance,
  pool: Pool,
  eventProducer: EventProducer | null = null,
  outboxRelay: OutboxRelay | null = null,
  leaderElection: LeaderElection | null = null,
  sseManager: SSEManager | null = null,
  sseSubscriber: SSERedisSubscriber | null = null
): void {
  let isShuttingDown = false;

  async function shutdown(signal: string): Promise<void> {
    // 🔍 LEARNING NOTE: Guard against multiple signals.
    // SIGTERM + SIGINT can arrive nearly simultaneously.
    // Without this guard, you'd run cleanup twice.
    if (isShuttingDown) {
      log.warn({ signal }, "Shutdown already in process, ignoring signal");

      return;
    }

    isShuttingDown = true;
    log.info({ signal }, "🔄 Graceful shutdown initiated");

    // Force exit after timeout — the safety net
    const forceExitTimer = setTimeout(() => {
      log.error("❌ Graceful shutdown timed out, forcing exit");
      process.exit(1);
    }, config.shutdownTimeoutMs);

    // 🔍 LEARNING NOTE: unref() prevents this timer from keeping
    // the process alive. If everything shuts down cleanly before
    // the timeout, the process can exit without waiting for the timer.
    forceExitTimer.unref();

    try {
      // Step 1: Drain SSE connections (Phase 5)
      // 🔍 PHASE 5 ADDITION: Send 'shutdown' event to all SSE clients.
      // This gives clients a chance to show "reconnecting..." UI
      // instead of an error. EventSource auto-reconnection will kick in
      // and the client will land on another instance (if available).
      if (sseManager) {
        log.info('Draining SSE connections...');
        await sseManager.shutdown();
        log.info('✅ SSE connections drained');
      }

      // Step 2: Stop accepting new HTTP connections
      // In-flight requests will still be processed.
      log.info("Closing HTTP Server (no new connections)...");
      await app.close();
      log.info("✅ HTTP server closed");

      // Step 3: Stop leader election (releases lease → follower takes over)
      // 🔍 PHASE 4 ADDITION: This must happen BEFORE stopping the relay.
      // Releasing the lease tells other instances they can become leader.
      // The onLoseLeadership callback will stop the relay.
      if (leaderElection) {
        log.info('Stopping leader election...');
        await leaderElection.stop();
        log.info('✅ Leader election stopped');
      }

      // Step 4: Stop outbox relay (if still running - might already be stopped
      // be stopped by leadership loss callback)
      if (outboxRelay) {
        log.info('Stopping outbox relay...');
        await outboxRelay.stop();
        log.info('✅ Outbox relay stopped');
      }

      // Step 5: Disconnect Kafka producer (flush pending events)
      // 🔍 LEARNING NOTE: The producer's disconnect() flushes its
      // internal buffer, ensuring no events are lost. This must happen
      // BEFORE closing the DB pool because the producer might still
      // be serializing events that reference DB data.
      if (eventProducer) {
        log.info('Disconnecting Kafka producer...');
        await eventProducer.disconnect();
        log.info('✅ Kafka producer disconnected');
      }

      // Step 6: Disconnect SSE Redis subscriber (Phase 5)
      // 🔍 PHASE 5 ADDITION: The subscriber has its OWN Redis connection
      // (separate from the main Redis client used for caching/locks).
      // We disconnect it before the main Redis connection.
      if (sseSubscriber) {
        log.info('Disconnecting SSE Redis subscriber...');
        await sseSubscriber.disconnect();
        log.info('✅ SSE Redis subscriber disconnected');
      }

      // Step 7: Disconnect Redis (main)
      log.info('Disconnecting Redis...');
      await disconnectRedis();
      log.info('✅ Redis disconnected');

      // Step 8: Close database pool
      // This waits for active queries to finish, then closes connections.
      log.info("Closing database pool...");
      await pool.end();
      log.info("✅ Database pool closed");

      log.info("✅ Graceful shutdown complete");
      process.exit(0);
    } catch (err) {
      log.error({ err }, "❌ Error during graceful shutdown");
      process.exit(1);
    }
  }

  // 🔍 LEARNING NOTE:
  // SIGTERM — Sent by Kubernetes/Docker during normal shutdown.
  //           "Please terminate gracefully."
  // SIGINT  — Sent when you press Ctrl+C in terminal.
  //           Same handling, different trigger.
  // SIGKILL — Unblockable. Can't register a handler.
  //           Kubernetes sends this after terminationGracePeriodSeconds.

  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));

  // 🔍 LEARNING NOTE: Unhandled rejections and uncaught exceptions
  // are BUGS, not operational errors. Log them and crash.
  // A process in an unknown state is more dangerous than a crashed process.
  // Kubernetes will restart it. Let it crash, fix the bug.
  process.on("unhandledRejection", (reason) => {
    log.fatal({ err: reason }, "Unhandled promise rejection — crashing");
    process.exit(1);
  });

  process.on("uncaughtException", (err) => {
    log.fatal({ err }, "Uncaught exception — crashing");
    process.exit(1);
  });
}
