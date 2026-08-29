// ============================================================
// Graceful Shutdown (Phase 3: + Outbox Relay)
// ============================================================
// 🔍 LEARNING NOTE: Graceful shutdown is one of the most important
// production patterns, and one of the most commonly overlooked.
//
// What happens WITHOUT graceful shutdown:
// 1. Kubernetes sends SIGTERM to your pod during a rolling deploy
// 2. Your process immediately exits (process.exit or unhandled signal)
// 3. All in-flight HTTP requests get TCP RST (connection reset)
// 4. Customers see "connection refused" or partial responses
// 5. Database transactions are left half-committed
// 6. You get paged at 3 AM
//
// What happens WITH graceful shutdown:
// 1. SIGTERM received
// 2. Stop accepting NEW connections (server.close())
// 3. Wait for in-flight requests to complete (with timeout)
// 4. Stop outbox relay (flush any in-progress batch)
// 5. Close database connections cleanly
// 6. Exit with code 0
//
// 🔍 PHASE 3 EVOLUTION:
// Added OutboxRelay to the shutdown sequence. The relay must stop
// BEFORE the Kafka producer disconnects, because it uses the producer
// to publish events. And the producer must disconnect BEFORE the DB
// pool closes, because the relay reads from the DB.
//
// Shutdown order:
//   HTTP server → Outbox relay → Kafka producer → DB pool
// (reverse of startup order — a general best practice)

import { FastifyInstance } from "fastify";
import { Pool } from "pg";
import { createModuleLogger } from "./logger.js";
import { config } from "../config";
import { EventProducer } from "../kafka/producer.js";
import { OutboxRelay } from "../outbox/outbox-relay.js";

const log = createModuleLogger("shutdown");

export function setupGracefulShutdown(
  app: FastifyInstance,
  pool: Pool,
  eventProducer: EventProducer | null = null,
  outboxRelay: OutboxRelay | null = null
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
      // Step 1: Stop accepting new HTTP connections
      // In-flight requests will still be processed.
      log.info("Closing HTTP Server (no new connections)...");
      await app.close();
      log.info("✅ HTTP server closed");

      // Step 2: Stop outbox relay (wait for in-progress batch)
      // 🔍 PHASE 3 ADDITION: Stop the relay BEFORE disconnecting
      // the Kafka producer, because the relay needs the producer
      // to finish publishing any in-progress batch.
      if (outboxRelay) {
        log.info('Stop outbox relay...');
        await outboxRelay.stop();
        log.info('✅ Outbox relay stopped');
      }

      // Step 3: Disconnect Kafka producer (flush pending events)
      // 🔍 LEARNING NOTE: The producer's disconnect() flushes its
      // internal buffer, ensuring no events are lost. This must happen
      // BEFORE closing the DB pool because the producer might still
      // be serializing events that reference DB data.
      if (eventProducer) {
        log.info('Disconnecting Kafka producer...');
        await eventProducer.disconnect();
        log.info('✅ Kafka producer disconnected');
      }

      // Step 4: Close database pool
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
