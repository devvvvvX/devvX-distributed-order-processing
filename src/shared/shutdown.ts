// ============================================================
// Graceful Shutdown
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
// 4. Close database connections cleanly
// 5. Exit with code 0
//
// The timeout is critical: if an in-flight request is stuck (deadlock,
// slow query), you can't wait forever. The timeout forces exit.
//
// In Kubernetes, there's also a terminationGracePeriodSeconds
// (default 30s). If your app doesn't exit in that time, Kubernetes
// sends SIGKILL (unblockable). Your shutdown timeout should be
// LESS than terminationGracePeriodSeconds.
import { FastifyInstance } from "fastify";
import { Pool } from "pg";
import { createModuleLogger } from "./logger";
import { config } from "../config";

const log = createModuleLogger("shutdown");

export function setupGracefulShutdown(app: FastifyInstance, pool: Pool): void {
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

      // Step 2: Close database pool
      // This waits for active queries to finish, then close connections
      log.info("Closing database pool...");
      await pool.end();
      log.info("✅ Database pool closed");

      // 🔍 LEARNING NOTE: In Phase 2+, we'll also need to:
      // - Disconnect Kafka producers (flush pending messages first!)
      // - Disconnect Kafka consumers (commit offsets first!)
      // - Close Redis connections
      // - Close SSE connections
      // Each of these has its own shutdown ordering requirements.
      // Getting the order wrong causes data loss or duplicate processing.

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
