// ============================================================
// Entry Point — Application Startup
// ============================================================
// 🔍 LEARNING NOTE: This is the ONLY file that runs process-level code.
// It creates resources (DB pool, server) and wires everything together.
//
// Startup order matters:
// 1. Load config (validates env vars — fails fast)
// 2. Create DB pool (doesn't connect yet — lazy initialization)
// 3. Build server (registers routes and plugins)
// 4. Start listening (begins accepting connections)
// 5. Register shutdown handlers
//
// If any step fails, the process exits with a clear error.
// In production, the orchestrator (Kubernetes, ECS) restarts it.

import { config } from "./config";
import { createPool } from "./db/pool.js";
import { buildServer } from "./server.js";
import { setupGracefulShutdown } from "./shared/shutdown.js";
import { logger } from "./shared/logger.js";

async function main(): Promise<void> {
  logger.info(
    {
      nodeEnv: config.nodeEnv,
      port: config.port,
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

  //   Step 3: Build and start HTTP Server

  const app = await buildServer(pool);

  try {
    await app.listen({ port: config.port, host: config.host });
    logger.info(
      {
        port: config.port,
        host: config.host,
      },
      "✅ Server listening on http://${config.host}:${config.port}",
    );
  } catch (err) {
    logger.fatal({ err }, "❌ Failed to start server");
    process.exit(1);
  }

  //   Step 4: Register graceful shutdown handlers
  setupGracefulShutdown(app, pool);

  // Log startup summary
  logger.info("─".repeat(60));
  logger.info("📦 Order Platform — Phase 1 (Monolith)");
  logger.info(`   Environment:  ${config.nodeEnv}`);
  logger.info(`   Port:         ${config.port}`);
  logger.info(
    `   Notification: ${config.notificationDelayMs}ms delay, ${config.notificationFailureRate * 100}% failure rate`,
  );
  logger.info("");
  logger.info("   Endpoints:");
  logger.info("   POST   /api/v1/orders          — Create order");
  logger.info("   GET    /api/v1/orders           — List orders");
  logger.info("   GET    /api/v1/orders/:id       — Get order");
  logger.info("   PATCH  /api/v1/orders/:id/status — Update status");
  logger.info("   POST   /api/v1/orders/:id/cancel — Cancel order");
  logger.info("   GET    /api/v1/orders/:id/history — Status history");
  logger.info("   GET    /health                  — Health check");
  logger.info("─".repeat(60));
}

main().catch((err) => {
  logger.fatal({ err }, '❌ Unhandled error during startup');
  process.exit(1);
});
