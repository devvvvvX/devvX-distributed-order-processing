// ============================================================
// Health Check
// ============================================================
// Health checks are how infrastructure knows your service is alive.
// 1. LIVENESS  — "Is the process alive?"
// 2. READINESS — "Can this instance handle traffic?"
// 3. STARTUP   — "Has the service finished initializing?"

import { FastifyInstance } from "fastify";
import { Pool } from "pg";
import { createModuleLogger } from "./logger.js";

const log = createModuleLogger("health");

interface HealthCheckResult {
  status: "healthy" | "unhealthy";
  uptime: number;
  timestamp: string;
  version: string;
  checks: {
    database: {
      status: "up" | "down";
      latencyMs: number;
      poolSize: {
        total: number;
        idle: number;
        waiting: number;
      };
    };
  };
}

export async function registerHealthRoutes(
  app: FastifyInstance,
  pool: Pool,
): Promise<void> {
  app.get("/health", async (_request, reply) => {
    const startTime = Date.now();
    let dbStatus: "up" | "down" = "down";
    let dbLatency = 0;

    try {
      // 🔍 LEARNING NOTE: We run a trivial query to verify the connection works.
      // SELECT 1 is the standard "are you alive?" query.
      // Don't do SELECT * FROM large_table — that's a performance test, not a health check.
      await pool.query("SELECT 1");
      dbLatency = Date.now() - startTime;
      dbStatus = "up";
    } catch (err) {
      log.error({ err }, "Health check: database connection failed");
      dbLatency = Date.now() - startTime;
    }

    const result: HealthCheckResult = {
      status: dbStatus == "up" ? "healthy" : "unhealthy",
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
      version: "1.0.0",
      checks: {
        database: {
          status: dbStatus,
          latencyMs: dbLatency,
          poolSize: {
            total: pool.totalCount,
            idle: pool.idleCount,
            waiting: pool.waitingCount,
          },
        },
      },
    };

    const statusCode = result.status === "healthy" ? 200 : 503;
    return reply.status(statusCode).send(result);
  });
}
