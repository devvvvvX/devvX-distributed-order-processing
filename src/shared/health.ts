// ============================================================
// Health Check (Phase 4 + Redis)
// ============================================================
// Health checks are how infrastructure knows your service is alive.
// 1. LIVENESS  — "Is the process alive?"
// 2. READINESS — "Can this instance handle traffic?"
// 3. STARTUP   — "Has the service finished initializing?"
// 
// For Phase 1-4, we implement /health as a combined check.
// We'll separate these in Phase 6+ when we add Kubernetes.
// 
// 🔍 PHASE 4 EVOLUTION:
// Added Redis health check alongside PostgreSQL.
// The overall status is "healthy" only if ALL dependencies are up.
// Redis is optional — if not configured, its status is "not_configured".

import { FastifyInstance } from "fastify";
import { Pool } from "pg";
import Redis from 'ioredis';
import { createModuleLogger } from "./logger.js";

const log = createModuleLogger("health");

interface HealthCheckResult {
  status: "healthy" | "degraded" | "unhealthy";
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
    },
    // 🔍 PHASE 4 ADDITION: Redis health check.
    // We report latency and connection status so operators can
    // spot Redis performance degradation before it causes issues
    redis: {
      status: 'up' | 'down' | 'not_configured',
      latencyMs: number
    }
  };
}

export async function registerHealthRoutes(
  app: FastifyInstance,
  pool: Pool,
  redis: Redis | null = null
): Promise<void> {
  app.get("/health", async (_request, reply) => {
    let dbStatus: "up" | "down" = "down";
    let dbLatency = 0;
    let redisStatus: 'up' | 'down' | 'not_configured' = 'not_configured';
    let redisLatency = 0;


    try {
      const dbStart = Date.now();
      // 🔍 LEARNING NOTE: We run a trivial query to verify the connection works.
      // SELECT 1 is the standard "are you alive?" query.
      // Don't do SELECT * FROM large_table — that's a performance test, not a health check.
      await pool.query("SELECT 1");
      dbLatency = Date.now() - dbStart;
      dbStatus = "up";
    } catch (err) {
      log.error({ err }, "Health check: database connection failed");
    }

    // Check Redis (Phase 4)
    // 🔍 LEARNING NOTE: Redis PING is the standard health check.
    // It returns "PONG" if the server is alive and accepting commands.
    // This is the Redis equivalent of "SELECT 1" for PostgreSQL.
    if (redis) {
      try {
        const redisStart = Date.now();
        await redis.ping();
        redisLatency = Date.now() - redisStart;
        redisStatus = 'up';
      } catch (err) {
        log.error({ err }, "Health check: redis connection failed");
        redisStatus = 'down';
      }
    }

    // 🔍 PHASE 4 ADDITION: Three-state health.
    //   healthy  — all dependencies are up
    //   degraded — some dependencies are down (e.g., Redis down but DB up)
    //              API still works but with reduced functionality
    //   unhealthy — critical dependencies are down (e.g., DB down)
    //
    // This distinction is important for Kubernetes:
    //   healthy   → serve traffic normally
    //   degraded  → serve traffic but alert the team
    //   unhealthy → remove from load balancer, possibly restart
    let overallStatus: 'healthy' | 'degraded' | 'unhealthy';
    if (dbStatus === 'down') {
      overallStatus = 'unhealthy';
    } else if (redisStatus === 'down') {
      overallStatus = 'degraded'; // DB up but Redis down → degraded
    } else {
      overallStatus = 'healthy';
    }

    const result: HealthCheckResult = {
      status: overallStatus,
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
        redis: {
          status: redisStatus,
          latencyMs: redisLatency
        }
      },
    };

    // 🔍 LEARNING NOTE: Return 503 when unhealthy, not 500.
    // 503 = "Service Unavailable" = temporary, might recover.
    // 500 = "Internal Server Error" = bug in code.
    // Load balancers treat these differently.
    // Degraded still returns 200 — the API is functional.
    const statusCode = result.status === "unhealthy" ? 503 : 200;
    return reply.status(statusCode).send(result);
  });
}
