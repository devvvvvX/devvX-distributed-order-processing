// ============================================================
// Fastify Server — Application Bootstrap
// ============================================================
// 🔍 LEARNING NOTE: This file builds the Fastify application instance.
// It registers plugins, middleware, routes, and error handlers.
//
// Why Fastify over Express?
// 1. PERFORMANCE: Fastify is 2-3x faster than Express in benchmarks.
//    It uses a radix tree for routing instead of linear regex matching.
// 2. SCHEMA VALIDATION: Built-in JSON Schema support for request/response
//    validation (we use Zod instead, but the option exists).
// 3. PLUGIN SYSTEM: Encapsulated plugins prevent global state pollution.
// 4. TYPESCRIPT: First-class TypeScript support with proper generics.
// 5. LOGGING: Built-in Pino integration (the fastest Node.js logger).
//
// For our learning purposes, the key difference is that Fastify's
// plugin system teaches good architectural boundaries — something
// that becomes critical in distributed systems.

import Fastify, { FastifyInstance } from "fastify";
import cors from "@fastify/cors";
import { Pool } from "pg";
import { config } from "./config/index.js";
import { registerOrderRoutes } from "./modules/orders/order.routes.js";
import { registerHealthRoutes } from "./shared/health.js";
import { AppError } from "./shared/errors.js";
import { ApiResponse } from "./types/index.js";

export async function buildServer(pool: Pool): Promise<FastifyInstance> {
  // 🔍 LEARNING NOTE: In Fastify v5, the `logger` option only accepts a
  // plain config object — you cannot pass a Pino instance directly.
  // If you need a shared Pino instance elsewhere (e.g. for DB or queue
  // logging outside of request context), keep a separate module-level
  // logger (see shared/logger.ts). Fastify creates and manages its own
  // internal Pino instance based on the config you provide here.

  const isDev = config.nodeEnv === "development";

  const app = Fastify({
    logger: isDev
      ? {
          level: config.logLevel,
          base: { service: "order-platform", version: "1.0.0" },
          redact: {
            paths: ["req.headers.authorization", "req.headers.cookie"],
            censor: "[REDACTED]",
          },

          // Pretty-print in dev — raw JSON in production.
          // 🔍 LEARNING NOTE: Production log pipelines (Datadog, CloudWatch,
          // ELK) ingest raw JSON and do formatting themselves. Pretty-printing
          // in prod wastes CPU and produces unstructured output.
          transport: {
            target: "pino-pretty",
            options: {
              colorize: true,
              translateTime: "SYS:HH:MM:ss.l",
              ignore: "pid,hostname",
            },
          },
        }
      : {
          level: config.logLevel,
          base: { service: "order-platform", version: "1.0.0" },
          redact: {
            paths: ["req.headers.authorization", "req.headers.cookie"],
            censor: "[REDACTED]",
          },
        },

    // 🔍 LEARNING NOTE: Every request gets a unique ID.
    // This ID is included in ALL log lines for that request.
    // When debugging "why did order X fail?", you search for
    // the request ID and see the entire lifecycle in one view.
    // In Phase 6, this becomes a distributed trace ID that
    // follows requests across multiple services.
    genReqId: () => crypto.randomUUID(),

    // Request timeout
    // 🔍 LEARNING NOTE: Always set a timeout. Without one, a slow
    // client can hold a connection open forever, eventually exhausting
    // your server's connection limit.
    connectionTimeout: 10000,
  });

  // ─────────────────────────────────────────────────
  // Plugins
  // ─────────────────────────────────────────────────

  // CORS
  await app.register(cors, {
    origin: true, // Allow all origins in dev,
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH"],
    allowedHeaders: ["Content-Type", "Authorization", "Idempotency-Key"],
  });

  // ─────────────────────────────────────────────────
  // Request hooks
  // ─────────────────────────────────────────────────

  // Log request start with useful context
  app.addHook("onRequest", async (request) => {
    request.log.info(
      {
        method: request.method,
        url: request.url,
        // 🔍 LEARNING NOTE: Log IP for rate limiting and abuse detection.
        // In production behind a load balancer, use X-Forwarded-For instead.
        ip: request.ip,
      },
      `→ ${request.method} ${request.url}`,
    );
  });

  // Log response with timing
  app.addHook("onResponse", async (request, reply) => {
    request.log.info(
      {
        method: request.method,
        url: request.url,
        statusCode: reply.statusCode,
        responseTimeMs: Math.round(reply.elapsedTime),
      },
      `← ${request.method} ${request.url} ${reply.statusCode} ${Math.round(reply.elapsedTime)}ms`,
    );
  });

  // ─────────────────────────────────────────────────
  // Error handler
  // ─────────────────────────────────────────────────
  // 🔍 LEARNING NOTE: Centralized error handling ensures:
  // 1. Consistent error response format
  // 2. No stack traces leaked to clients
  // 3. All errors are logged
  // 4. Proper HTTP status codes

  app.setErrorHandler(async (error, request, reply) => {
    // Known application errors (expected)
    if (error instanceof AppError) {
      request.log.warn(
        {
          err: error,
          code: error.code,
          statusCode: error.statusCode,
        },
        error.message,
      );

      const response: ApiResponse<never> = {
        success: false,
        error: {
          code: error.code,
          message: error.message,
          // Include validation details if available
          ...("details" in error
            ? { details: (error as { details: unknown }).details }
            : {}),
        },
      };

      return reply.status(error.statusCode).send(response);
    }

    // Unknown errors (bugs - should not happen)
    request.log.error(
      { err: error },
      "💥 Unhandled error — this is likely a bug",
    );

    const errorMessage = error instanceof Error ? error.message : String(error);
    const response: ApiResponse<never> = {
      success: false,
      error: {
        code: "INTERNAL_ERROR",
        // 🔍 LEARNING NOTE: NEVER expose internal error details to clients.
        // "Error: password authentication failed for user postgres"
        // is helpful for attackers, not for users.
        message:
          config.nodeEnv === "development"
            ? errorMessage
            : "An internal error occurred",
      },
    };

    return reply.status(500).send(response);
  });

  // ─────────────────────────────────────────────────
  // Routes
  // ─────────────────────────────────────────────────

  await registerHealthRoutes(app, pool);
  await registerOrderRoutes(app, pool);

  return app;
}
