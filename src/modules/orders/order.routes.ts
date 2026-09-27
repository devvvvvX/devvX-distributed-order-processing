// ============================================================
// Order Routes — HTTP API Layer (Phase 4: + Redis Coordination)
// ============================================================
// 🔍 LEARNING NOTE: Routes are the thinnest layer. They:
// 1. Parse/validate the HTTP request
// 2. Call the service layer
// 3. Format the HTTP response
//
// Routes should NOT contain business logic.
// If you find yourself writing "if order.status === ..." in a route,
// that logic belongs in the service.
// 
// 🔍 PHASE 4 EVOLUTION:
// Routes now accept a Redis client for three features:
// 1. Distributed locking on status updates (prevent race conditions)
// 2. Cache-aside for order lookups (reduce DB load)
// 3. Order assignment endpoint (driver → order coordination)

import { FastifyInstance, FastifyRequest, FastifyReply } from "fastify";
import { Pool } from "pg";
import Redis from 'ioredis';
import { OrderService } from "./order.service";
import {
  createOrderSchema,
  updateOrderStatusSchema,
  paginationSchema,
} from "./order.schemas";
import { OrderStatusType } from "./order.types";
import { createModuleLogger } from "../../shared/logger";
import { ApiError, ApiResponse } from "../../types/index.js";
import { Order, OrderStatusHistoryEntry } from "../orders/order.types.js";
import { CacheService } from "../../redis/cache.js";
import { DistributedLock } from "../../redis/distributed-lock";
import { OrderAssignmentService } from "./order-assignment.service";
import { config } from "../../config";

const log = createModuleLogger("order-routes");

export async function registerOrderRoutes(
  app: FastifyInstance,
  pool: Pool,
  redis: Redis | null
): Promise<void> {
  // 🔍 PHASE 3 CHANGE: OrderService no longer needs a Kafka producer.
  // Events are written to the outbox table atomically with orders.
  // The OutboxRelay (running in the API process) handles Kafka publishing.
  const orderService = new OrderService(pool);

  // 🔍 PHASE 4: Create Redis-backed services (or null if Redis not configured)
  const cache = redis ? new CacheService(redis) : null;
  const lock = redis ? new DistributedLock(redis) : null;
  const assignmentService = redis ? new OrderAssignmentService(redis) : null;

  // ─────────────────────────────────────────────────
  // POST /api/v1/orders — Create Order
  // ─────────────────────────────────────────────────
  app.post(
    "/api/v1/orders",
    async (
      request: FastifyRequest<{
        Body: unknown;
        Headers: { "idempotency-key"?: string };
      }>,
      reply: FastifyReply,
    ) => {
      const startTime = Date.now();

      // Validate request body
      const parseResult = createOrderSchema.safeParse(request.body);
      if (!parseResult.success) {
        const response: ApiResponse<ApiError> = {
          success: false,
          error: {
            code: "VALIDATION_ERROR",
            message: "Invalid request body",
            details: parseResult.error.flatten(),
          },
        };
        return reply.status(422).send(response);
      }

      // 🔍 LEARNING NOTE: The Idempotency-Key header is a standard pattern.
      // Stripe, PayPal, and most payment APIs use it.
      // The client generates a unique key (usually UUID) for each
      // distinct operation. If the network fails and they retry,
      // they send the SAME key, and we return the SAME response.
      const idempotencyKey = request.headers["idempotency-key"] as
        | string
        | undefined;

      const order = await orderService.createOrder(
        parseResult.data,
        idempotencyKey,
      );

      const latency = Date.now() - startTime;
      log.info(
        { orderId: order.id, latencyMs: latency },
        `POST /api/v1/orders completed ${latency}ms`,
      );

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.status(201).send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders/:id — Get Order by ID
  // ─────────────────────────────────────────────────
  // 🔍 PHASE 4 CHANGE: Cache-aside pattern.
  // First check Redis cache → HIT? return cached → MISS? query DB → cache it.
  // This reduces PostgreSQL load for frequently-accessed orders.
  app.get(
    "/api/v1/orders/:id",
    async (
      request: FastifyRequest<{ Params: { id: string } }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;

      // 🔍 PHASE 4: Try cache first, fall back to DB
      let order: Order;
      if (cache) {
        order = await cache.getOrSet<Order>(
          `order:${id}`,
          () => orderService.getOrder(id),
          config.cacheOrderTtlSeconds
        );
      } else {
        order = await orderService.getOrder(id);
      }

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.status(200).send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders — List Orders (Paginated)
  // ─────────────────────────────────────────────────
  // 🔍 LEARNING NOTE: We do NOT cache list queries.
  // Cache invalidation for lists is much harder:
  //   - Any order change invalidates ALL lists
  //   - Different pagination params = different cache keys
  //   - Different filters = more cache keys
  // The complexity isn't worth it for our use case.
  app.get(
    "/api/v1/orders",
    async (
      request: FastifyRequest<{
        Querystring: {
          page?: string;
          limit?: string;
          status?: string;
          customerId?: string;
        };
      }>,
      reply: FastifyReply,
    ) => {
      // Validate the pagination params
      const paginationResult = paginationSchema.safeParse(request.query);
      if (!paginationResult.success) {
        const response: ApiResponse<ApiError> = {
          success: false,
          error: {
            code: "VALIDATION_ERROR",
            message: "Invalid pagination parameters",
            details: paginationResult.error.flatten(),
          },
        };

        return reply.status(422).send(response);
      }

      const { page, limit } = paginationResult.data;
      const filters: { status?: OrderStatusType; customerId?: string } = {};

      if (request.query.status) {
        filters["status"] = request.query.status as OrderStatusType;
      }

      if (request.query.customerId) {
        filters["customerId"] = request.query.customerId;
      }

      const result = await orderService.listOrders(page, limit, filters);

      const response: ApiResponse<Order[]> = {
        success: true,
        data: result.orders,
        meta: {
          page: result.page,
          limit: result.limit,
          total: result.total,
          totalPages: result.totalPages,
        },
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // PATCH /api/v1/orders/:id/status — Update Status
  // ─────────────────────────────────────────────────
  // 🔍 PHASE 4 CHANGE: Distributed lock + cache invalidation.
  // 1. Acquire lock on order (prevent concurrent status updates)
  // 2. Update status in DB
  // 3. Invalidate cache (next read fetches fresh data)
  // 4. Release lock
  app.patch(
    "/api/v1/orders/:id/status",
    async (
      request: FastifyRequest<{ Params: { id: string }; Body: unknown }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;

      const parseResult = updateOrderStatusSchema.safeParse(request.body);
      if (!parseResult.success) {
        const response: ApiResponse<ApiError> = {
          success: false,
          error: {
            code: "VALIDATION_ERROR",
            message: "Invalid request body",
            details: parseResult.error.flatten(),
          },
        };
        return reply.status(422).send(response);
      }

      // 🔍 PHASE 4: Acquire distributed lock before status update.
      // This prevents two API instances from racing on the same order.
      // Lock TTL = 5 seconds (enough for a DB roundtrip).
      if (lock) {
        const lockResult = await lock.withLock(
          `order:${id}`,
          5000,
          async () => {
            const order = await orderService.updateOrderStatus(id, parseResult.data);
            // Invalidate cache after successful update
            await cache?.invalidate(`order:${id}`);
            return order;
          }
        );

        if (!lockResult.success) {
          // 🔍 PHASE 4 FIX: Distinguish Redis-down from actual contention.
          // If Redis is unreachable, fall through to the no-lock path
          // so the API remains functional. Only return 409 for real contention.
          if (lockResult.reason === 'error') {
            log.warn(
              { orderId: id },
              '⚠️ Redis unavailable — falling through to no-lock status update'
            );
            // Fall through to the no-lock path below
          } else {
            // Another instance is updating this order right now
            const response: ApiResponse<never> = {
              success: false,
              error: {
                code: 'CONFLICT',
                message: 'Order is being updated by another request. Please retry'
              }
            };
            return reply.status(409).send(response);
          }
        } else {
          const response: ApiResponse<Order> = {
            success: true,
            data: lockResult.result!,
          };

          return reply.send(response);
        }
      }

      // Fallback without Redis: no lock, just update
      const order = await orderService.updateOrderStatus(id, parseResult.data);

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // POST /api/v1/orders/:id/cancel — Cancel Order
  // ─────────────────────────────────────────────────
  app.post(
    "/api/v1/orders/:id/cancel",
    async (
      request: FastifyRequest<{
        Params: { id: string };
        Body: { reason?: string } | undefined;
      }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;
      const reason =
        request.body &&
          typeof request.body === "object" &&
          "reason" in request.body
          ? (request.body as { reason?: string }).reason
          : undefined;

      const order = await orderService.cancelOrder(id, reason);

      // 🔍 PHASE 4: Invalidate cache after cancellation
      await cache?.invalidate(`order:${id}`);

      const response: ApiResponse<Order> = {
        success: true,
        data: order,
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders/:id/history — Get Status History
  // ─────────────────────────────────────────────────
  app.get(
    "/api/v1/orders/:id/history",
    async (
      request: FastifyRequest<{
        Params: { id: string };
      }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;
      const history = await orderService.getOrderStatusHistory(id);

      const response: ApiResponse<OrderStatusHistoryEntry[]> = {
        success: true,
        data: history,
      };

      return reply.send(response);
    },
  );

  // ─────────────────────────────────────────────────
  // POST /api/v1/orders/:id/assign — Assign Driver (Phase 4)
  // ─────────────────────────────────────────────────
  // 🔍 PHASE 4 NEW ENDPOINT: Atomic driver assignment using Redis.
  // Only ONE driver can be assigned to an order. Redis NX guarantees
  // that concurrent requests from different drivers don't cause
  // double-assignment.
  //
  // In a real system, this would:
  // 1. Check that the order is in READY_FOR_PICKUP status
  // 2. Verify the driver is available and authorized
  // 3. Notify the driver and customer
  // 4. Persist the assignment to PostgreSQL
  //
  // For Phase 4, we focus on the COORDINATION problem (Redis NX)
  // and skip the business validation for simplicity.
  app.post(
    '/api/v1/orders/:id/assign',
    async (
      request: FastifyRequest<{
        Params: { id: string };
        Body: unknown;
      }>,
      reply: FastifyReply
    ) => {
      if (!assignmentService) {
        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'SERVICE_UNAVAILABLE',
            message: 'Order assigment requires Redis. Redis is not configured.',
          }
        }
        return reply.status(503).send(response);
      }

      const { id: orderId } = request.params;

      // Validate request body
      const body = request.body as Record<string, unknown> | null;
      const driverId = body?.driverId as string | undefined;

      if (!driverId || typeof driverId !== 'string' || driverId.trim() === '') {
        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'VALIDATION_ERROR',
            message: 'driverId is required and must be a non-empty string',
          }
        }
        return reply.status(422).send(response);
      }
      try {
        const result = await assignmentService.assignDriver(orderId, driverId.trim());

        if (result.assigned) {
          // This driver won the race!
          const response: ApiResponse<typeof result> = {
            success: true,
            data: result
          };

          return reply.status(201).send(response);
        }

        // Another driver was already assigned
        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'ALREADY_ASSIGNED',
            message: `Order ${orderId} is already assigned to driver ${result.driverId}.`
          }
        }
        return reply.status(409).send(response);
      } catch (err) {
        request.log.error({ err, orderId, driverId }, "Redis error during driver assignment");
        const response: ApiResponse<never> = {
          success: false,
          error: {
            code: 'SERVICE_UNAVAILABLE',
            message: 'Order assignment requires Redis, which is currently unavailable.'
          }
        };
        return reply.status(503).send(response);
      }
    }
  )

  // ─────────────────────────────────────────────────
  // GET /api/v1/orders/:id/assignment — Get Assignment (Phase 4)
  // ─────────────────────────────────────────────────
  app.get(
    '/api/v1/orders/:id/assignment',
    async (
      request: FastifyRequest<{ Params: { id: string } }>,
      reply: FastifyReply
    ) => {
      if (!assignmentService) {
        return reply.status(503).send({
          success: false,
          error: {
            code: 'SERVICE_UNAVAILABLE',
            message: 'Redis is not configured.'
          }
        })
      }

      const { id: orderId } = request.params;
      const driverId = await assignmentService.getAssignment(orderId);

      if (!driverId) {
        return reply.status(404).send({
          success: false,
          error: {
            code: 'NOT_FOUND',
            message: `No driver assigned to order ${orderId}`
          }
        });
      }

      return reply.send({
        success: true,
        data: { orderId, driverId }
      })
    }
  )
}
