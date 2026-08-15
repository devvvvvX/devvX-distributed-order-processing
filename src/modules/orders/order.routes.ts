// ============================================================
// Order Routes — HTTP API Layer
// ============================================================
// 🔍 LEARNING NOTE: Routes are the thinnest layer. They:
// 1. Parse/validate the HTTP request
// 2. Call the service layer
// 3. Format the HTTP response
//
// Routes should NOT contain business logic.
// If you find yourself writing "if order.status === ..." in a route,
// that logic belongs in the service.

import { FastifyInstance, FastifyRequest, FastifyReply } from "fastify";
import { Pool } from "pg";
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
import { EventProducer } from "../../kafka/producer.js";

const log = createModuleLogger("order-routes");

export async function registerOrderRoutes(
  app: FastifyInstance,
  pool: Pool,
  eventProducer: EventProducer | null = null,
): Promise<void> {
  const orderService = new OrderService(pool, eventProducer);

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
  app.get(
    "/api/v1/orders/:id",
    async (
      request: FastifyRequest<{ Params: { id: string } }>,
      reply: FastifyReply,
    ) => {
      const { id } = request.params;
      const order = await orderService.getOrder(id);

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
}
