// ============================================================
// Order Service — Business Logic
// ============================================================
// The service layer is where business logic lives.
// It orchestrates between:
//   - Repository (data access)
//   - Notification service (side effects)
//   - Validation rules (state machine)
//
// It should NOT:
//   - Know about HTTP (no request/response objects)
//   - Contain SQL queries (that's the repository's job)
//   - Contain framework-specific code
//
// This separation makes the service testable and portable.
// When we extract this into a microservice (Phase 2+), the
// service logic stays exactly the same

import { Pool } from "pg";
import { OrderRepository } from "./order.repository";
import {
  Order,
  OrderStatusHistoryEntry,
  CreateOrderInput,
  UpdateOrderStatusInput,
  VALID_STATUS_TRANSITIONS,
  OrderStatusType,
} from "./order.types.js";
import { NotificationService } from "../notifications/notification.service.js";
import { NotificationRepository } from "../notifications/notification.repository.js";
import { NotFoundError, ConflictError } from "../../shared/errors.js";
import { createModuleLogger } from "../../shared/logger.js";

const log = createModuleLogger("order-service");

export class OrderService {
  private readonly orderRepo: OrderRepository;
  private readonly notificationService: NotificationService;

  constructor(pool: Pool) {
    this.orderRepo = new OrderRepository(pool);

    const notificationRepo = new NotificationRepository(pool);
    this.notificationService = new NotificationService(notificationRepo);
  }

  // ─────────────────────────────────────────────────
  // CREATE ORDER
  // ─────────────────────────────────────────────────
  async createOrder(
    input: CreateOrderInput,
    idempotencyKey?: string,
  ): Promise<Order> {
    const startTime = Date.now();

    // Step 1: Idempotency check
    // 🔍 LEARNING NOTE: Check if we've seen this idempotency key before.
    // If yes, return the existing order — don't create a duplicate.
    //
    // Race condition: Two identical requests arrive simultaneously.
    // Both check findByIdempotencyKey → both get null → both try to insert.
    // The UNIQUE constraint on idempotency_key in PostgreSQL will cause
    // the second insert to fail with a unique violation error.
    // We catch that error and return the existing order.
    //
    // This is database-level idempotency protection. In Phase 3+,
    // we'll also need application-level idempotency for Kafka consumers

    if (idempotencyKey) {
      const existingOrder =
        await this.orderRepo.findByIdempotencyKey(idempotencyKey);

      if (existingOrder) {
        log.info(
          {
            orderId: existingOrder.id,
            idempotencyKey,
          },
          "Returning existing order (idempotent request)",
        );
        return existingOrder;
      }
    }

    // Step 2: Calculate totals
    // 🔍 LEARNING NOTE: Server-side calculation of totals is mandatory.
    // NEVER trust client-supplied totals. The client might:
    // - Send totalAmount=0 for a $50 order (fraud)
    // - Have stale prices from client-side cache
    // - Have rounding errors in JavaScript (0.1 + 0.2 ≠ 0.3)

    const totalAmount = input.items.reduce(
      (sum, item) => sum + item.quantity * item.unitPrice,
      0,
    );

    // Round to avoid floating point issues
    const roundedTotalAmount = Math.round(totalAmount * 100) / 100;
    const deliveryFee = this.calculateDeliveryFee(roundedTotalAmount);
    const taxAmount = Math.round(roundedTotalAmount * 0.08 * 100) / 100; // 8% tax
    const grandTotal =
      Math.round((roundedTotalAmount + deliveryFee + taxAmount) * 100) / 100;

    // Step 3: Create order in database (transactional)
    let order: Order;
    try {
      order = await this.orderRepo.create(input, idempotencyKey, {
        totalAmount: roundedTotalAmount,
        deliveryFee,
        taxAmount,
        grandTotal,
      });
    } catch (err: unknown) {
      // Handle unique constraint violation on idempotency_key
      // 🔍 LEARNING NOTE: This catches the race condition described above.
      // Error code 23505 is PostgreSQL's "unique_violation"

      if (
        err instanceof Error &&
        "code" in err &&
        (err as Record<string, unknown>).code === "23505" &&
        idempotencyKey
      ) {
        const existingOrder =
          await this.orderRepo.findByIdempotencyKey(idempotencyKey);

        if (existingOrder) {
          log.info(
            {
              orderId: existingOrder.id,
              idempotencyKey,
            },
            "Returning existing order (concurrent idempotent request)",
          );
          return existingOrder;
        }
      }

      throw err;
    }

    log.info(
      {
        orderId: order.id,
        customerId: order.customerId,
        itemCount: order.items.length,
        grandTotal: order.grandTotal,
      },
      "📦 Order created",
    );

    // Step 4: Send notification (SYNCHRONOUS — the bottleneck!)
    // 🔍 LEARNING NOTE: This is where the monolith hurts.
    // The order is already in the database, but we're about to
    // block the HTTP response to send a notification.
    //
    // Measuring the impact:
    //   DB write: ~5-20ms
    //   Notification: ~500ms (configurable via NOTIFICATION_DELAY_MS)
    //   Total: ~520ms instead of ~20ms
    //
    // The customer is staring at a loading spinner for 500ms
    // because we're sending an email they won't read for 10 minutes.

    try {
      const { subject, content } =
        this.notificationService.buildOrderConfirmationContent({
          id: order.id,
          customerName: order.customerName,
          restaurantName: order.restaurantName,
          grandTotal: order.grandTotal,
        });

      await this.notificationService.send({
        orderId: order.id,
        type: "ORDER_CONFIRMED",
        channel: "EMAIL",
        recipient: order.customerEmail,
        subject,
        content,
      });
    } catch (err) {
      // 🔍 LEARNING NOTE: We catch and log, but still return the order.
      // This is a pragmatic compromise: the order was saved successfully,
      // so we shouldn't fail the API call just because email is down.
      //
      // BUT: the customer won't get a notification. There's no retry mechanism.
      // The notification is just lost. In Phase 2, Kafka ensures notifications
      // are reliably processed even if the notification service is temporarily down.
      log.error(
        {
          err,
          orderId: order.id,
        },
        "⚠️  Notificaiton failed but order was created successfully",
      );
    }

    const totalLatency = Date.now() - startTime;
    log.info(
      {
        orderId: order.id,
        totalLatencyMs: totalLatency,
      },
      `Order creation completed in ${totalLatency}ms`,
    );

    return order;
  }

  // ─────────────────────────────────────────────────
  // GET ORDER
  // ─────────────────────────────────────────────────
  async getOrder(id: string): Promise<Order> {
    const order = await this.orderRepo.findById(id);
    if (!order) {
      throw new NotFoundError("Order", id);
    }
    return order;
  }

  // ─────────────────────────────────────────────────
  // LIST ORDERS
  // ─────────────────────────────────────────────────
  async listOrders(
    page: number,
    limit: number,
    filters?: { status?: OrderStatusType; customerId?: string },
  ): Promise<{
    orders: Order[];
    total: number;
    page: number;
    limit: number;
    totalPages: number;
  }> {
    const { orders, total } = await this.orderRepo.findAll(
      page,
      limit,
      filters,
    );
    return {
      orders,
      total,
      page,
      limit,
      totalPages: Math.ceil(total / limit),
    };
  }

  // ─────────────────────────────────────────────────
  // UPDATE ORDER STATUS
  // ─────────────────────────────────────────────────
  async updateOrderStatus(
    id: string,
    input: UpdateOrderStatusInput,
  ): Promise<Order> {
    // Step 1: Get current order
    const currentOrder = await this.orderRepo.findById(id);
    if (!currentOrder) {
      throw new NotFoundError("order", id);
    }

    // Step 2: Validate state transition
    // 🔍 LEARNING NOTE: The state machine prevents invalid transitions.
    // This is especially important in distributed systems where
    // out-of-order events could try to set invalid states.
    const allowedTransitions = VALID_STATUS_TRANSITIONS[currentOrder.status];
    if (!allowedTransitions?.includes(input.status)) {
      throw new ConflictError(
        `Cannot transition from ${currentOrder.status} to ${input.status}. ` +
          `Allowed transitions: ${allowedTransitions?.join(", ") || "none (terminal state)"}`,
        "INVALID_STATUS_TRANSITION",
      );
    }

    // Step 3: Update status (with optimistic concurrency)
    const updatedOrder = await this.orderRepo.updateStatus(
      id,
      currentOrder.status,
      input.status,
      input.changedBy ?? "system",
      input.reason,
    );

    if (!updatedOrder) {
      // 🔍 LEARNING NOTE: This means the status changed between our
      // SELECT and UPDATE — a classic TOCTOU (Time Of Check, Time Of Use) race.
      // The optimistic locking in the repository caught it.

      throw new ConflictError(
        "Order status was modified by another request. Please retry",
        "CONCURRENT_MODIFICATION",
      );
    }

    log.info(
      {
        id: id,
        from: currentOrder.status,
        to: input.status,
        changedBy: input.changedBy,
      },
      `📋 Order status updated: ${currentOrder.status} → ${input.status}`,
    );

    // Step 4: Send status update notification (SYNCHRONOUS - again, the bottleneck)
    try {
      const { subject, content } =
        this.notificationService.buildOrderUpdateContent({
          id,
          customerName: updatedOrder.customerName,
          status: updatedOrder.status,
        });

      await this.notificationService.send({
        orderId: updatedOrder.id,
        type:
          updatedOrder.status === "CANCELLED"
            ? "ORDER_CANCELLED"
            : "ORDER_CONFIRMED",
        channel: "EMAIL",
        recipient: updatedOrder.customerEmail,
        subject,
        content,
      });
    } catch (err) {
      log.error(
        {
          err,
          orderId: id,
        },
        "⚠️  Status notification failed but status was updated",
      );
    }
    return updatedOrder;
  }

  // ─────────────────────────────────────────────────
  // CANCEL ORDER
  // ─────────────────────────────────────────────────
  async cancelOrder(id: string, reason?: string): Promise<Order> {
    return this.updateOrderStatus(id, {
      status: "CANCELLED",
      changedBy: "customer",
      reason: reason ?? "Cancelled by customer",
    });
  }

  // ─────────────────────────────────────────────────
  // GET STATUS HISTORY
  // ─────────────────────────────────────────────────
  async getOrderStatusHistory(orderId: string) : Promise<OrderStatusHistoryEntry []> {
    // Verify order status
    const order = await this.orderRepo.findById(orderId);
    if(!order){
      throw new NotFoundError('Order', orderId);
    }

    return this.orderRepo.getStatusHistory(orderId);
  }


  // ─────────────────────────────────────────────────
  // Delivery fee calculation (simple for Phase 1)
  // ─────────────────────────────────────────────────
  private calculateDeliveryFee(orderAmount: number): number {
    // Free delivery for orders over $30, otherwise $5
    // 🔍 LEARNING NOTE: In production, delivery fee calculation is
    // a complex service considering distance, demand, time of day, etc.
    // Uber/DoorDash have entire teams working on dynamic pricing.
    return orderAmount >= 30 ? 0 : 5;
  }
}
