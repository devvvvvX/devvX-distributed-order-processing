// ============================================================
// Order Service — Business Logic (Phase 2: Event-Driven)
// ============================================================
// The service layer is where business logic lives.
// It orchestrates between:
//   - Repository (data access)
//   - Event producer (kafka - async side effects)
//   - Validation rules (state machine)
//
// 🔍 PHASE 2 EVOLUTION:
// In Phase 1, this service called NotificationService.send() SYNCHRONOUSLY
// during order creation. That added 500ms to every order API call.
//
// In Phase 2, we replaced that with EventProducer.publishOrderCreated(),
// which publishes an event to Kafka in ~2ms. The notification is now
// processed asynchronously by a separate consumer process.
//
// The service layer itself barely changed — we swapped one dependency
// for another. This is the benefit of clean architecture: business logic
// (validation, state machine, idempotency) stays identical. Only the
// "how do we trigger side effects?" part changed.

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
import { EventProducer } from "../../kafka/producer.js";
import { NotFoundError, ConflictError } from "../../shared/errors.js";
import { createModuleLogger } from "../../shared/logger.js";

const log = createModuleLogger("order-service");

export class OrderService {
  private readonly orderRepo: OrderRepository;

  // 🔍 LEARNING NOTE: EventProducer is optional (nullable).
  // If Kafka is not configured (e.g., in tests or dev without Kafka),
  // the service still works — it just doesn't publish events.
  // This is graceful degradation: the core function (order CRUD) works
  // even if the event bus is unavailable.
  private readonly eventProducer: EventProducer | null;

  constructor(pool: Pool, eventProducer: EventProducer | null = null) {
    this.orderRepo = new OrderRepository(pool);
    this.eventProducer = eventProducer;
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

    // Step 4: Publish event (ASYNCHRONOUS - ~2ms, non-blocking!)
    //
    // ═══════════════════════════════════════════════════════════════
    // 🔍 PHASE 2 CHANGE: This is THE key architectural change.
    //
    // BEFORE (Phase 1 — synchronous):
    //   await this.notificationService.send({ ... });
    //   // Blocks for 500ms. Customer waits. Bad UX.
    //
    // AFTER (Phase 2 — event-driven):
    //   await this.eventProducer.publishOrderCreated(order);
    //   // Returns in ~2ms. Customer gets fast response. 🎉
    //
    // The notification is now processed ASYNCHRONOUSLY by the
    // notification consumer (src/consumers/notification-consumer.ts).
    //
    // What happens if Kafka is down?
    //   - The event is NOT published (logged as error)
    //   - The order is still saved in the DB (customer's order is safe)
    //   - The notification won't be sent (acceptable trade-off)
    //   - In Phase 3, the Transactional Outbox pattern eliminates this gap
    // ═══════════════════════════════════════════════════════════════
    if (this.eventProducer) {
      await this.eventProducer.publishOrderCreated({
        id: order.id,
        customerId: order.customerId,
        customerName: order.customerName,
        customerEmail: order.customerEmail,
        customerPhone: order.customerPhone,
        restaurantId: order.restaurantId,
        restaurantName: order.restaurantName,
        status: order.status,
        totalAmount: order.totalAmount,
        deliveryFee: order.deliveryFee,
        taxAmount: order.taxAmount,
        grandTotal: order.grandTotal,
        deliveryAddress: order.deliveryAddress,
        items: order.items.map((item) => ({
          itemName: item.itemName,
          quantity: item.quantity,
          unitPrice: item.unitPrice,
          totalPrice: item.totalPrice,
        })),
      });
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

    // Step 4: Publish status change event (ASYNCHRONOUS)
    // 🔍 PHASE 2 CHANGE: Same pattern as createOrder — publish event
    // instead of calling notification service synchronously.
    if (this.eventProducer) {
      await this.eventProducer.publishOrderStatusChanged({
        orderId: updatedOrder.id,
        previousStatus: currentOrder.status,
        newStatus: updatedOrder.status,
        changedBy: input.changedBy ?? 'system',
        customerEmail: updatedOrder.customerEmail,
        customerName: updatedOrder.customerName,
        restaurantName: updatedOrder.restaurantName
      });
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
  async getOrderStatusHistory(orderId: string): Promise<OrderStatusHistoryEntry[]> {
    // Verify order status
    const order = await this.orderRepo.findById(orderId);
    if (!order) {
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
