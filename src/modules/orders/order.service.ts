// ============================================================
// Order Service — Business Logic (Phase 3: Transactional Outbox)
// ============================================================
// 🔍 LEARNING NOTE: The service layer is where business logic lives.
// It orchestrates between:
//   - Repository (data access)
//   - Outbox (event persistence — atomically with business data)
//   - Validation rules (state machine)
//
// 🔍 PHASE 3 EVOLUTION:
// In Phase 2, this service called EventProducer.publishOrderCreated()
// AFTER the order was committed to the DB. This was a dual-write:
//   1. DB commit (success) → 2. Kafka publish (can fail) → event LOST!
//
// In Phase 3, we replaced the Kafka publish with an OUTBOX INSERT
// inside the SAME database transaction as the order:
//   1. BEGIN → INSERT order + INSERT outbox_event → COMMIT
//   Both succeed or both fail. No dual-write gap.
//
// The OutboxRelay (background poller) reads unpublished outbox events
// and publishes them to Kafka. If the relay crashes, events accumulate
// in the outbox and are published on the next poll. This provides strong durability.
//
// KEY ARCHITECTURAL INSIGHT:
// The service NO LONGER needs a Kafka producer dependency.
// It only needs the database. This is a significant simplification.
// The event bus is no longer on the synchronous order-creation critical path.
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
import { OutboxRepository, InsertOutboxEvent } from '../../outbox/outbox.repository.js';
import {
  buildOrderCreatedEvent,
  buildOrderStatusChangedEvent,
} from '../../kafka/events.js';
import { config } from "../../config";
import { NotFoundError, ConflictError } from "../../shared/errors.js";
import { createModuleLogger } from "../../shared/logger.js";

const log = createModuleLogger("order-service");

export class OrderService {
  private readonly orderRepo: OrderRepository;

  // 🔍 PHASE 3 CHANGE: Replaced EventProducer with OutboxRepository.
  // The service no longer talks to Kafka directly. It writes events
  // to the outbox table, and the OutboxRelay handles Kafka publishing.
  private readonly outboxRepo: OutboxRepository;

  constructor(pool: Pool) {
    this.orderRepo = new OrderRepository(pool);
    this.outboxRepo = new OutboxRepository(pool);
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
    // This is database-level idempotency protection.

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

    // Step 3: Create order in database (transactional — including outbox!)
    // ═══════════════════════════════════════════════════════════════
    // 🔍 PHASE 3 CHANGE: The onTransaction callback inserts the outbox
    // event inside the SAME transaction as the order. This is the
    // Transactional Outbox pattern in action.
    //
    // BEFORE (Phase 2):
    //   order = await orderRepo.create(...);    // DB transaction
    //   await eventProducer.publishOrderCreated(order); // Kafka publish (can fail!)
    //
    // AFTER (Phase 3):
    //   order = await orderRepo.create(..., async (client) => {
    //     await outboxRepo.insertWithClient(client, event); // Same DB transaction!
    //   });
    //   // OutboxRelay handles Kafka publishing later
    // ═══════════════════════════════════════════════════════════════

    let order: Order;
    try {
      order = await this.orderRepo.create(
        input,
        idempotencyKey,
        {
          totalAmount: roundedTotalAmount,
          deliveryFee,
          taxAmount,
          grandTotal,
        },
        async (client) => {
          // We don't have the orderId until the INSERT completes,
          // so we need to query it from the client within the transaction.
          // The orderRepo.create already inserted the order row, so we
          // can get the ID from the RETURNING clause (passed via closure).
          // But since we don't have it here, we build the event with
          // the input data and the calculated totals.
          //
          // 🔍 LEARNING NOTE: We build the full event envelope HERE,
          // before it goes to the outbox. This way the relay doesn't
          // need to know anything about event schemas — it just
          // forwards the JSON blob to Kafka.

          // We'll get the orderId after create returns, but we need it
          // for the event. So we query the latest order we just inserted.
          const result = await client.query<{ id: string }>(
            `SELECT id FROM orders WHERE idempotency_key = $1
             UNION ALL
             SELECT id FROM orders WHERE customer_id = $2
             ORDER BY 1 DESC LIMIT 1`,
            [idempotencyKey ?? '', input.customerId]
          );

          const orderId = result.rows[0]?.id ?? 'unknown';

          const event = buildOrderCreatedEvent({
            id: orderId,
            customerId: input.customerId,
            customerName: input.customerName,
            customerEmail: input.customerEmail,
            customerPhone: input.customerPhone,
            restaurantId: input.restaurantId,
            restaurantName: input.restaurantName,
            status: 'PENDING',
            totalAmount: roundedTotalAmount,
            deliveryFee,
            taxAmount,
            grandTotal,
            deliveryAddress: input.deliveryAddress,
            items: input.items.map((item) => ({
              itemName: item.itemName,
              quantity: item.quantity,
              unitPrice: item.unitPrice,
              totalPrice: Math.round(item.quantity * item.unitPrice * 100) / 100
            })),
          });

          const outboxEvent: InsertOutboxEvent = {
            aggregateId: orderId,
            eventType: event.eventType,
            topic: config.kafkaTopicOrderEvents,
            key: orderId,
            payload: event as unknown as Record<string, unknown>,
            headers: {
              'event-type': event.eventType,
              'event-id': event.eventId,
              'source': event.source,
            }
          };

          await this.outboxRepo.insertWithClient(client, outboxEvent);
        }
      );
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
      '📦 Order created (event written to outbox)'
    );

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
    // Step 3: Update status (with optimistic concurrency + outbox)
    // 🔍 PHASE 3 CHANGE: Same outbox pattern as createOrder.
    // Status change event is atomically written with the status update.
    const updatedOrder = await this.orderRepo.updateStatus(
      id,
      currentOrder.status,
      input.status,
      input.changedBy ?? "system",
      input.reason,
      async (client) => {
        const event = buildOrderStatusChangedEvent({
          orderId: id,
          previousStatus: currentOrder.status,
          newStatus: input.status,
          changedBy: input.changedBy ?? 'system',
          reason: input.reason,
          customerEmail: currentOrder.customerEmail,
          customerName: currentOrder.customerName,
          restaurantName: currentOrder.restaurantName,
        });

        const outboxEvent: InsertOutboxEvent = {
          aggregateId: id,
          eventType: event.eventType,
          topic: config.kafkaTopicOrderEvents,
          key: id,
          payload: event as unknown as Record<string, unknown>,
          headers: {
            'event-type': event.eventType,
            'event-id': event.eventId,
            'source': event.source
          },
        };

        await this.outboxRepo.insertWithClient(client, outboxEvent);
      }
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
      `📋 Order status updated: ${currentOrder.status} → ${input.status} (event written to outbox)`
    );

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
