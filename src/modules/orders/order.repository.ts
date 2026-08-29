// ============================================================
// Order Repository — Data Access Layer
// ============================================================

import { Pool, PoolClient } from "pg";
import {
  Order,
  OrderStatusHistoryEntry,
  CreateOrderInput,
  OrderStatusType,
} from "./order.types.js";
import { createModuleLogger } from "../../shared/logger.js";

const log = createModuleLogger("order-repository");

export class OrderRepository {
  constructor(private readonly pool: Pool) { }

  // ─────────────────────────────────────────────────
  // CREATE — Transactional multi-table insert
  // ─────────────────────────────────────────────────
  // 🔍 LEARNING NOTE: This method uses a DATABASE TRANSACTION.
  //
  // Why transactions matter:
  //   Without a transaction, if we insert the order but crash before
  //   inserting order_items, we have a corrupt order in the DB.
  //   With a transaction, it's ALL or NOTHING:
  //   - BEGIN: start the transaction
  //   - INSERT order: ✅
  //   - INSERT items: ✅ or ❌
  //   - If any step fails → ROLLBACK: everything is undone
  //   - If all succeed → COMMIT: everything is persisted atomically
  //
  // This is ACID (Atomicity, Consistency, Isolation, Durability).
  // You get this FOR FREE with PostgreSQL transactions.
  // In distributed systems (Phase 2+), you DON'T get this for free.
  // That's when things get hard.
  //
  // 🔍 PHASE 3 EVOLUTION: The onTransaction callback
  //
  // The create method now accepts an optional callback that runs
  // INSIDE the same transaction, BEFORE commit. The OrderService
  // uses this to insert outbox events atomically with the order:
  //
  //   BEGIN
  //     INSERT INTO orders       ← order data
  //     INSERT INTO order_items  ← item data
  //     INSERT INTO outbox_events← event data (via callback)
  //   COMMIT
  //
  // All three writes succeed or fail together. No dual-write gap.
  async create(
    input: CreateOrderInput,
    idempotencyKey: string | undefined,
    calculatedTotals: {
      totalAmount: number;
      deliveryFee: number;
      taxAmount: number;
      grandTotal: number;
    },
    onTransaction?: (client: PoolClient) => Promise<void>
  ): Promise<Order> {
    const client: PoolClient = await this.pool.connect();

    try {
      await client.query("BEGIN");

      if (idempotencyKey) {
        // 🔍 LEARNING NOTE: pg_advisory_xact_lock acts as a mutex for this specific key.
        // It prevents race conditions cleanly without triggering unique constraint violation
        // exceptions. If two concurrent requests have the same key, the second one waits here
        // until the first transaction completes, then it proceeds and finds the existing order.
        await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
          idempotencyKey,
        ]);

        const existingOrderResult = await client.query<OrderRow>(
          `SELECT * FROM orders WHERE idempotency_key = $1`,
          [idempotencyKey],
        );

        if (existingOrderResult.rows.length > 0) {
          const existingOrder = existingOrderResult.rows[0]!;
          const existingItemsResult = await client.query<OrderItemRow>(
            `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
            [existingOrder.id],
          );

          await client.query("COMMIT");
          return this.mapToOrder(existingOrder, existingItemsResult.rows);
        }
      }

      // Insert Order
      const orderResult = await client.query<OrderRow>(
        `INSERT INTO orders (
                    status,
                    customer_id, customer_name, customer_email, customer_phone,
                    restaurant_id, restaurant_name,
                    total_amount, delivery_fee, tax_amount, grand_total,
                    delivery_address, delivery_lat, delivery_lng,
                    notes, idempotency_key
                ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
                    ) RETURNING *`,
        [
          "PENDING",
          input.customerId,
          input.customerName,
          input.customerEmail,
          input.customerPhone ?? null,

          input.restaurantId,
          input.restaurantName,

          calculatedTotals.totalAmount,
          calculatedTotals.deliveryFee,
          calculatedTotals.taxAmount,
          calculatedTotals.grandTotal,

          input.deliveryAddress,
          input.deliveryLat ?? null,
          input.deliveryLng ?? null,

          input.notes ?? null,
          idempotencyKey ?? null,
        ],
      );

      const orderRow = orderResult.rows[0]!;

      // Insert order items in batch
      // 🔍 LEARNING NOTE: We insert items one-by-one in a loop here.
      // For small item counts (1-10 per order), this is fine.
      // For bulk inserts (1000+ rows), you'd use:
      //   - COPY command (fastest)
      //   - Multi-row INSERT VALUES (...), (...), (...)
      //   - unnest() with array parameters
      // Premature optimization is the root of all evil.
      const items: OrderItemRow[] = [];
      for (const item of input.items) {
        const totalPrice = item.quantity * item.unitPrice;
        const itemResult = await client.query<OrderItemRow>(
          `INSERT INTO order_items (
                            order_id, item_name, quantity, unit_price, total_price, customizations 
                        ) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
          [
            orderRow.id,
            item.itemName,
            item.quantity,
            item.unitPrice,
            totalPrice,
            item.customizations ?? null,
          ],
        );

        items.push(itemResult.rows[0]!);
      }

      // Insert intial status history
      await client.query(
        `INSERT INTO order_status_history (
                    order_id, from_status, to_status, changed_by, reason
                ) VALUES ($1, NULL, $2, $3, $4)`,
        [orderRow.id, "PENDING", "system", "Order created"],
      );

      // 🔍 PHASE 3: Execute the outbox insert callback (same transaction!)
      if (onTransaction) {
        await onTransaction(client);
      }


      await client.query("COMMIT");

      log.info(
        { orderId: orderRow.id, itemCount: items.length },
        "Order created in database",
      );

      return this.mapToOrder(orderRow, items);
    } catch (err) {
      await client.query("ROLLBACK");
      log.error({ err }, "Failed to create order, transaction rolled back");
      throw err;
    } finally {
      // ALWAYS release the client back to the pool.
      client.release();
    }
  }

  // ─────────────────────────────────────────────────
  // FIND BY ID
  // ─────────────────────────────────────────────────
  async findById(id: string): Promise<Order | null> {
    const orderResult = await this.pool.query<OrderRow>(
      `SELECT * FROM orders where id = $1`,
      [id],
    );

    if (orderResult.rows.length === 0) {
      return null;
    }

    const itemResult = await this.pool.query<OrderItemRow>(
      `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
      [id],
    );

    return this.mapToOrder(orderResult.rows[0]!, itemResult.rows);
  }

  // ─────────────────────────────────────────────────
  // FIND BY IDEMPOTENCY KEY
  // ─────────────────────────────────────────────────
  async findByIdempotencyKey(key: string): Promise<Order | null> {
    const result = await this.pool.query<OrderRow>(
      `SELECT * FROM orders where idempotency_key = $1`,
      [key],
    );

    if (result.rows.length === 0) return null;

    const itemResult = await this.pool.query<OrderItemRow>(
      `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
      [result.rows[0]!.id],
    );

    return this.mapToOrder(result.rows[0]!, itemResult.rows);
  }

  // ─────────────────────────────────────────────────
  // LIST (Paginated)
  // ─────────────────────────────────────────────────
  // 🔍 LEARNING NOTE: We use OFFSET-based pagination here because it's simple.
  // In production with millions of rows, OFFSET is SLOW because PostgreSQL
  // still scans all offset rows before returning results.
  //
  // Better alternatives for large datasets:
  // - Cursor-based pagination (WHERE id > last_seen_id LIMIT N)
  // - Keyset pagination
  //
  // We'll stick with OFFSET for now — premature optimization is evil.
  // But know that this WILL become a problem at scale.
  async findAll(
    page: number,
    limit: number,
    filters?: { status?: OrderStatusType; customerId?: string },
  ): Promise<{ orders: Order[]; total: number }> {
    const conditions: string[] = [];
    const params: unknown[] = [];
    let paramIndex = 1;

    if (filters?.status) {
      conditions.push(`status=$${paramIndex++}`);
      params.push(filters.status);
    }

    if (filters?.customerId) {
      conditions.push(`customer_id=$${paramIndex++}`);
      params.push(filters.customerId);
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    // Count total
    const countResult = await this.pool.query<{ count: string }>(
      `SELECT COUNT(*) FROM orders ${whereClause}`,
      params,
    );

    const total = parseInt(countResult.rows[0]!.count, 10);

    // Fetch Page
    const offset = (page - 1) * limit;
    const orderResult = await this.pool.query<OrderRow>(
      `SELECT * FROM orders ${whereClause}
         ORDER BY created_at DESC
         LIMIT $${paramIndex++} OFFSET $${paramIndex}`,
      [...params, limit, offset],
    );

    // Fetch items for all orders in one query (N+1 prevention)
    // One query with WHERE IN (...) to fetch all the order_items in single query

    const orderIds = orderResult.rows.map((r) => r.id);
    let itemsByOrderId: Map<string, OrderItemRow[]> = new Map();

    if (orderIds.length > 0) {
      const itemResult = await this.pool.query<OrderItemRow>(
        `SELECT * FROM order_items WHERE order_id = ANY($1) ORDER BY created_at`,
        [orderIds],
      );

      for (const item of itemResult.rows) {
        const existing = itemsByOrderId.get(item.order_id) ?? [];
        existing.push(item);
        itemsByOrderId.set(item.order_id, existing);
      }
    }

    const orders = orderResult.rows.map((row) =>
      this.mapToOrder(row, itemsByOrderId.get(row.id) ?? []),
    );

    return { orders, total };
  }

  // ─────────────────────────────────────────────────
  // UPDATE STATUS
  // ─────────────────────────────────────────────────
  async updateStatus(
    id: string,
    fromStatus: OrderStatusType,
    toStatus: OrderStatusType,
    changedBy: string,
    reason?: string,
    onTransaction?: (client: PoolClient) => Promise<void>
  ): Promise<Order | null> {
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      // LEARNING NOTE: The WHERE clause includes status = $2 (fromStatus).
      // This is OPTIMISTIC CONCURRENCY CONTROL.
      // If two requests try to update the same order simultaneously:
      //   Request A: PENDING → CONFIRMED (succeeds, rowCount = 1)
      //   Request B: PENDING → CONFIRMED (fails, rowCount = 0, because status is now CONFIRMED)
      // Without this check, both could succeed and we'd have inconsistent history.
      // This pattern becomes critical in distributed systems where multiple
      // consumers might process the same order event.

      const result = await client.query<OrderRow>(
        `UPDATE orders SET status = $1
        WHERE id = $2 AND status = $3
        RETURNING *
        `,
        [toStatus, id, fromStatus],
      );

      if (result.rows.length === 0) {
        await client.query("ROLLBACK");
        return null;
      }

      // Record status change history
      await client.query(
        `INSERT INTO order_status_history (
            order_id, from_status, to_status, changed_by, reason
        ) VALUES ($1, $2, $3, $4, $5)`,
        [id, fromStatus, toStatus, changedBy, reason ?? null],
      );

      // 🔍 PHASE 3: Execute the outbox insert callback (same transaction!)
      if (onTransaction) {
        await onTransaction(client);
      }

      // Fetch items for the response before COMMIT so a failure can still roll back.
      const itemsResult = await client.query<OrderItemRow>(
        `SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at`,
        [id],
      );

      await client.query("COMMIT");

      return this.mapToOrder(result.rows[0]!, itemsResult.rows);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async getStatusHistory(orderId: string): Promise<OrderStatusHistoryEntry[]> {
    const result = await this.pool.query<StatusHistoryRow>(
      `SELECT * from order_status_history where order_id = $1
        ORDER BY created_at ASC
        `,
      [orderId],
    );

    return result.rows.map((row) => ({
      id: row.id,
      orderId: row.order_id,
      fromStatus: row.from_status,
      toStatus: row.to_status,
      changedBy: row.changed_by,
      reason: row.reason ?? undefined,
      metadata: row.metadata ?? undefined,
      createdAt: row.created_at,
    }));
  }

  // ─────────────────────────────────────────────────
  // Row-to-Domain mapping
  // ─────────────────────────────────────────────────

  private mapToOrder(row: OrderRow, itemRows: OrderItemRow[]): Order {
    return {
      id: row.id,
      status: row.status as OrderStatusType,

      customerId: row.customer_id,
      customerName: row.customer_name,
      customerEmail: row.customer_email,
      customerPhone: row.customer_phone ?? undefined,

      restaurantId: row.restaurant_id,
      restaurantName: row.restaurant_name,

      totalAmount: parseFloat(row.total_amount),
      deliveryFee: parseFloat(row.delivery_fee),
      taxAmount: parseFloat(row.tax_amount),
      grandTotal: parseFloat(row.grand_total),

      deliveryAddress: row.delivery_address,
      deliveryLat:
        row.delivery_lat !== null ? parseFloat(row.delivery_lat) : undefined,
      deliveryLng:
        row.delivery_lng !== null ? parseFloat(row.delivery_lng) : undefined,
      notes: row.notes ?? undefined,
      idempotencyKey: row.idempotency_key ?? undefined,
      items: itemRows.map((item) => ({
        id: item.id,
        orderId: item.order_id,
        itemName: item.item_name,
        quantity: item.quantity,
        unitPrice: parseFloat(item.unit_price),
        totalPrice: parseFloat(item.total_price),
        customizations: item.customizations ?? undefined,
        createdAt: item.created_at,
      })),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }
}

// ─────────────────────────────────────────────────
// Raw DB Row types (snake_case, matching SQL schema)
// ─────────────────────────────────────────────────

interface OrderRow {
  id: string;
  status: string;

  // Customer
  customer_id: string;
  customer_name: string;
  customer_email: string;
  customer_phone: string | null;

  // Restaurant
  restaurant_id: string;
  restaurant_name: string;

  // Payment
  total_amount: string; // NUMERIC comes back as string from pg driver
  delivery_fee: string;
  tax_amount: string;
  grand_total: string;

  // Delivery
  delivery_address: string;
  delivery_lat: string | null;
  delivery_lng: string | null;
  notes: string | null;

  idempotency_key: string | null;
  created_at: Date;
  updated_at: Date;
}

interface OrderItemRow {
  id: string;
  order_id: string;

  item_name: string;
  quantity: number;
  unit_price: string;
  total_price: string;
  customizations: string | null;
  created_at: Date;
}

interface StatusHistoryRow {
  id: string;
  order_id: string;
  from_status: OrderStatusType | null;
  to_status: OrderStatusType;
  changed_by: string;
  reason: string;
  metadata: Record<string, unknown> | null;
  created_at: Date;
}
