// ============================================================
// Kafka Event Types and Builders
// ============================================================
// 🔍 LEARNING NOTE: Event design is a critical architectural decision.
//
// Rules for good event design:
//
// 1. SELF-CONTAINED: Events carry all data needed to process them.
//    The notification consumer shouldn't query the orders DB to send
//    an email. If it has to, you haven't actually decoupled.
//
// 2. IMMUTABLE: Once published, an event never changes. If you need
//    to correct something, publish a NEW event.
//
// 3. VERSIONED: Include a schema version so consumers can handle
//    both old and new event formats during upgrades.
//
// 4. GLOBALLY UNIQUE ID: eventId prevents duplicate processing
//    (we'll use this for idempotency in Phase 3).
//
// 5. ENVELOPE PATTERN: Every event has the same outer structure
//    (eventId, eventType, timestamp, etc.) with a type-specific payload.

import { randomUUID } from 'crypto'

// ─────────────────────────────────────────────────
// Event Envelope — Common structure for ALL events
// ─────────────────────────────────────────────────
export interface OrderEvent<T = unknown> {
    eventId: string;            // Globally unique — used for idempotency
    eventType: OrderEventType;
    aggregateId: string;        // orderId — the entity this event belongs to
    timestamp: string;          // ISO 8601
    version: number;            // Schema version for evolution
    source: string;             // Which service produced this event
    payload: T;
}

export const ORDER_EVENT_TYPES = {
    ORDER_CREATED: 'ORDER_CREATED',
    ORDER_STATUS_CHANGED: 'ORDER_STATUS_CHANGED',
    ORDER_CANCELLED: 'ORDER_CANCELLED'
} as const;

export type OrderEventType = (typeof ORDER_EVENT_TYPES)[keyof typeof ORDER_EVENT_TYPES];

// ─────────────────────────────────────────────────
// Event Payloads — Type-specific data
// ─────────────────────────────────────────────────

export interface OrderCreatedPayload {
    order: {
        id: string;
        customerId: string,
        customerName: string,
        customerEmail: string,
        customerPhone?: string;

        restaurantId: string;
        restaurantName: string;

        status: string;

        totalAmount: number;
        deliveryFee: number;
        taxAmount: number;
        grandTotal: number;

        deliveryAddress: string;
        items: Array<{
            itemName: string;
            quantity: number;
            unitPrice: number;
            totalPrice: number;
        }>;
    };
}

export interface OrderStatusChangedPayload {
    orderId: string;
    previousStatus: string;
    newStatus: string;
    changedBy: string;
    reason?: string;

    // 🔍 LEARNING NOTE: We include customer info directly in the event
    // so the notification consumer doesn't need to query the DB.
    // This is the "self-contained event" principle.

    customerEmail: string;
    customerName: string;
    restaurantName: string;
}

// ─────────────────────────────────────────────────
// Event Builder Functions
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: Builder functions ensure events are always
// well-formed. Every event gets a UUID, timestamp, and version.
// Nobody can forget a required field.

export function buildOrderCreatedEvent(
    order: OrderCreatedPayload['order']
): OrderEvent<OrderCreatedPayload> {
    return {
        eventId: randomUUID(),
        eventType: ORDER_EVENT_TYPES.ORDER_CREATED,
        aggregateId: order.id,
        timestamp: new Date().toISOString(),
        version: 1,
        source: 'order-api',
        payload: { order }
    }
}

export function buildOrderStatusChangedEvent(
    data: OrderStatusChangedPayload,
): OrderEvent<OrderStatusChangedPayload> {
    return {
        eventId: randomUUID(),
        eventType: data.newStatus === 'CANCELLED'
            ? ORDER_EVENT_TYPES.ORDER_CANCELLED
            : ORDER_EVENT_TYPES.ORDER_STATUS_CHANGED,

        aggregateId: data.orderId,
        timestamp: new Date().toISOString(),
        version: 1,
        source: 'order-api',
        payload: data,
    }
}

// ─────────────────────────────────────────────────
// Serialization helpers
// ─────────────────────────────────────────────────

export function serializeEvent(event: OrderEvent): string {
    return JSON.stringify(event);
}

export function deserializeEvent(data: string): OrderEvent {
    return JSON.parse(data) as OrderEvent;
}