// ============================================================
// SSE Types — Realtime Streaming (Phase 5)
// ============================================================
// 🔍 LEARNING NOTE: Server-Sent Events (SSE) is an HTTP-based protocol
// for unidirectional server-to-client streaming. Unlike WebSockets,
// SSE uses plain HTTP — no protocol upgrade, no binary framing,
// no ping/pong. Just a response body that never closes.
//
// The SSE protocol defines a simple text format:
//   event: status_update\n      ← optional event name
//   data: {"status":"CONFIRMED"}\n   ← the payload (can be multi-line)
//   id: evt_123\n                ← optional event ID for resume
//   retry: 3000\n                ← optional reconnect delay hint
//   \n                           ← empty line = end of message
//
// SSE comments start with a colon and are used for keepalive:
//   : heartbeat\n\n              ← ignored by EventSource, resets proxy timeout

// ─────────────────────────────────────────────────
// SSE Event — What gets sent over the wire
// ─────────────────────────────────────────────────
export interface SSEEvent {
    /** Optional event name. Client uses addEventListener(name, ...) to filter. */
    event?: string;

    /** The payload data. Will be JSON.stringify'd if object. */
    data: string | Record<string, unknown>;

    /** Event ID. Client sends Last-Event-ID on reconnect for resume. */
    id?: string;

    /** Suggested reconnection delay in milliseconds. Sent once on initial connect. */
    retry?: number;
}

// ─────────────────────────────────────────────────
// Connection State Machine
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: SSE connections have a lifecycle:
//   OPEN → DRAINING → CLOSED
//
// OPEN: Normal operation. Events are written immediately.
// DRAINING: Backpressure detected. Events are buffered or dropped.
//           Waiting for the client to catch up (Node.js 'drain' event).
// CLOSED: Connection terminated. No more writes allowed.
//         Cleanup has been performed.
export enum SSEConnectionState {
    OPEN = 'OPEN',
    DRAINING = 'DRAINING',
    CLOSED = 'CLOSED',
}

// ─────────────────────────────────────────────────
// Backpressure Configuration
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: Backpressure occurs when the server writes data
// faster than the client can read it. The OS TCP send buffer fills up.
//
// Without handling:
//   Server writes 1MB/s → Client reads 100KB/s
//   → Node.js buffers in memory → unbounded growth → OOM crash
//
// With handling:
//   Server detects buffer filling → applies policy → system stays stable
//
// Real-world analogy: A bathtub with water coming in faster than
// the drain can remove it. You need to either turn down the faucet
// (slow producer) or open the overflow (drop/disconnect).
export interface BackpressureConfig {
    /** Buffer size that triggers a WARNING log. Default: 64KB */
    highWaterMark: number;

    /** Buffer size that triggers the backpressure POLICY. Default: 256KB */
    criticalMark: number;

    /** What to do when critical mark is exceeded */
    policy: BackpressurePolicy;
}

// ─────────────────────────────────────────────────
// Backpressure POLICY
// ─────────────────────────────────────────────────
export enum BackpressurePolicy {
    /** Drop the oldest buffered event (keep the newest) */
    DROP_OLDEST = 'DROP_OLDEST',

    /** Drop the newest event (keep existing buffer) */
    DROP_NEWEST = 'DROP_NEWEST',

    /** Disconnect the slow client entirely */
    DISCONNECT = 'DISCONNECT',
}

// ─────────────────────────────────────────────────
// Order Stream Events — Domain-specific SSE payloads
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: These are the events that flow over the SSE
// connection for order tracking. They are DIFFERENT from Kafka events.
//
// Kafka events: internal system events for service communication
// SSE events: client-facing events for UI updates
//
// The consumer TRANSLATES Kafka events into SSE events.
// This is intentional — the client shouldn't know about Kafka,
// outbox patterns, or consumer groups. It just sees clean status updates.
export interface OrderStreamEvent {
    /** The event type for SSE routing */
    type: OrderStreamEventType;

    /** The order ID this event belongs to */
    orderId: string;

    /** Event-specific payload */
    payload: Record<string, unknown>;

    /** ISO 8601 timestamp */
    timestamp: string;
}

export type OrderStreamEventType =
    | 'status_update'
    | 'order_assigned'
    | 'delivery_update'
    | 'order_cancelled';

// ─────────────────────────────────────────────────
// SSE Manager Metrics
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: Metrics are essential for production SSE.
// Without them, you can't answer:
//   - How many connections are open right now?
//   - Are clients experiencing backpressure?
//   - How many connections disconnected in the last hour?
//   - Is our connection limit about to be hit?
//
// These feed into the health check endpoint and will be
// scraped by Prometheus in Phase 6.
export interface SSEMetrics {
    /** Currently active SSE connections */
    activeConnections: number;

    /** Number of distinct orders being tracked */
    activeOrders: number;

    /** Total connections opened since startup */
    totalConnectionsOpened: number;

    /** Total connections closed since startup */
    totalConnectionsClosed: number;

    /** Number of backpressure events triggerd */
    backpressureEventsTotal: number;

    /** Number of clients disconnected due to backpressure */
    backpressureDisconnectsTotal: number;

    /* Total events sent across all connections */
    eventsSentTotal: number;

    /* Total events dropped due to backpressure */
    eventsDroppedTotal: number;
}

// ─────────────────────────────────────────────────
// Redis Pub/Sub Message Format
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: This is what gets published to Redis Pub/Sub
// by the notification consumer and received by the SSE subscriber.
// It's a simple JSON envelope — Redis Pub/Sub doesn't have schemas
// or message types, so we include them in the payload.
export interface RedisPubSubMessage {
    /** The SSE event type to use */
    eventType: OrderStreamEventType;

    /** The order this message belongs to */
    orderId: string;

    /** Event-specific data to send to the client */
    data: Record<string, unknown>;

    /** ISO 8601 timestamp of the original event */
    timestamp: string;

    /** Source system that generated this event */
    source: string;
}