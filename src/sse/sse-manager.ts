// ============================================================
// SSE Manager — Connection Registry & Fanout (Phase 5)
// ============================================================
// 🔍 LEARNING NOTE: The SSEManager is the central coordination point
// for all SSE connections. It solves three problems:
//
// 1. ROUTING: When a status update arrives via Redis Pub/Sub,
//    which SSE connection(s) should receive it?
//    Answer: Look up orderId → Set<SSEConnection> mapping.
//
// 2. LIFECYCLE: When a client disconnects, clean up the connection
//    and potentially unsubscribe from the Redis channel.
//
// 3. RESOURCE PROTECTION: Limit the total number of connections
//    to prevent memory exhaustion. Each SSE connection consumes:
//    - ~5KB of Node.js memory (response object, buffers, metadata)
//    - 1 file descriptor (for the TCP socket)
//    - CPU for heartbeat timers
//    With 10,000 connections, that's ~50MB of memory and 10,000 FDs.
//
// REAL-WORLD COMPARISON:
//   Uber: 1M+ concurrent WebSocket/SSE connections per region
//   Slack: Uses a similar manager pattern with connection pools
//   Discord: Manages millions of WebSocket connections per gateway
//   Our system: Starts at 1,000 max (configurable)
//
// The key insight is that SSE connections are STATEFUL.
// Unlike HTTP requests (stateless, milliseconds), SSE connections
// live for minutes to hours. This fundamentally changes capacity
// planning, memory management, and shutdown behavior.

import { ServerResponse } from "http";
import { createModuleLogger } from "../shared/logger";
import { SSEConnection } from "./sse-connection";
import {
    SSEEvent,
    SSEMetrics,
    SSEConnectionState,
    BackpressureConfig
} from './types.js';

const log = createModuleLogger('sse-manager');

export interface SSEManagerConfig {
    // Maximum total SSE connections across all orders
    maxTotalConnections: number;

    // Maximum SSE connections per single order
    maxConnectionsPerOrder: number;

    // Heartbeat interval in milliseconds
    heartbeatIntervalMs: number;

    // Client retry interval hint in milliseconds
    clientRetryMs: number;

    // Backpressure configuration
    backpressureConfig: BackpressureConfig;
}

export class SSEManager {
    // 🔍 LEARNING NOTE: We use a Map of orderId → Set<SSEConnection>.
    // A Set ensures O(1) add/remove/has operations.
    // Multiple clients can track the same order (e.g., customer + driver + restaurant).
    private readonly connections = new Map<string, Set<SSEConnection>>();

    // Metrics
    private _totalConnectionsOpened = 0;
    private _totalConnectionsClosed = 0;
    private _backpressureDisconnects = 0;
    private _eventsSentTotal = 0;
    private _eventsDroppedTotal = 0;

    // Callbacks for external coordination
    // 🔍 LEARNING NOTE: The manager doesn't know about Redis directly.
    // Instead, it calls back to the Redis subscriber when it needs to
    // subscribe/unsubscribe from channels. This is dependency inversion —
    // the manager depends on an abstraction (callback), not a concrete
    // Redis client. This makes testing easier and reduces coupling.
    private onFirstConnection: ((orderId: string) => void) | null = null;
    private onLastDisconnection: ((orderId: string) => void) | null = null;

    constructor(private readonly config: SSEManagerConfig) {
        log.info(
            {
                maxTotalConnections: config.maxTotalConnections,
                maxConnectionsPerOrder: config.maxConnectionsPerOrder,
                heartbeatIntervalMs: config.heartbeatIntervalMs,
            },
            '📡 SSE Manager initialized'
        );
    }

    // ─────────────────────────────────────────────────
    // Set external event handlers
    // ─────────────────────────────────────────────────
    setConnectionHandlers(
        onFirst: (orderId: string) => void,
        onLast: (orderId: string) => void,
    ): void {
        this.onFirstConnection = onFirst;
        this.onLastDisconnection = onLast;
    }

    // ─────────────────────────────────────────────────
    // Register a new SSE connection
    // ─────────────────────────────────────────────────
    addConnection(res: ServerResponse, orderId: string): SSEConnection | null {
        // Check total connection limit
        const totalActive = this.getActiveConnectionCount();
        if (totalActive >= this.config.maxTotalConnections) {
            log.warn(
                {
                    orderId,
                    activeConnections: totalActive,
                    maxConnections: this.config.maxTotalConnections,
                },
                `⚠️  SSE connection rejected — max connections reached (${totalActive}/${this.config.maxTotalConnections})`
            );
            return null;
        }

        // Check per-order connection limit
        const orderConnections = this.connections.get(orderId);
        if (orderConnections && orderConnections.size >= this.config.maxConnectionsPerOrder) {
            log.warn(
                {
                    orderId,
                    activeForOrder: orderConnections.size,
                    maxPerOrder: this.config.maxConnectionsPerOrder,
                },
                `⚠️  SSE connection rejected — max connections per order reached`
            );
            return null;
        }

        // Create the connection
        const connection = new SSEConnection(
            res,
            orderId,
            this.config.heartbeatIntervalMs,
            this.config.backpressureConfig
        );

        // Add to registry
        if (!this.connections.has(orderId)) {
            this.connections.set(orderId, new Set());
        }

        this.connections.get(orderId)!.add(connection);
        this._totalConnectionsOpened++;

        // First connection for this order — notify subscriber to listen
        if (this.connections.get(orderId)!.size === 1) {
            this.onFirstConnection?.(orderId);
        }

        // Send initial connection event with retry hint
        // 🔍 LEARNING NOTE: The 'retry' field tells the browser's
        // EventSource how long to wait before reconnecting after
        // a connection drops. Default is 3 seconds. This is sent
        // only on the initial connection — the client remembers it.
        connection.send({
            event: 'connected',
            data: {
                connetionId: connection.id,
                orderId,
                message: 'SSE stream established. You will receive realtime order updates.'
            },
            retry: this.config.clientRetryMs,
        });

        // Register cleanup handler
        // 🔍 LEARNING NOTE: When the connection closes (for any reason),
        // we need to remove it from the registry and potentially
        // unsubscribe from the Redis channel. The SSEConnection calls
        // close() which triggers the 'close' event on the response.
        // We listen for that here.
        res.on('close', () => {
            this.removeConnection(connection);
        });

        log.info(
            {
                connectionId: connection.id,
                orderId,
                totalActive: totalActive + 1,
                activeForOrder: this.connections.get(orderId)!.size,
            },
            `📡 SSE connection added (${totalActive + 1} total active)`
        );

        return connection;
    }

    // ─────────────────────────────────────────────────
    // Remove a connection from the registry
    // ─────────────────────────────────────────────────
    private removeConnection(connection: SSEConnection): void {
        const orderConnections = this.connections.get(connection.orderId);
        if (!orderConnections) return;

        orderConnections.delete(connection);
        this._totalConnectionsClosed++;

        // Track backpressure disconnects
        if (connection.state === SSEConnectionState.CLOSED) {
            const stats = connection.backpressureStats;
            if (stats.backpressureEvents > 0) {
                this._backpressureDisconnects++;
            }
            this._eventsDroppedTotal += stats.droppedEvents;
        }

        // Last connection for this order — notify subscriber to stop listening
        if (orderConnections.size === 0) {
            this.connections.delete(connection.orderId);
            this.onLastDisconnection?.(connection.orderId);

            log.debug(
                { orderId: connection.orderId },
                `No more listeners for order ${connection.orderId} — unsubscribed`
            );
        }
    }

    // ─────────────────────────────────────────────────
    // Broadcast an event to all connections for an order
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This is the FANOUT operation.
    // One event → delivered to N connected clients tracking the same order.
    // In production at scale (Uber, Swiggy), fanout is the primary
    // source of CPU load in SSE/WebSocket servers.
    broadcastToOrder(orderId: string, event: SSEEvent): number {
        const orderConnections = this.connections.get(orderId);
        if (!orderConnections || orderConnections.size === 0) {
            return 0;
        }

        let delivered = 0;
        const toRemove: SSEConnection[] = [];
        for (const connection of orderConnections) {
            if (!connection.isOpen) {
                toRemove.push(connection);
                continue;
            }

            const sent = connection.send(event);
            if (sent) {
                delivered++;
                this._eventsSentTotal++;
            } else {
                this._eventsDroppedTotal++;
            }
        }

        // Clean up closed connections discovered during broadcast
        for (const dead of toRemove) {
            this.removeConnection(dead);
        }

        log.debug(
            {
                orderId,
                totalListeners: orderConnections.size,
                delivered,
                dropped: orderConnections.size - delivered - toRemove.length,
                removed: toRemove.length,
            },
            `📡 Broadcast to ${delivered}/${orderConnections.size} listeners for order ${orderId}`
        );

        return delivered;
    }

    // ─────────────────────────────────────────────────
    // Get metrics
    // ─────────────────────────────────────────────────
    getMetrics(): SSEMetrics {
        return {
            activeConnections: this.getActiveConnectionCount(),
            activeOrders: this.connections.size,
            totalConnectionsOpened: this._totalConnectionsOpened,
            totalConnectionsClosed: this._totalConnectionsClosed,
            backpressureEventsTotal: this.getBackpressureEventsTotal(),
            backpressureDisconnectsTotal: this._backpressureDisconnects,
            eventsSentTotal: this._eventsSentTotal,
            eventsDroppedTotal: this._eventsDroppedTotal
        };
    }

    private getBackpressureEventsTotal(): number {
        let total = 0;
        for (const conns of this.connections.values()) {
            for (const conn of conns) {
                total += conn.backpressureStats.backpressureEvents;
            }
        }
        return total;
    }

    // ─────────────────────────────────────────────────
    // Check if any connections exist for an order
    // ─────────────────────────────────────────────────
    hasListeners(orderId: string): boolean {
        const conns = this.connections.get(orderId);
        return !!conns && conns.size > 0;
    }

    // ─────────────────────────────────────────────────
    // Get active connection count
    // ─────────────────────────────────────────────────
    getActiveConnectionCount(): number {
        let count = 0;
        for (const conss of this.connections.values()) {
            count += conss.size;
        }
        return count;
    }

    // ─────────────────────────────────────────────────
    // Graceful shutdown — drain all connections
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: During shutdown, we send a 'shutdown' event
    // to all connected clients. Well-behaved clients will see this
    // and show a "reconnecting..." UI instead of an error.
    //
    // EventSource auto-reconnection will attempt to reconnect,
    // and the client will land on another instance (if available).
    //
    // We drain connections BEFORE closing the HTTP server so the
    // shutdown events actually get delivered.
    async shutdown(): Promise<void> {
        const activeCount = this.getActiveConnectionCount();
        if (activeCount === 0) {
            log.info('No active SSE connections to drain');
            return;
        }

        log.info(
            { activeConnections: activeCount },
            `📡 Draining ${activeCount} SSE connections...`
        );

        // Send shutdown event to all connections
        for (const [orderId, conns] of this.connections.entries()) {
            for (const conn of conns) {
                conn.send({
                    event: 'shutdown',
                    data: { message: 'Server is shutting down. Reconnecting...' },
                });
                conn.close('server_shutdown');
            }
            conns.clear();
            this.onLastDisconnection?.(orderId);
        }

        this.connections.clear();
        log.info('✅ All SSE connections drained');
    }
}