// ============================================================
// SSE Connection — Individual Client Stream (Phase 5)
// ============================================================
// 🔍 LEARNING NOTE: Each SSE connection wraps a Node.js HTTP response
// object and manages the lifecycle of a single client stream.
//
// SSE PROTOCOL AT THE HTTP LEVEL:
//
// Normal HTTP:
//   Client: GET /orders/123        → Server: 200 OK + body → connection closes
//
// SSE HTTP:
//   Client: GET /orders/123/stream → Server: 200 OK + ... keeps writing ... forever
//                                              ↑
//                                    Content-Type: text/event-stream
//                                    Cache-Control: no-cache
//                                    Connection: keep-alive
//
// The response NEVER closes (until client disconnects or server shuts down).
// The server writes SSE-formatted text to the response body continuously.
//
// SSE MESSAGE FORMAT (each field ends with \n, messages end with \n\n):
//
//   event: status_update\n
//   data: {"orderId":"123","status":"CONFIRMED"}\n
//   id: evt_abc123\n
//   \n
//
// Multi-line data is supported by repeating the 'data:' prefix:
//   data: line 1\n
//   data: line 2\n
//   \n
//
// Comments (used for heartbeat/keepalive):
//   : this is a comment\n
//   \n

import { ServerResponse } from 'http';
import { randomUUID } from 'crypto';
import { createModuleLogger } from '../shared/logger.js';
import {
    SSEEvent,
    SSEConnectionState,
    BackpressureConfig
} from './types.js';
import {
    BackpressureTracker,
    DEFAULT_BACKPRESSURE_CONFIG
} from './sse-backpressure.js';

const log = createModuleLogger('sse-connection');

export class SSEConnection {
    readonly id: string;
    readonly orderId: string;
    readonly connectedAt: Date;

    private _state: SSEConnectionState = SSEConnectionState.OPEN;
    private readonly res: ServerResponse;
    private readonly backpressure: BackpressureTracker;
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    private _eventsSent = 0;
    private _lastEventId = 0; // Incrementing ID for Last-Event-ID resume

    constructor(
        res: ServerResponse,
        orderId: string,
        heartbeatIntervalMs: number,
        backpressureConfig: BackpressureConfig = DEFAULT_BACKPRESSURE_CONFIG
    ) {
        this.id = randomUUID();
        this.orderId = orderId;
        this.connectedAt = new Date();
        this.res = res;
        this.backpressure = new BackpressureTracker(this.id, backpressureConfig);

        // ─────────────────────────────────────────────────
        // Set SSE headers
        // ─────────────────────────────────────────────────
        // 🔍 LEARNING NOTE: These three headers are the SSE contract:
        //
        // Content-Type: text/event-stream
        //   Tells the browser this is an SSE stream, not a regular response.
        //   The browser's EventSource API expects this exact content type.
        //
        // Cache-Control: no-cache
        //   Prevents proxies/CDNs from caching the response.
        //   A cached SSE stream would be stale and never update.
        //
        // Connection: keep-alive
        //   Tells intermediaries to keep the TCP connection open.
        //   Without this, some proxies/load balancers close the connection
        //   after the first "complete" response (which, for SSE, is never).
        //
        // X-Accel-Buffering: no
        //   Nginx-specific header. By default, Nginx buffers response bodies
        //   before forwarding them to the client. For SSE, this means events
        //   would be delayed until the buffer fills (potentially minutes).
        //   This header disables buffering for this response.
        this.res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
            // 🔍 LEARNING NOTE: CORS headers for cross-origin SSE.
            // EventSource respects CORS. Without this, a frontend on
            // localhost:3001 can't connect to SSE on localhost:3000.
            'Access-Control-Allow-Origin': '*',
        });

        // ─────────────────────────────────────────────────
        // Handle client disconnect
        // ─────────────────────────────────────────────────
        // 🔍 LEARNING NOTE: When the client closes the connection
        // (navigates away, closes tab, network drops), Node.js emits
        // a 'close' event on the response object. We MUST clean up:
        // 1. Stop the heartbeat timer (prevent memory leak)
        // 2. Mark connection as CLOSED (prevent writes to dead socket)
        //
        // Without this cleanup:
        // - Heartbeat timers accumulate (memory leak)
        // - Writes to closed sockets throw EPIPE errors
        // - The SSEManager thinks the connection is still active
        this.res.on('close', () => {
            this.close('client_disconnect');
        });

        // Handle drain event (backpressure relieved)
        this.res.on('drain', () => {
            this.backpressure.onDrain();
            if (this._state === SSEConnectionState.DRAINING) {
                this._state = SSEConnectionState.OPEN;
                log.debug(
                    { connectionId: this.id, orderId: this.orderId },
                    'Connection drained — resuming normal operation'
                )
            }
        });

        // ─────────────────────────────────────────────────
        // Start heartbeat
        // ─────────────────────────────────────────────────
        // 🔍 LEARNING NOTE: Heartbeats solve THREE problems:
        //
        // 1. PROXY TIMEOUT: Many proxies/load balancers (Nginx, AWS ALB,
        //    Cloudflare) close idle connections after 60-120 seconds.
        //    Heartbeats keep the connection "active" from the proxy's
        //    perspective. Without heartbeats, the proxy kills the connection
        //    and the client has to reconnect.
        //
        // 2. DEAD CONNECTION DETECTION: If the client's network drops
        //    (e.g., phone enters a tunnel), TCP doesn't notice immediately.
        //    The server might keep writing to a dead socket for minutes.
        //    Heartbeat writes will eventually trigger an EPIPE error,
        //    letting us detect and clean up the dead connection faster.
        //
        // 3. CLIENT-SIDE RECONNECTION: The EventSource API has built-in
        //    reconnection, but it only triggers when it detects the
        //    connection is closed. Regular heartbeats help the client
        //    distinguish between "server is quiet" and "connection is dead".
        //
        // We use SSE comments (lines starting with ':') for heartbeats.
        // The EventSource API ignores comments — they don't trigger events
        // on the client. They're invisible to application code but keep
        // the connection alive at the protocol level.
        this.heartbeatTimer = setInterval(() => {
            this.sendHeartbeat();
        }, heartbeatIntervalMs);

        // Don't let heartbeat timer prevent Node.js from exiting
        this.heartbeatTimer.unref();

        log.info(
            {
                connectionId: this.id,
                orderId: this.orderId,
                heartbeatIntervalMs,
            },
            `📡 SSE connection opened for order ${orderId}`
        );
    }

    // ─────────────────────────────────────────────────
    // Send an SSE event
    // ─────────────────────────────────────────────────
    send(event: SSEEvent): boolean {
        if (this._state === SSEConnectionState.CLOSED) {
            return false;
        }

        // Format the SSE message
        const formatted = this.formatSSEMessage(event);
        const bytes = Buffer.byteLength(formatted, 'utf-8');

        // Check backpressure before writing
        const bpAction = this.backpressure.shouldAcceptEvent(bytes);
        if (bpAction.action === 'DROP') {
            return false;
        }

        if (bpAction.action === 'DISCONNECT') {
            this.close('backpressure_disconnect');
            return false;
        }

        // Write to the response stream
        // 🔍 LEARNING NOTE: res.write() returns a boolean:
        //   true  → data was flushed to kernel (or is small enough to buffer safely)
        //   false → internal buffer is full, you SHOULD stop writing until 'drain'
        //
        // We track this return value for backpressure detection.
        // Most tutorials ignore it. Production systems can't afford to.
        const writeAccepted = this.res.write(formatted);
        this.backpressure.recordWrite(bytes, writeAccepted);
        this._eventsSent++;

        if (!writeAccepted) {
            this._state = SSEConnectionState.DRAINING;
            log.debug(
                { connectionId: this.id, orderId: this.orderId, bytes },
                'Write buffer full — entering DRAINING state'
            );
        }

        return true;
    }

    // ─────────────────────────────────────────────────
    // Send heartbeat comment
    // ─────────────────────────────────────────────────
    private sendHeartbeat(): void {
        if (this._state === SSEConnectionState.CLOSED) {
            return;
        }

        // 🔍 LEARNING NOTE: SSE comments start with ':'.
        // They are completely ignored by the EventSource API.
        // We include a timestamp for debugging — if you tcpdump
        // the connection, you can see when heartbeats were sent.
        const heartbeat = `: heatbeat ${new Date().toISOString()}\n\n`;

        try {
            this.res.write(heartbeat);
        } catch {
            // Write failed — connection is probably dead
            this.close('heartbeat_write_failed');
        }
    }

    // ─────────────────────────────────────────────────
    // Format an SSE message
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: The SSE format is strictly defined:
    //   - Each field is on its own line: "field: value\n"
    //   - Messages are separated by an empty line: "\n"
    //   - Data can be multi-line (each line prefixed with "data: ")
    //   - Unknown fields are ignored by the client
    //   - Comments start with ":"
    //
    // The 'id' field is especially important. When the client
    // reconnects (EventSource auto-reconnection), it sends the
    // last received ID in the `Last-Event-ID` header. The server
    // can use this to resume the stream from where the client left off.
    private formatSSEMessage(event: SSEEvent): string {
        let message = '';

        // Event name (optional — defaults to 'message' on the client)
        if (event.event) {
            message += `event: ${event.event}\n`;
        }

        // Data (required)
        const data = typeof event.data === 'string'
            ? event.data
            : JSON.stringify(event.data);

        // Handle multi-line data
        // 🔍 LEARNING NOTE: Each line of a multi-line data field must be
        // prefixed with "data: ". The client concatenates all data lines
        // with newlines to reconstruct the original message.
        const lines = data.split('\n');
        for (const line of lines) {
            message += `data: ${line}\n`;
        }

        // ID (for Last-Event-ID resume)
        if (event.id) {
            message += `id: ${event.id}\n`;
        } else {
            // Auto-generate incrementing IDs
            this._lastEventId++;
            message += `id: ${this._lastEventId}\n`;
        }

        // Retry hint (only sent once, on initial connection)
        if (event.retry !== undefined) {
            message += `retry: ${event.retry}\n`;
        }

        // Empty line terminates the message
        message += '\n';

        return message;
    }

    // ─────────────────────────────────────────────────
    // Close the connection
    // ─────────────────────────────────────────────────
    close(reason: string): void {
        if (this._state === SSEConnectionState.CLOSED) {
            return;
        }

        this._state = SSEConnectionState.CLOSED;

        // Stop heartbeat
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        // Try to send a final event before closing
        // (won't work if client already disconnected, that's fine)
        try {
            if (reason !== 'client_disconnect') {
                const closeMessage = `event: close\ndata: ${JSON.stringify({ reason })}\n\n`;
                this.res.write(closeMessage);
            }
            this.res.end();
        } catch {
            // Connection might already be dead — ignore write errors
        }

        log.info(
            {
                connectionId: this.id,
                orderId: this.orderId,
                reason,
                eventsSent: this._eventsSent,
                durationMs: Date.now() - this.connectedAt.getTime(),
            },
            `📡 SSE connection closed: ${reason} (sent ${this._eventsSent} events)`
        );
    }

    // ─────────────────────────────────────────────────
    // Accessors
    // ─────────────────────────────────────────────────
    get state(): SSEConnectionState { return this._state; }
    get eventsSent(): number { return this._eventsSent; }
    get isOpen(): boolean { return this._state !== SSEConnectionState.CLOSED; }
    get backpressureStats() {
        return {
            bufferedBytes: this.backpressure.bufferedBytes,
            isDraining: this.backpressure.isDraining,
            droppedEvents: this.backpressure.droppedEvents,
            backpressureEvents: this.backpressure.backpressureEvents
        };
    }
}