// ============================================================
// SSE Routes — Realtime Order Streaming Endpoint (Phase 5)
// ============================================================
// 🔍 LEARNING NOTE: SSE routes are different from normal HTTP routes.
//
// Normal route:
//   Client → Request → Server processes → Response → Connection closes
//   Duration: milliseconds
//
// SSE route:
//   Client → Request → Server sends headers → keeps connection open...
//   ...server pushes events over time... → Client eventually disconnects
//   Duration: minutes to hours
//
// This has major implications:
//
// 1. RESOURCE CONSUMPTION: Each SSE connection holds:
//    - 1 TCP socket (file descriptor)
//    - 1 Node.js response object (~5KB heap)
//    - 1 heartbeat timer
//    - Buffer space for backpressure
//    You need to limit total connections or you'll exhaust resources.
//
// 2. LOAD BALANCER BEHAVIOR: The LB sees an active connection.
//    With connection-based load balancing, SSE clients "stick" to
//    one backend. This can cause uneven load distribution.
//    Consider using least-connections instead of round-robin.
//
// 3. TIMEOUT BEHAVIOR: Many proxies (Nginx, AWS ALB) have idle
//    timeouts. Without heartbeats, they'll close "idle" SSE
//    connections. AWS ALB default idle timeout: 60 seconds.
//    Nginx default: 60 seconds. Our heartbeat: 15 seconds.

import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { Pool } from 'pg';
import { SSEManager } from './sse-manager.js';
import { OrderService } from '../modules/orders/order.service.js';
import { createModuleLogger } from '../shared/logger';
import { ApiResponse } from '../types/index.js';

const log = createModuleLogger('sse-routes');

export async function registerSSERoutes(
    app: FastifyInstance,
    pool: Pool,
    sseManager: SSEManager
): Promise<void> {
    const orderService = new OrderService(pool);

    // ─────────────────────────────────────────────────
    // GET /api/v1/orders/:id/stream — Realtime Order Updates
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This is NOT a normal Fastify route handler.
    //
    // In a normal handler, you return a response and Fastify sends it.
    // In an SSE handler, you hijack the response and write to it directly.
    //
    // Fastify has a concept of "hijacking" where you take control of
    // the underlying Node.js response object. This is necessary because
    // Fastify's normal response pipeline (serialization, hooks, etc.)
    // assumes a single response per request. SSE breaks that assumption.
    //
    // We use reply.hijack() to tell Fastify: "I'm taking over this
    // response. Don't touch it." Then we work directly with reply.raw
    // (the Node.js http.ServerResponse).
    app.get('/api/v1/orders/:id/stream',
        async (
            request: FastifyRequest<{ Params: { id: string } }>,
            reply: FastifyReply
        ) => {
            const { id: orderId } = request.params;

            // Step 1: Validate the order exists
            // 🔍 LEARNING NOTE: We check if the order exists BEFORE opening
            // the SSE connection. This prevents phantom connections where a
            // client connects to a non-existent order and sits there forever
            // receiving only heartbeats.
            //
            // In production, you'd also check authorization here:
            //   - Does this customer own this order?
            //   - Is this driver assigned to this order?
            //   - Is this restaurant fulfilling this order?

            try {
                await orderService.getOrder(orderId);
            } catch (err) {
                // Order not found — return normal HTTP error
                const response: ApiResponse<never> = {
                    success: false,
                    error: {
                        code: 'NOT_FOUND',
                        message: `Order ${orderId} not found`,
                    },
                };
                return reply.status(404).send(response);
            }

            // Step 2: Hijack the response
            // 🔍 LEARNING NOTE: reply.hijack() tells Fastify to stop
            // managing this response. After this call:
            //   - Fastify won't send any response headers or body
            //   - Fastify won't call onResponse hooks for this request
            //   - We have full control over reply.raw (Node.js ServerResponse)
            //   - We MUST handle the response ourselves
            //
            // This is the only way to do SSE in Fastify because Fastify's
            // response pipeline assumes a finite response body.
            await reply.hijack();

            // 🔍 LEARNING NOTE: Disable the per-socket idle timeout for SSE connections.
            //
            // The server has `connectionTimeout: 10000` which calls server.setTimeout(10000).
            // This sets an inactivity timeout on the underlying TCP socket — if no data
            // flows for 10 seconds, the socket is destroyed.
            //
            // For SSE, data only flows SERVER → CLIENT (events, heartbeats). The client
            // sends nothing after the initial GET request. From the kernel's perspective,
            // the incoming side of the socket is always "idle."
            //
            // Our heartbeat fires every 15 seconds — 5 seconds AFTER the 10-second timeout
            // would fire. So every SSE connection would die before its first heartbeat.
            //
            // The fix: disable the timeout for THIS specific socket.
            // We use setTimeout(0) to remove the timeout. This only affects SSE connections.
            // Normal API requests keep their 10-second timeout and are unaffected.
            reply.raw.socket?.setTimeout(0);

            // Step 3: Register the SSE connection
            const connection = sseManager.addConnection(reply.raw, orderId);

            if (!connection) {
                // Max connections reached — send error and close
                // 🔍 LEARNING NOTE: We return 503 (Service Unavailable) not 429
                // (Too Many Requests). 503 tells the client "try again later"
                // and load balancers can route to another instance.
                reply.raw.writeHead(503, { 'Content-Type': 'application/json' });
                reply.raw.end(JSON.stringify({
                    success: false,
                    error: {
                        code: 'SERVICE_UNAVAILABLE',
                        message: 'Maximum SSE connections reached. Please retry'
                    },
                }));
                return;
            }

            // Step 4: Send the current order state as the first event
            // 🔍 LEARNING NOTE: The first SSE event contains the CURRENT state.
            // This is important because between the time the client decides to
            // connect and the connection is established (network latency, TCP
            // handshake, TLS negotiation), the order status might have changed.
            //
            // Without this initial state event, the client would have to
            // make a separate GET request AND open the SSE stream, creating
            // a race condition where they might miss updates between the GET
            // and the stream opening.
            //
            // By sending the current state as the first SSE event, we guarantee
            // the client is synchronized from the moment the connection opens.
            try {
                const order = await orderService.getOrder(orderId);
                connection.send({
                    event: 'initial_state',
                    data: {
                        orderId: order.id,
                        status: order.status,
                        customerName: order.customerName,
                        restaurantName: order.restaurantName,
                        grandTotal: order.grandTotal,
                        deliveryAddress: order.deliveryAddress,
                        updatedAt: order.updatedAt
                    },
                });
            } catch (err) {
                log.error(
                    { err, orderId, connectionId: connection.id },
                    'Failed to send initial state — connection will receive future updates'
                );
            }

            log.info(
                {
                    orderId,
                    connectionId: connection.id,
                    remoteAddress: request.ip,
                    totalActive: sseManager.getActiveConnectionCount(),
                },
                `📡 SSE stream opened: ${request.method} ${request.url}`
            );

            // The connection stays open. Events will be pushed by the SSEManager
            // when Redis Pub/Sub messages arrive. The connection closes when:
            // 1. Client disconnects (navigates away, closes tab)
            // 2. Server shuts down (graceful shutdown sends 'shutdown' event)
            // 3. Backpressure policy disconnects a slow client
            // 4. Heartbeat write fails (dead connection detected)
        }
    );
}