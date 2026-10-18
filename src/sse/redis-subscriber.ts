// ============================================================
// Redis Pub/Sub Subscriber — SSE Fanout Bridge (Phase 5)
// ============================================================
// 🔍 LEARNING NOTE: This module bridges Redis Pub/Sub with SSE.
// It's the critical link that makes cross-instance realtime work.
//
// THE CROSS-INSTANCE PROBLEM:
//
// Scenario: 3 API instances behind a load balancer
//   Instance-1: holds SSE for orders A, C, E
//   Instance-2: holds SSE for orders B, D, F
//   Instance-3: holds SSE for orders G, H, I
//
// A notification consumer processes ORDER_STATUS_CHANGED for order B.
// It needs to push an update to the client watching order B.
// But order B's SSE connection is on Instance-2.
//
// The consumer can't directly write to Instance-2's HTTP response.
// Different process, different machine, different network.
//
// SOLUTION: Redis Pub/Sub as a broadcast bus
//   1. Consumer publishes to Redis channel "order:B"
//   2. ALL instances subscribe to channels for their active SSE connections
//   3. Instance-2 receives the message on channel "order:B"
//   4. Instance-2 looks up order B's SSE connection and pushes the update
//   5. Instance-1 and Instance-3 receive the message too, but have no
//      listeners for order B, so they ignore it
//
// REDIS PUB/SUB INTERNALS:
//
// SUBSCRIBE requires a DEDICATED Redis connection.
// A connection in "subscribe mode" can ONLY receive messages — it can't
// execute SET, GET, or any other command. This is why we create a
// SEPARATE Redis connection for subscriptions (not the main one used
// for caching, locks, etc.).
//
// Messages are FIRE-AND-FORGET. If nobody is subscribed when a message
// is published, it's lost forever. This is fine for SSE because:
// 1. Events are persisted in Kafka (the source of truth)
// 2. Pub/Sub is just the DELIVERY mechanism, not storage
// 3. If a client reconnects, they get the current state, not missed events
//
// REDIS PUB/SUB vs REDIS STREAMS vs KAFKA:
//
// | Feature        | Pub/Sub        | Streams          | Kafka             |
// |----------------|----------------|------------------|-------------------|
// | Persistence    | None           | Yes (in Redis)   | Yes (on disk)     |
// | Consumer groups| No             | Yes              | Yes               |
// | Replay         | No             | Yes (by ID)      | Yes (by offset)   |
// | Latency        | Sub-ms         | Sub-ms           | 1-10ms            |
// | Use case       | Fanout/notify  | Task queues      | Event sourcing    |
//
// For SSE fanout, Pub/Sub is the right choice: lowest latency, simplest
// semantics, and we don't need persistence (Kafka handles that).

import Redis from 'ioredis';
import { createModuleLogger } from '../shared/logger';
import { SSEManager } from './sse-manager';
import { RedisPubSubMessage } from './types.js';

const log = createModuleLogger('sse-redis-subscriber');

export class SSERedisSubscriber {
    // 🔍 LEARNING NOTE: We need a DEDICATED Redis connection for SUBSCRIBE.
    // Once a connection enters "subscribe mode", it can only receive
    // messages and manage subscriptions (SUBSCRIBE/UNSUBSCRIBE).
    // It CANNOT execute normal commands (GET, SET, etc.).
    //
    // This is a Redis protocol constraint, not a library limitation.
    // Every Redis client library documents this.
    private subscriber: Redis;
    private readonly activeChannels = new Set<string>();
    private isConnected = false;

    constructor(
        redisUrl: string,
        private readonly sseManager: SSEManager,
        private readonly keyPrefix: string = 'op:'
    ) {
        // Create a dedicated connection for subscribing
        this.subscriber = new Redis(redisUrl, {
            // 🔍 LEARNING NOTE: lazyConnect = true means we control when
            // the connection is established. This is important because
            // we want to set up event handlers BEFORE connecting.
            lazyConnect: true,

            retryStrategy(times: number) {
                // 🔍 LEARNING NOTE: Never return null — that permanently closes
                // the connection. Cap at 10s and keep retrying so we auto-recover
                // when Redis comes back.
                const delay = Math.min(times * 50, 10000);
                if (times <= 5 || times % 10 === 0) {
                    log.warn({ attempt: times, delayMs: delay }, 'Redis subscriber reconnecting...');
                }
                return delay;
            },

            maxRetriesPerRequest: null, // Subscriber doesn't make requests

            connectTimeout: 5000,
        });

        // ─────────────────────────────────────────────────
        // Handle incoming messages
        // ─────────────────────────────────────────────────
        this.subscriber.on('message', (channel: string, message: string) => {
            this.handleMessage(channel, message);
        })

        // ─────────────────────────────────────────────────
        // Handle reconnection
        // ─────────────────────────────────────────────────
        // 🔍 LEARNING NOTE: When Redis reconnects after a disconnect,
        // we need to RE-SUBSCRIBE to all active channels. Redis doesn't
        // remember subscriptions across disconnections.
        //
        // This is a common production gotcha: "SSE stopped working after
        // Redis restarted" — because nobody re-subscribed.
        this.subscriber.on('ready', () => {
            log.info('✅ Redis subscriber connected');
            this.isConnected = true;
            this.resubscribeAll();
        });

        this.subscriber.on('error', (err) => {
            log.error({ err }, '❌ Redis subscriber error');
        });

        this.subscriber.on('close', () => {
            log.warn('Redis subscriber connection closed');
            this.isConnected = false;
        });
    }

    // ─────────────────────────────────────────────────
    // Connect to Redis
    // ─────────────────────────────────────────────────
    async connect(): Promise<void> {
        try {
            await this.subscriber.connect();
            log.info('✅ Redis subscriber started');
        } catch (err) {
            log.error({ err }, '❌ Failed to connect Redis subscriber');
            throw err;
        }
    }

    // ─────────────────────────────────────────────────
    // Subscribe to an order channel
    // ─────────────────────────────────────────────────
    // Called by SSEManager when the first SSE client connects for an order.
    async subscribe(orderId: string): Promise<void> {
        const channel = this.getChannelName(orderId);

        if (this.activeChannels.has(channel)) {
            return; // Already subscribed
        }

        this.activeChannels.add(channel);

        if (this.isConnected) {
            try {
                await this.subscriber.subscribe(channel);
                log.debug(
                    { channel, orderId, totalChannels: this.activeChannels.size },
                    `📡 Subscribed to Redis channel: ${channel}`
                );
            } catch (err) {
                log.error({ err, channel }, 'Failed to subscribe to Redis channel');
                this.activeChannels.delete(channel);
            }
            // If not connected, it will be subscribed in resubscribeAll()
        }
    }

    // ─────────────────────────────────────────────────
    // Unsubscribe from an order channel
    // ─────────────────────────────────────────────────
    // Called by SSEManager when the last SSE client disconnects for an order.
    async unsubscribe(orderId: string): Promise<void> {
        const channel = this.getChannelName(orderId);

        if (!this.activeChannels.has(channel)) {
            return; // Not subscribed
        }

        this.activeChannels.delete(channel);

        if (this.isConnected) {
            try {
                await this.subscriber.unsubscribe(channel);
                log.debug(
                    { channel, orderId, totalChannels: this.activeChannels.size },
                    `📡 Unsubscribed from Redis channel: ${channel}`
                );
            } catch (err) {
                log.error({ err, channel }, 'Failed to unsubscribe from Redis channel');
            }
        }
    }

    // ─────────────────────────────────────────────────
    // Handle incoming Redis Pub/Sub message
    // ─────────────────────────────────────────────────
    private handleMessage(channel: string, rawMessage: string): void {
        const orderId = this.extractOrderId(channel);
        if (!orderId) {
            log.warn({ channel }, 'Received message on unknown channel format');
            return;
        }

        // Parse the message
        let message: RedisPubSubMessage;
        try {
            message = JSON.parse(rawMessage) as RedisPubSubMessage;
        } catch (err) {
            log.error(
                { err, channel, rawMessage: rawMessage.slice(0, 200) },
                '❌ Failed to parse Redis Pub/Sub message'
            );
            return;
        }

        // Forward to SSE manager for fanout to connected clients
        const delivered = this.sseManager.broadcastToOrder(orderId, {
            event: message.eventType,
            data: message.data
        });

        log.debug(
            {
                channel,
                orderId,
                eventType: message.eventType,
                delivered,
            },
            `📡 Redis → SSE: ${message.eventType} for order ${orderId} (${delivered} clients)`
        );
    }

    // ─────────────────────────────────────────────────
    // Re-subscribe to all active channels after reconnect
    // ─────────────────────────────────────────────────
    private async resubscribeAll(): Promise<void> {
        if (this.activeChannels.size === 0) return;

        log.info(
            { channelCount: this.activeChannels.size },
            `🔄 Re-subscribing to ${this.activeChannels.size} channels after reconnect`
        );

        const channels = Array.from(this.activeChannels);
        try {
            await this.subscriber.subscribe(...channels);
            log.info(
                { channelCount: channels.length },
                `✅ Re-subscribed to ${channels.length} channels`
            );
        } catch (err) {
            log.error(
                { err, channelCount: channels.length },
                '❌ Failed to re-subscribe after reconnect'
            );
        }
    }

    // ─────────────────────────────────────────────────
    // Channel naming convention
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: Channel names include the key prefix for
    // namespacing. This prevents collisions when multiple applications
    // share the same Redis instance.
    //
    // Format: {prefix}sse:order:{orderId}
    // Example: op:sse:order:abc-123-def
    private getChannelName(orderId: string): string {
        return `${this.keyPrefix}sse:order:${orderId}`;
    }

    private extractOrderId(channel: string): string | null {
        const prefix = `${this.keyPrefix}sse:order:`;
        if (channel.startsWith(prefix)) {
            return channel.slice(prefix.length);
        }
        return null;
    }

    // ─────────────────────────────────────────────────
    // Disconnect
    // ─────────────────────────────────────────────────
    async disconnect(): Promise<void> {
        if (!this.isConnected) return;

        try {
            // Unsubscribe from all channels
            if (this.activeChannels.size > 0) {
                const channels = Array.from(this.activeChannels);
                await this.subscriber.unsubscribe(...channels);
                this.activeChannels.clear();
            }

            await this.subscriber.quit();
            this.isConnected = false;
            log.info('✅ Redis subscriber disconnected');
        } catch (err) {
            log.error({ err }, 'Error disconnecting Redis subscriber');
            // Force disconnect
            this.subscriber.disconnect();
            this.isConnected = false;
        }
    }

    // ─────────────────────────────────────────────────
    // Status
    // ─────────────────────────────────────────────────
    get connected(): boolean { return this.isConnected; }
    get channelCount(): number { return this.activeChannels.size; }
}

// ─────────────────────────────────────────────────
// Static helper: Publish to Redis Pub/Sub
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: This is used by the notification consumer
// (a separate process) to publish status updates into Redis.
// The consumer uses the MAIN Redis connection (not the subscriber).
// Publishing does NOT require a dedicated connection — only SUBSCRIBE does.
//
// This is a static function because the consumer doesn't need an
// SSERedisSubscriber instance — it only needs to PUBLISH.
export async function publishOrderUpdate(
    redis: Redis,
    keyPrefix: string,
    message: RedisPubSubMessage
): Promise<void> {
    const channel = `${keyPrefix}sse:order:${message.orderId}`;
    const serialized = JSON.stringify(message);

    try {
        const subscriberCount = await redis.publish(channel, serialized);

        log.debug(
            {
                channel,
                orderId: message.orderId,
                eventType: message.eventType,
                subscriberCount,
            },
            `📡 Published to Redis: ${message.eventType} for order ${message.orderId} (${subscriberCount} subscribers)`
        );
    } catch (err) {
        // 🔍 LEARNING NOTE: Redis Pub/Sub publish failures are NOT critical.
        // The event is already persisted in Kafka. The SSE update is a
        // nice-to-have — if it fails, the client can poll or reconnect.
        // We log the error and continue. Never crash the consumer for SSE.
        log.error(
            { err, channel, orderId: message.orderId },
            '❌ Failed to publish to Redis Pub/Sub (SSE update will be missed)'
        );
    }
}