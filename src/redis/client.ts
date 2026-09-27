// ============================================================
// Redis Client — Distributed Coordination Layer
// ============================================================
// 🔍 LEARNING NOTE: Redis is NOT just a cache.
// It's a distributed data structure server that provides:
//
// 1. ATOMIC OPERATIONS: INCR, SETNX, Lua scripts — operations that
//    are guaranteed to complete without interruption. No race conditions.
//
// 2. EXPIRING KEYS (TTL): Every key can auto-expire. This is the
//    foundation of rate limiting, leader election, and distributed locks.
//
// 3. SUB-MILLISECOND LATENCY: In-memory storage means Redis responds
//    in <1ms. Compare to PostgreSQL at 1-5ms per query.
//
// 4. SINGLE-THREADED EVENT LOOP: Redis processes commands sequentially.
//    This means operations like SETNX (set-if-not-exists) are inherently
//    atomic — no need for database locks.
//
// WHY REDIS AND NOT JUST POSTGRES?
// - PostgreSQL locks are scoped to a single DB connection.
//   If you have 3 API instances with 3 DB pools, their locks are independent.
// - Redis is a SHARED coordination point that all instances connect to.
//   A lock in Redis is visible to ALL instances immediately.
// - Redis TTLs auto-expire. PostgreSQL advisory locks require manual release.
// - Redis is 10-100x faster for simple key-value operations.
//
// WHEN TO USE POSTGRES INSTEAD OF REDIS:
// - Data must survive Redis restart (Redis is ephemeral by default)
// - Transactional guarantees are needed (Redis is not ACID)
// - Complex queries (joins, aggregations, full-text search)
// - Data is already in Postgres (avoid adding another dependency)

import Redis from 'ioredis';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger';

const log = createModuleLogger('redis-client');

let redisClient: Redis | null = null;

// ─────────────────────────────────────────────────
// Create and connect the Redis client
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: We use a singleton pattern for Redis.
// Unlike PostgreSQL (which uses a connection pool), Redis uses
// a single TCP connection with pipelining. One connection handles
// thousands of operations per second because:
// 1. Redis is single-threaded — one connection can saturate it
// 2. ioredis pipelines commands automatically (batches on the wire)
// 3. Multiple connections add overhead without improving throughput
export function createRedisClient(): Redis {
    if (redisClient) {
        return redisClient;
    }

    redisClient = new Redis(config.redisUrl, {
        // 🔍 LEARNING NOTE: Key prefix namespaces all our keys.
        // Without it, different applications sharing the same Redis
        // instance could collide (e.g., both using a key called "lock").
        // With prefix "op:", our keys become "op:lock:order:123".
        keyPrefix: config.redisKeyPrefix,

        // Retry strategy
        // 🔍 LEARNING NOTE: If Redis is temporarily down (restart, network blip),
        // ioredis will automatically reconnect. The retryStrategy controls
        // how long to wait between reconnection attempts.
        retryStrategy(times: number) {
            // 🔍 LEARNING NOTE: We NEVER return null here.
            // Returning null tells ioredis to permanently close the connection,
            // and it will NEVER reconnect — even if Redis comes back.
            // Instead, we keep retrying with a capped delay so the client
            // auto-recovers when Redis returns.
            const delay = Math.min(times * 50, 10000);

            // Log less frequently after initial burst to avoid log spam
            if (times <= 5 || times % 10 === 0) {
                log.warn({ attempt: times, delayMs: delay }, `Redis reconnecting in ${delay}ms`);
            }

            return delay;
        },

        // 🔍 LEARNING NOTE: maxRetriesPerRequest = 3 means if a single
        // command fails (e.g., SET), ioredis retries it up to 3 times
        // before rejecting the promise. For coordination operations,
        // we want fast failures (don't block the API waiting for Redis).
        maxRetriesPerRequest: 3,

        // 🔍 LEARNING NOTE: enableOfflineQueue MUST be false for our use case.
        // When true, commands are silently queued while Redis is disconnected
        // and replayed when it reconnects. This sounds helpful, but it causes
        // HTTP requests to HANG INDEFINITELY — the rate limiter's redis.multi()
        // never resolves, never rejects, just waits forever for reconnection.
        //
        // With false, commands fail IMMEDIATELY with an error when disconnected,
        // which lets our catch blocks fall open (allow the request through).
        // This is the correct behavior for coordination features that are
        // optional — we'd rather skip rate limiting than block all traffic.
        enableOfflineQueue: false,

        // Connection timeout
        connectTimeout: 5000,
    });

    // Connection event handlers
    redisClient.on('connect', () => {
        log.info('✅ Redis connected');
    });

    redisClient.on('ready', () => {
        log.info('✅ Redis ready (accepting commands)');
    });

    redisClient.on('error', (err) => {
        log.error({ err }, '❌ Redis connection error');
    });

    redisClient.on('close', () => {
        log.warn('Redis connection closed');
    });

    redisClient.on('reconnecting', (delay: number) => {
        log.info({ delayMs: delay }, 'Redis reconnecting...');
    });

    return redisClient;
}

// ─────────────────────────────────────────────────
// Get the existing Redis client (or throw)
// ─────────────────────────────────────────────────
export function getRedisClient(): Redis {
    if (!redisClient) {
        throw new Error('Redis client not initialized. Call createRedisClient() first.');
    }
    return redisClient;
}

// ─────────────────────────────────────────────────
// Disconnect Redis
// ─────────────────────────────────────────────────
export async function disconnectRedis(): Promise<void> {
    if (redisClient) {
        await redisClient.quit();
        redisClient = null;
        log.info('✅ Redis disconnected');
    }
}