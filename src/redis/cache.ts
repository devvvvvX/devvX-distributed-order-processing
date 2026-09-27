// ============================================================
// Cache Service — Cache-Aside Pattern with Redis
// ============================================================
// 🔍 LEARNING NOTE: Caching is one of the most powerful and
// most dangerous optimization techniques.
//
// POWERFUL because:
//   Redis GET: ~0.1ms (in-memory, single-threaded, no query planning)
//   PostgreSQL SELECT: ~1-5ms (disk I/O, query planner, transaction overhead)
//   10-50x speedup for read-heavy workloads.
//
// DANGEROUS because:
//   Cache invalidation is one of the "two hardest problems in CS"
//   (the other being naming things and off-by-one errors).
//
//   If your cache is stale, users see old data:
//     - Order status shows "PENDING" but it's actually "DELIVERED"
//     - Customer sees wrong price
//     - Inventory shows available but it's sold out
//
// CACHE-ASIDE PATTERN (what we implement):
//   Read:
//     1. Check cache → HIT? Return cached data
//     2. MISS? Query database → store in cache with TTL → return
//   Write:
//     1. Update database
//     2. Invalidate cache (DELETE key)
//     3. Next read will fetch fresh data and re-populate cache
//
// WHY NOT WRITE-THROUGH?
//   Write-through: Update DB AND cache on every write.
//   Problem: What if DB succeeds but cache write fails? Inconsistency.
//   Cache-aside with invalidation is simpler and safer:
//     Worst case: cache miss on next read → one extra DB query.
//
// WHY NOT WRITE-BEHIND?
//   Write-behind: Write to cache, asynchronously sync to DB.
//   Problem: If Redis crashes before sync, data is LOST.
//   Only use for data you can afford to lose (metrics, analytics).

import Redis from 'ioredis';
import { createModuleLogger } from '../shared/logger.js';

const log = createModuleLogger('cache');

export class CacheService {
    constructor(private readonly redis: Redis) { }

    // ─────────────────────────────────────────────────
    // Get a value from cache
    // ─────────────────────────────────────────────────
    async get<T>(key: string): Promise<T | null> {
        try {
            const cached = await this.redis.get(`cache:${key}`);
            if (cached) {
                log.debug({ key }, '🎯 Cache HIT');
                return JSON.parse(cached) as T;
            }
            log.debug({ key }, '❌ Cache MISS');
            return null;
        } catch (err) {
            // 🔍 LEARNING NOTE: Cache errors should NEVER break the application.
            // If Redis is down, we just go to the database. The app is slower
            // but still functional. This is graceful degradation.
            log.error({ err, key }, 'Cache GET error — falling through to DB');
            return null;
        }
    }

    // ─────────────────────────────────────────────────
    // Set a value in cache with TTL
    // ─────────────────────────────────────────────────
    async set(key: string, value: unknown, ttlSeconds: number): Promise<void> {
        try {
            // 🔍 LEARNING NOTE: We serialize to JSON. This works for most
            // objects but has limitations:
            // - Date objects become strings (need to parse on read)
            // - undefined values are dropped
            // - Circular references throw
            // - Large objects are expensive to serialize/deserialize
            //
            // For high-performance caching, consider MessagePack or Protocol Buffers.
            const serialized = JSON.stringify(value);
            await this.redis.setex(`cache:${key}`, ttlSeconds, serialized);
            log.debug({ key, ttlSeconds }, '📦 Cache SET');
        } catch (err) {
            // Don't throw — cache write failure is non-critical
            log.error({ err, key }, 'Cache SET error — data not cached');
        }
    }

    // ─────────────────────────────────────────────────
    // Invalidate (delete) a cache entry
    // ─────────────────────────────────────────────────
    async invalidate(key: string): Promise<void> {
        try {
            await this.redis.del(`cache:${key}`);
            log.debug({ key }, '🗑️  Cache INVALIDATED');
        } catch (err) {
            // 🔍 LEARNING NOTE: Even cache invalidation failures are non-critical.
            // The worst that happens: the cache serves stale data until TTL expires.
            // The TTL is our safety net — eventual consistency.
            log.error({ err, key }, 'Cache INVALIDATE error — stale data may persist until TTL');
        }
    }

    // ─────────────────────────────────────────────────
    // Get-or-Set (the cache-aside pattern in one call)
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This is the most commonly used method.
    // It encapsulates the entire cache-aside pattern:
    //   1. Try to get from cache
    //   2. If miss, call the fetch function (DB query)
    //   3. Store the result in cache for next time
    //   4. Return the result
    //
    // CACHE STAMPEDE PREVENTION:
    // If 100 requests hit a cold cache simultaneously:
    //   Without protection: 100 DB queries (N+1 at the cache level)
    //   With protection: lock or collapse requests so only 1 queries DB
    //
    // For Phase 4, we accept the stampede risk (it's rare for orders).
    // In Phase 6+, we could add a mutex or probabilistic early refresh.
    async getOrSet<T>(
        key: string,
        fetchFn: () => Promise<T>,
        ttlSeconds: number,
    ): Promise<T> {
        // Try cache first
        const cached = await this.get<T>(key);
        if (cached !== null) {
            return cached;
        }

        // Cache miss — fetch from source
        const fresh = await fetchFn();

        // Store in cache (fire-and-forget — don't await)
        // 🔍 LEARNING NOTE: We don't await the cache write because:
        // 1. The user doesn't need to wait for the cache to be populated
        // 2. If caching fails, the response is still correct
        // 3. Reduces response latency
        this.set(key, fresh, ttlSeconds).catch((err => {
            log.error({ err, key }, 'Failed to populate cache after miss');
        }));

        return fresh;
    }
}
