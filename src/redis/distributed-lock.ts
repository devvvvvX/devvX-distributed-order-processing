// ============================================================
// Distributed Lock — Redis-based Mutual Exclusion
// ============================================================
// 🔍 LEARNING NOTE: A distributed lock ensures that only ONE process
// across ALL instances can execute a critical section at a time.
//
// WHY DO WE NEED THIS?
// In Phase 3, we have optimistic locking in PostgreSQL:
//   UPDATE orders SET status = $1 WHERE id = $2 AND status = $3
//   This catches the case where two updates happen simultaneously.
//
// But there's a SUBTLER problem:
//   Instance A: reads order (status=PENDING)
//   Instance B: reads order (status=PENDING)
//   Instance A: validates PENDING→CONFIRMED (valid) → updates → ✅
//   Instance B: validates PENDING→PREPARING (valid!) → update fails
//              because status is now CONFIRMED, not PENDING
//
// Without a distributed lock, Instance B wasted a DB roundtrip
// and got a confusing error. With a lock:
//   Instance A: acquires lock on order:123 → reads → updates → releases
//   Instance B: tries to acquire lock → BLOCKED → retries → reads fresh state
//
// REDIS LOCK ALGORITHM (simplified Redlock):
//   Acquire: SET key value NX PX ttl
//     NX = set only if Not eXists (atomic!)
//     PX = expire after ttl milliseconds
//     value = unique token (prevents releasing someone else's lock)
//
//   Release: Lua script (atomic check-and-delete)
//     if redis.call("GET", key) == token then
//       redis.call("DEL", key)
//     end
//
// WHY A LUA SCRIPT FOR RELEASE?
// Without Lua, you'd need two commands:
//   1. GET key → check if token matches
//   2. DEL key → delete if matched
// But between GET and DEL, another process could acquire the lock.
// The Lua script runs atomically in Redis (single-threaded).
//
// LOCK TTL (Time-To-Live):
// The TTL is a SAFETY NET. If the lock holder crashes without releasing,
// the lock auto-expires after TTL milliseconds. This prevents deadlocks.
// Trade-off:
//   Too short: Lock expires while you're still processing → two holders
//   Too long: Crashed holder blocks others for too long
//   Rule of thumb: 3-5x your expected critical section duration

import Redis from 'ioredis';
import { createModuleLogger } from '../shared/logger';

const log = createModuleLogger('distributed-lock');

// 🔍 LEARNING NOTE: The Lua script for releasing locks.
// This runs ATOMICALLY in Redis — no race conditions.
// It checks that the lock value matches our token before deleting.
// If someone else acquired the lock (different token), we don't delete it.
const RELEASE_LOCK_SCRIPT = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
`;

export interface LockResult {
    acquired: boolean,
    token: string | null;
    // 🔍 LEARNING NOTE: 'reason' lets callers distinguish between
    // "someone else holds the lock" (contention → 409) and
    // "Redis is unreachable" (error → fall through to no-lock path).
    reason?: 'contention' | 'error';
}

export class DistributedLock {
    constructor(private readonly redis: Redis) { }

    // ─────────────────────────────────────────────────
    // Acquire a lock on a resource
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: Returns immediately — does NOT block/wait.
    // If the lock is held by someone else, returns { acquired: false }.
    // The caller decides what to do (retry, fail, queue, etc.).
    //
    // This is a "try lock" / "non-blocking lock" pattern.
    // A blocking lock (wait until available) is harder to implement
    // correctly and can cause issues in request handlers (holding
    // connections while waiting).
    async acquireLock(resource: string, ttlMs: number): Promise<LockResult> {
        // 🔍 LEARNING NOTE: The token must be unique to this lock acquisition.
        // We use a random value so that when we release, we can verify
        // we're releasing OUR lock, not someone else's.
        //
        // Scenario without unique token:
        //   Process A acquires lock → Process A's lock TTL expires
        //   Process B acquires lock → Process A calls release
        //   Process A releases Process B's lock! → UNSAFE
        //
        // With unique token:
        //   Process A calls release → checks token → doesn't match → no-op
        const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

        try {
            // SET key value NX PX ttl
            // NX = only set if key does NOT exist
            // PX = set expiry in milliseconds
            const result = await this.redis.set(
                `lock:${resource}`,
                token,
                'PX',
                ttlMs,
                'NX'
            );

            if (result === 'OK') {
                log.debug(
                    { resource, ttlMs, token: token.slice(0, 12) },
                    `🔒 Lock acquired: ${resource}`
                );
                return { acquired: true, token };
            }

            log.debug(
                { resource },
                `🔒 Lock NOT acquired (held by another): ${resource}`
            );

            return { acquired: false, token: null, reason: 'contention' };
        } catch (err) {
            // 🔍 LEARNING NOTE: This catch block fires when Redis is
            // unreachable (ECONNREFUSED, timeout, etc). We tag the
            // result with reason='error' so callers can distinguish
            // "Redis is down" from "another request holds the lock".
            log.error({ err, resource }, '❌ Failed to acquire lock (Redis connection error)');
            return { acquired: false, token: null, reason: 'error' };
        }
    }

    // ─────────────────────────────────────────────────
    // Release a lock
    // ─────────────────────────────────────────────────
    async releaseLock(resource: string, token: string): Promise<boolean> {
        try {
            // 🔍 LEARNING NOTE: We use eval() to run the Lua script.
            // The script atomically checks the token and deletes the key.
            // This prevents the race condition described above.
            const result = await this.redis.eval(
                RELEASE_LOCK_SCRIPT,
                1,                      // number of KEYS
                `lock:${resource}`,     // KEYS[1]
                token                   // ARGV[1]
            );

            const released = result === 1;
            if (released) {
                log.debug({ resource }, `🔓 Lock released: ${resource}`);
            } else {
                log.warn(
                    { resource },
                    `⚠️ Lock release failed (expired or held by another): ${resource}`
                );
            }
            return released;;
        } catch (err) {
            log.error({ err, resource }, '❌ Failed to release lock');
            return false;
        }
    }

    // ─────────────────────────────────────────────────
    // Execute a function while holding a lock
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: This is the convenience wrapper.
    // It handles acquire → execute → release in one call.
    //
    // The finally block ensures the lock is ALWAYS released,
    // even if the function throws. Without this, a thrown
    // exception would leave the lock held until TTL expiry.
    async withLock<T>(
        resource: string,
        ttlMs: number,
        fn: () => Promise<T>
    ): Promise<{ success: boolean; result?: T, error?: string, reason?: 'contention' | 'error' }> {
        const lock = await this.acquireLock(resource, ttlMs);

        if (!lock.acquired || !lock.token) {
            return {
                success: false,
                error: `Could not acquire lock on ${resource}`,
                reason: lock.reason,
            };
        }

        try {
            const result = await fn();
            return { success: true, result };
        } finally {
            await this.releaseLock(resource, lock.token)
        }
    }
}
