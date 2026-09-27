// ============================================================
// Rate Limiter — Fixed Window Counter with Redis
// ============================================================
// 🔍 LEARNING NOTE: Rate limiting prevents abuse and protects your API.
//
// WITHOUT rate limiting:
//   A single client sends 10,000 requests/second
//   → Your API is overwhelmed
//   → Database connection pool exhausted
//   → ALL customers get 503 errors
//   → One bad actor takes down the entire platform
//
// WITH rate limiting:
//   Same client sends 10,000 requests/second
//   → First 30 succeed (within the window)
//   → Remaining 9,970 get 429 Too Many Requests
//   → Other customers are unaffected
//
// WHY REDIS? WHY NOT IN-MEMORY?
// If you have 3 API instances behind a load balancer:
//
//   In-memory rate limiter (BROKEN):
//     Instance 1: counter=10 (30 remaining)
//     Instance 2: counter=10 (30 remaining)
//     Instance 3: counter=10 (30 remaining)
//     Total: 30 requests served, but limit is 30!
//     Client actually sent 30, and each instance thinks it's fine.
//     With round-robin, client gets 90 requests through (3x the limit).
//
//   Redis rate limiter (CORRECT):
//     Redis: counter=30 (shared across all instances)
//     Next request → 429 (regardless of which instance receives it)
//
// ALGORITHM: Fixed Window Counter
//   Key:   ratelimit:{customerId}:{windowStart}
//   Value: number of requests in this window
//   TTL:   window size (60 seconds)
//
// Simpler than sliding window, good enough for most use cases.
// The edge case: a burst at window boundaries can briefly allow 2x
// the limit. For our learning purposes, this is acceptable.
// Stripe and GitHub use variations of this approach.

import { FastifyRequest, FastifyReply } from 'fastify';
import Redis from 'ioredis';
import { config } from '../config';
import { createModuleLogger } from '../shared/logger';


const log = createModuleLogger('rate-limiter');

export function createRateLimiter(redis: Redis) {
    return async function rateLimiter(
        request: FastifyRequest,
        reply: FastifyReply,
    ): Promise<void> {
        // 🔍 LEARNING NOTE: We rate-limit by customer ID if available,
        // falling back to IP address. Customer ID is more precise
        // (one customer, one limit) but requires the request body to
        // be parsed. IP-based limiting can be too broad (shared NAT)
        // or too narrow (multiple IPs via VPN).
        const identifier = extractIdentifier(request);

        // Calculate the current time window
        // 🔍 LEARNING NOTE: We divide timestamp by window size and floor it.
        // This gives us a window ID that's the same for all requests
        // within the same window period.
        // Example with 60-second window:
        //   timestamp 1717785600000 → window 1717785600000
        //   timestamp 1717785630000 → window 1717785600000 (same window!)
        //   timestamp 1717785660000 → window 1717785660000 (new window)
        const windowStart = Math.floor(Date.now() / config.rateLimitWindowMs) * config.rateLimitWindowMs;
        const key = `ratelimit:${identifier}:${windowStart}`;

        try {
            // 🔍 LEARNING NOTE: We use Redis MULTI/EXEC (transaction) to make
            // INCR and EXPIRE atomic. Without this, a race condition could
            // cause the key to exist without an expiry (memory leak).
            //
            // MULTI:  start batching commands
            // INCR:   increment counter (creates key with value 1 if doesn't exist)
            // EXPIRE: set TTL (only needed for new keys, but idempotent)
            // EXEC:   execute all commands atomically
            const results = await redis.multi()
                .incr(key)
                .pexpire(key, config.rateLimitWindowMs)
                .exec();

            if (!results) {
                // MULTI/EXEC failed — Redis might be in a bad state.
                // Fall open: allow the request (don't break the API because Redis is slow).
                log.warn('Rate limiter: MULTI/EXEC returned null — falling open');
                return;
            }

            // INCR result is the first command's result
            const [incrErr, currentCount] = results[0]!;
            if (incrErr) {
                log.error({ err: incrErr }, 'Rate limiter: INCR failed — falling open');
                return;
            }

            const count = currentCount as number;
            const remaining = Math.max(0, config.rateLimitMaxRequests - count);
            const resetTime = windowStart + config.rateLimitWindowMs;

            // 🔍 LEARNING NOTE: Always include rate limit headers in the response.
            // These tell the client:
            // - X-RateLimit-Limit: max requests per window
            // - X-RateLimit-Remaining: how many requests they have left
            // - X-RateLimit-Reset: when the window resets (Unix timestamp)
            //
            // Well-behaved clients use these headers to self-throttle.
            // API clients like Stripe's SDK automatically back off when
            // they see X-RateLimit-Remaining approaching 0.
            reply.header('X-RateLimit-Limit', config.rateLimitMaxRequests);
            reply.header('X-RateLimit-Remaining', remaining);
            reply.header('X-RateLimit-Reset', Math.ceil(resetTime / 1000));

            if (count > config.rateLimitMaxRequests) {
                // 🔍 LEARNING NOTE: 429 Too Many Requests is the standard
                // HTTP status code for rate limiting. The Retry-After header
                // tells the client how long to wait before trying again.
                const retryAfterMs = resetTime - Date.now();
                const retryAfterSec = Math.ceil(retryAfterMs / 1000);

                reply.header('Retry-After', retryAfterSec);

                log.warn(
                    {
                        identifier,
                        count,
                        limit: config.rateLimitMaxRequests,
                        retryAfterSec,
                    },
                    `🚫 Rate limit exceeded for ${identifier}`
                );

                reply.status(429).send({
                    success: false,
                    error: {
                        code: 'RATE_LIMIT_EXCEEDED',
                        message: `Rate limit exceeded. Maximum ${config.rateLimitMaxRequests} requests per ${config.rateLimitWindowMs / 1000} seconds.`,
                        retryAfter: retryAfterSec
                    },
                });
                return;
            }
        } catch (err) {
            // 🔍 LEARNING NOTE: FAIL OPEN, not fail closed.
            // If Redis is down, we let requests through. The alternative
            // (fail closed = reject all requests) means a Redis outage
            // takes down your entire API. That's worse than no rate limiting.
            //
            // In production with strict security requirements, you might
            // want a local in-memory fallback rate limiter that's less
            // accurate but still provides some protection.
            log.error({ err }, 'Rate limiter error — falling open');
        }
    }

    // ─────────────────────────────────────────────────
    // Extract rate limit identifier from request
    // ─────────────────────────────────────────────────
    function extractIdentifier(request: FastifyRequest): string {
        // Try to get customer ID from the request body (for POST/PATCH)
        if (request.body && typeof request.body === 'object' && 'customerId' in request.body) {
            return `cust:${(request.body as Record<string, unknown>).customerId}`;
        }

        // Fall back to IP address
        // 🔍 LEARNING NOTE: In production behind a reverse proxy/load balancer,
        // request.ip might be the proxy's IP, not the client's.
        // Use X-Forwarded-For header instead (but trust it carefully —
        // it can be spoofed by the client).
        return `ip:${request.ip}`;
    }
}