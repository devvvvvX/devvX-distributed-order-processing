// ============================================================
// Retry with Exponential Backoff + Jitter
// ============================================================
// 🔍 LEARNING NOTE: Retry logic is deceptively complex. Naive retries
// (retry immediately, or with fixed delay) cause THUNDERING HERD:
//
// Scenario: Email service goes down for 30 seconds.
//   - 100 consumers are all retrying
//   - Email service comes back up
//   - All 100 consumers retry AT THE SAME TIME
//   - Email service is overwhelmed → goes down again
//   - Cycle repeats → cascading failure
//
// EXPONENTIAL BACKOFF fixes this:
//   Retry 1: wait 1s
//   Retry 2: wait 2s
//   Retry 3: wait 4s
//   Retry 4: wait 8s
//   Each retry waits 2x longer, spreading out the load.
//
// JITTER makes it even better:
//   Without jitter: All consumers wait exactly 1s, 2s, 4s...
//     → They all retry at the same millisecond. Still a thundering herd.
//   With jitter: Each consumer adds random delay (0-50% of backoff)
//     → Retries are spread across the time window. No thundering herd.
//
// FULL JITTER (what we use):
//   delay = random(0, baseDelay * 2^attempt)
//   This has been proven optimal by Amazon's research:
//   https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
//
// MAX RETRIES: After N retries, give up and send to DLQ.
// You don't want to retry forever — some errors are permanent:
//   - Malformed event (bad JSON, missing fields)
//   - Business logic violation (order doesn't exist)
//   - Expired data (event is 3 days old, no longer relevant)
// These will NEVER succeed, no matter how many times you retry.

import { createModuleLogger } from "../shared/logger.js";

const log = createModuleLogger('retry');

export interface RetryConfig {
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs?: number;     // Cap the backoff (default: 30s)
}

export interface RetryResult {
    success: boolean;
    attempts: number;
    lastError: Error | null;
}

// ─────────────────────────────────────────────────
// Execute a function with retry + exponential backoff
// ─────────────────────────────────────────────────
export async function withRetry(
    fn: () => Promise<void>,
    retryConfig: RetryConfig,
    context?: Record<string, unknown>
): Promise<RetryResult> {
    const { maxRetries, baseDelayMs, maxDelayMs = 30000 } = retryConfig;
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            await fn();
            return { success: true, attempts: attempt + 1, lastError: null }
        } catch (err) {
            lastError = err instanceof Error ? err : new Error(String(err));

            if (attempt === maxRetries) {
                // All retries exhausted
                log.warn(
                    { ...context, attempt: attempt + 1, maxRetries, error: lastError.message },
                    `❌ All ${maxRetries + 1} attempts failed — giving up`
                )
                break;
            }
            // Calculate backoff with full jitter
            // 🔍 LEARNING NOTE: Full jitter formula:
            //   delay = random(0, min(maxDelay, baseDelay * 2^attempt))
            //
            // Why 2^attempt?
            //   attempt=0: random(0, 1000ms)  → avg 500ms
            //   attempt=1: random(0, 2000ms)  → avg 1000ms
            //   attempt=2: random(0, 4000ms)  → avg 2000ms
            //   attempt=3: random(0, 8000ms)  → avg 4000ms
            //
            // Why random? Without it, all consumers compute the exact same delay
            // and still retry simultaneously. The random spread is what prevents
            // thundering herd.
            const exponentialDelay = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
            const jitteredDelay = Math.floor(Math.random() * exponentialDelay);

            log.debug(
                {
                    ...context,
                    attempt: attempt + 1,
                    maxRetries: maxRetries + 1,
                    delayMs: jitteredDelay,
                    error: lastError.message
                },
                `⏳ Retry ${attempt + 1}/${maxRetries + 1} in ${jitteredDelay}ms`
            );

            // 🔍 LEARNING NOTE: This is a blocking sleep inside the consumer.
            // During this sleep, the consumer is NOT processing other messages
            // from this partition. This is acceptable because:
            // 1. If the message fails, subsequent messages likely will too
            //    (same downstream dependency is probably down)
            // 2. Per-partition ordering is maintained
            // 3. Other partitions (other consumer instances) are unaffected
            await sleep(jitteredDelay);
        }
    }
    return { success: false, attempts: maxRetries + 1, lastError };
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}