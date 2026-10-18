// ============================================================
// SSE Backpressure Handler (Phase 5)
// ============================================================
// 🔍 LEARNING NOTE: Backpressure is the most production-critical
// concept in streaming systems. It's also the most commonly
// overlooked — leading to memory exhaustion and OOM crashes.
//
// HOW BACKPRESSURE WORKS IN NODE.JS:
//
// When you call res.write(data), Node.js does this:
//   1. Copies data into the kernel's TCP send buffer
//   2. Returns true (buffer accepted, keep writing)
//   OR
//   3. Buffer is full → Node.js queues data in userspace memory
//   4. Returns false (stop writing until 'drain' event!)
//
// If you IGNORE the false return value:
//   - Node.js keeps buffering in memory
//   - Memory grows with every write
//   - Eventually: OOM kill, or V8 heap limit crash
//
// The Node.js documentation explicitly warns about this:
//   "If false is returned, further attempts to write data to the
//    stream should stop until the 'drain' event is emitted."
//
// REAL-WORLD FAILURE SCENARIO:
//   - 500 SSE clients connected
//   - Status update published → written to all 500 connections
//   - 20 clients are on slow 3G connections
//   - Server buffers ~1MB per slow client per minute
//   - 20 × 1MB × 60 minutes = 1.2 GB of unbounded buffer growth
//   - Container with 512MB limit → OOM-killed
//   - ALL 500 connections drop (including the 480 fast clients)
//
// This module prevents that scenario.

import { createModuleLogger } from "../shared/logger.js";
import {
    BackpressureConfig,
    BackpressurePolicy
} from './types.js';

const log = createModuleLogger('sse-backpressure');

// ─────────────────────────────────────────────────
// Default Configuration
// ─────────────────────────────────────────────────
export const DEFAULT_BACKPRESSURE_CONFIG: BackpressureConfig = {
    // 🔍 LEARNING NOTE: High water mark = "start worrying".
    // 64KB of buffered data means the client is falling behind.
    // We log a warning but keep writing — transient slowdowns are normal.
    highWaterMark: 65536,       // 64 KB

    // 🔍 LEARNING NOTE: Critical mark = "take action".
    // 256KB means the client is severely lagging. At this point,
    // we apply the backpressure policy to protect the server.
    criticalMark: 262144,       // 256 KB


    // 🔍 LEARNING NOTE: DROP_NEWEST is the safest default policy.
    // We keep the buffer as-is (it will drain eventually) and
    // drop new events until the client catches up. The client
    // will see a gap in events but the connection stays alive.
    //
    // Alternatives:
    // - DROP_OLDEST: Client gets newest data but misses history
    // - DISCONNECT: Strongest protection but disrupts client experience
    policy: BackpressurePolicy.DROP_NEWEST
};

// ─────────────────────────────────────────────────
// Backpressure Tracker — Per-Connection State
// ─────────────────────────────────────────────────
// 🔍 LEARNING NOTE: Each SSE connection gets its own tracker.
// This is critical because backpressure is PER-CLIENT.
// A slow mobile client on 3G shouldn't affect a fast desktop client.
export class BackpressureTracker {
    private _bufferedBytes = 0;
    private _isDraining = false;
    private _droppedEvents = 0;
    private _backpressureEvents = 0;
    private _lastWarningTime = 0;

    constructor(
        private readonly connectionId: string,
        private readonly config: BackpressureConfig = DEFAULT_BACKPRESSURE_CONFIG
    ) { }

    // ─────────────────────────────────────────────────
    // Record bytes written
    // ─────────────────────────────────────────────────
    // Called after every res.write(). If write returned false,
    // the caller should set isDraining.
    recordWrite(bytes: number, writeAccepted: boolean): void {
        if (writeAccepted) {
            // Kernel accepted the data — buffer tracking is approximate
            // because we can't know exactly how much the kernel has sent.
            // We conservatively assume it's buffered until 'drain' fires.
            this._bufferedBytes += bytes;
        } else {
            // Write was NOT accepted — Node.js is buffering in userspace
            this._bufferedBytes += bytes;
            this._isDraining = true;

            // Check thresholds
            this.checkThresholds();
        }
    }

    // ─────────────────────────────────────────────────
    // Handle drain event
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: The 'drain' event fires when the kernel
    // has consumed enough data from the send buffer that it's safe
    // to write again. We reset our tracking — the client caught up.
    onDrain(): void {
        const previousBytes = this._bufferedBytes;
        this._bufferedBytes = 0;
        this._isDraining = false;

        if (previousBytes > this.config.highWaterMark) {
            log.info(
                {
                    connectionId: this.connectionId,
                    previousBufferedBytes: previousBytes,
                },
                `✅ Backpressure relieved — client caught up (was ${previousBytes} bytes buffered)`
            );
        }
    }

    // ─────────────────────────────────────────────────
    // Should we accept a new event?
    // ─────────────────────────────────────────────────
    // Returns the action to take for the next event.
    shouldAcceptEvent(eventSizeBytes: number): BackpressureAction {
        const projectedSize = this._bufferedBytes + eventSizeBytes;

        // Under high water mark — always accept
        if (projectedSize <= this.config.highWaterMark) {
            return { action: 'ACCEPT' };
        }

        // Between high water mark and critical mark — accept with warning
        if (projectedSize <= this.config.criticalMark) {
            this.emitWarning(projectedSize);
            return { action: 'ACCEPT' };
        }

        // Over critical mark — apply policy
        this._backpressureEvents++;
        return this.applyPolicy(projectedSize);
    }

    // ─────────────────────────────────────────────────
    // Apply backpressure policy
    // ─────────────────────────────────────────────────
    private applyPolicy(projectedSize: number): BackpressureAction {
        switch (this.config.policy) {
            case BackpressurePolicy.DROP_NEWEST:
                // Don't write this event — keep existing buffer
                this._droppedEvents++;
                log.warn(
                    {
                        connectionId: this.connectionId,
                        bufferedBytes: this._bufferedBytes,
                        projectedBytes: projectedSize,
                        droppedTotal: this._droppedEvents,
                        policy: 'DROP_NEWEST',
                    },
                    `⚠️  Backpressure: dropping event (buffer would exceed ${this.config.criticalMark} bytes)`
                );
                return { action: 'DROP' };

            case BackpressurePolicy.DROP_OLDEST:
                // The caller should flush the oldest buffered event
                // and write the new one instead
                this._droppedEvents++;
                log.warn(
                    {
                        connectionId: this.connectionId,
                        bufferedBytes: this._bufferedBytes,
                        policy: 'DROP_OLDEST',
                    },
                    `⚠️  Backpressure: dropping oldest event to make room`
                );
                return { action: 'DROP' };

            case BackpressurePolicy.DISCONNECT:
                // Terminal — the connection should be closed
                log.error(
                    {
                        connectionId: this.connectionId,
                        bufferedBytes: this._bufferedBytes,
                        projectedBytes: projectedSize,
                        policy: 'DISCONNECT',
                    },
                    `🔌 Backpressure: disconnecting slow client (buffer at ${this._bufferedBytes} bytes)`
                );
                return { action: 'DISCONNECT' };

            default:
                return { action: 'DROP' };
        }
    }

    // ─────────────────────────────────────────────────
    // Throttled warning emission
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: We throttle warnings to avoid flooding logs.
    // Without throttling, a consistently slow client would generate
    // a warning on EVERY write — potentially thousands per minute.
    private checkThresholds(): void {
        this.emitWarning(this._bufferedBytes);
    }

    private emitWarning(bufferSize: number): void {
        const now = Date.now();

        // At most one warning per 10 seconds per connection
        if (now - this._lastWarningTime < 10_000) return;
        this._lastWarningTime = now;

        log.warn(
            {
                connectionId: this.connectionId,
                bufferedBytes: bufferSize,
                highWaterMark: this.config.highWaterMark,
                criticalMark: this.config.criticalMark,
                isDraining: this._isDraining,
            },
            `⚠️  SSE backpressure warning: ${bufferSize} bytes buffered for connection ${this.connectionId}`
        );
    }
    // ─────────────────────────────────────────────────
    // Metrics
    // ─────────────────────────────────────────────────
    get bufferedBytes(): number { return this._bufferedBytes; }
    get isDraining(): boolean { return this._isDraining; }
    get droppedEvents(): number { return this._droppedEvents; }
    get backpressureEvents(): number { return this._backpressureEvents; }
}

// ─────────────────────────────────────────────────
// Backpressure Action — What the caller should do
// ─────────────────────────────────────────────────
export interface BackpressureAction {
    action: 'ACCEPT' | 'DROP' | 'DISCONNECT'
}