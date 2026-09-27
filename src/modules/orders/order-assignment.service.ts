// ============================================================
// Order Assignment Service — Distributed Driver Assignment
// ============================================================
// 🔍 LEARNING NOTE: This simulates the real-world problem of
// assigning a delivery driver to an order. Only ONE driver
// should be assigned to each order.
//
// THE PROBLEM:
//   Order #123 is READY_FOR_PICKUP.
//   Driver A and Driver B both see it on their app.
//   Both tap "Accept" at the exact same time.
//   Who gets the order?
//
// WITHOUT COORDINATION:
//   Both drivers' requests hit different API instances.
//   Both read the order → no driver assigned → both update.
//   Last write wins → one driver gets a phantom assignment.
//   The other driver shows up at the restaurant → confusion.
//
// WITH REDIS COORDINATION:
//   Driver A: SET assignment:order:123 driver-A NX PX 300000
//     → "OK" (assigned!)
//   Driver B: SET assignment:order:123 driver-B NX PX 300000
//     → null (already assigned to driver-A)
//
// Redis NX guarantees that only ONE driver wins, regardless
// of which API instance handles the request.
//
// WHY NOT USE THE DATABASE?
//   You COULD add a `driver_id` column to the orders table and use:
//     UPDATE orders SET driver_id = $1 WHERE id = $2 AND driver_id IS NULL
//   This works! But:
//   1. It requires a DB roundtrip even for the losing driver
//   2. Under high contention (10 drivers, 1 order), that's 10 DB queries
//   3. Redis NX is ~0.1ms vs ~2ms for a DB query
//   4. The assignment is EPHEMERAL (expires if driver doesn't pick up)
//      — Redis TTL handles this naturally
//   5. The DB stores the FINAL assignment; Redis handles the RACE.

import Redis from 'ioredis';
import { createModuleLogger } from '../../shared/logger';

const log = createModuleLogger('order-assignment');

export interface AssignmentResult {
    assigned: boolean;
    driverId: string;
    orderId: string;
    expiresAt?: Date;
}

export class OrderAssignmentService {
    // Assignment TTL: 5 minutes to pick up the order
    // 🔍 LEARNING NOTE: If the driver doesn't pick up the order within
    // this time, the assignment expires and the order becomes available
    // again. This prevents "ghost assignments" where a driver accepts
    // but never shows up.
    private readonly ASSIGNMENT_TTL_MS = 300000; // 5 minutes

    constructor(private readonly redis: Redis) { }

    // ─────────────────────────────────────────────────
    // Assign a driver to an order
    // ─────────────────────────────────────────────────
    async assignDriver(orderId: string, driverId: string): Promise<AssignmentResult> {
        const key = `assignment:order:${orderId}`;

        try {
            // 🔍 LEARNING NOTE: SET NX PX is the atomic compare-and-set.
            // NX: only set if key does NOT exist (no current assignment)
            // PX: auto-expire after TTL (driver must pick up in time)
            //
            // This single command handles the entire race condition:
            // - If no assignment exists → create it → return OK
            // - If assignment already exists → return null (someone else won)
            // No locks needed. No transactions. One atomic operation.
            const result = await this.redis.set(
                key,
                driverId,
                'PX',
                this.ASSIGNMENT_TTL_MS,
                'NX',
            );

            if (result === 'OK') {
                const expiresAt = new Date(Date.now() + this.ASSIGNMENT_TTL_MS);

                log.info(
                    { orderId, driverId, expiresAt: expiresAt.toISOString() },
                    `🚗 Driver ${driverId} assigned to order ${orderId}`
                );

                return {
                    assigned: true,
                    driverId,
                    orderId,
                    expiresAt
                };
            }

            // Someone else got it first
            const currentDriver = await this.redis.get(key);
            log.info(
                { orderId, driverId, currentDriver },
                `🚫 Order ${orderId} already assigned to ${currentDriver}`
            );

            return {
                assigned: false,
                driverId: currentDriver ?? 'unknown',
                orderId,
            }

        } catch (err) {
            log.error({ err, orderId, driverId }, '❌ Failed to assign driver');
            throw err;
        }
    }

    // ─────────────────────────────────────────────────
    // Get the current assignment for an order
    // ─────────────────────────────────────────────────
    async getAssignment(orderId: string): Promise<string | null> {
        try {
            return await this.redis.get(`assignment:order:${orderId}`);
        } catch (err) {
            log.error({ err, orderId }, '❌ Failed to get assignment');
            return null;
        }
    }

    // ─────────────────────────────────────────────────
    // Release an assignment (driver cancels or times out)
    // ─────────────────────────────────────────────────
    // 🔍 LEARNING NOTE: We use the same Lua script pattern as the
    // distributed lock — only delete if the value matches.
    // This prevents Driver A from releasing Driver B's assignment.
    async releaseAssignment(orderId: string, driverId: string): Promise<boolean> {
        try {
            const result = await this.redis.eval(
                `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`,
                1,
                `assignment:order:${orderId}`,
                driverId
            );

            const released = result === 1;
            if (released) {
                log.info({ orderId, driverId }, `🚗 Assignment released for order ${orderId}`);

            }
            return released;
        } catch (err) {
            log.error({ err, orderId, driverId }, '❌ Failed to release assignment');
            return false;
        }
    }

}