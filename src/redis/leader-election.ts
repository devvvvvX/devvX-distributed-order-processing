// ============================================================
// Leader Election — Redis Lease-Based Leadership
// ============================================================
// 🔍 LEARNING NOTE: Leader election is a FUNDAMENTAL distributed
// systems problem. It answers the question:
//   "Among N identical instances, who is the ONE that should do X?"
//
// In our case, X = "run the outbox relay".
//
// WITHOUT LEADER ELECTION (Phase 3):
//   API Instance 1: outbox relay → polls → publishes event A
//   API Instance 2: outbox relay → polls → publishes event A (AGAIN!)
//   API Instance 3: outbox relay → polls → publishes event A (AGAIN!!)
//   Consumer: idempotency catches duplicates (but wasted work)
//
// WITH LEADER ELECTION (Phase 4):
//   API Instance 1: ← LEADER → runs outbox relay
//   API Instance 2: ← FOLLOWER → relay stopped
//   API Instance 3: ← FOLLOWER → relay stopped
//   Only one relay runs. Clean. Efficient.
//
// ALGORITHM: Renewable Lease
//   1. Try to acquire leadership: SET leader-key instanceId NX PX ttl
//      NX = only if key doesn't exist (no current leader)
//      PX = auto-expire after TTL (safety net if leader crashes)
//
//   2. If acquired: I am the leader. Start heartbeat.
//      Heartbeat renews the lease: SET leader-key instanceId XX PX ttl
//      XX = only if key already exists AND has my value
//
//   3. If not acquired: I am a follower. Check periodically.
//      When the leader's lease expires (crash, network partition),
//      a follower acquires the lease.
//
// FAILURE MODES:
//   Leader crashes: Lease expires after TTL → follower takes over.
//   Network partition (leader loses Redis): Can't renew → loses leadership.
//     Brief partition (<TTL): Leader might still think it's leader.
//     This causes BRIEF duplicate relay (idempotency handles it).
//   Redis crashes: All instances lose leadership → no relay runs.
//     Events accumulate in outbox → published when Redis+relay return.
//
// WHY NOT ZOOKEEPER/ETCD/CONSUL?
//   These provide stronger leader election guarantees (consensus-based)
//   but are more complex to operate. For our use case (outbox relay
//   where duplicates are handled by idempotency), Redis is sufficient.
//   The worst case (brief duplicate publishing) is safe.

import Redis from 'ioredis';
import { config } from '../config';
import { createModuleLogger } from '../shared/logger';
import { hostname } from 'os';

const log = createModuleLogger('leader-election');

// Lua script for renewing the lease (only if we still hold it)
// 🔍 LEARNING NOTE: This is the same "check-then-write" pattern
// as the lock release, but instead of deleting, we extend the TTL.
const RENEW_LEASE_SCRIPT = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("PSETEX", KEYS[1], ARGV[2], ARGV[1])
    else
        return nil
    end
`;

export class LeaderElection {
    private readonly instanceId: string;
    private readonly leaderKey = 'leader:outbox-relay';
    private intervalHandle: ReturnType<typeof setInterval> | null = null;
    private _isLeader = false;
    private onBecomeLeader: (() => void) | null = null;
    private onLoseLeadership: (() => void) | null = null;

    constructor(private readonly redis: Redis) {
        // 🔍 LEARNING NOTE: The instance ID must be unique across all instances.
        // hostname + PID + random suffix ensures uniqueness even if:
        // - Multiple processes run on the same host (different PIDs)
        // - Containers have the same hostname (different random suffixes)
        this.instanceId = `${hostname()}-${process.pid}-${Math.random().toString(36).slice(2, 8)}`;
        log.info({ instanceId: this.instanceId }, 'Leader election initialized');
    }

    get isLeader(): boolean {
        return this._isLeader;
    }

    // ─────────────────────────────────────────────────
    // Start the election loop
    // ─────────────────────────────────────────────────
    start(
        onBecomeLeader: () => void,
        onLoseLeadership: () => void
    ): void {
        this.onBecomeLeader = onBecomeLeader;
        this.onLoseLeadership = onLoseLeadership;

        // Try immediately on start
        this.tryAcquireOrRenew();

        // Then check periodically
        this.intervalHandle = setInterval(
            () => this.tryAcquireOrRenew(),
            config.leaderElectionHeartbeatMs
        );
        this.intervalHandle.unref();

        log.info(
            {
                heartbeatMs: config.leaderElectionHeartbeatMs,
                leaseTtlMs: config.leaderElectionLeaseTtlMs,
                instanceId: this.instanceId,
            },
            '🗳️  Leader election started'
        );
    }

    // ─────────────────────────────────────────────────
    // Stop the election loop
    // ─────────────────────────────────────────────────
    async stop(): Promise<void> {
        if (this.intervalHandle) {
            clearInterval(this.intervalHandle);
            this.intervalHandle = null;
        }

        // If we're the leader, release the lease so a follower can
        // take over immediately (instead of waiting for TTL expiry)
        if (this._isLeader) {
            try {
                // Only delete if we still hold it (same Lua pattern as lock release)
                await this.redis.eval(
                    `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`,
                    1,
                    this.leaderKey,
                    this.instanceId
                );
                log.info('🏳️  Leadership relinquished (graceful shutdown)');
            } catch (err) {
                log.warn({ err }, 'Failed to relinquish leadership — lease will expire');
            }
            this._isLeader = false;
        }

        log.info('🗳️  Leader election stopped');
    }


    private async tryAcquireOrRenew(): Promise<void> {
        try {
            if (this._isLeader) {
                // Already leader — renew the lease
                await this.renewLease();
            } else {
                // Not leader — try to acquire
                await this.tryAcquire();
            }
        } catch (err) {
            // 🔍 LEARNING NOTE: If Redis is down, we can't determine leadership.
            // If we WERE the leader, we assume we've lost it (conservative).
            // This prevents split-brain: two instances both thinking they're leader.
            log.error({ err }, '❌ Leader election check failed');
            if (this._isLeader) {
                this._isLeader = false;
                log.warn('⚠️  Lost leadership due to Redis error');
                this.onLoseLeadership?.();
            }
        }
    }

    private async tryAcquire(): Promise<void> {
        // SET key value NX PX ttl
        const result = await this.redis.set(
            this.leaderKey,
            this.instanceId,
            'PX',
            config.leaderElectionLeaseTtlMs,
            'NX'
        );

        if (result == 'OK') {
            this._isLeader = true;
            log.info(
                { instanceId: this.instanceId },
                '👑 Became LEADER — starting outbox relay'
            );
            this.onBecomeLeader?.();
        } else {
            // Someone else is the leader — that's fine
            const currentLeader = await this.redis.get(this.leaderKey);
            log.debug(
                { currentLeader, instanceId: this.instanceId },
                'Following current leader'
            );
        }
    }

    private async renewLease(): Promise<void> {
        // Renew via Lua script (atomic check-and-set)
        const result = await this.redis.eval(
            RENEW_LEASE_SCRIPT,
            1,
            this.leaderKey,
            this.instanceId,
            config.leaderElectionLeaseTtlMs.toString()
        );

        if (result === null) {
            // 🔍 LEARNING NOTE: We thought we were the leader, but the key
            // either expired (we were too slow) or was taken by someone else.
            // This is a LEADERSHIP LOSS event.
            this._isLeader = false;
            log.warn(
                { instanceId: this.instanceId },
                '👑→🏳️  Lost leadership (lease expired or taken)'
            );
            this.onLoseLeadership?.();
        } else {
            log.debug({ instanceId: this.instanceId }, '👑 Leadership renewed');
        }
    }
}