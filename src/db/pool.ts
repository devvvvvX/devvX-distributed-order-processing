// ============================================================
// PostgreSQL Connection Pool
// ============================================================
import { Pool, PoolConfig } from 'pg';
import { config } from '../config/index.js';
import { createModuleLogger } from '../shared/logger.js';

const log = createModuleLogger('db');

export function createPool(): Pool {
    const poolConfig: PoolConfig = {
        connectionString: config.databaseUrl,
        min: config.dbPoolMin,
        max: config.dbPoolMax,
        idleTimeoutMillis: config.dbPoolIdleTimeoutMs,
        connectionTimeoutMillis: config.dbConnectionTimeoutMs,

        // Statement timeout prevents runaway queries from holding connections forever. A query that takes 30 seconds is probably stuck or hitting a missing index. Kill it, don't wait. Without this, a slow query can starve the pool.
        statement_timeout: 30000,

        // Setting session-level parameters for observability. application_name shows up in pg_stat_activity, so you can see which service's queries are running: SELECT * FROM pg_stat_activity;
        application_name: 'order-platform'
    }

    const pool = new Pool(poolConfig);

    // ─────────────────────────────────────────────────
    // Pool event handlers — Observability
    // ─────────────────────────────────────────────────

    pool.on('connect', () => {
        log.debug(
            {
                totalCount: pool.totalCount,
                idleCount: pool.idleCount,
                waitingCount: pool.waitingCount,
            },
            'New client connected to PostgreSQL'
        );
    });

    pool.on('error', (err) => {
        // Pool-level errors are SERIOUS. 
        // They typically mean:
        // 1. PostgreSQL crashed or restarted
        // 2. Network partition between app and DB
        // 3. Connection was idle too long and the server closed it
        //
        // The pool automatically removes dead connections and creates new ones.
        // But if ALL connections fail, your health check will report unhealthy,
        // and the load balancer should stop sending traffic to this instance.
        log.error({ err }, '❌ Unexpected datbase pool error');
    });

    pool.on('remove', () => {
        log.debug(
            {
                totalCount: pool.totalCount,
                idleCount: pool.idleCount,
            },
            'Client removed from pool'
        );
    });

    log.info(
        {
            min: config.dbPoolMin,
            max: config.dbPoolMax,
            idleTimeoutMs: config.dbPoolIdleTimeoutMs,
            connectionTimeoutMs: config.dbConnectionTimeoutMs
        },
        '📦 PostgreSQL connection pool created'
    );

    return pool;
}