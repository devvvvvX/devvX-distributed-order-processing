// ============================================================
// SSE Module — Barrel Export (Phase 5)

export { SSEConnection } from './sse-connection.js';
export { SSEManager } from './sse-manager';
export type { SSEManagerConfig } from './sse-manager';
export { SSERedisSubscriber, publishOrderUpdate } from './redis-subscriber';
export { BackpressureTracker, DEFAULT_BACKPRESSURE_CONFIG } from './sse-backpressure';
export { registerSSERoutes } from './sse-routes';
export {
    SSEConnectionState,
    BackpressurePolicy
} from './types';
export type {
    SSEEvent,
    SSEMetrics,
    BackpressureConfig,
    OrderStreamEvent,
    OrderStreamEventType,
    RedisPubSubMessage
} from './types.js'