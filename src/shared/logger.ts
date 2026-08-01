// ============================================================
// Structured Logger — Pino
// ============================================================
import pino from 'pino';
import { config } from '../config';

export const logger = pino({
    level: config.logLevel,

    ...(config.nodeEnv === 'development' && {
        transport: {
            target: 'pino-pretty',
            options: {
                colorize: true,
                translateTime: 'SYS:HH:MM:ss.l',
                ignore: 'pid,hostname',
            }
        }
    }),

    // To know which service produced the log line
    base: {
        service: 'order-platform',
        version: '1.0.0',
    },

    // Never log passwords, tokens, cookies
    redact: {
        paths: ['req.headers.authorization', 'req.headers.cookie'],
        censor: '[REDACTED]'
    },

    // Seralize errors with stack trace
    serializers: {
        err: pino.stdSerializers.err,
        req: pino.stdSerializers.req,
        res: pino.stdSerializers.res,
    },
});

// Create child logger with additional context
export function createModuleLogger(module: string): pino.Logger {
    return logger.child({ module });
}