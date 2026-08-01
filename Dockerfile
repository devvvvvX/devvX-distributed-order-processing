# ============================================================
# Multi-stage Dockerfile for Order Platform
# ============================================================
# 🔍 LEARNING NOTE: Multi-stage builds keep production images small.
# Stage 1 (builder) has devDependencies for compilation.
# Stage 2 (runner) only has production dependencies + compiled JS.
# This matters in production: smaller image = faster deploys = faster scaling.

# --- Stage 1: Build ---
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files first for better Docker layer caching
# 🔍 LEARNING NOTE: Docker caches layers. If package.json hasn't changed,
# npm install is skipped entirely on rebuild. This is why we copy
# package files BEFORE copying source code.

COPY package.json package-lock.json* ./

RUN npm install

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

# --- Stage 2: Production ---
FROM node:20-alpine AS runner

WORKDIR /app

# Security: don't run as root in production
# 🔍 LEARNING NOTE: Running containers as root is a security risk.
# If an attacker exploits your app, they get root access to the container.
# Always create a non-root user.

RUN addgroup --system --gid 1001 appgroup && \
    adduser --system --uid 1001 appuser

COPY package.json package-lock.json* ./

# Production dependenciesn only — no typescript, no dev tools
RUN npm install --omit=dev && npm cache clean --force

COPY --from=builder /app/dist ./dist

USER appuser

EXPOSE 3000

# 🔍 LEARNING NOTE: Use "node" directly, not "npm start".
# npm adds a wrapper process that doesn't forward signals properly.
# Graceful shutdown (SIGTERM) needs to reach your Node.js process directly.
# This is critical when Kubernetes sends SIGTERM during pod termination.
CMD ["node", "dist/index.js"]