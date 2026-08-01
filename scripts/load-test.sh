#!/bin/bash
# ============================================================
# Phase 1 — Load Test Script
# ============================================================
# 🔍 LEARNING NOTE: This script lets you FEEL the monolith's limits.
#
# Experiments to try:
#
# 1. BASELINE (notification delay = 0ms):
#    Set NOTIFICATION_DELAY_MS=0 in docker-compose.yml
#    Run this script → observe fast response times (~20-50ms)
#
# 2. SYNCHRONOUS BOTTLENECK (notification delay = 2000ms):
#    Set NOTIFICATION_DELAY_MS=2000
#    Run this script → observe 2000ms+ response times
#    The customer waits 2 seconds because we're sending email synchronously.
#
# 3. NOTIFICATION FAILURES (50% failure rate):
#    Set NOTIFICATION_FAILURE_RATE=0.5
#    Run this script → observe ~50% of orders log notification failures
#
# 4. CONCURRENT LOAD (connection pool exhaustion):
#    Run 20+ concurrent requests → observe connection timeout errors
#    when pool (max=10) is exhausted.
#
# Usage:
#   ./scripts/load-test.sh                    # Default: 10 sequential orders
#   ./scripts/load-test.sh 50                 # 50 sequential orders
#   ./scripts/load-test.sh 20 concurrent      # 20 concurrent orders

set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:3000}"
NUM_ORDERS="${1:-10}"
MODE="${2:-sequential}"  # "sequential" or "concurrent"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Phase 1 Load Test — Monolith Bottleneck Demo${NC}"
echo -e "${BLUE}════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Orders:  ${YELLOW}${NUM_ORDERS}${NC}"
echo -e "  Mode:    ${YELLOW}${MODE}${NC}"
echo -e "  Target:  ${YELLOW}${BASE_URL}${NC}"
echo ""

# Check if server is running
if ! curl -sf "${BASE_URL}/health" > /dev/null 2>&1; then
  echo -e "${RED}❌ Server not reachable at ${BASE_URL}${NC}"
  echo "   Start with: docker compose up --build -d"
  exit 1
fi

echo -e "${GREEN}✅ Server is healthy${NC}"
echo ""

# Function to create a single order and measure time
# Helper: get current time in milliseconds (macOS + Linux compatible)
now_ms() {
  python3 -c 'import time; print(int(time.time()*1000))'
}

create_order() {
  local order_num=$1
  local idem_key="load-test-$(date +%s)-${order_num}-${RANDOM}"

  local start_time
  start_time=$(now_ms)

  local response
  response=$(curl -sf -w "\n%{http_code}\n%{time_total}" \
    -X POST "${BASE_URL}/api/v1/orders" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: ${idem_key}" \
    -d "{
      \"customerId\": \"cust-$(printf '%03d' $((order_num % 10)))\",
      \"customerName\": \"Test Customer ${order_num}\",
      \"customerEmail\": \"customer${order_num}@test.com\",
      \"customerPhone\": \"+1555000${order_num}\",
      \"restaurantId\": \"rest-$(printf '%03d' $((order_num % 5)))\",
      \"restaurantName\": \"Restaurant $((order_num % 5))\",
      \"deliveryAddress\": \"${order_num} Test Street, Test City\",
      \"deliveryLat\": 37.7749,
      \"deliveryLng\": -122.4194,
      \"notes\": \"Order from load test #${order_num}\",
      \"items\": [
        {
          \"itemName\": \"Burger\",
          \"quantity\": $((RANDOM % 3 + 1)),
          \"unitPrice\": 12.99,
          \"customizations\": \"No onions\"
        },
        {
          \"itemName\": \"Fries\",
          \"quantity\": 1,
          \"unitPrice\": 4.99
        }
      ]
    }" 2>/dev/null)

  local end_time
  end_time=$(now_ms)
  local total_ms=$((end_time - start_time))

  # Extract status code (second-to-last line)
  local http_code=$(echo "$response" | tail -2 | head -1)
  local curl_time=$(echo "$response" | tail -1)

  if [ "$http_code" = "201" ]; then
    echo -e "  ${GREEN}✅ Order #${order_num}${NC}  HTTP ${http_code}  ${YELLOW}${total_ms}ms${NC}"
  else
    echo -e "  ${RED}❌ Order #${order_num}${NC}  HTTP ${http_code}  ${YELLOW}${total_ms}ms${NC}"
  fi
}

echo -e "${BLUE}Starting ${MODE} load test...${NC}"
echo ""

TOTAL_START=$(now_ms)

if [ "$MODE" = "concurrent" ]; then
  # 🔍 LEARNING NOTE: Concurrent mode sends ALL requests simultaneously.
  # This tests connection pool exhaustion. With pool_max=10 and 20
  # concurrent requests, 10 requests will be waiting for connections.
  # If NOTIFICATION_DELAY_MS is high, those waiters will timeout.
  echo -e "${YELLOW}⚠️  Concurrent mode — watch for connection pool exhaustion!${NC}"
  echo ""

  for i in $(seq 1 "$NUM_ORDERS"); do
    create_order "$i" &
  done
  wait
else
  # Sequential mode
  for i in $(seq 1 "$NUM_ORDERS"); do
    create_order "$i"
  done
fi

TOTAL_END=$(now_ms)
TOTAL_TIME=$((TOTAL_END - TOTAL_START))

echo ""
echo -e "${BLUE}════════════════════════════════════════════════${NC}"
echo -e "  Total time: ${YELLOW}${TOTAL_TIME}ms${NC} for ${NUM_ORDERS} orders"
echo -e "  Avg time:   ${YELLOW}$((TOTAL_TIME / NUM_ORDERS))ms${NC} per order"
echo -e "${BLUE}════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}💡 Experiments to try:${NC}"
echo "  1. Set NOTIFICATION_DELAY_MS=2000 → watch orders take 2+ seconds each"
echo "  2. Set NOTIFICATION_FAILURE_RATE=0.5 → watch notification errors"
echo "  3. Run: ./scripts/load-test.sh 20 concurrent → watch pool exhaustion"
echo "  4. Compare: Phase 2 with Kafka will handle this without blocking"
