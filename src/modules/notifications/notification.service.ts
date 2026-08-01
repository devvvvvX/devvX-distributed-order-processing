// ============================================================
// Notification Service — The Intentional Bottleneck
// ============================================================
// This service simulates sending notifications
// (email, SMS, push) SYNCHRONOUSLY within the order creation flow.

// THIS IS THE #1 ARCHITECTURAL PROBLEM IN PHASE 1.
//
// In reality, sending an email might:
// - Take 200ms-2000ms (external API call to SendGrid/SES/Twilio)
// - Fail intermittently (external service down, rate limited)
// - Time out (network issues)
//
// Because we call this SYNCHRONOUSLY in the order creation flow:
// 1. Order creation latency = DB write + notification latency
//    A 50ms order write becomes 550ms with notification delay.
// 2. If notification fails, order creation fails
//    Customer can't place an order because... email is down? Bad UX.
// 3. If notification service is slow, all orders are slow
//    One slow external API affects ALL customers.
// 4. Can't retry notifications independently
//    If email fails, we'd have to retry the entire order creation.
//
// PHASE 2 FIX: Publish an "OrderCreated" event to Kafka.
// A separate notification consumer processes it asynchronously.
// Order creation returns in 50ms regardless of notification status.

import { config } from "../../config/index.js";
import { createModuleLogger } from "../../shared/logger.js";
import { NotificationRepository } from "./notification.repository.js";
import { SendNotificationInput, Notification } from "./notification.types.js";

const log = createModuleLogger("notification-service");

export class NotificationService {
  constructor(private readonly repository: NotificationRepository) {}

  // ─────────────────────────────────────────────────
  // Send notification (synchronously — the bottleneck)
  // ─────────────────────────────────────────────────
  async send(input: SendNotificationInput): Promise<Notification> {
    const startTime = Date.now();

    // Persist the notification attempt
    const notification = await this.repository.create(input);

    try {
      // 🔍 LEARNING NOTE: This simulates calling an external API.
      // The configurable delay lets you FEEL the bottleneck.
      //
      // Try this experiment:
      // 1. Set NOTIFICATION_DELAY_MS=0 → order creation is fast (~50ms)
      // 2. Set NOTIFICATION_DELAY_MS=2000 → order creation takes ~2050ms
      // 3. Set NOTIFICATION_FAILURE_RATE=0.5 → 50% of orders fail
      //
      // This is not hypothetical. This is exactly what happens when
      // your email provider has a partial outage.

      await this.simulateExternalApiCall();

      // Makr as sent
      await this.repository.updateStatus(notification.id, "SENT");

      const latency = Date.now() - startTime;
      log.info(
        {
          notificaitonId: notification.id,
          orderId: notification.orderId,
          channel: notification.channel,
          type: notification.type,
          latencyMs: latency,
        },
        `✉️ Notification sent (${input.channel})`,
      );

      return { ...notification, status: "SENT", sentAt: new Date() };
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error";

      // Mark as failed
      await this.repository.updateStatus(notification.id, "FAILED", errorMessage);

      const latency = Date.now() - startTime;
      log.info(
        {
          notificaitonId: notification.id,
          orderId: notification.orderId,
          channel: notification.channel,
          error: errorMessage,
          latencyMs: latency,
        },
        `❌ Notification failed`,
      );

      // 🔍 LEARNING NOTE: We RE-THROW the error here.
      // This means the notification failure propagates up to the
      // order creation flow, causing the entire order API call to fail.
      //
      // Is this the right behavior? NO! The order was already saved
      // to the database. The customer should get their order even if
      // the notification fails. But in a synchronous monolith,
      // we don't have a clean way to handle this.
      //
      // Options in Phase 1 (all bad):
      // a) Re-throw → order creation fails (current behavior)
      // b) Swallow the error → customer never gets notified, no retry
      // c) Inline retry → order creation takes even longer
      //
      // The REAL fix is async event-driven processing (Phase 2).
      throw err;
    }
  }

  // ─────────────────────────────────────────────────
  // Build notification content for order events
  // ─────────────────────────────────────────────────
  buildOrderConfirmationContent(order: {
    id: string;
    customerName: string;
    restaurantName: string;
    grandTotal: number;
  }): { subject: string; content: string } {
    return {
      subject: `Order Confirmed - #${order.id.slice(0, 8)}`,
      content: [
        `Hi ${order.customerName},`,
        `Your order from ${order.restaurantName} has been received!`,
        `Order Id: ${order.id}`,
        `Total: $${order.grandTotal.toFixed(2)}`,
        "",
        "We will notify you when your order is being prepared.",
        "",
        "Thank you for your order!",
      ].join("\n"),
    };
  }

  buildOrderUpdateContent(order: {
    id: string;
    customerName: string;
    status: string;
  }): { subject: string; content: string } {
    const statusMessages: Record<string, string> = {
      CONFIRMED: "Your order has been confirmed by the restaurant!",
      PREPARING: "Your order is being prepared!",
      READY_FOR_PICKUP:
        "Your order is ready for pickup by the delivery partner!",
      OUT_FOR_DELIVERY: "Your order is out for delivery!",
      DELIVERED: "Your order is delivered. Enjoy your meal!",
      CANCELLED: "Your order has been cancelled.",
    };

    return {
      subject: `Order Update - #${order.id.slice(0, 8)} - ${order.status}`,
      content: [
        `Hi ${order.customerName}`,
        "",
        statusMessages[order.status] ??
          `Your order status is now: ${order.status}`,
        `Order Id: ${order.id}`,
        "",
        "Thank you!",
      ].join("/n"),
    };
  }

  private async simulateExternalApiCall() {
    // Simulate network latency
    if (config.notificationDelayMs > 0) {
      await new Promise((resolve) =>
        setTimeout(resolve, config.notificationDelayMs),
      );
    }

    // Simulate randome failures
    if (config.notificationFailureRate > 0) {
      if (Math.random() < config.notificationFailureRate) {
        throw new Error(
          "External notification API failed (simulated): Connection timeout to email service",
        );
      }
    }
  }
}
