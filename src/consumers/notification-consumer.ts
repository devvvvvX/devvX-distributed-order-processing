// ============================================================
// Notification Consumer — Event-Driven Notification Processing
// ============================================================
// 🔍 LEARNING NOTE: This is the consumer that replaces the synchronous
// notification call in the order service.
//
// BEFORE (Phase 1 — synchronous):
//   Client → OrderService.createOrder() → NotificationService.send() → respond
//   Total latency: ~520ms (20ms order + 500ms notification)
//
// AFTER (Phase 2 — event-driven):
//   Client → OrderService.createOrder() → Kafka.publish() → respond (20ms!)
//   ... later, asynchronously ...
//   NotificationConsumer ← Kafka.poll() → NotificationService.send()
//
// The customer gets a fast response. The notification happens in the background.
// If this consumer is down, Kafka retains the events. When it comes back up,
// it processes the backlog. No messages are lost.

import { EachMessagePayload } from "kafkajs";
import { Pool } from 'pg';
import { MessageHandler } from "../kafka/consumer";
import {
    OrderEvent,
    OrderCreatedPayload,
    OrderStatusChangedPayload,
    ORDER_EVENT_TYPES,
    deserializeEvent
} from '../kafka/events.js'
import { NotificationService } from "../modules/notifications/notification.service";
import { NotificationRepository } from "../modules/notifications/notification.repository";
import { createModuleLogger } from "../shared/logger";

const log = createModuleLogger('notification-consumer');

export class NotificationConsumerHandler implements MessageHandler {
    private readonly notificationService: NotificationService;

    constructor(pool: Pool) {
        const notificationRepo = new NotificationRepository(pool);
        this.notificationService = new NotificationService(notificationRepo);
    }

    // ─────────────────────────────────────────────────
    // Handle incoming Kafka messages
    // ─────────────────────────────────────────────────
    async handle(payload: EachMessagePayload): Promise<void> {
        const { message, partition } = payload;
        const value = message.value?.toString();

        if (!value) {
            log.warn({ partition, offset: message.offset }, 'Empty message — skipping');
            return;
        }

        // Deserialize the event envelope
        let event: OrderEvent;
        try {
            event = deserializeEvent(value);
        } catch (err) {
            // 🔍 LEARNING NOTE: This is a "poison pill" — a malformed message
            // that can't be deserialized. We log it and skip it rather than
            // crashing. In Phase 3, we'll send it to a Dead Letter Queue.
            log.error(
                { err, partition, offset: message.offset, rawValue: value.slice(0, 200) },
                '❌ Failed to deserialize event — skipping (poison pill)'
            );
            return;
        }

        log.info(
            {
                eventType: event.eventType,
                eventId: event.eventId,
                aggregateId: event.aggregateId,
                partition,
                offset: message.offset,
            },
            `📨 Received event: ${event.eventType} for order ${event.aggregateId}`
        );

        // Route by event type
        // 🔍 LEARNING NOTE: A single consumer can handle multiple event types
        // from the same topic. This is more efficient than having separate
        // topics for each event type (fewer Kafka resources, simpler config).
        switch (event.eventType) {
            case ORDER_EVENT_TYPES.ORDER_CREATED:
                await this.handleOrderCreated(event as OrderEvent<OrderCreatedPayload>);
                break;
            case ORDER_EVENT_TYPES.ORDER_STATUS_CHANGED:
            case ORDER_EVENT_TYPES.ORDER_CANCELLED:
                await this.handleOrderStatusChanged(event as OrderEvent<OrderStatusChangedPayload>);
                break;
            default: {
                // 🔍 LEARNING NOTE: TypeScript narrows eventType to `never` here
                // because all known enum cases are handled above. But at runtime,
                // events from Kafka could contain any string (e.g., a new event type
                // from a newer version of the producer). So we cast to string.
                const unknownType = event.eventType as string;
                log.warn(
                    { eventType: unknownType, eventId: event.eventId },
                    `⚠️  Unknown event type: ${unknownType} — skipping`
                );
            }
        }
    }

    // ─────────────────────────────────────────────────
    // Handle ORDER_CREATED
    // ─────────────────────────────────────────────────
    private async handleOrderCreated(
        event: OrderEvent<OrderCreatedPayload>
    ): Promise<void> {
        const { order } = event.payload;
        const startTime = Date.now();

        // 🔍 LEARNING NOTE: We reuse the same NotificationService from Phase 1.
        // The business logic is identical — only the TRIGGER changed:
        //   Phase 1: Triggered synchronously by OrderService.createOrder()
        //   Phase 2: Triggered asynchronously by Kafka event
        //
        // This is the beauty of clean architecture: the notification logic
        // doesn't know or care how it was invoked

        const { subject, content } = this.notificationService.buildOrderConfirmationContent({
            id: order.id,
            customerName: order.customerName,
            restaurantName: order.restaurantName,
            grandTotal: order.grandTotal,
        });

        // 🔍 PHASE 3 CHANGE: We NO LONGER catch errors here.
        // Errors propagate up to the KafkaConsumer's withRetry() wrapper,
        // which handles retry with exponential backoff → DLQ on exhaustion.
        //
        // BEFORE (Phase 2): catch → log → swallow → notification LOST
        // AFTER  (Phase 3): throw → withRetry catches → retry 3x → DLQ if all fail
        await this.notificationService.send({
            orderId: order.id,
            type: 'ORDER_CONFIRMED',
            channel: 'EMAIL',
            recipient: order.customerEmail,
            subject,
            content
        });

        const latency = Date.now() - startTime;
        log.info(
            {
                orderId: order.id,
                eventId: event.eventId,
                latencyMs: latency,
            },
            `✉️  Order confirmation sent for ${order.id} (${latency}ms)`
        );
    }

    // ─────────────────────────────────────────────────
    // Handle ORDER_STATUS_CHANGED / ORDER_CANCELLED
    // ─────────────────────────────────────────────────
    private async handleOrderStatusChanged(
        event: OrderEvent<OrderStatusChangedPayload>
    ): Promise<void> {
        const { orderId, customerEmail, customerName, newStatus } = event.payload;
        const startTime = Date.now();

        const { subject, content } = this.notificationService.buildOrderUpdateContent({
            id: orderId,
            customerName,
            status: newStatus,
        })

        // 🔍 PHASE 3 CHANGE: Same as above — let errors propagate for retry+DLQ.
        await this.notificationService.send({
            orderId,
            type: newStatus === 'CANCELLED' ? 'ORDER_CANCELLED' : 'ORDER_STATUS_CHANGED',
            channel: 'EMAIL',
            recipient: customerEmail,
            subject,
            content,
        });

        const latency = Date.now() - startTime;
        log.info(
            {
                orderId,
                eventId: event.eventId,
                newStatus,
                latencyMs: latency,
            },
            `✉️  Status notification sent: ${newStatus} for ${orderId} (${latency}ms)`
        );
    }
}