// ============================================================
// Notification Repository
// ============================================================

import { Pool } from "pg";
import {
  Notification,
  SendNotificationInput,
  NotificationStatusValue,
} from "./notification.types.js";

export class NotificationRepository {
  constructor(private readonly pool: Pool) {}

  async create(input: SendNotificationInput): Promise<Notification> {
    const result = await this.pool.query<NotificationRow>(
      `INSERT INTO notifications (
        order_id, type, channel, status, recipient, subject, content
      ) VALUES ($1, $2, $3, 'PENDING', $4, $5, $6)
       RETURNING *`,
      [
        input.orderId,
        input.type,
        input.channel,
        input.recipient,
        input.subject ?? null,
        input.content,
      ],
    );

    return this.mapToNotification(result.rows[0]!);
  }

  async updateStatus(
    id: string,
    status: NotificationStatusValue,
    failureReason?: string,
  ): Promise<void> {
    await this.pool.query(
      `UPDATE notifications
       SET status = $1::notification_status,
           failure_reason = $2,
           sent_at = CASE WHEN $1::notification_status = 'SENT' THEN NOW() ELSE sent_at END,
           retry_count = CASE WHEN $1::notification_status = 'FAILED' THEN retry_count + 1 ELSE retry_count END
       WHERE id = $3`,
      [status, failureReason ?? null, id],
    );
  }

  async findByOrderId(orderId: string): Promise<Notification[]> {
    const result = await this.pool.query<NotificationRow>(
      `SELECT * FROM notifications WHERE order_id = $1 ORDER BY created_at DESC`,
      [orderId],
    );

    return result.rows.map((row) => this.mapToNotification(row));
  }

  private mapToNotification(row: NotificationRow): Notification {
    return {
      id: row.id,
      orderId: row.order_id,
      type: row.type,
      channel: row.channel,
      status: row.status,
      recipient: row.recipient,
      subject: row.subject ?? undefined,
      content: row.content,
      retryCount: row.retry_count,
      failureReason: row.failure_reason ?? undefined,
      sentAt: row.sent_at ?? undefined,
      createdAt: row.created_at,
    };
  }
}

interface NotificationRow {
  id: string;
  order_id: string;
  type: Notification["type"];
  channel: Notification["channel"];
  status: Notification["status"];
  recipient: string;
  subject: string | null;
  content: string;
  retry_count: number;
  failure_reason: string | null;
  sent_at: Date | null;
  created_at: Date;
}
