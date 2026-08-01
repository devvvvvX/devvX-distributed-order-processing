// ============================================================
// Notification Types
// ============================================================

export const NotificationType = {
  ORDER_CONFIRMED: 'ORDER_CONFIRMED',
  ORDER_STATUS_CHANGED: 'ORDER_STATUS_CHANGED',
  ORDER_CANCELLED: 'ORDER_CANCELLED',
  DELIVERY_UPDATE: 'DELIVERY_UPDATE',
} as const;

export type NotificationTypeValue =
  (typeof NotificationType)[keyof typeof NotificationType];

export const NotificationChannel = {
  EMAIL: 'EMAIL',
  SMS: 'SMS',
  PUSH: 'PUSH',
} as const;

export type NotificationChannelValue =
  (typeof NotificationChannel)[keyof typeof NotificationChannel];

export const NotificationStatus = {
  PENDING: 'PENDING',
  SENT: 'SENT',
  FAILED: 'FAILED',
} as const;

export type NotificationStatusValue =
  (typeof NotificationStatus)[keyof typeof NotificationStatus];

export interface Notification {
  id: string;
  orderId: string;
  type: NotificationTypeValue;
  channel: NotificationChannelValue;
  status: NotificationStatusValue;
  recipient: string;
  subject?: string;
  content: string;
  retryCount: number;
  failureReason?: string;
  sentAt?: Date;
  createdAt: Date;
}

export interface SendNotificationInput {
  orderId: string;
  type: NotificationTypeValue;
  channel: NotificationChannelValue;
  recipient: string;
  subject?: string;
  content: string;
}
