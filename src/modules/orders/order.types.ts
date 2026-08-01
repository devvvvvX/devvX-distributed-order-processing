// ============================================================
// Order Types
// ============================================================
// Order status follows a state machine pattern.
// Not every transition is valid:
//   PENDING → CONFIRMED ✅
//   PENDING → DELIVERED ❌ (can't skip steps)
//   DELIVERED → PENDING ❌ (can't go backwards)
//   CANCELLED → anything ❌ (terminal state)

export const OrderStatus = {
  PENDING: "PENDING",
  CONFIRMED: "CONFIRMED",
  PREPARING: "PREPARING",
  READY_FOR_PICKUP: "READY_FOR_PICKUP",
  OUT_FOR_DELIVERY: "OUT_FOR_DELIVERY",
  DELIVERED: "DELIVERED",
  CANCELLED: "CANCELLED",
} as const;

export type OrderStatusType = (typeof OrderStatus)[keyof typeof OrderStatus];

// Valid state transitions as a directed graph.
// This prevents impossible state changes. In production, this is
// usually stored as a proper state machine (e.g., XState).
// We keep it simple here — a map of "from" → "allowed to" states.
export const VALID_STATUS_TRANSITIONS: Record<
  OrderStatusType,
  OrderStatusType[]
> = {
  PENDING: ["CONFIRMED", "CANCELLED"],
  CONFIRMED: ["PREPARING", "CANCELLED"],
  PREPARING: ["READY_FOR_PICKUP", "CANCELLED"],
  READY_FOR_PICKUP: ["OUT_FOR_DELIVERY", "CANCELLED"],
  OUT_FOR_DELIVERY: ["DELIVERED"],
  DELIVERED: [], // Terminal state — no transitions allowed
  CANCELLED: [], // Terminal state — no transitions allowed
};

// ─────────────────────────────────────────────────
// Domain Types
// ─────────────────────────────────────────────────

export interface OrderItem {
  id: string;
  orderId: string;

  itemName: string;
  quantity: number;

  // Store monetary values in the smallest currency unit
  // (e.g. cents/paise).
  unitPrice: number;
  totalPrice: number;

  customizations?: string;

  createdAt: Date;
}

export interface Order {
  id: string;
  status: OrderStatusType;

  // Customer
  customerId: string;
  customerName: string;
  customerEmail: string;
  customerPhone?: string;

  // Restaurant
  restaurantId: string;
  restaurantName: string;

  // Payment
  totalAmount: number;
  deliveryFee: number;
  taxAmount: number;
  grandTotal: number;

  // Delivery
  deliveryAddress: string;
  deliveryLat?: number;
  deliveryLng?: number;
  notes?: string;

  // Used to prevent duplicate order creation
  idempotencyKey?: string;

  items: OrderItem[];

  createdAt: Date;
  updatedAt: Date;
}

export interface OrderStatusHistoryEntry {
  id: string;
  orderId: string;

  fromStatus: OrderStatusType | null;
  toStatus: OrderStatusType;

  changedBy: string;
  reason?: string;

  metadata?: Record<string, unknown>;

  createdAt: Date;
}

// ============================================================
// Request DTOs
// ============================================================

export interface CreateOrderItemInput {
  itemName: string;
  quantity: number;
  unitPrice: number;
  customizations?: string;
}

export interface CreateOrderInput {
  // Customer
  customerId: string;
  customerName: string;
  customerEmail: string;
  customerPhone?: string;

  // Restaurant
  restaurantId: string;
  restaurantName: string;

  // Delivery
  deliveryAddress: string;
  deliveryLat?: number;
  deliveryLng?: number;
  notes?: string;

  items: CreateOrderItemInput[];
}

export interface UpdateOrderStatusInput {
  status: OrderStatusType;
  changedBy?: string;
  reason?: string;
}