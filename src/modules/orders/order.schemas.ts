// ============================================================
// Order Validation Schemas (Zod)
// ============================================================
// Validation at the API boundary is critical.
// Never trust client input. Ever.

import { z } from "zod";

export const createOrderItemSchema = z.object({
  itemName: z
    .string()
    .min(1, "Item name is required")
    .max(255, "Item name is too long"),

  quantity: z
    .number()
    .int("Quantity must be a whole number")
    .positive("Quantity must be positive"),

  unitPrice: z.number().nonnegative("Unit price cannot be negative"),

  customizations: z.string().max(1000, "Customizations too long").optional(),
});

export const createOrderSchema = z.object({
  customerId: z.string().min(1, "Customer ID is required"),
  customerName: z.string().min(1, "Customer name is required").max(255),
  customerEmail: z.email("Customer name is required"),
  customerPhone: z.string().max(50).optional(),

  restaurantId: z.string().min(1, "Restaurant ID is required"),
  restaurantName: z.string().min(1, "Restaurant name is required").max(255),
  deliveryAddress: z.string().min(1, "Delivery address is required"),

  deliveryLat: z.number().min(-90).max(90).optional(),
  deliveryLng: z.number().min(-180).max(180).optional(),
  notes: z.string().max(1000).optional(),

  items: z
    .array(createOrderItemSchema)
    .min(1, "At least one item is required")
    .max(50, "Maximum 50 items per order"),
});

export const updateOrderStatusSchema = z.object({
  status: z.enum([
    'PENDING',
    'CONFIRMED',
    'PREPARING',
    'READY_FOR_PICKUP',
    'OUT_FOR_DELIVERY',
    'DELIVERED',
    'CANCELLED',
  ]),
  changedBy: z.string().max(255).optional().default('system'),
  reason: z.string().max(1000).optional(),
});

// Pagination query params
export const paginationSchema = z.object({
    page: z.coerce.number().int().positive().default(1),
    limit: z.coerce.number().int().min(1).max(100).default(1),
});

// Export inferred types (derived from schemas — no duplication)
export type CreateOrderSchemaType = z.infer<typeof createOrderSchema>;
export type UpdateOrderStatusSchemaType = z.infer<typeof updateOrderStatusSchema>;
export type PaginationSchemaType = z.infer<typeof paginationSchema>;
