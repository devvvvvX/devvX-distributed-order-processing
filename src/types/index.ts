// ============================================================
// Shared Type Definitions
// ============================================================

// Standard API error
export interface ApiError {
  code: string;
  message: string;
  details?: unknown;
}

// Pagination metadata
export interface PaginationMeta {
  page: number;
  limit: number;
  total: number;
  totalPages: number;
}

// Standard API response envelope
export type ApiResponse<T> =
  | {
      success: true;
      data: T;
      meta?: PaginationMeta;
    }
  | {
      success: false;
      error: ApiError;
    };

// Pagination request parameters
export interface PaginationParams {
  page: number;
  limit: number;
}

// Common timestamp fields
export interface Timestamps {
  createdAt: Date;
  updatedAt: Date;
}