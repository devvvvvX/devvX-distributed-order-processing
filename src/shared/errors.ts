// ============================================================
// Custom Error Classes
// ============================================================

export class AppError extends Error {
  public readonly statusCode: number;
  public readonly code: string;
  public readonly isOperational: boolean;

  constructor(
    message: string,
    statusCode: number = 500,
    code: string = "INTERNAL_ERROR",
    isOperational: boolean = true,
  ) {
    super(message);
    this.name = this.constructor.name;
    this.statusCode = statusCode;
    this.code = code;
    this.isOperational = isOperational;

    // This is needed for proper instanceof checks
    // when extending built-in classes in TypeScript.
    Object.setPrototypeOf(this, new.target.prototype);
    Error.captureStackTrace(this, this.constructor);
  }
}

// 404 — Resource doesn't exist
export class NotFoundError extends AppError {
  constructor(resource: string, id: string) {
    super(
      `${resource} with id ${id} not found`,
      404,
      `${resource.toUpperCase()}_NOT_FOUND`,
    );
  }
}

// 409 — Conflict (e.g., duplicate idempotency key, invalid state transition)
export class ConflictError extends AppError {
  constructor(message: string, code: string = "CONFLICT") {
    super(message, 409, code);
  }
}

// 422 — Validation failed
export class ValidationError extends AppError {
  public readonly details: unknown;

  constructor(message: string, details?: unknown) {
    super(message, 422, "VALIDATION_ERROR");
    this.details = details;
  }
}

// 503 — Service temporarily unavailable (e.g., DB connection failed)
export class ServiceUnavailableError extends AppError {
  constructor(service: string) {
    super(`${service} is temporarily unavailable`, 503, "SERVICE_UNAVAILABLE");
  }
}
