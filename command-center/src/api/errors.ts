export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export function normalizeApiError(status: number, payload: unknown): ApiError {
  const body = isRecord(payload) && 'detail' in payload ? payload.detail : payload
  if (isRecord(body) && typeof body.message === 'string') {
    return new ApiError(status, typeof body.code === 'string' ? body.code : 'request_failed', body.message)
  }
  if (typeof body === 'string' && body.trim() && !body.trim().startsWith('<')) {
    return new ApiError(status, 'request_failed', body)
  }
  if (Array.isArray(body)) {
    const messages = body.flatMap((entry: unknown) =>
      isRecord(entry) && typeof entry.msg === 'string' ? [entry.msg] : [],
    )
    if (messages.length) return new ApiError(status, 'validation_error', messages.join('; '))
  }
  return new ApiError(status, 'request_failed', `The API rejected this request (HTTP ${status}).`)
}

export function asApiError(error: unknown): ApiError {
  if (error instanceof ApiError) return error
  if (error instanceof Error) return new ApiError(0, 'unexpected_error', error.message)
  return new ApiError(0, 'unexpected_error', 'An unexpected error occurred. Refresh before repeating an action.')
}

export function isAborted(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError'
}
