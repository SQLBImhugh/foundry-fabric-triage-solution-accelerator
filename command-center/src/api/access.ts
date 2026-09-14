import { requestJson } from './client'
import type { TokenProvider } from './client'
import { ApiError, isRecord } from './errors'

export const APP_ROLES = ['reader', 'operator', 'approver', 'admin'] as const
export const ENTRA_MANAGEMENT_URL = 'https://entra.microsoft.com/'
export type AppRole = typeof APP_ROLES[number]

export interface AppRoleDefinition {
  id: AppRole
  label: string
  description: string
}

export interface EffectiveAccess {
  source: 'entra_app_roles' | 'synthetic_demo'
  current_user: { id: string; display_name: string; roles: AppRole[] }
  role_catalog: AppRoleDefinition[]
  tenant_id: string | null
  application_id: string | null
  token_issued_at: string | null
  token_expires_at: string | null
  management_url: typeof ENTRA_MANAGEMENT_URL
}

function isText(value: unknown): value is string {
  return typeof value === 'string' && Boolean(value.trim())
}

function isRole(value: unknown): value is AppRole {
  return APP_ROLES.some((role) => role === value)
}

function isAuthorityId(value: unknown): value is string | null {
  return value === null || (typeof value === 'string'
    && /^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(value)
    && value !== '00000000-0000-0000-0000-000000000000')
}

function isTokenDate(value: unknown): value is string | null {
  if (value === null) return true
  if (typeof value !== 'string'
    || !/^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.test(value)
    || !Number.isFinite(Date.parse(value))) return false
  // Date.parse normalizes impossible calendar dates, such as February 30.
  const calendarDate = new Date(`${value.slice(0, 10)}T00:00:00Z`)
  return Number.isFinite(calendarDate.getTime()) && calendarDate.toISOString().slice(0, 10) === value.slice(0, 10)
}

function isEffectiveAccess(value: unknown): value is EffectiveAccess {
  return isRecord(value) && (value.source === 'entra_app_roles' || value.source === 'synthetic_demo')
    && isRecord(value.current_user) && isText(value.current_user.id) && isText(value.current_user.display_name)
    && Array.isArray(value.current_user.roles) && value.current_user.roles.length > 0
    && value.current_user.roles.every(isRole)
    && new Set(value.current_user.roles).size === value.current_user.roles.length
    && Array.isArray(value.role_catalog) && value.role_catalog.length === APP_ROLES.length
    && value.role_catalog.every((role) => isRecord(role) && isRole(role.id)
      && isText(role.label) && isText(role.description))
    && new Set(value.role_catalog.map((role) => role.id)).size === APP_ROLES.length
    && isAuthorityId(value.tenant_id) && isAuthorityId(value.application_id)
    && isTokenDate(value.token_issued_at) && isTokenDate(value.token_expires_at)
    && (value.token_issued_at === null || value.token_expires_at === null
      || Date.parse(value.token_expires_at) > Date.parse(value.token_issued_at))
    && value.management_url === ENTRA_MANAGEMENT_URL
}

export class AccessApiClient {
  constructor(private readonly getToken: TokenProvider) {}

  async current(signal?: AbortSignal): Promise<EffectiveAccess> {
    const result = await requestJson<unknown>('/access', { signal }, this.getToken)
    if (!isEffectiveAccess(result)) {
      throw new ApiError(200, 'invalid_access_response',
        'The API returned incomplete or inconsistent access details. Effective permissions are unavailable; no access has been inferred.')
    }
    return result
  }
}
