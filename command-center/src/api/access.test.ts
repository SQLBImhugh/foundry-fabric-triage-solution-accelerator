import { describe, expect, it, vi } from 'vitest'
import { AccessApiClient, APP_ROLES, ENTRA_MANAGEMENT_URL } from './access'
import type { EffectiveAccess } from './access'
import { ApiError } from './errors'

const access: EffectiveAccess = {
  source: 'entra_app_roles',
  current_user: {
    id: '20000000-0000-0000-0000-000000000002', display_name: 'Casey Example', roles: ['reader', 'operator'],
  },
  role_catalog: APP_ROLES.map((id) => ({ id, label: id, description: `App ${id} permissions.` })),
  tenant_id: '10000000-0000-0000-0000-000000000001',
  application_id: '30000000-0000-0000-0000-000000000003',
  token_issued_at: '2026-09-14T12:00:00.000000+00:00',
  token_expires_at: '2026-09-14T13:00:00+00:00',
  management_url: ENTRA_MANAGEMENT_URL,
}

function response(value: unknown, status = 200) {
  return new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
}

describe('read-only effective access transport', () => {
  it('uses the shared same-origin GET transport, current API token and abort signal', async () => {
    const getToken = vi.fn().mockResolvedValue('test-api-token')
    const fetch = vi.mocked(globalThis.fetch).mockImplementation(async () => response(access))
    const signal = new AbortController().signal
    const api = new AccessApiClient(getToken)
    expect(await api.current(signal)).toEqual(access)
    expect(fetch.mock.calls[0]![0]).toBe('/api/access')
    const options = fetch.mock.calls[0]![1]
    expect(options).toMatchObject({ credentials: 'omit', cache: 'no-store', signal })
    expect(options?.method ?? 'GET').toBe('GET')
    expect(options?.body).toBeUndefined()
    expect(new Headers(options?.headers).get('Authorization')).toBe('Bearer test-api-token')
    await api.current()
    expect(getToken).toHaveBeenCalledTimes(2)
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it('has no permission mutation, audit editor or user roster API', () => {
    expect(Object.getOwnPropertyNames(AccessApiClient.prototype)).toEqual(['constructor', 'current'])
  })

  it.each(['synthetic_demo', 'entra_app_roles'] as const)('accepts unavailable authority metadata without inventing it (%s)', async (source) => {
    const result: EffectiveAccess = {
      ...access, source, current_user: { id: 'test-reader', display_name: 'Test reader', roles: ['reader'] },
      tenant_id: null, application_id: null, token_issued_at: null, token_expires_at: null,
    }
    vi.mocked(globalThis.fetch).mockResolvedValue(response(result))
    expect(await new AccessApiClient(async () => null).current()).toEqual(result)
  })

  it.each([
    { source: 'managed' }, { source: null }, { source: undefined },
    { current_user: null },
    { current_user: { ...access.current_user, id: '' } },
    { current_user: { ...access.current_user, display_name: 42 } },
    { current_user: { ...access.current_user, roles: [] } },
    { current_user: { ...access.current_user, roles: ['owner'] } },
    { current_user: { ...access.current_user, roles: ['reader', 'reader'] } },
    { current_user: { ...access.current_user, roles: ['reader', null] } },
    { current_user: { ...access.current_user, roles: 'admin' } },
    { tenant_id: 'configured-tenant' }, { tenant_id: undefined },
    { tenant_id: '00000000-0000-0000-0000-000000000000' },
    { application_id: 'https://example.test/application' }, { application_id: undefined },
    { role_catalog: [] }, { role_catalog: undefined },
    { role_catalog: [access.role_catalog[0], ...access.role_catalog.slice(0, 3)] },
    { role_catalog: [{ id: 'owner', label: 'Owner', description: 'Unknown role.' }, ...access.role_catalog.slice(1)] },
    { role_catalog: [{ ...access.role_catalog[0], label: '' }, ...access.role_catalog.slice(1)] },
    { role_catalog: [{ ...access.role_catalog[0], description: null }, ...access.role_catalog.slice(1)] },
    { token_issued_at: undefined }, { token_expires_at: undefined },
    { token_issued_at: 123 }, { token_expires_at: 'not a date' },
    { token_issued_at: '2026-09-14' }, { token_issued_at: '2026-09-14T12:00:00' },
    { token_issued_at: '2026-02-30T12:00:00Z' }, { token_issued_at: '2026-09-14T24:00:00Z' },
    { token_expires_at: access.token_issued_at },
    { token_expires_at: '2026-09-14T11:59:59Z' },
  ])('rejects malformed or inconsistent access details: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...access, ...updates }))
    await expect(new AccessApiClient(async () => null).current())
      .rejects.toMatchObject({ code: 'invalid_access_response' })
    expect(globalThis.fetch).toHaveBeenCalledOnce()
  })

  it.each([
    '/admin', 'http://entra.microsoft.com/', 'https://entra.microsoft.com.evil.test/',
    'https://evil.test/?next=https://entra.microsoft.com/', 'javascript:alert(1)',
    'https://entra.microsoft.com/#view/users', 'https://entra.microsoft.com/tenant',
    'https://user@entra.microsoft.com/', null,
  ])('accepts only the agreed Entra portal root, not an arbitrary management link (%s)', async (management_url) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...access, management_url }))
    await expect(new AccessApiClient(async () => null).current())
      .rejects.toMatchObject({ code: 'invalid_access_response' })
  })

  it('compares token dates by instant rather than by offset text', async () => {
    const result = { ...access, token_issued_at: '2026-09-14T12:30:00+02:00', token_expires_at: '2026-09-14T11:30:00Z' }
    vi.mocked(globalThis.fetch).mockResolvedValue(response(result))
    expect(await new AccessApiClient(async () => null).current()).toEqual(result)
  })

  it.each([401, 403, 404, 503])('preserves explicit API errors without an ACL or demo fallback (%s)', async (status) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({
      detail: { code: 'access_unavailable', message: 'Effective access cannot be read.' },
    }, status))
    await expect(new AccessApiClient(async () => null).current()).rejects.toMatchObject({
      status, code: 'access_unavailable', message: 'Effective access cannot be read.',
    })
    expect(globalThis.fetch).toHaveBeenCalledOnce()
  })

  it.each([null, [], 'access'])('does not reinterpret a non-object response (%j)', async (result) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(result))
    await expect(new AccessApiClient(async () => null).current()).rejects.toMatchObject({ code: 'invalid_response' })
  })

  it('does not send an unauthenticated retry after a token failure', async () => {
    const failure = new ApiError(401, 'sign_in_required', 'Sign in again.')
    await expect(new AccessApiClient(async () => { throw failure }).current()).rejects.toBe(failure)
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it('surfaces network failures and preserves cancellation without retrying', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockRejectedValueOnce(new TypeError('Connection lost'))
    const api = new AccessApiClient(async () => null)
    await expect(api.current()).rejects.toMatchObject({ code: 'network_error' })
    const aborted = Object.assign(new Error('Canceled'), { name: 'AbortError' })
    fetch.mockRejectedValueOnce(aborted)
    await expect(api.current()).rejects.toBe(aborted)
    expect(fetch).toHaveBeenCalledTimes(2)
  })
})
