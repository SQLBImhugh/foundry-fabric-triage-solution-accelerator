import { describe, expect, it, vi } from 'vitest'
import { MonitoringApiClient, monitoringTargetKey, readMonitoringPages } from './monitoring'
import type { ScopeDefinition, ScopePreviewRequest } from './monitoring'
import { ApiError } from './errors'
import {
  ids, monitoringBootstrap, monitoringConnector, monitoringDomains, monitoringInventory, monitoringPage,
  monitoringPreview, monitoringReceipt, monitoringScope, monitoringSnapshot, monitoringTarget,
  monitoringVersion, monitoringWorkspaces,
  monitoringWrongTenantBootstrap,
} from '../test/monitoringFixtures'

const scope: ScopeDefinition = {
  tenant_id: ids.tenant, epoch: ids.epoch, scope_id: ids.scope, name: monitoringScope.name,
  enabled: true, rules: monitoringScope.rules, cadence: monitoringScope.cadence,
}
const previewRequest: ScopePreviewRequest = { expected: monitoringVersion, idempotency_id: ids.submission, scope }
const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
const api = () => new MonitoringApiClient(async () => null)

describe('monitoring wire contracts', () => {
  it('accepts a native scope with no projected update timestamp without inferring one', async () => {
    const nativeScope = { ...monitoringScope, updated_at: null }
    vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringPage([nativeScope])))
    const result = await api().scopes()
    expect(result.items).toEqual([nativeScope])
    expect(result.items[0]?.revision).toBe(monitoringScope.revision)
  })

  it.each([undefined, '', 'not-a-timestamp', 0, true])('rejects malformed scope update metadata: %j', async (updated_at) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringPage([{ ...monitoringScope, updated_at }])))
    await expect(api().scopes()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('uses current tokens, abort signals and the same-origin no-store transport', async () => {
    const token = vi.fn().mockResolvedValue('unit-test-token')
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringSnapshot))
    const signal = new AbortController().signal
    expect(await new MonitoringApiClient(token).snapshot(signal)).toEqual(monitoringSnapshot)
    expect(token).toHaveBeenCalledOnce()
    expect(fetch.mock.calls[0]![0]).toBe('/api/monitoring/snapshot')
    expect(fetch.mock.calls[0]![1]).toMatchObject({ signal, cache: 'no-store', credentials: 'omit' })
    expect(new Headers(fetch.mock.calls[0]![1]?.headers).has('Authorization')).toBe(true)
  })

  it('reads bootstrap and all six record families without flattening target identities', async () => {
    const fetch = vi.mocked(globalThis.fetch)
    fetch.mockResolvedValueOnce(response(monitoringBootstrap))
    const client = api()
    expect(await client.bootstrap()).toEqual(monitoringBootstrap)
    const reads = [
      () => client.scopes(), () => client.inventory(), () => client.targets(),
      () => client.workspaces(), () => client.domains(), () => client.connectors(),
    ]
    const values = [[monitoringScope], monitoringInventory, [monitoringTarget], monitoringWorkspaces, monitoringDomains, [monitoringConnector]]
    for (let index = 0; index < reads.length; index += 1) {
      fetch.mockResolvedValueOnce(response(monitoringPage<unknown>(values[index]!)))
      expect((await reads[index]!()).items).toEqual(values[index])
    }
    expect(fetch.mock.calls.map(([path]) => path)).toEqual([
      '/api/monitoring/bootstrap', '/api/monitoring/scopes', '/api/monitoring/inventory', '/api/monitoring/targets',
      '/api/monitoring/workspaces', '/api/monitoring/domains', '/api/monitoring/connectors',
    ])
    expect(monitoringTargetKey(monitoringTarget.identity)).toContain(`${ids.workspace}:${ids.model}`)
  })

  it('encodes pagination and supported workload filters and retains inactive-target selection', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringPage([monitoringTarget])))
    await api().targets({ workload: 'powerbi', workspace_id: ids.workspace, limit: 25, cursor: 'page+2/&', include_inactive: true })
    const path = new URL(String(fetch.mock.calls[0]![0]), 'https://example.test')
    expect(Object.fromEntries(path.searchParams)).toEqual({
      workload: 'powerbi', workspace_id: ids.workspace, limit: '25', cursor: 'page+2/&', include_inactive: 'true',
    })
  })

  it.each([
    { can_admin: undefined }, { can_admin: false }, { user: undefined }, { user: { id: 'u', display_name: 'User', roles: ['owner'] } },
    { control: { ...monitoringSnapshot.control, schema_version: 2 } },
    { coverage: { ...monitoringSnapshot.coverage, revision: 4 } },
    { coverage: { ...monitoringSnapshot.coverage, epoch: ids.tenant } },
    { coverage: { ...monitoringSnapshot.coverage, current_count: undefined } },
    { coverage: { ...monitoringSnapshot.coverage, action_enabled_count: 5 } },
    { coverage: { ...monitoringSnapshot.coverage, current_count: -1 } },
    { coverage: { ...monitoringSnapshot.coverage, scope_item_count: 1 } },
    { coverage: { ...monitoringSnapshot.coverage, inventory_completeness: 'complete', scope_item_count: null } },
    { coverage: { ...monitoringSnapshot.coverage, gaps: [] } },
  ])('rejects missing or inconsistent coverage and permission fields: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...monitoringSnapshot, ...updates }))
    await expect(api().snapshot()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each([
    { status: 'ready', control: null },
    { status: 'ready', found_schema_version: undefined },
    { status: 'maintenance' },
    { expected_tenant_id: ids.epoch },
    { detail: undefined },
  ])('does not reinterpret invalid bootstrap evidence as ready: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...monitoringBootstrap, ...updates }))
    await expect(api().bootstrap()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each(['missing', 'incompatible', 'maintenance', 'wrong_tenant'] as const)('preserves explicit deployment status %s', async (status) => {
    const value = {
      ...monitoringBootstrap, status,
      control: status === 'missing' || status === 'incompatible' || status === 'wrong_tenant'
        ? null : { ...monitoringSnapshot.control, maintenance: status === 'maintenance' },
      expected_tenant_id: ids.tenant,
      found_schema_version: status === 'missing' || status === 'wrong_tenant' ? null : status === 'incompatible' ? 2 : 1,
    }
    vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    expect(await api().bootstrap()).toEqual(value)
  })

  it('accepts sanitized wrong-tenant diagnostics without requesting or retaining foreign control metadata', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringWrongTenantBootstrap))
    expect(await api().bootstrap()).toEqual(monitoringWrongTenantBootstrap)
    expect(fetch).toHaveBeenCalledOnce()
    fetch.mockResolvedValue(response({
      ...monitoringWrongTenantBootstrap, found_schema_version: 1,
      control: { ...monitoringSnapshot.control, tenant_id: ids.epoch },
    }))
    await expect(api().bootstrap()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each([
    { items: undefined }, { next_cursor: undefined }, { as_of: undefined },
    { version: { ...monitoringVersion, revision: -1 } },
    { items: [{ ...monitoringInventory[0], unsupported_reason: 'Conflict' }] },
    { items: [{ ...monitoringInventory[2], unsupported_reason: null }] },
    { items: [{ ...monitoringInventory[0], tenant_id: ids.epoch }] },
  ])('does not hide invalid inventory fields or a different tenant behind empty defaults: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...monitoringPage(monitoringInventory), ...updates }))
    await expect(api().inventory()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each([
    { identity: undefined, workspace_id: ids.workspace, item_id: ids.model },
    { state: 'paused' }, { admission_basis: 'pending_review' },
    { observation: undefined },
    { action: { enabled: true, action: 'powerbi_refresh' } },
    { action: { ...monitoringTarget.action, action: 'pipeline_rerun' } },
  ])('rejects flattened or inconsistent action/observation target records: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringPage([{ ...monitoringTarget, ...updates }])))
    await expect(api().targets()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each([
    { state: 'ready' }, { state: 'blocked', gaps: [] }, { desired_definition: undefined }, { sources: undefined },
    { endpoint: { namespace: 'https://example.test/path', entity: 'events', consumer_group: 'worker' } },
  ])('does not infer ready connector delivery from configuration: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringPage([{ ...monitoringConnector, ...updates }])))
    await expect(api().connectors()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('accepts a ready connector only with matching topology and identity/delivery proof', async () => {
    const connector = {
      ...monitoringConnector, state: 'ready', observed_definition: {
        destinations: monitoringConnector.desired_definition.destinations, sources: monitoringConnector.desired_definition.sources,
      },
      endpoint: { namespace: 'events.example.test', entity: 'stream', consumer_group: 'monitoring' },
      identity_verified_at: '2026-09-15T12:00:00Z', delivery_verified_at: '2026-09-15T12:01:00Z',
    }
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(response(monitoringPage([connector])))
    expect((await api().connectors()).items).toEqual([connector])
    fetch.mockResolvedValueOnce(response(monitoringPage([{ ...connector, observed_definition: { changed: true } }])))
    await expect(api().connectors()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('accepts explicitly unassigned planned topology identities without claiming ready or hiding omitted fields', async () => {
    const connector = { ...monitoringConnector, state: 'planned', workspace_id: null, eventstream_id: null, destination_id: null }
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(response(monitoringPage([connector])))
    expect((await api().connectors()).items[0]!.workspace_id).toBeNull()
    fetch.mockResolvedValueOnce(response(monitoringPage([{ ...connector, state: 'ready' }])))
    await expect(api().connectors()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
    fetch.mockResolvedValueOnce(response(monitoringPage([{ ...connector, workspace_id: undefined }])))
    await expect(api().connectors()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('queues inventory discovery and never labels an asynchronous acknowledgement complete', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response({ work_id: ids.work, status: 'queued' }))
    const input = { expected: monitoringVersion, idempotency_id: ids.submission, selector: monitoringScope.rules[0]!.selector }
    expect(await api().refreshInventory(input)).toEqual({ work_id: ids.work, status: 'queued' })
    expect(fetch.mock.calls[0]![0]).toBe('/api/monitoring/inventory/refresh')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual(input)
    fetch.mockResolvedValue(response({ work_id: ids.work, status: 'completed' }))
    await expect(api().refreshInventory(input)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('posts and validates the exact scope, preview, expected revision and activation ID', async () => {
    const plan = monitoringPreview(previewRequest)
    const request = { expected: monitoringVersion, idempotency_id: ids.submission }
    const receipt = monitoringReceipt(plan.plan_id, request, scope)
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(response(plan)).mockResolvedValueOnce(response(receipt))
    const client = api()
    expect(await client.preview(previewRequest)).toEqual(plan)
    expect(await client.activate(plan.plan_id, request)).toEqual(receipt)
    expect(fetch.mock.calls[0]![0]).toBe('/api/monitoring/scopes/preview')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual(previewRequest)
    expect(fetch.mock.calls[1]![0]).toBe(`/api/monitoring/plans/${plan.plan_id}/activate`)
    expect(JSON.parse(String(fetch.mock.calls[1]![1]?.body))).toEqual(request)
    expect(JSON.parse(String(fetch.mock.calls[1]![1]?.body))).not.toHaveProperty('scope')
  })

  it.each([
    { idempotency_id: ids.epoch },
    { expected: { ...monitoringVersion, revision: 4 } },
    { scope: { ...scope, enabled: false } },
    { changes: [{ identity: monitoringTarget.identity, change: 'remove', reason: 'Uncertain', basis: 'uncertain_inventory' }] },
    { gaps: [] }, { expires_at: '2026-09-15T11:00:00Z' },
  ])('refuses a preview for changed intent or inconsistent evidence: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...monitoringPreview(previewRequest), ...updates }))
    await expect(api().preview(previewRequest)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('reads plans and reconciles activation receipts without posting another activation', async () => {
    const plan = monitoringPreview(previewRequest)
    const receipt = monitoringReceipt(plan.plan_id, previewRequest, scope)
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(response(plan)).mockResolvedValueOnce(response(receipt))
    expect(await api().plan(ids.plan)).toEqual(plan)
    expect(await api().activation(ids.submission)).toEqual(receipt)
    expect(fetch.mock.calls.map(([path]) => path)).toEqual([
      `/api/monitoring/plans/${ids.plan}`, `/api/monitoring/activations/${ids.submission}`,
    ])
    expect(fetch.mock.calls.every(([, options]) => !options?.method || options.method === 'GET')).toBe(true)
  })

  it('does not accept an activation receipt for another submission or scope revision', async () => {
    const request = { expected: monitoringVersion, idempotency_id: ids.submission }
    const receipt = monitoringReceipt(ids.plan, request, scope)
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(response({ ...receipt, idempotency_id: ids.epoch }))
    await expect(api().activate(ids.plan, request)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
    fetch.mockResolvedValueOnce(response({ ...receipt, scope: { ...receipt.scope, revision: 100 } }))
    await expect(api().activation(ids.submission)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each([401, 403, 404, 409, 429, 503])('surfaces HTTP %s without demo fallback or an automatic POST retry', async (status) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ detail: { code: 'monitoring_unavailable', message: 'Monitoring is unavailable.' } }, status))
    await expect(api().activate(ids.plan, { expected: monitoringVersion, idempotency_id: ids.submission }))
      .rejects.toMatchObject({ status, code: 'monitoring_unavailable' })
    expect(globalThis.fetch).toHaveBeenCalledOnce()
  })

  it('preserves a canceled read and reports ambiguous writes without retrying with a new ID', async () => {
    const aborted = Object.assign(new Error('Canceled'), { name: 'AbortError' })
    const fetch = vi.mocked(globalThis.fetch).mockRejectedValueOnce(aborted).mockRejectedValueOnce(new TypeError('Connection lost'))
    await expect(api().snapshot()).rejects.toBe(aborted)
    await expect(api().activate(ids.plan, { expected: monitoringVersion, idempotency_id: ids.submission }))
      .rejects.toMatchObject({ code: 'network_error', status: 0 })
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it('does not perform unauthenticated reads after token failure or POST invalid scope/cadence', async () => {
    const failure = new ApiError(401, 'sign_in_required', 'Sign in again.')
    await expect(new MonitoringApiClient(async () => { throw failure }).snapshot()).rejects.toBe(failure)
    await expect(api().preview({ ...previewRequest, scope: { ...scope, cadence: { poll_seconds: 14, reconciliation_seconds: 900 } } }))
      .rejects.toMatchObject({ status: 400 })
    await expect(api().preview({ ...previewRequest, scope: { ...scope, tenant_id: ids.epoch } })).rejects.toMatchObject({ status: 400 })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })
})

describe('monitoring collection pagination', () => {
  const signal = () => new AbortController().signal
  it('exhausts continuation pages including an empty intermediate page', async () => {
    const read = vi.fn().mockResolvedValueOnce(monitoringPage([monitoringInventory[0]!], 'second'))
      .mockResolvedValueOnce(monitoringPage([], 'third')).mockResolvedValueOnce(monitoringPage([monitoringInventory[1]!]))
    const items = await readMonitoringPages(read, monitoringVersion, signal(), (item: { item_id: string }) => item.item_id)
    expect(items).toEqual(monitoringInventory.slice(0, 2))
    expect(read.mock.calls).toEqual([[undefined], ['second'], ['third']])
  })
  it('rejects a mixed revision instead of combining pages or enabling stale setup', async () => {
    const read = vi.fn().mockResolvedValueOnce(monitoringPage([monitoringInventory[0]!], 'second'))
      .mockResolvedValueOnce(monitoringPage([], null, { ...monitoringVersion, revision: 4 }))
    await expect(readMonitoringPages(read, monitoringVersion, signal(), (item: { item_id: string }) => item.item_id))
      .rejects.toMatchObject({ status: 409, code: 'monitoring_revision_changed' })
  })
  it('surfaces a repeated cursor or duplicate record rather than silently truncating', async () => {
    const read = vi.fn().mockResolvedValue(monitoringPage([], 'repeat'))
    await expect(readMonitoringPages(read, monitoringVersion, signal(), () => '')).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
    expect(read).toHaveBeenCalledTimes(2)
    const duplicate = vi.fn().mockResolvedValueOnce(monitoringPage([monitoringInventory[0]!], 'second'))
      .mockResolvedValueOnce(monitoringPage([monitoringInventory[0]!]))
    await expect(readMonitoringPages(duplicate, monitoringVersion, signal(), (item: { item_id: string }) => item.item_id))
      .rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })
  it('stops before another page when context cancellation arrives', async () => {
    const controller = new AbortController()
    const read = vi.fn().mockImplementation(async () => { controller.abort(); return monitoringPage([], 'next') })
    await expect(readMonitoringPages(read, monitoringVersion, controller.signal, () => '')).rejects.toMatchObject({ name: 'AbortError' })
    expect(read).toHaveBeenCalledOnce()
  })
})
