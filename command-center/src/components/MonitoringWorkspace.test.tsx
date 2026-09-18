import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { MONITORING_ACTIVATION_STORAGE_KEY, MonitoringWorkspace } from './MonitoringWorkspace'
import type { MonitoringWorkspaceProps } from './MonitoringWorkspace'
import { ApiError } from '../api/errors'
import type { AppRole } from '../api/access'
import type { MonitoringPlan, OwnedConnectorManifest, RecordPage, ScopePolicy, WorkspaceMetadata } from '../api/monitoring'
import {
  ids, monitoringBootstrap, monitoringConnector, monitoringFixtureApi, monitoringInventory,
  monitoringPage, monitoringPreview, monitoringReceipt, monitoringSnapshot, monitoringVersion, monitoringWorkspaces,
  monitoringWrongTenantBootstrap, monitoringScope, monitoringSafetyReview, monitoringTarget,
} from '../test/monitoringFixtures'

afterEach(() => window.sessionStorage.removeItem(MONITORING_ACTIVATION_STORAGE_KEY))

function setup(options: Partial<MonitoringWorkspaceProps> = {}) {
  const props: MonitoringWorkspaceProps = {
    api: monitoringFixtureApi(), roles: ['reader', 'admin'], userId: 'operator-1', fresh: true,
    permissionRevision: 0, onChanged: vi.fn(), ...options,
  }
  const rendered = render(<MonitoringWorkspace {...props} />)
  return { ...rendered, props, api: props.api, user: userEvent.setup() }
}
async function fillScope(user: ReturnType<typeof userEvent.setup>) {
  await user.type(await screen.findByRole('textbox', { name: 'Scope name' }), 'Daily monitoring')
  await user.selectOptions(screen.getByRole('combobox', { name: 'Include workspace' }), ids.workspace)
}
async function preparePreview(user: ReturnType<typeof userEvent.setup>) {
  await fillScope(user)
  await user.click(screen.getByRole('button', { name: 'Preview scope changes' }))
  await screen.findByRole('region', { name: 'Scope preview' })
}

describe('monitoring inspection and readiness', () => {
  it('keeps the current permission-generation editor usable during a slow background catalogue read', async () => {
    vi.useFakeTimers()
    let finish: ((value: RecordPage<WorkspaceMetadata>) => void) | undefined
    const api = monitoringFixtureApi()
    const { rerender, props, unmount } = setup({ api })
    try {
      await act(async () => {})
      const selector = screen.getByRole('combobox', { name: 'Include workspace' })
      expect(selector.matches(':disabled')).toBe(false)
      vi.mocked(api.workspaces).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
      await act(async () => { await vi.advanceTimersByTimeAsync(15000) })
      expect(api.workspaces).toHaveBeenCalledTimes(2)
      expect(selector.matches(':disabled')).toBe(false)
      fireEvent.change(selector, { target: { value: ids.workspace } })
      expect((selector as HTMLSelectElement).value).toBe(ids.workspace)
      expect(api.activate).not.toHaveBeenCalled()
      rerender(<MonitoringWorkspace {...props} fresh={false} permissionRevision={1} />)
      expect(screen.queryByRole('combobox', { name: 'Include workspace' })?.matches(':disabled') ?? true).toBe(true)
      await act(async () => { finish?.(monitoringPage(monitoringWorkspaces)) })
    } finally {
      unmount()
      vi.useRealTimers()
    }
  })

  it.each(['reader', 'operator', 'approver'] as AppRole[])('lets %s inspect truthful coverage without setup controls', async (role) => {
    const api = monitoringFixtureApi([role])
    setup({ api, roles: [role] })
    const coverage = within(await screen.findByRole('region', { name: 'Monitoring coverage' }))
    await screen.findByRole('region', { name: 'Admitted monitoring targets' })
    for (const [label, value] of [['Discovered', '4'], ['Access verified', '1'], ['Admitted', '2'], ['Current', '1'], ['Action enabled', '0']]) {
      expect(coverage.getByText(label!).parentElement?.querySelector('dd')?.textContent).toBe(value)
    }
    expect(coverage.getByText('Scope denominator: Unknown')).toBeTruthy()
    expect(coverage.getByText(/Known inventory is retained/)).toBeTruthy()
    expect(screen.getByText('Example User')).toBeTruthy()
    expect(screen.queryByRole('textbox', { name: 'Scope name' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Queue inventory refresh' })).toBeNull()
    expect(screen.queryByRole('button', { name: /activate|approve|rerun|grant|edit/i })).toBeNull()
    expect(api.activate).not.toHaveBeenCalled()
    expect(api.refreshInventory).not.toHaveBeenCalled()
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it.each([
    ['Configuring', [monitoringConnector]],
    ['Partial', []],
    ['Blocked', [{ ...monitoringConnector, state: 'blocked', gaps: [{ code: 'identity_blocked', detail: 'Managed-identity consumption has not been proved.' }] }]],
  ] as [string, OwnedConnectorManifest[]][])('distinguishes %s from monitoring readiness', async (label, connectors) => {
    const api = monitoringFixtureApi(['reader'])
    vi.mocked(api.connectors).mockResolvedValue(monitoringPage(connectors))
    setup({ api, roles: ['reader'] })
    await screen.findByRole('region', { name: 'Event connectors and proof' })
    expect(within(screen.getByRole('region', { name: 'Monitoring coverage' })).getByText(label, { selector: '.badge' })).toBeTruthy()
    expect(screen.getByText(/Monitoring does not authorize remediation/)).toBeTruthy()
    if (!connectors.length) expect(screen.getByText('Event layer not configured')).toBeTruthy()
  })

  it('keeps target setup inspectable while a planned connector has no assigned topology IDs', async () => {
    const api = monitoringFixtureApi()
    vi.mocked(api.connectors).mockResolvedValue(monitoringPage([{
      ...monitoringConnector, state: 'planned', workspace_id: null, eventstream_id: null, destination_id: null,
    }]))
    setup({ api })
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByText(/Monitoring workspace identity not assigned/)).toBeTruthy()
    expect(screen.getByText('Configuring / Planned')).toBeTruthy()
    expect(screen.queryByText('Ready', { selector: '.badge' })).toBeNull()
  })

  it.each(['missing', 'incompatible', 'maintenance', 'wrong_tenant'] as const)('stops setup on bootstrap %s without a fallback inventory', async (status) => {
    const api = monitoringFixtureApi()
    vi.mocked(api.bootstrap).mockResolvedValue({
      ...monitoringBootstrap, status, detail: `Deployment ${status}.`,
      control: status === 'maintenance' ? { ...monitoringSnapshot.control, maintenance: true } : null,
      found_schema_version: status === 'maintenance' ? 1 : status === 'incompatible' ? 2 : null,
    })
    setup({ api })
    expect(await screen.findByRole('region', { name: 'Monitoring deployment blocked' })).toBeTruthy()
    expect(screen.getByText(`Deployment ${status}.`)).toBeTruthy()
    expect(screen.queryByRole('region', { name: 'Monitoring coverage' })).toBeNull()
    expect(screen.queryByRole('textbox', { name: 'Scope name' })).toBeNull()
    expect(api.snapshot).not.toHaveBeenCalled()
    expect(api.inventory).not.toHaveBeenCalled()
  })

  it('renders the sanitized wrong-tenant diagnosis without a generic invalid-response error or a control lookup', async () => {
    const api = monitoringFixtureApi(['reader'])
    vi.mocked(api.bootstrap).mockResolvedValue(monitoringWrongTenantBootstrap)
    setup({ api, roles: ['reader'] })
    expect(await screen.findByRole('heading', { name: 'Wrong monitoring tenant' })).toBeTruthy()
    expect(screen.getByText(monitoringWrongTenantBootstrap.detail)).toBeTruthy()
    expect(screen.queryByText('invalid_monitoring_response')).toBeNull()
    expect(api.snapshot).not.toHaveBeenCalled()
    expect(api.scopes).not.toHaveBeenCalled()
  })

  it.each(['reader', 'operator', 'approver'] as AppRole[])('lets a %s open an existing read-only safety review without mutation controls', async (role) => {
    const api = monitoringFixtureApi([role])
    const review = { ...monitoringSafetyReview, state: 'verified' as const, exact_correlation_verified: true }
    vi.mocked(api.targets).mockResolvedValue(monitoringPage([{
      ...monitoringTarget, action: { enabled: true, action: review.action, review_id: review.review_id, review_revision: review.revision },
    }]))
    vi.mocked(api.safetyReview).mockResolvedValue(review)
    const { user } = setup({ api, roles: [role] })
    await user.click(await screen.findByText('Inspect Operations model'))
    await user.click(screen.getByRole('button', { name: 'Safety review for Operations model' }))
    expect(await screen.findByRole('heading', { name: 'Returned review record' })).toBeTruthy()
    expect(vi.mocked(api.safetyReview).mock.calls[0]![0]).toBe(review.review_id)
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Revoke safety review' })).toBeNull()
    expect(screen.queryByRole('combobox', { name: 'Action profile' })).toBeNull()
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })

  it('keeps coverage visible when resource metadata fails rather than showing an empty successful list', async () => {
    const api = monitoringFixtureApi()
    vi.mocked(api.workspaces).mockRejectedValue(new ApiError(503, 'workspace_metadata_unavailable', 'Workspace metadata is unavailable.'))
    setup({ api })
    expect(await screen.findByRole('region', { name: 'Monitoring coverage' })).toBeTruthy()
    expect(await screen.findByText('Workspace metadata is unavailable.')).toBeTruthy()
    expect(screen.queryByRole('region', { name: 'Discovered monitoring inventory' })).toBeNull()
    expect(screen.queryByRole('textbox', { name: 'Scope name' })).toBeNull()
    expect(screen.getByText(/Missing data is not an empty or complete inventory/)).toBeTruthy()
  })

  it.each([false, true])('distinguishes incomplete discovery from an empty complete scope (complete=%s)', async (complete) => {
    const api = monitoringFixtureApi(['reader'])
    vi.mocked(api.snapshot).mockResolvedValue({
      ...monitoringSnapshot, can_admin: false, user: { ...monitoringSnapshot.user, roles: ['reader'] },
      coverage: {
        ...monitoringSnapshot.coverage, inventory_completeness: complete ? 'complete' : 'unknown',
        capability_completeness: complete ? 'complete' : 'unknown', scope_item_count: complete ? 0 : null,
        discovered_count: 0, access_verified_count: 0, admitted_count: 0, current_count: 0, unsupported_count: 0,
        backlog_count: 0, gaps: complete ? [] : monitoringSnapshot.coverage.gaps,
      },
    })
    vi.mocked(api.inventory).mockResolvedValue(monitoringPage([]))
    vi.mocked(api.scopes).mockResolvedValue(monitoringPage([]))
    vi.mocked(api.targets).mockResolvedValue(monitoringPage([]))
    vi.mocked(api.connectors).mockResolvedValue(monitoringPage([]))
    setup({ api, roles: ['reader'] })
    const inventory = within(await screen.findByRole('region', { name: 'Discovered monitoring inventory' }))
    expect(inventory.getByText(complete ? /No returned inventory items/ : /not proof of an empty scope/)).toBeTruthy()
    expect(screen.getByText(`Scope denominator: ${complete ? '0' : 'Unknown'}`)).toBeTruthy()
    expect(screen.getByText(complete ? 'Not configured' : 'Partial', { selector: '.badge' })).toBeTruthy()
  })

  it('shows raw target identities, separate action admission and real connector proof fields', async () => {
    const { container, user } = setup()
    const targets = within(await screen.findByRole('region', { name: 'Admitted monitoring targets' }))
    expect(targets.getByText('Review required', { selector: '.badge' })).toBeTruthy()
    expect(targets.getAllByText('Detection only / actions disabled')).toHaveLength(2)
    await user.click(targets.getByText('Inspect Operations model'))
    const detail = within(targets.getByText('Inspect Operations model').closest('details')!)
    expect(detail.getByText(ids.model)).toBeTruthy()
    expect(detail.getByText(ids.capability)).toBeTruthy()
    expect(detail.getByText('No review reference reported')).toBeTruthy()
    const connector = within(screen.getByRole('region', { name: 'Pipeline event connector' }))
    expect(connector.getByText('Configuring / Provisioning')).toBeTruthy()
    expect(connector.getByText('Not observed')).toBeTruthy()
    expect(connector.getByText('Delivery proof recorded').parentElement?.querySelector('dd')?.textContent).toBe('Not reported')
    expect(container.querySelector('a[href], img')).toBeNull()
  })

  it('renders record content as text, not raw HTML, images or links', async () => {
    const api = monitoringFixtureApi(['reader'])
    const malicious = '<img src=x onerror=alert(1)> [open](javascript:alert(1))'
    vi.mocked(api.inventory).mockResolvedValue(monitoringPage([{ ...monitoringInventory[0]!, name: malicious }]))
    const { container } = setup({ api, roles: ['reader'] })
    const inventory = within(await screen.findByRole('region', { name: 'Discovered monitoring inventory' }))
    expect(inventory.getByText(malicious)).toBeTruthy()
    expect(container.querySelector('img, a[href], script')).toBeNull()
  })
})

describe('monitoring scope editor', () => {
  it.each(['unknown', 'missing'] as const)('can pause unchanged saved rules and exclusions when workspace metadata is %s', async (state) => {
    const api = monitoringFixtureApi()
    const saved: ScopePolicy = { ...monitoringScope, rules: [...monitoringScope.rules, {
      ...monitoringScope.rules[0]!, rule_id: ids.otherModel, effect: 'exclude',
      selector: { ...monitoringScope.rules[0]!.selector, workspace_id: ids.otherWorkspace },
    }] }
    vi.mocked(api.scopes).mockResolvedValue(monitoringPage([saved]))
    vi.mocked(api.workspaces).mockResolvedValue(monitoringPage(state === 'missing' ? []
      : monitoringWorkspaces.map((workspace) => ({ ...workspace, state: 'unknown' }))))
    vi.mocked(api.preview).mockImplementation(async (input) => ({
      ...monitoringPreview(input), changes: [{ identity: monitoringTarget.identity, change: 'pause', reason: 'The existing policy is disabled.', basis: 'explicit_policy' }],
    }))
    const { user } = setup({ api })
    await user.click(await screen.findByRole('button', { name: 'Edit Operations scope' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    await user.click(screen.getByRole('checkbox', { name: 'Scope enabled' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(false)
    await user.click(screen.getByRole('button', { name: 'Preview scope changes' }))
    await screen.findByRole('region', { name: 'Scope preview' })
    const input = vi.mocked(api.preview).mock.calls[0]![0]
    expect(input.expected).toEqual(monitoringVersion)
    expect(input.scope).toMatchObject({ scope_id: saved.scope_id, enabled: false, rules: saved.rules })
    expect(api.activate).not.toHaveBeenCalled()
  })

  it('does not use the pause exemption for new scopes, rule changes, exclusions or re-enabling unknown resources', async () => {
    const api = monitoringFixtureApi()
    vi.mocked(api.workspaces).mockResolvedValue(monitoringPage(
      monitoringWorkspaces.map((workspace) => ({ ...workspace, state: 'unknown' }))))
    const { user } = setup({ api })
    await user.type(await screen.findByRole('textbox', { name: 'Scope name' }), 'New paused scope')
    await user.click(screen.getByRole('checkbox', { name: 'Scope enabled' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    await user.click(screen.getByRole('button', { name: 'Edit Operations scope' }))
    await user.click(screen.getByRole('checkbox', { name: 'Scope enabled' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(false)
    await user.click(screen.getByRole('checkbox', { name: 'Automatically enroll future items for detection only' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    await user.click(screen.getByRole('checkbox', { name: 'Automatically enroll future items for detection only' }))
    await user.click(screen.getByRole('button', { name: 'Add exclusion' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    await user.click(screen.getByRole('button', { name: 'Remove exclusion 1' }))
    await user.click(screen.getByRole('checkbox', { name: 'Scope enabled' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    expect(api.preview).not.toHaveBeenCalled()
  })

  it('keeps an existing-policy pause locked through a permission refresh even when saved rules are valid', async () => {
    const api = monitoringFixtureApi()
    vi.mocked(api.workspaces).mockResolvedValue(monitoringPage([]))
    const { user, props, rerender } = setup({ api })
    await user.click(await screen.findByRole('button', { name: 'Edit Operations scope' }))
    await user.click(screen.getByRole('checkbox', { name: 'Scope enabled' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(false)
    rerender(<MonitoringWorkspace {...props} fresh={false} permissionRevision={1} />)
    await screen.findByRole('button', { name: 'Preview scope changes' })
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    expect(api.preview).not.toHaveBeenCalled()
  })

  it('builds a named domain scope, descendants and a named item exclusion with explicit detection-only enrollment', async () => {
    const { api, user } = setup()
    await user.type(await screen.findByRole('textbox', { name: 'Scope name' }), 'Domain coverage')
    expect(screen.getByRole<HTMLInputElement>('checkbox', { name: 'Automatically enroll future items for detection only' }).checked).toBe(false)
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include scope' }), 'domain')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include domain' }), ids.domain)
    await user.click(screen.getByRole('checkbox', { name: 'Include descendant domains for include' }))
    await user.click(screen.getByRole('checkbox', { name: 'Automatically enroll future items for detection only' }))
    await user.click(screen.getByRole('button', { name: 'Add exclusion' }))
    await user.selectOptions(screen.getByRole('combobox', { name: 'Exclusion 1 scope' }), 'item')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Exclusion 1 workspace' }), ids.otherWorkspace)
    await user.selectOptions(screen.getByRole('combobox', { name: 'Exclusion 1 item' }), ids.otherModel)
    await user.click(screen.getByRole('button', { name: 'Preview scope changes' }))
    const preview = within(await screen.findByRole('region', { name: 'Scope preview' }))
    expect(preview.getByText('Collector workspace access')).toBeTruthy()
    expect(preview.getByText('Semantic-model Write for refresh history')).toBeTruthy()
    expect(preview.getByText('Future inventory pages still require reconciliation.')).toBeTruthy()
    expect(vi.mocked(api.preview).mock.calls[0]![0]).toMatchObject({
      expected: monitoringVersion,
      scope: {
        name: 'Domain coverage', tenant_id: ids.tenant, epoch: ids.epoch,
        rules: [
          { effect: 'include', selector: { kind: 'domain', domain_id: ids.domain, workspace_id: null, item_id: null, include_descendants: true }, auto_enrol_detection_only: true },
          { effect: 'exclude', selector: { kind: 'item', workspace_id: ids.otherWorkspace, item_id: ids.otherModel, include_descendants: false }, auto_enrol_detection_only: false },
        ], cadence: { poll_seconds: 300, reconciliation_seconds: 900 },
      },
    })
    expect(api.activate).not.toHaveBeenCalled()
  })

  it('clears dependent item/domain selections and keeps unsupported items visible but unselectable', async () => {
    const { user } = setup()
    await fillScope(user)
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include scope' }), 'item')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include workspace' }), ids.workspace)
    const items = screen.getByRole<HTMLSelectElement>('combobox', { name: 'Include item' })
    expect(within(items).getByRole<HTMLOptionElement>('option', { name: /Notebook analysis.*Unsupported/ }).disabled).toBe(true)
    await user.selectOptions(items, ids.model)
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include workspace' }), ids.otherWorkspace)
    expect(items.value).toBe('')
    expect(within(items).queryByRole('option', { name: /Scheduled ingestion/ })).toBeNull()
    await user.selectOptions(items, ids.otherModel)
    await user.click(within(screen.getByRole('group', { name: 'Include workloads' })).getByRole('checkbox', { name: 'Power BI semantic models' }))
    expect(items.value).toBe('')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include scope' }), 'domain')
    expect(screen.queryByRole('combobox', { name: 'Include item' })).toBeNull()
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include domain' }), ids.childDomain)
    await user.click(screen.getByRole('checkbox', { name: 'Include descendant domains for include' }))
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include scope' }), 'tenant')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Include scope' }), 'domain')
    expect(screen.getByRole<HTMLSelectElement>('combobox', { name: 'Include domain' }).value).toBe('')
    expect(screen.getByRole<HTMLInputElement>('checkbox', { name: 'Include descendant domains for include' }).checked).toBe(false)
    await user.selectOptions(screen.getByRole('combobox', { name: 'Inventory workload' }), 'fabric_pipeline')
    const inventory = within(screen.getByRole('region', { name: 'Discovered monitoring inventory' }))
    expect(inventory.getByText('Notebook analysis')).toBeTruthy()
    expect(inventory.getByText('Standalone notebooks have no monitoring detector contract.')).toBeTruthy()
    expect(inventory.queryByText('Operations model')).toBeNull()
  })

  it('loads every metadata page so later named workspaces remain selectable', async () => {
    const api = monitoringFixtureApi()
    vi.mocked(api.workspaces).mockImplementation(async (options) => options?.cursor
      ? monitoringPage([monitoringWorkspaces[1]!]) : monitoringPage([monitoringWorkspaces[0]!], 'second-page'))
    setup({ api })
    const workspaces = await screen.findByRole('combobox', { name: 'Include workspace' })
    expect(within(workspaces).getByRole('option', { name: /Planning workspace/ })).toBeTruthy()
    expect(vi.mocked(api.workspaces).mock.calls[1]![0]?.cursor).toBe('second-page')
  })

  it('preserves scope and rule identities when editing and pausing a saved scope', async () => {
    const { api, user } = setup()
    await user.click(await screen.findByRole('button', { name: 'Edit Operations scope' }))
    expect(screen.getByRole<HTMLInputElement>('textbox', { name: 'Scope name' }).value).toBe('Operations scope')
    await user.click(screen.getByRole('checkbox', { name: 'Scope enabled' }))
    await user.click(screen.getByRole('button', { name: 'Preview scope changes' }))
    await screen.findByRole('region', { name: 'Scope preview' })
    const input = vi.mocked(api.preview).mock.calls[0]![0]
    expect(input.scope.scope_id).toBe(ids.scope)
    expect(input.scope.rules[0]!.rule_id).toBe(ids.rule)
    expect(input.scope.enabled).toBe(false)
    expect(input.scope).not.toHaveProperty('revision')
    expect(input.scope).not.toHaveProperty('updated_at')
  })

  it('requires supported workloads and valid whole-second cadence before preview', async () => {
    const { api, user } = setup()
    await fillScope(user)
    const interval = screen.getByRole('spinbutton', { name: 'Polling interval (seconds)' })
    await user.clear(interval)
    await user.type(interval, '14')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    await user.clear(interval)
    await user.type(interval, '60.5')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    await user.clear(interval)
    await user.type(interval, '60')
    const group = within(screen.getByRole('group', { name: 'Include workloads' }))
    await user.click(group.getByRole('checkbox', { name: 'Power BI semantic models' }))
    await user.click(group.getByRole('checkbox', { name: 'Scheduled Fabric pipelines' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    expect(api.preview).not.toHaveBeenCalled()
  })

  it('invalidates preview immediately on an edit and ignores canceled late preview responses', async () => {
    const { api, user } = setup()
    await preparePreview(user)
    await user.type(screen.getByRole('textbox', { name: 'Scope name' }), ' revised')
    expect(screen.queryByRole('button', { name: 'Activate current preview' })).toBeNull()
    let finish!: (value: MonitoringPlan) => void
    vi.mocked(api.preview).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    await user.click(screen.getByRole('button', { name: 'Preview scope changes' }))
    const [input, signal] = vi.mocked(api.preview).mock.calls[1]!
    await user.type(screen.getByRole('textbox', { name: 'Scope name' }), ' again')
    expect(signal?.aborted).toBe(true)
    await act(async () => finish(monitoringPreview(input)))
    expect(screen.queryByRole('region', { name: 'Scope preview' })).toBeNull()
    expect(api.activate).not.toHaveBeenCalled()
  })

  it('aborts old inventory reads and clears a preview when the permission generation changes', async () => {
    const { api, user, props, rerender } = setup()
    await preparePreview(user)
    const oldSignal = vi.mocked(api.inventory).mock.calls[0]![1]
    rerender(<MonitoringWorkspace {...props} fresh={false} permissionRevision={1} />)
    await waitFor(() => expect(oldSignal?.aborted).toBe(true))
    expect(screen.queryByRole('button', { name: 'Activate current preview' })).toBeNull()
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    rerender(<MonitoringWorkspace {...props} permissionRevision={1} />)
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.queryByRole('button', { name: 'Activate current preview' })).toBeNull()
    expect(api.activate).not.toHaveBeenCalled()
  })

  it('ignores old-epoch metadata that completes after a canceled read', async () => {
    const api = monitoringFixtureApi()
    let finish!: (value: ReturnType<typeof monitoringPage<InventoryItemForTest>>) => void
    type InventoryItemForTest = typeof monitoringInventory[number]
    vi.mocked(api.inventory).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    const { props, rerender } = setup({ api })
    await waitFor(() => expect(api.inventory).toHaveBeenCalledOnce())
    const oldSignal = vi.mocked(api.inventory).mock.calls[0]![1]
    rerender(<MonitoringWorkspace {...props} permissionRevision={1} fresh={false} />)
    await screen.findByRole('textbox', { name: 'Scope name' })
    await act(async () => finish(monitoringPage([{ ...monitoringInventory[0]!, name: 'Stale response item' }])))
    expect(oldSignal?.aborted).toBe(true)
    expect(screen.queryByText('Stale response item')).toBeNull()
  })

  it.each(['blocked', 'expired'])('will not activate a %s preview', async (state) => {
    const api = monitoringFixtureApi()
    vi.mocked(api.preview).mockImplementation(async (input) => ({
      ...monitoringPreview(input),
      ...(state === 'blocked' ? { status: 'blocked' as const } : { expires_at: '2020-01-01T00:00:00Z' }),
    }))
    const { user } = setup({ api })
    await preparePreview(user)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Activate current preview' }).disabled).toBe(true)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    expect(api.activate).not.toHaveBeenCalled()
  })
})

describe('monitoring activation and discovery submissions', () => {
  it('activates exactly the reviewed plan, reports Configuring and refreshes the parent snapshot', async () => {
    const { api, user, props } = setup()
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Configuring: Daily monitoring')
    expect(api.activate).toHaveBeenCalledOnce()
    const [planId, input] = vi.mocked(api.activate).mock.calls[0]!
    expect(planId).toBe(ids.plan)
    expect(input.expected).toEqual(monitoringVersion)
    expect(input.idempotency_id).toMatch(/^[a-f0-9-]{36}$/)
    expect(props.onChanged).toHaveBeenCalledOnce()
    expect(screen.getByText(/No remediation was approved by this activation/)).toBeTruthy()
    expect(api.activation).not.toHaveBeenCalled()
    expect(screen.queryByRole('button', { name: /rerun|grant|approve/ })).toBeNull()
  })

  it('reconciles a lost activation response by the same submission ID without another POST', async () => {
    const { api, user, props } = setup()
    vi.mocked(api.activate).mockRejectedValueOnce(new ApiError(0, 'network_error', 'Activation response was lost.'))
    vi.mocked(api.activation).mockImplementation(async () => {
      const [planId, input] = vi.mocked(api.activate).mock.calls[0]!
      return monitoringReceipt(planId, input, vi.mocked(api.preview).mock.calls[0]![0].scope)
    })
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Configuring: Daily monitoring')
    expect(api.activation).toHaveBeenCalledExactlyOnceWith(vi.mocked(api.activate).mock.calls[0]![1].idempotency_id)
    expect(api.activate).toHaveBeenCalledOnce()
    expect(props.onChanged).toHaveBeenCalledOnce()
  })

  it('retains an ambiguous submission across permission refresh and only retries its original ID', async () => {
    const { api, user, props, rerender } = setup()
    vi.mocked(api.activate).mockRejectedValueOnce(new ApiError(0, 'network_error', 'Activation response was lost.'))
    vi.mocked(api.activation).mockRejectedValue(new ApiError(404, 'activation_not_found', 'No receipt yet.'))
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Activation outcome is unconfirmed')
    const original = vi.mocked(api.activate).mock.calls[0]!
    expect(screen.getByText(original[1].idempotency_id)).toBeTruthy()
    rerender(<MonitoringWorkspace {...props} permissionRevision={1} fresh={false} />)
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Retry same activation' }).disabled).toBe(true)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    expect(screen.getByText(original[1].idempotency_id)).toBeTruthy()
    rerender(<MonitoringWorkspace {...props} permissionRevision={1} />)
    await waitFor(() => expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Retry same activation' }).disabled).toBe(false))
    await user.click(screen.getByRole('button', { name: 'Retry same activation' }))
    await screen.findByText('Configuring: Daily monitoring')
    expect(vi.mocked(api.activate).mock.calls).toEqual([original, original])
  })

  it('preserves the submission pointer across a remount and reloads the original server preview before retry', async () => {
    const { api, user, unmount } = setup()
    vi.mocked(api.activate).mockRejectedValueOnce(new ApiError(0, 'network_error', 'Activation response was lost.'))
    vi.mocked(api.activation).mockRejectedValue(new ApiError(404, 'activation_not_found', 'No receipt yet.'))
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Activation outcome is unconfirmed')
    const original = vi.mocked(api.activate).mock.calls[0]!
    const plan = monitoringPreview(vi.mocked(api.preview).mock.calls[0]![0])
    vi.spyOn(api, 'plan').mockResolvedValue(plan)
    expect(window.sessionStorage.getItem(MONITORING_ACTIVATION_STORAGE_KEY)).toContain(original[1].idempotency_id)
    unmount()
    setup({ api })
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByText(original[1].idempotency_id)).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Retry same activation' })).toBeNull()
    expect(api.activate).toHaveBeenCalledOnce()
    await user.click(screen.getByRole('button', { name: 'Load original preview' }))
    await screen.findByRole('region', { name: 'Scope preview' })
    expect(api.plan).toHaveBeenCalledExactlyOnceWith(original[0], expect.any(AbortSignal))
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Configuring: Daily monitoring')
    expect(vi.mocked(api.activate).mock.calls).toEqual([original, original])
    expect(window.sessionStorage.getItem(MONITORING_ACTIVATION_STORAGE_KEY)).toBeNull()
  })

  it('does not submit when the browser cannot preserve an activation pointer', async () => {
    const { api, user } = setup()
    await preparePreview(user)
    const storage = vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() => { throw new Error('Storage unavailable') })
    try {
      await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
      await screen.findByText(/could not preserve the activation submission ID/)
      expect(api.activate).not.toHaveBeenCalled()
      expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    } finally {
      storage.mockRestore()
    }
  })

  it('does not silently discard a malformed recovery pointer and send a new activation', async () => {
    window.sessionStorage.setItem(MONITORING_ACTIVATION_STORAGE_KEY, '{"planId":"incomplete"}')
    const { api } = setup()
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByText(/could not read the unresolved activation pointer/)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    expect(api.activate).not.toHaveBeenCalled()
  })

  it('keeps receipt reconciliation readable after Admin is revoked without exposing a new setup action', async () => {
    const { api, user, props, rerender } = setup()
    vi.mocked(api.activate).mockRejectedValueOnce(new ApiError(503, 'timeout', 'Activation response was not confirmed.'))
    vi.mocked(api.activation).mockRejectedValue(new ApiError(404, 'activation_not_found', 'No receipt yet.'))
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Activation outcome is unconfirmed')
    vi.mocked(api.snapshot).mockResolvedValue({ ...monitoringSnapshot, can_admin: false, user: { ...monitoringSnapshot.user, roles: ['reader'] } })
    rerender(<MonitoringWorkspace {...props} roles={['reader']} permissionRevision={1} />)
    await screen.findByRole('region', { name: 'Admitted monitoring targets' })
    expect(screen.queryByRole('textbox', { name: 'Scope name' })).toBeNull()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Retry same activation' }).disabled).toBe(true)
    await user.click(screen.getByRole('button', { name: 'Check activation receipt' }))
    await screen.findByText('Activation outcome is unconfirmed')
    expect(api.activation).toHaveBeenCalledTimes(2)
    expect(api.activate).toHaveBeenCalledOnce()
  })

  it('clears a rejected revision preview after confirming no activation receipt exists', async () => {
    const { api, user, props } = setup()
    vi.mocked(api.activate).mockRejectedValueOnce(new ApiError(409, 'monitoring_revision_conflict', 'A different administrator changed this configuration.'))
    vi.mocked(api.activation).mockRejectedValueOnce(new ApiError(404, 'activation_not_found', 'No receipt exists.'))
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    expect(await screen.findByText('A different administrator changed this configuration.')).toBeTruthy()
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.queryByRole('button', { name: 'Activate current preview' })).toBeNull()
    expect(screen.queryByRole('region', { name: 'Activation submission' })).toBeNull()
    expect(api.activate).toHaveBeenCalledOnce()
    expect(props.onChanged).not.toHaveBeenCalled()
  })

  it('queues discovery for a named workspace without treating the acknowledgement as completed inventory', async () => {
    const { api, user } = setup()
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Inventory workspace' }), ids.otherWorkspace)
    await user.click(screen.getByRole('button', { name: 'Queue inventory refresh' }))
    await screen.findByText(/Inventory discovery queued/)
    expect(vi.mocked(api.refreshInventory).mock.calls[0]![0]).toMatchObject({
      expected: monitoringVersion, selector: { tenant_id: ids.tenant, kind: 'workspace', workspace_id: ids.otherWorkspace, include_descendants: false },
    })
    expect(screen.getByText(/This is not a completed scan or verified coverage/)).toBeTruthy()
    expect(api.activate).not.toHaveBeenCalled()
  })

  it('retries uncertain discovery with the original ID and selector after a filter edit', async () => {
    const { api, user } = setup()
    vi.mocked(api.refreshInventory).mockRejectedValueOnce(new ApiError(0, 'network_error', 'Discovery acknowledgement was lost.'))
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Inventory workspace' }), ids.workspace)
    await user.click(screen.getByRole('button', { name: 'Queue inventory refresh' }))
    await screen.findByRole('button', { name: 'Retry same inventory request' })
    const original = vi.mocked(api.refreshInventory).mock.calls[0]![0]
    await user.selectOptions(screen.getByRole('combobox', { name: 'Inventory workspace' }), ids.otherWorkspace)
    await user.click(screen.getByRole('button', { name: 'Retry same inventory request' }))
    await screen.findByText(/Inventory discovery queued/)
    expect(vi.mocked(api.refreshInventory).mock.calls.map(([input]) => input)).toEqual([original, original])
  })
})
