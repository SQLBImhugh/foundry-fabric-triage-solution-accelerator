import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { App } from './App'
import { ApiClient } from './api/client'
import { AccessApiClient, APP_ROLES, ENTRA_MANAGEMENT_URL } from './api/access'
import type { AppRole, EffectiveAccess } from './api/access'
import { IncidentApiClient } from './api/incidents'
import type { IncidentCase } from './api/incidents'
import { ApiError } from './api/errors'
import type { AuthSession } from './api/auth'
import type { Snapshot, WorkItem } from './api/types'
import { config, detail, proposal, snapshot, workItem } from './test/fixtures'

const auth: AuthSession = { enabled: false, signedIn: true, getToken: async () => null, signIn: async () => undefined, signOut: async () => undefined }
afterEach(() => window.history.replaceState({}, '', '/'))

function incidentCase(item: WorkItem): IncidentCase {
  return {
    detail: { ...detail, item, proposal: null, incident: { id: item.source_id }, runs: [], timeline: [] },
    tracking: { status: item.status === 'resolved' ? 'resolved' : 'open', version: 0, source_revision: '1'.repeat(64), resolved_at: null, resolved_by: null, resolution_note: null },
    activity: [], capabilities: { note: false, resolve: false, ask: true },
  }
}

describe('attention and workload filters', () => {
  function setupQueue(items?: WorkItem[]) {
    const incident = (id: string, title: string, status: string): WorkItem => ({
      ...workItem, id: `incident:${id}`, source_id: id, kind: 'incident',
      title, status, incident_id: id, expires_at: null, can_decide: false,
    })
    const records = items ?? [
      incident('review-1', 'Missing dataset identifiers', 'needs_review'),
      incident('review-2', 'Repeated refresh failure', 'needs_review'),
      incident('resolved-1', 'Resolved refresh', 'resolved'),
    ]
    const api = new ApiClient(async () => null)
    vi.spyOn(api, 'snapshot').mockResolvedValue({
      ...snapshot, work_items: records,
      counts: { pending_approvals: 0, needs_investigation: 2, running: 0, verification_pending: 0, resolved: 1 },
    })
    vi.spyOn(api, 'detail').mockImplementation(async (selection) => {
      const item = records.find((record) => record.source_id === selection.source_id)
      if (!item) throw new Error('Unexpected detail request')
      return { ...detail, item, proposal: null }
    })
    render(<App api={api} config={config} auth={auth} />)
    return records
  }

  it('shows both counted needs-review incidents when the investigation tile is selected', async () => {
    const user = userEvent.setup()
    setupQueue()
    await user.click(await screen.findByRole('button', { name: '02 Needs investigation' }))
    const queue = within(screen.getByRole('region', { name: 'Work queue' }))
    expect(queue.getByRole('button', { name: /Missing dataset identifiers/ })).toBeTruthy()
    expect(queue.getByRole('button', { name: /Repeated refresh failure/ })).toBeTruthy()
    expect(queue.queryByRole('button', { name: /Resolved refresh/ })).toBeNull()
    expect(queue.getAllByText('Needs investigation', { selector: '.badge' })).toHaveLength(2)
    expect(queue.queryByText('Needs review')).toBeNull()
    expect(queue.getByRole<HTMLSelectElement>('combobox', { name: 'Filter work status' }).value).toBe('investigation')
  })

  it('describes monitoring configuration as System overview and the signed-in person as User', async () => {
    const user = userEvent.setup()
    setupQueue()
    await screen.findByRole('region', { name: 'Work queue' })
    expect(screen.getByRole('img', { name: 'BI triage' }).getAttribute('src')).toBe('/triage-logo.png')
    await user.click(screen.getByRole('button', { name: 'System overview' }))
    const dialog = within(screen.getByRole('dialog', { name: 'System overview' }))
    expect(dialog.getByRole('heading', { name: 'User' })).toBeTruthy()
    expect(dialog.getByRole('heading', { name: 'Monitored resources' })).toBeTruthy()
    expect(dialog.queryByText('Current actor')).toBeNull()
    expect(screen.queryByText('Target context')).toBeNull()
  })

  it('clears search and workload constraints when selecting a global attention count', async () => {
    const user = userEvent.setup()
    setupQueue()
    await screen.findByRole('region', { name: 'Work queue' })
    await user.type(screen.getByRole('searchbox', { name: 'Search work queue' }), 'does not match')
    await user.selectOptions(screen.getByRole('combobox', { name: 'Filter workload' }), 'powerbi')
    await user.click(screen.getByRole('button', { name: '02 Needs investigation' }))
    expect(screen.getByRole<HTMLInputElement>('searchbox', { name: 'Search work queue' }).value).toBe('')
    expect(screen.getByRole<HTMLSelectElement>('combobox', { name: 'Filter workload' }).value).toBe('all')
    const queue = within(screen.getByRole('region', { name: 'Work queue' }))
    expect(queue.getAllByRole('listitem')).toHaveLength(2)
    await user.click(screen.getByRole('button', { name: '02 Needs investigation' }))
    expect(queue.getAllByRole('listitem')).toHaveLength(3)
  })

  it('uses the same investigation group when selected through the status dropdown', async () => {
    const user = userEvent.setup()
    setupQueue()
    await user.selectOptions(await screen.findByRole('combobox', { name: 'Filter work status' }), 'investigation')
    const queue = within(screen.getByRole('region', { name: 'Work queue' }))
    expect(queue.getAllByRole('listitem')).toHaveLength(2)
    expect(queue.getAllByText('Needs investigation', { selector: '.badge' })).toHaveLength(2)
    expect(screen.getByRole('button', { name: '02 Needs investigation' }).getAttribute('aria-pressed')).toBe('true')
  })

  it.each(['search', 'workload'])('reapplies the selected attention group after adding a %s constraint', async (constraint) => {
    const user = userEvent.setup()
    setupQueue()
    const attention = await screen.findByRole('button', { name: '02 Needs investigation' })
    await user.click(attention)
    if (constraint === 'search') {
      await user.type(screen.getByRole('searchbox', { name: 'Search work queue' }), 'does not match')
    } else {
      await user.selectOptions(screen.getByRole('combobox', { name: 'Filter workload' }), 'fabric_pipeline')
    }
    expect(attention.getAttribute('aria-pressed')).toBe('false')
    await user.click(attention)
    expect(screen.getByRole<HTMLSelectElement>('combobox', { name: 'Filter work status' }).value).toBe('investigation')
    expect(screen.getByRole<HTMLInputElement>('searchbox', { name: 'Search work queue' }).value).toBe('')
    expect(screen.getByRole<HTMLSelectElement>('combobox', { name: 'Filter workload' }).value).toBe('all')
    expect(within(screen.getByRole('region', { name: 'Work queue' })).getAllByRole('listitem')).toHaveLength(2)
    expect(attention.getAttribute('aria-pressed')).toBe('true')
  })

  it('uses a dedicated incident directory rather than a second copy of the command-center queue', async () => {
    const user = userEvent.setup()
    const records = setupQueue()
    vi.spyOn(IncidentApiClient.prototype, 'list').mockResolvedValue({ items: records, total: records.length, offset: 0, limit: 25 })
    await screen.findByRole('region', { name: 'Work queue' })
    await user.click(screen.getByRole('button', { name: 'Incidents' }))
    expect(await screen.findByRole('region', { name: 'Incident directory' })).toBeTruthy()
    expect(screen.queryByRole('region', { name: 'Work queue' })).toBeNull()
    expect(screen.queryByLabelText('Attention summary')).toBeNull()
    expect(screen.getAllByRole('heading', { name: 'Incidents', level: 1 })).toHaveLength(1)
    await user.click(screen.getByRole('button', { name: 'Command center' }))
    await user.click(screen.getByRole('button', { name: '02 Needs investigation' }))
    expect(screen.queryByRole('region', { name: 'Incident directory' })).toBeNull()
    expect(within(screen.getByRole('region', { name: 'Work queue' })).getAllByRole('listitem')).toHaveLength(2)
  })

  it('opens the inspector link as a full-page incident using the raw source ID', async () => {
    const user = userEvent.setup()
    const records = setupQueue()
    const selected = records[0]!
    const read = vi.spyOn(IncidentApiClient.prototype, 'detail').mockResolvedValue(incidentCase(selected))
    await user.click(await screen.findByRole('button', { name: 'See full incident details' }))
    expect(await screen.findByRole('heading', { name: selected.title, level: 1 })).toBeTruthy()
    expect(read.mock.calls[0]![0]).toBe(selected.source_id)
    expect(new URLSearchParams(window.location.search).get('incident')).toBe(selected.source_id)
    expect(screen.queryByRole('region', { name: 'Work queue' })).toBeNull()
    expect(screen.queryByRole('complementary', { name: 'Selected request inspector' })).toBeNull()
  })

  it('restores a direct incident link and responds to browser history changes', async () => {
    window.history.replaceState({}, '', '/?view=incidents&incident=review-1')
    const records = setupQueue()
    vi.spyOn(IncidentApiClient.prototype, 'detail').mockImplementation(async (id) => {
      const item = records.find((record) => record.source_id === id)
      if (!item) throw new Error('Unexpected incident ID')
      return incidentCase(item)
    })
    expect(await screen.findByRole('heading', { name: records[0]!.title, level: 1 })).toBeTruthy()
    await act(async () => {
      window.history.replaceState({}, '', '/?view=incidents&incident=review-2')
      window.dispatchEvent(new PopStateEvent('popstate'))
    })
    expect(await screen.findByRole('heading', { name: records[1]!.title, level: 1 })).toBeTruthy()
    expect(screen.queryByRole('region', { name: 'Work queue' })).toBeNull()
  })

  it.each([false, true])('keeps supported workloads visible when pipeline records are absent (empty=%s)', async (empty) => {
    const user = userEvent.setup()
    setupQueue(empty ? [] : undefined)
    const filter = await screen.findByRole<HTMLSelectElement>('combobox', { name: 'Filter workload' })
    expect(within(filter).getByRole('option', { name: 'Power BI' })).toBeTruthy()
    expect(within(filter).getByRole('option', { name: 'Fabric pipeline' })).toBeTruthy()
    expect(within(filter).queryByRole('option', { name: /notebook/i })).toBeNull()
    expect(screen.getByText(/Standalone notebook jobs are not monitored/)).toBeTruthy()
    await user.selectOptions(filter, 'fabric_pipeline')
    const queue = within(screen.getByRole('region', { name: 'Work queue' }))
    expect(queue.getByText('No matching work')).toBeTruthy()
    expect(filter.value).toBe('fabric_pipeline')
  })
})

describe('Access & permissions navigation', () => {
  const current: EffectiveAccess = {
    source: 'synthetic_demo',
    current_user: { id: 'operator-1', display_name: 'Demo operator', roles: ['reader'] },
    tenant_id: null, application_id: null, token_issued_at: null, token_expires_at: null,
    role_catalog: APP_ROLES.map((id) => ({ id, label: id[0]!.toUpperCase() + id.slice(1), description: `${id} app permissions.` })),
    management_url: ENTRA_MANAGEMENT_URL,
  }

  function setup(roles: AppRole[] = ['reader']) {
    const api = new ApiClient(async () => null)
    const snapshots = vi.spyOn(api, 'snapshot').mockResolvedValue({ ...snapshot, actor: { ...snapshot.actor, roles } })
    vi.spyOn(api, 'detail').mockResolvedValue(detail)
    const read = vi.spyOn(AccessApiClient.prototype, 'current').mockResolvedValue({
      ...current, current_user: { ...current.current_user, roles },
    })
    const refreshAccess = vi.fn<() => Promise<void>>().mockResolvedValue(undefined)
    const session = { ...auth, refreshAccess }
    return { ...render(<App api={api} config={config} auth={session} />), api, snapshots, read, refreshAccess }
  }

  it('mounts the full read-only workspace once without duplicating its heading or the queue', async () => {
    const user = userEvent.setup()
    const { read } = setup(['admin', 'reader', 'operator', 'approver'])
    await user.click(await screen.findByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    expect(read).toHaveBeenCalledOnce()
    expect(screen.getAllByRole('heading', { name: 'Access & permissions', level: 1 })).toHaveLength(1)
    expect(screen.queryByRole('region', { name: 'Work queue' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'New investigation' })).toBeNull()
    expect(screen.queryByRole('button', { name: /add user|edit roles|enable user|delete user|invite/i })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Admin center' })).toBeNull()
    expect(document.title).toBe('Access & permissions | BI triage')
    expect(new URLSearchParams(window.location.search).get('view')).toBe('access')
    expect(screen.getByRole('button', { name: 'Access & permissions' }).getAttribute('aria-current')).toBe('page')
  })

  it.each(['reader', 'operator', 'approver'] as const)('lets a %s open their own effective access without exposing scenario validation', async (role) => {
    const { read } = setup(role === 'reader' ? ['reader'] : ['reader', role])
    await screen.findByRole('region', { name: 'Work queue' })
    expect(screen.getByRole('button', { name: 'Access & permissions' })).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Admin center' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Scenario validation' })).toBeNull()
    expect(read).not.toHaveBeenCalled()
    await userEvent.setup().click(screen.getByRole('button', { name: 'Access & permissions' }))
    expect(await screen.findByRole('region', { name: 'Current user' })).toBeTruthy()
    expect(read).toHaveBeenCalledOnce()
  })

  it.each(['access', 'admin'])('restores ?view=%s as the same read-only page for a Reader', async (view) => {
    window.history.replaceState({}, '', `/?view=${view}`)
    const { read } = setup()
    expect(await screen.findByRole('region', { name: 'Current user' })).toBeTruthy()
    expect(read).toHaveBeenCalledOnce()
    expect(screen.getAllByRole('heading', { name: 'Access & permissions', level: 1 })).toHaveLength(1)
    expect(screen.queryByRole('region', { name: 'Work queue' })).toBeNull()
    expect(screen.queryByRole('button', { name: /edit|add user/i })).toBeNull()
  })

  it('keeps the effective-access endpoint and management instructions reachable when the snapshot fails', async () => {
    window.history.replaceState({}, '', '/?view=access')
    const api = new ApiClient(async () => null)
    vi.spyOn(api, 'snapshot').mockRejectedValue(new ApiError(503, 'snapshot_unavailable', 'Snapshot records are unavailable.'))
    const read = vi.spyOn(AccessApiClient.prototype, 'current').mockResolvedValue(current)
    render(<App api={api} config={config} auth={auth} />)
    expect(await screen.findByRole('region', { name: 'Current user' })).toBeTruthy()
    expect(screen.getByText('Snapshot records are unavailable.')).toBeTruthy()
    expect(screen.getByRole('link', { name: /Open Microsoft Entra admin center/ })).toBeTruthy()
    expect(screen.queryByText('No records loaded')).toBeNull()
    expect(read).toHaveBeenCalledOnce()
  })

  it('restores browser navigation to the legacy read-only alias without losing incident navigation', async () => {
    const user = userEvent.setup()
    setup()
    await screen.findByRole('region', { name: 'Work queue' })
    await act(async () => {
      window.history.replaceState({}, '', '/?view=admin')
      window.dispatchEvent(new PopStateEvent('popstate'))
    })
    await screen.findByRole('region', { name: 'Current user' })
    const list = vi.spyOn(IncidentApiClient.prototype, 'list').mockResolvedValue({ items: [], total: 0, offset: 0, limit: 25 })
    await user.click(screen.getByRole('button', { name: 'Incidents' }))
    expect(await screen.findByRole('region', { name: 'Incident directory' })).toBeTruthy()
    expect(list).toHaveBeenCalled()
    expect(new URLSearchParams(window.location.search).get('view')).toBe('incidents')
    expect(screen.queryByRole('heading', { name: 'Access & permissions' })).toBeNull()
  })

  it('awaits forced renewal before refreshing both endpoints and locks pre-renewal snapshot capabilities', async () => {
    const user = userEvent.setup()
    const { read, snapshots, refreshAccess } = setup(['admin', 'reader', 'operator', 'approver'])
    await screen.findByRole('region', { name: 'Work queue' })
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    let finishRefresh!: () => void
    let finishSnapshot!: (value: Snapshot) => void
    refreshAccess.mockImplementationOnce(() => new Promise<void>((resolve) => { finishRefresh = resolve }))
    snapshots.mockImplementationOnce(() => new Promise((resolve) => { finishSnapshot = resolve }))
    read.mockResolvedValue(current)
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    expect(refreshAccess).toHaveBeenCalledOnce()
    expect(read).toHaveBeenCalledOnce()
    expect(snapshots).toHaveBeenCalledOnce()
    await act(async () => finishRefresh())
    await waitFor(() => expect(read).toHaveBeenCalledTimes(2))
    expect(snapshots).toHaveBeenCalledTimes(2)
    await screen.findByText(/stale, read-only records, not evidence of current permissions/)
    await user.click(screen.getByRole('button', { name: 'Command center' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'New investigation' }).disabled).toBe(true)
    await act(async () => finishSnapshot({
      ...snapshot, actor: { ...snapshot.actor, roles: ['reader'] },
      capabilities: { approve: false, ask: false, request_triage: false },
    }))
    expect(screen.queryByRole('button', { name: 'Scenario validation' })).toBeNull()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'New investigation' }).disabled).toBe(true)
    expect(screen.getByText('Your current role cannot request new investigations.')).toBeTruthy()
  })

  it('retains a failed renewal across page changes and normal snapshots instead of inferring previous privileges', async () => {
    const user = userEvent.setup()
    const { read, snapshots, refreshAccess } = setup(['reader', 'operator'])
    await screen.findByRole('region', { name: 'Work queue' })
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    refreshAccess.mockRejectedValueOnce(new ApiError(401, 'access_account_changed', 'Reload after the account changed.'))
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    expect(await screen.findByText('Reload after the account changed.')).toBeTruthy()
    expect(read).toHaveBeenCalledOnce()
    expect(snapshots).toHaveBeenCalledOnce()
    expect(screen.queryByRole('list', { name: 'Reported app roles' })).toBeNull()
    await user.click(screen.getByRole('button', { name: 'Command center' }))
    expect(screen.getByText('Reload after the account changed.')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Refresh command center' }))
    await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'New investigation' }).disabled).toBe(true)
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    expect(screen.getByText('Reload after the account changed.')).toBeTruthy()
    expect(screen.queryByRole('list', { name: 'Reported app roles' })).toBeNull()
    expect(read).toHaveBeenCalledOnce()
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    expect(refreshAccess).toHaveBeenCalledTimes(2)
    expect(read).toHaveBeenCalledTimes(2)
    expect(snapshots).toHaveBeenCalledTimes(3)
    expect(screen.queryByText('Reload after the account changed.')).toBeNull()
  })

  it('shares an in-flight token renewal after leaving and reopening the access page', async () => {
    const user = userEvent.setup()
    const { refreshAccess, snapshots } = setup(['reader', 'operator'])
    await screen.findByRole('region', { name: 'Work queue' })
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    let finishRefresh!: () => void
    refreshAccess.mockImplementationOnce(() => new Promise<void>((resolve) => { finishRefresh = resolve }))
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    await user.click(screen.getByRole('button', { name: 'Command center' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'New investigation' }).disabled).toBe(true)
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    expect(refreshAccess).toHaveBeenCalledOnce()
    expect(snapshots).toHaveBeenCalledOnce()
    await act(async () => finishRefresh())
    await screen.findByRole('region', { name: 'Current user' })
    await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
    expect(refreshAccess).toHaveBeenCalledOnce()
    expect(screen.queryByRole('alert')).toBeNull()
  })

  it('keeps API renewal distinct from an explicit sign-in action', async () => {
    window.history.replaceState({}, '', '/?view=access')
    const api = new ApiClient(async () => null)
    vi.spyOn(api, 'snapshot').mockResolvedValue({ ...snapshot, mode: 'live' })
    vi.spyOn(AccessApiClient.prototype, 'current').mockResolvedValue({ ...current, source: 'entra_app_roles' })
    const session: AuthSession = {
      ...auth, enabled: true, refreshAccess: vi.fn().mockRejectedValue(new ApiError(401, 'sign_in_required', 'A fresh API token requires sign-in.')),
      signIn: vi.fn().mockResolvedValue(undefined),
    }
    render(<App api={api} config={{ ...config, mode: 'live' }} auth={session} />)
    await screen.findByRole('region', { name: 'Current user' })
    await userEvent.setup().click(screen.getByRole('button', { name: 'Refresh permissions' }))
    await screen.findByText('A fresh API token requires sign-in.')
    expect(session.signIn).not.toHaveBeenCalled()
    await userEvent.setup().click(screen.getByRole('button', { name: 'Sign in again' }))
    expect(session.signIn).toHaveBeenCalledOnce()
  })
})

describe('approval deep links', () => {
  it('waits for the snapshot, selects the linked approval, and retains later operator selection', async () => {
    const user = userEvent.setup()
    const id = 'linked/request+1'
    window.history.replaceState({}, '', `/?approval=${encodeURIComponent(id)}`)
    const api = new ApiClient(async () => null)
    const linked = { ...workItem, id: `approval:${id}`, source_id: id, title: 'Linked approval' }
    const data = { ...snapshot, work_items: [workItem, linked] }
    let resolveSnapshot!: (value: Snapshot) => void
    const snapshots = vi.spyOn(api, 'snapshot').mockImplementationOnce(() => new Promise((resolve) => { resolveSnapshot = resolve })).mockResolvedValue(data)
    const details = vi.spyOn(api, 'detail').mockImplementation(async (selection) => ({
      ...detail,
      item: selection.source_id === id ? linked : workItem,
      proposal: selection.source_id === id ? { ...proposal, request_id: id } : proposal,
    }))
    render(<App api={api} config={config} auth={auth} />)
    expect(details).not.toHaveBeenCalled()
    await act(async () => resolveSnapshot(data))
    await waitFor(() => expect(details.mock.calls[0]![0]).toEqual({ kind: 'approval', source_id: id }))
    const inspector = screen.getByRole('complementary', { name: 'Selected request inspector' })
    expect(await within(inspector).findByRole('heading', { name: 'Linked approval' })).toBeTruthy()
    await user.click(screen.getByRole('button', { name: /Review refresh retry/ }))
    await waitFor(() => expect(details.mock.calls.at(-1)![0].source_id).toBe(workItem.source_id))
    await user.click(screen.getByRole('button', { name: 'Refresh command center' }))
    await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
    expect(details.mock.calls.at(-1)![0].source_id).toBe(workItem.source_id)
    expect(within(screen.getByRole('complementary', { name: 'Selected request inspector' })).getByRole('heading', { name: 'Review refresh retry' })).toBeTruthy()
  })
  it('resolves a linked approval through detail even when the bounded queue omits it', async () => {
    window.history.replaceState({}, '', '/?approval=approval-1')
    const api = new ApiClient(async () => null)
    vi.spyOn(api, 'snapshot').mockResolvedValue({ ...snapshot, work_items: [], truncated: true })
    const details = vi.spyOn(api, 'detail').mockResolvedValue(detail)
    render(<App api={api} config={config} auth={auth} />)
    await waitFor(() => expect(details.mock.calls[0]![0]).toEqual({ kind: 'approval', source_id: 'approval-1' }))
    expect(await screen.findByRole('heading', { name: 'Review refresh retry' })).toBeTruthy()
  })
})
