import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { App } from './App'
import { AccessApiClient, APP_ROLES, ENTRA_MANAGEMENT_URL } from './api/access'
import type { AppRole } from './api/access'
import type { AuthSession } from './api/auth'
import { ApiClient } from './api/client'
import { ApiError } from './api/errors'
import { MonitoringApiClient } from './api/monitoring'
import type { SafetyReview } from './api/monitoring'
import { MONITORING_ACTIVATION_STORAGE_KEY } from './components/MonitoringWorkspace'
import { SAFETY_REVIEW_RECOVERY_KEY } from './components/MonitoringSafetyReview'
import type { Snapshot } from './api/types'
import { config, snapshot } from './test/fixtures'
import {
  acceptedSafetyReviewResponse, ids, monitoringBootstrap, monitoringConnector, monitoringDomains,
  monitoringFixtureApi, monitoringInventory, monitoringPage, monitoringSafetyIntent, monitoringScope,
  monitoringSnapshot, monitoringTarget, monitoringVersion, monitoringWorkspaces, publishedSafetyReview,
} from './test/monitoringFixtures'

afterEach(() => {
  window.history.replaceState({}, '', '/')
  window.sessionStorage.removeItem(MONITORING_ACTIVATION_STORAGE_KEY)
  window.sessionStorage.removeItem(SAFETY_REVIEW_RECOVERY_KEY)
})
function setup(roles: AppRole[] = ['reader', 'admin'], userId = snapshot.actor.id) {
  const api = new ApiClient(async () => null)
  const snapshots = vi.spyOn(api, 'snapshot').mockResolvedValue({
    ...snapshot, work_items: [], actor: { ...snapshot.actor, id: userId, roles },
  })
  const monitoring = monitoringFixtureApi(roles)
  vi.mocked(monitoring.snapshot).mockResolvedValue({
    ...monitoringSnapshot, can_admin: roles.includes('admin'), user: { ...monitoringSnapshot.user, id: userId, roles },
  })
  vi.spyOn(MonitoringApiClient.prototype, 'bootstrap').mockImplementation(monitoring.bootstrap)
  vi.spyOn(MonitoringApiClient.prototype, 'snapshot').mockImplementation(monitoring.snapshot)
  vi.spyOn(MonitoringApiClient.prototype, 'scopes').mockImplementation(monitoring.scopes)
  vi.spyOn(MonitoringApiClient.prototype, 'inventory').mockImplementation(monitoring.inventory)
  vi.spyOn(MonitoringApiClient.prototype, 'targets').mockImplementation(monitoring.targets)
  vi.spyOn(MonitoringApiClient.prototype, 'domains').mockImplementation(monitoring.domains)
  vi.spyOn(MonitoringApiClient.prototype, 'workspaces').mockImplementation(monitoring.workspaces)
  vi.spyOn(MonitoringApiClient.prototype, 'connectors').mockImplementation(monitoring.connectors)
  vi.spyOn(MonitoringApiClient.prototype, 'preview').mockImplementation(monitoring.preview)
  vi.spyOn(MonitoringApiClient.prototype, 'activate').mockImplementation(monitoring.activate)
  vi.spyOn(MonitoringApiClient.prototype, 'activation').mockImplementation(monitoring.activation)
  vi.spyOn(MonitoringApiClient.prototype, 'refreshInventory').mockImplementation(monitoring.refreshInventory)
  vi.spyOn(MonitoringApiClient.prototype, 'safetyReview').mockImplementation(monitoring.safetyReview)
  vi.spyOn(MonitoringApiClient.prototype, 'safetyReviewOperation').mockImplementation(monitoring.safetyReviewOperation)
  vi.spyOn(MonitoringApiClient.prototype, 'recordSafetyReview').mockImplementation(monitoring.recordSafetyReview)
  vi.spyOn(AccessApiClient.prototype, 'current').mockResolvedValue({
    source: 'synthetic_demo', current_user: { ...snapshot.actor, id: userId, roles },
    tenant_id: null, application_id: null, token_issued_at: null, token_expires_at: null,
    management_url: ENTRA_MANAGEMENT_URL,
    role_catalog: APP_ROLES.map((id) => ({ id, label: id, description: `${id} app permissions` })),
  })
  const refreshAccess = vi.fn<() => Promise<void>>().mockResolvedValue(undefined)
  const auth: AuthSession = {
    enabled: false, signedIn: true, getToken: async () => null, refreshAccess,
    signIn: async () => undefined, signOut: async () => undefined,
  }
  return { api, snapshots, monitoring, auth, refreshAccess }
}
async function preparePreview(user: ReturnType<typeof userEvent.setup>) {
  await user.type(await screen.findByRole('textbox', { name: 'Scope name' }), 'App monitoring')
  await user.selectOptions(screen.getByRole('combobox', { name: 'Include workspace' }), ids.workspace)
  await user.click(screen.getByRole('button', { name: 'Preview scope changes' }))
  await screen.findByRole('region', { name: 'Scope preview' })
}

describe('Monitoring setup app integration', () => {
  it('is lazy, preserves the existing shell and mounts one full-page monitoring workspace', async () => {
    const { api, auth, monitoring } = setup()
    const user = userEvent.setup()
    render(<App api={api} auth={auth} config={config} />)
    await screen.findByRole('region', { name: 'Work queue' })
    expect(monitoring.bootstrap).not.toHaveBeenCalled()
    await user.click(screen.getByRole('button', { name: 'Monitoring setup' }))
    expect(await screen.findByRole('region', { name: 'Monitoring coverage' })).toBeTruthy()
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getAllByRole('heading', { name: 'Monitoring setup', level: 1 })).toHaveLength(1)
    expect(screen.queryByRole('region', { name: 'Work queue' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'New investigation' })).toBeNull()
    expect(screen.getByRole('img', { name: 'BI triage' }).getAttribute('src')).toBe('/triage-logo.png')
    expect(screen.getByRole('button', { name: 'Switch to light theme' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Access & permissions' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Incidents' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Monitoring setup' }).getAttribute('aria-current')).toBe('page')
    expect(document.title).toBe('Monitoring setup | BI triage')
    expect(new URLSearchParams(window.location.search).get('view')).toBe('monitoring')
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    expect(await screen.findByRole('region', { name: 'Current user' })).toBeTruthy()
    expect(screen.queryByRole('heading', { name: 'Monitoring setup' })).toBeNull()
  })

  describe('Safety-review app permission integration', () => {
    async function openReview(user: ReturnType<typeof userEvent.setup>) {
      await user.click(await screen.findByText('Inspect Operations model'))
      await user.click(screen.getByRole('button', { name: 'Safety review for Operations model' }))
      await screen.findByRole('combobox', { name: 'Action profile' })
    }
    async function fillReview(user: ReturnType<typeof userEvent.setup>) {
      await user.selectOptions(screen.getByRole('combobox', { name: 'Action profile' }), 'powerbi_refresh')
      await user.selectOptions(screen.getByRole('combobox', { name: 'Requested review state' }), 'verified')
      fireEvent.change(screen.getByLabelText('Review expiry (your local time)'), { target: { value: '2098-12-15T12:00' } })
      await user.type(screen.getByRole('textbox', { name: 'Reason for this review change' }), 'Reviewed the selected profile.')
    }
    it('renders accepted intent through the registry revision change and reads publication without another submission', async () => {
      window.history.replaceState({}, '', '/?view=monitoring')
      const { api, auth, monitoring, snapshots } = setup(['reader', 'admin'], ids.reviewer)
      const user = userEvent.setup()
      let accepted!: SafetyReview
      const version = { ...monitoringVersion, revision: 4 }
      vi.mocked(monitoring.recordSafetyReview).mockImplementation(async (input) => {
        accepted = acceptedSafetyReviewResponse(input)
        vi.mocked(monitoring.safetyReview).mockResolvedValue(accepted)
        vi.mocked(monitoring.bootstrap).mockResolvedValue({
          ...monitoringBootstrap, control: { ...monitoringSnapshot.control, ...version },
        })
        vi.mocked(monitoring.snapshot).mockResolvedValue({
          ...monitoringSnapshot, control: { ...monitoringSnapshot.control, ...version },
          coverage: { ...monitoringSnapshot.coverage, ...version }, user: { ...monitoringSnapshot.user, id: ids.reviewer },
        })
        vi.mocked(monitoring.scopes).mockResolvedValue(monitoringPage([monitoringScope], null, version))
        vi.mocked(monitoring.inventory).mockResolvedValue(monitoringPage(monitoringInventory, null, version))
        vi.mocked(monitoring.targets).mockResolvedValue(monitoringPage([monitoringTarget], null, version))
        vi.mocked(monitoring.domains).mockResolvedValue(monitoringPage(monitoringDomains, null, version))
        vi.mocked(monitoring.workspaces).mockResolvedValue(monitoringPage(monitoringWorkspaces, null, version))
        vi.mocked(monitoring.connectors).mockResolvedValue(monitoringPage([monitoringConnector], null, version))
        return accepted
      })
      render(<App api={api} auth={auth} config={config} />)
      await openReview(user)
      await fillReview(user)
      await user.click(screen.getByRole('button', { name: 'Save safety review' }))
      await screen.findByText(/Safety-review intent accepted: verified, revision 1/)
      await screen.findByText('Pending validation', { selector: '.badge' })
      expect((await screen.findByRole<HTMLButtonElement>('button', { name: 'Save safety review' })).disabled).toBe(true)
      expect(screen.getByText(/target policy revision is not current/)).toBeTruthy()
      await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
      const published = publishedSafetyReview(accepted, 'verified', 4)
      vi.mocked(monitoring.safetyReview).mockResolvedValue(published)
      vi.mocked(monitoring.targets).mockResolvedValue(monitoringPage([{
        ...monitoringTarget, policy_revision: 4,
        action: { enabled: false, action: published.action, review_id: published.review_id, review_revision: published.revision },
      }], null, version))
      await user.click(screen.getByRole('button', { name: 'Refresh monitoring records' }))
      await screen.findByText('Verified', { selector: '.badge' })
      expect(screen.getByText('Publication').parentElement?.querySelector('dd')?.textContent).toBe('Published')
      expect(screen.getByText(/current server target keeps this action disabled/)).toBeTruthy()
      expect(monitoring.recordSafetyReview).toHaveBeenCalledOnce()
    })

    it('does not let newly published review data unlock actions while the renewed parent snapshot is pending', async () => {
      window.history.replaceState({}, '', '/?view=monitoring')
      const { api, auth, monitoring, snapshots, refreshAccess } = setup(['reader', 'admin'], ids.reviewer)
      const accepted = acceptedSafetyReviewResponse({
        request_id: ids.submission, expected: { ...monitoringVersion, revision: 2 }, expected_review_revision: 1,
        review: { ...monitoringSafetyIntent, revision: 2, policy_revision: 2, state: 'verified', exact_correlation_verified: true },
      })
      vi.mocked(monitoring.safetyReview).mockResolvedValue(accepted)
      vi.mocked(monitoring.targets).mockResolvedValue(monitoringPage([{
        ...monitoringTarget, policy_revision: 2,
        action: { enabled: false, action: accepted.action, review_id: accepted.review_id, review_revision: 1 },
      }]))
      const user = userEvent.setup()
      render(<App api={api} auth={auth} config={config} />)
      await openReview(user)
      await screen.findByText('Pending validation', { selector: '.badge' })
      await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
      await screen.findByRole('region', { name: 'Current user' })
      let finishRefresh!: () => void
      let finishSnapshot!: (value: Snapshot) => void
      refreshAccess.mockImplementationOnce(() => new Promise((resolve) => { finishRefresh = resolve }))
      snapshots.mockImplementationOnce(() => new Promise((resolve) => { finishSnapshot = resolve }))
      await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
      const published = publishedSafetyReview(accepted, 'verified', 3)
      vi.mocked(monitoring.safetyReview).mockResolvedValue(published)
      vi.mocked(monitoring.targets).mockResolvedValue(monitoringPage([{
        ...monitoringTarget, action: { enabled: true, action: published.action, review_id: published.review_id, review_revision: 2 },
      }]))
      await act(async () => finishRefresh())
      await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
      await user.click(screen.getByRole('button', { name: 'Monitoring setup' }))
      await screen.findByText('Verified', { selector: '.badge' })
      expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
      expect(screen.queryByText(/current server target admits this action profile/)).toBeNull()
      vi.mocked(monitoring.snapshot).mockResolvedValue({
        ...monitoringSnapshot, can_admin: false, user: { ...monitoringSnapshot.user, id: ids.reviewer, roles: ['reader'] },
      })
      await act(async () => finishSnapshot({
        ...snapshot, work_items: [], actor: { ...snapshot.actor, id: ids.reviewer, roles: ['reader'] },
        capabilities: { approve: false, ask: false, request_triage: false },
      }))
      await screen.findByText(/Safety-review records are read-only/)
      expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
      expect(monitoring.recordSafetyReview).not.toHaveBeenCalled()
    })

    it('refreshes targets, coverage and the parent snapshot after a saved review without inferring action readiness', async () => {
      window.history.replaceState({}, '', '/?view=monitoring')
      const { api, auth, monitoring, snapshots } = setup(['reader', 'admin'], ids.reviewer)
      const user = userEvent.setup()
      render(<App api={api} auth={auth} config={config} />)
      await openReview(user)
      await fillReview(user)
      await user.click(screen.getByRole('button', { name: 'Save safety review' }))
      await screen.findByText(/Safety review recorded as verified, revision 1/)
      await screen.findByText(/current server target keeps this action disabled/)
      expect(monitoring.recordSafetyReview).toHaveBeenCalledOnce()
      await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
      expect(vi.mocked(monitoring.targets).mock.calls.length).toBeGreaterThanOrEqual(2)
      expect(vi.mocked(monitoring.snapshot).mock.calls.length).toBeGreaterThanOrEqual(2)
      expect(monitoring.activate).not.toHaveBeenCalled()
    })

    it('locks review submission throughout token renewal and the pending new-generation snapshot, then removes Admin controls', async () => {
      window.history.replaceState({}, '', '/?view=monitoring')
      const { api, auth, monitoring, snapshots, refreshAccess } = setup(['reader', 'admin'], ids.reviewer)
      const user = userEvent.setup()
      render(<App api={api} auth={auth} config={config} />)
      await openReview(user)
      await fillReview(user)
      fireEvent.change(screen.getByRole('textbox', { name: 'Reviewed parameters (JSON)' }), { target: { value: '{"batch":"volatile-draft"}' } })
      await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
      await screen.findByRole('region', { name: 'Current user' })
      let finishRefresh!: () => void
      let finishSnapshot!: (value: Snapshot) => void
      refreshAccess.mockImplementationOnce(() => new Promise((resolve) => { finishRefresh = resolve }))
      snapshots.mockImplementationOnce(() => new Promise((resolve) => { finishSnapshot = resolve }))
      await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
      await user.click(screen.getByRole('button', { name: 'Monitoring setup' }))
      await screen.findByRole('combobox', { name: 'Action profile' })
      expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
      expect(screen.queryByDisplayValue('{"batch":"volatile-draft"}')).toBeNull()
      await act(async () => finishRefresh())
      await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
      await screen.findByRole('combobox', { name: 'Action profile' })
      expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
      vi.mocked(monitoring.snapshot).mockResolvedValue({
        ...monitoringSnapshot, can_admin: false, user: { ...monitoringSnapshot.user, id: ids.reviewer, roles: ['reader'] },
      })
      await act(async () => finishSnapshot({ ...snapshot, work_items: [], actor: { ...snapshot.actor, id: ids.reviewer, roles: ['reader'] },
        capabilities: { approve: false, ask: false, request_triage: false } }))
      await screen.findByText(/Safety-review records are read-only/)
      expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
      expect(monitoring.recordSafetyReview).not.toHaveBeenCalled()
    })

    it('never restores safety-review permission from an old Admin snapshot after a failed renewal', async () => {
      window.history.replaceState({}, '', '/?view=monitoring')
      const { api, auth, monitoring, snapshots, refreshAccess } = setup(['reader', 'admin'], ids.reviewer)
      const user = userEvent.setup()
      render(<App api={api} auth={auth} config={config} />)
      await openReview(user)
      await fillReview(user)
      await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
      await screen.findByRole('region', { name: 'Current user' })
      refreshAccess.mockRejectedValueOnce(new ApiError(401, 'sign_in_required', 'Permission renewal failed.'))
      await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
      await screen.findByText('Permission renewal failed.')
      await user.click(screen.getByRole('button', { name: 'Command center' }))
      await user.click(screen.getByRole('button', { name: 'Refresh command center' }))
      await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
      await user.click(screen.getByRole('button', { name: 'Monitoring setup' }))
      await screen.findByRole('combobox', { name: 'Action profile' })
      expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
      expect(monitoring.recordSafetyReview).not.toHaveBeenCalled()
    })
  })

  it('restores a Reader monitoring deep link and browser navigation without adding Snapshot fields', async () => {
    window.history.replaceState({}, '', '/?view=monitoring')
    const { api, auth } = setup(['reader'])
    render(<App api={api} auth={auth} config={config} />)
    await screen.findByRole('region', { name: 'Monitoring coverage' })
    expect(screen.queryByRole('textbox', { name: 'Scope name' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Scenario validation' })).toBeNull()
    await act(async () => {
      window.history.replaceState({}, '', '/?view=access')
      window.dispatchEvent(new PopStateEvent('popstate'))
    })
    expect(await screen.findByRole('region', { name: 'Current user' })).toBeTruthy()
    await act(async () => {
      window.history.replaceState({}, '', '/?view=monitoring')
      window.dispatchEvent(new PopStateEvent('popstate'))
    })
    expect(await screen.findByRole('region', { name: 'Monitoring coverage' })).toBeTruthy()
    expect(screen.getAllByRole('heading', { name: 'Monitoring setup', level: 1 })).toHaveLength(1)
  })

  it('keeps deployment diagnostics reachable but locked when the parent snapshot fails', async () => {
    window.history.replaceState({}, '', '/?view=monitoring')
    const { api, auth, snapshots } = setup()
    snapshots.mockRejectedValue(new ApiError(503, 'snapshot_unavailable', 'Main snapshot is unavailable.'))
    render(<App api={api} auth={auth} config={config} />)
    await screen.findByRole('region', { name: 'Monitoring coverage' })
    expect(screen.getByText('Main snapshot is unavailable.')).toBeTruthy()
    expect(screen.getByText(/Previously reported Admin roles cannot unlock setup/)).toBeTruthy()
    expect(screen.queryByRole('textbox', { name: 'Scope name' })).toBeNull()
    expect(screen.queryByText('No records loaded')).toBeNull()
  })

  it('clears monitoring previews and holds the token-refresh lock until the new-generation parent snapshot succeeds', async () => {
    window.history.replaceState({}, '', '/?view=monitoring')
    const { api, auth, monitoring, snapshots, refreshAccess } = setup()
    const user = userEvent.setup()
    render(<App api={api} auth={auth} config={config} />)
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    let finishRefresh!: () => void
    let finishSnapshot!: (value: Snapshot) => void
    refreshAccess.mockImplementationOnce(() => new Promise((resolve) => { finishRefresh = resolve }))
    snapshots.mockImplementationOnce(() => new Promise((resolve) => { finishSnapshot = resolve }))
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    await user.click(screen.getByRole('button', { name: 'Monitoring setup' }))
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.queryByRole('button', { name: 'Activate current preview' })).toBeNull()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    await act(async () => finishRefresh())
    await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Queue inventory refresh' }).disabled).toBe(true)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    vi.mocked(monitoring.snapshot).mockResolvedValue({ ...monitoringSnapshot, can_admin: false, user: { ...monitoringSnapshot.user, roles: ['reader'] } })
    await act(async () => finishSnapshot({ ...snapshot, work_items: [], actor: { ...snapshot.actor, roles: ['reader'] } }))
    await screen.findByRole('region', { name: 'Admitted monitoring targets' })
    expect(screen.queryByRole('textbox', { name: 'Scope name' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Queue inventory refresh' })).toBeNull()
    expect(monitoring.activate).not.toHaveBeenCalled()
  })

  it('does not restore monitoring Admin setup after a failed token renewal and an ordinary snapshot reload', async () => {
    window.history.replaceState({}, '', '/?view=monitoring')
    const { api, auth, refreshAccess, snapshots } = setup()
    const user = userEvent.setup()
    render(<App api={api} auth={auth} config={config} />)
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    refreshAccess.mockRejectedValueOnce(new ApiError(401, 'sign_in_required', 'Permission renewal failed.'))
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    await screen.findByText('Permission renewal failed.')
    await user.click(screen.getByRole('button', { name: 'Command center' }))
    await user.click(screen.getByRole('button', { name: 'Refresh command center' }))
    await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
    await user.click(screen.getByRole('button', { name: 'Monitoring setup' }))
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    expect(screen.queryByRole('button', { name: 'Activate current preview' })).toBeNull()
    expect(screen.getByText('Permission renewal failed.')).toBeTruthy()
  })

  it('refreshes the existing snapshot after activation so investigation targets can update', async () => {
    window.history.replaceState({}, '', '/?view=monitoring')
    const { api, auth, snapshots } = setup()
    const user = userEvent.setup()
    render(<App api={api} auth={auth} config={config} />)
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Configuring: App monitoring')
    await waitFor(() => expect(snapshots).toHaveBeenCalledTimes(2))
  })

  it('retains an uncertain activation ID across navigation instead of reopening a new submission', async () => {
    window.history.replaceState({}, '', '/?view=monitoring')
    const { api, auth, monitoring } = setup()
    vi.mocked(monitoring.activate).mockRejectedValueOnce(new ApiError(0, 'network_error', 'Activation response was lost.'))
    vi.mocked(monitoring.activation).mockRejectedValue(new ApiError(404, 'activation_not_found', 'Receipt not yet available.'))
    const user = userEvent.setup()
    render(<App api={api} auth={auth} config={config} />)
    await preparePreview(user)
    await user.click(screen.getByRole('button', { name: 'Activate current preview' }))
    await screen.findByText('Activation outcome is unconfirmed')
    const submissionId = vi.mocked(monitoring.activate).mock.calls[0]![1].idempotency_id
    await user.click(screen.getByRole('button', { name: 'Access & permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    await user.click(screen.getByRole('button', { name: 'Monitoring setup' }))
    await screen.findByRole('textbox', { name: 'Scope name' })
    expect(screen.getByText(submissionId)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Preview scope changes' }).disabled).toBe(true)
    expect(monitoring.activate).toHaveBeenCalledOnce()
  })
})
