import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { AccessApiClient, ENTRA_MANAGEMENT_URL } from '../api/access'
import type { EffectiveAccess } from '../api/access'
import { ApiError } from '../api/errors'
import { AccessCenter } from './AccessCenter'

const current: EffectiveAccess = {
  source: 'entra_app_roles',
  current_user: { id: '20000000-0000-0000-0000-000000000002', display_name: 'Casey Example', roles: ['reader', 'operator'] },
  tenant_id: '10000000-0000-0000-0000-000000000001',
  application_id: '30000000-0000-0000-0000-000000000003',
  token_issued_at: '2026-09-14T12:00:00Z', token_expires_at: '2026-09-14T13:00:00Z',
  role_catalog: [
    { id: 'reader', label: 'Reader', description: 'Read incidents, run history, knowledge and effective access.' },
    { id: 'operator', label: 'Operator', description: 'Request investigations and manage incident tracking.' },
    { id: 'approver', label: 'Approver', description: 'Approve or deny proposed remediation after reviewing evidence.' },
    { id: 'admin', label: 'Admin', description: 'All app capabilities, including scenario validation and command reconciliation.' },
  ],
  management_url: ENTRA_MANAGEMENT_URL,
}
const demo: EffectiveAccess = {
  ...current, source: 'synthetic_demo',
  current_user: { id: 'demo-reader', display_name: 'Synthetic reader', roles: ['reader'] },
  tenant_id: null, application_id: null, token_issued_at: null, token_expires_at: null,
}

beforeEach(() => vi.spyOn(Date, 'now').mockReturnValue(Date.parse('2026-09-14T12:30:00Z')))

function setup(data = current, refreshAccess = vi.fn<() => Promise<void>>().mockResolvedValue(undefined)) {
  const api = new AccessApiClient(async () => null)
  const read = vi.spyOn(api, 'current').mockResolvedValue(data)
  const props = { api, fresh: true, mode: data.source === 'synthetic_demo' ? 'demo' as const : 'live' as const, refreshAccess }
  return { ...render(<AccessCenter {...props} />), api, read, refreshAccess, props }
}

describe('read-only Access & permissions', () => {
  it('shows only API-reported identity, authority IDs and effective roles with their definitions', async () => {
    setup()
    const identity = await screen.findByRole('region', { name: 'Current user' })
    expect(identity.textContent).toContain(current.current_user.display_name)
    expect(identity.textContent).toContain(current.current_user.id)
    expect(identity.textContent).toContain(current.tenant_id)
    expect(identity.textContent).toContain(current.application_id)
    const roles = within(screen.getByRole('list', { name: 'Reported app roles' }))
    expect(roles.getByText('Reader')).toBeTruthy()
    expect(roles.getByText('Operator')).toBeTruthy()
    expect(roles.queryByText('Approver')).toBeNull()
    expect(roles.queryByText('Admin')).toBeNull()
    const table = within(screen.getByRole('table'))
    for (const role of current.role_catalog) expect(table.getByText(role.description)).toBeTruthy()
    expect(within(table.getByRole('row', { name: /^Operator / })).getByText('Reported as effective')).toBeTruthy()
    expect(within(table.getByRole('row', { name: /^Approver / })).getByText('Not reported')).toBeTruthy()
    expect(screen.getByText(/Operator and Approver are separate/)).toBeTruthy()
    expect(screen.getByText(/Admin includes all app capabilities, not directory administration/)).toBeTruthy()
  })

  it('has no user roster, membership claims or permission editing controls', async () => {
    const { container } = setup()
    await screen.findByRole('region', { name: 'Current user' })
    expect(screen.getAllByRole('button').map((button) => button.textContent)).toEqual(['Refresh permissions'])
    expect(container.querySelector('form, input, textarea, select')).toBeNull()
    expect(screen.queryByRole('button', { name: /add|edit|enable|delete|invite|invitation|save/i })).toBeNull()
    expect(screen.queryByRole('heading', { name: /app users|audit|group members/i })).toBeNull()
    expect(screen.getByText(/cannot tell you which group supplied a role/)).toBeTruthy()
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it('links to the actual portal root with generic labeling and explains external group ownership', async () => {
    setup()
    await screen.findByRole('region', { name: 'Current user' })
    const link = screen.getByRole('link', { name: 'Open Microsoft Entra admin center (opens in a new tab)' })
    expect(link.getAttribute('href')).toBe(ENTRA_MANAGEMENT_URL)
    expect(link.getAttribute('target')).toBe('_blank')
    expect(link.getAttribute('rel')).toBe('noopener noreferrer')
    expect(screen.getByText('Entra ID > Enterprise applications > this app > Users and groups')).toBeTruthy()
    expect(screen.getByText(/four Entra security groups mapped to the Reader, Operator, Approver and Admin/)).toBeTruthy()
    expect(screen.getByText(/Group owners manage membership in those groups/)).toBeTruthy()
    expect(screen.getByText(/These roles do not grant Azure, Fabric or Entra directory permissions/)).toBeTruthy()
    expect(screen.getByText(/does not revoke other users' already-issued tokens/)).toBeTruthy()
    expect(screen.getByText(/The backend checks app roles on every request/)).toBeTruthy()
  })

  it('exposes the API token issue and expiry dates without claiming a live directory check', async () => {
    setup()
    const token = await screen.findByRole('region', { name: 'Token freshness' })
    expect([...token.querySelectorAll('time')].map((element) => element.dateTime)).toEqual([
      current.token_issued_at, current.token_expires_at, '2026-09-14T12:30:00.000Z',
    ])
    expect(within(token).getByText(/not a new directory check/)).toBeTruthy()
  })

  it('labels demo identities and missing metadata without inventing Entra verification', async () => {
    const { refreshAccess } = setup(demo)
    const identity = await screen.findByRole('region', { name: 'Current user' })
    expect(within(identity).getByText('Synthetic demo')).toBeTruthy()
    expect(screen.getByText(/not evidence of Entra access/)).toBeTruthy()
    expect(screen.queryByText(/validated|verified/i)).toBeNull()
    expect(screen.getAllByText('Not available in synthetic demo')).toHaveLength(4)
    expect(within(screen.getByRole('region', { name: 'Token freshness' })).getByText(/No Entra token is issued or renewed/)).toBeTruthy()
    expect(refreshAccess).not.toHaveBeenCalled()
  })

  it('does not infer live authority IDs or token dates when the API reports null', async () => {
    setup({ ...demo, source: 'entra_app_roles' })
    await screen.findByRole('region', { name: 'Current user' })
    expect(screen.getAllByText('Not reported by the API')).toHaveLength(4)
    expect(screen.getByRole('region', { name: 'Token freshness' }).querySelectorAll('time')).toHaveLength(1)
  })

  it.each(['stale', 'expired'])('labels cached read-only token evidence when %s', async (reason) => {
    const view = setup(reason === 'expired' ? { ...current, token_expires_at: '2026-09-14T12:29:59Z' } : current)
    if (reason === 'stale') view.rerender(<AccessCenter {...view.props} fresh={false} />)
    expect(await screen.findByText(/stale, read-only records, not evidence of current permissions/)).toBeTruthy()
    expect(screen.getByText('Roles in the last received API response.')).toBeTruthy()
    expect(screen.getByRole('list', { name: 'Reported app roles' })).toBeTruthy()
  })

  it('forces token renewal before reloading roles, prevents duplicate refreshes and does not retain old grants', async () => {
    const user = userEvent.setup()
    let finishRefresh!: () => void
    const refresh = vi.fn(() => new Promise<void>((resolve) => { finishRefresh = resolve }))
    const { read } = setup(current, refresh)
    await screen.findByRole('region', { name: 'Current user' })
    const next: EffectiveAccess = { ...current, current_user: { ...current.current_user, roles: ['reader'] } }
    read.mockResolvedValue(next)
    await user.dblClick(screen.getByRole('button', { name: 'Refresh permissions' }))
    expect(refresh).toHaveBeenCalledOnce()
    expect(read).toHaveBeenCalledOnce()
    expect(screen.queryByRole('list', { name: 'Reported app roles' })).toBeNull()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Refresh permissions' }).disabled).toBe(true)
    await act(async () => finishRefresh())
    await waitFor(() => expect(read).toHaveBeenCalledTimes(2))
    const roles = await screen.findByRole('list', { name: 'Reported app roles' })
    expect(within(roles).getByText('Reader')).toBeTruthy()
    expect(within(roles).queryByText('Operator')).toBeNull()
  })

  it('keeps a token refresh failure explicit without rereading an old token, then permits an explicit retry', async () => {
    const user = userEvent.setup()
    const { read, refreshAccess } = setup()
    await screen.findByRole('region', { name: 'Current user' })
    refreshAccess.mockRejectedValueOnce(new ApiError(401, 'sign_in_required', 'Sign in again to refresh permissions.'))
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    expect(await screen.findByText('Sign in again to refresh permissions.')).toBeTruthy()
    expect(read).toHaveBeenCalledOnce()
    expect(refreshAccess).toHaveBeenCalledOnce()
    expect(screen.queryByRole('region', { name: 'Current user' })).toBeNull()
    expect(screen.getByText(/No permissions have been inferred from earlier responses/)).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    await screen.findByRole('region', { name: 'Current user' })
    expect(read).toHaveBeenCalledTimes(2)
    expect(refreshAccess).toHaveBeenCalledTimes(2)
    expect(screen.queryByRole('alert')).toBeNull()
  })

  it.each([403, 503])('hides prior permissions after an access API failure (%s)', async (status) => {
    const user = userEvent.setup()
    const { read } = setup()
    await screen.findByRole('region', { name: 'Current user' })
    read.mockRejectedValueOnce(new ApiError(status, 'access_unavailable', 'Current access is unavailable.'))
    await user.click(screen.getByRole('button', { name: 'Refresh permissions' }))
    expect(await screen.findByText('Current access is unavailable.')).toBeTruthy()
    expect(screen.queryByRole('list', { name: 'Reported app roles' })).toBeNull()
    expect(screen.getByRole('link', { name: /Open Microsoft Entra admin center/ })).toBeTruthy()
    expect(read).toHaveBeenCalledTimes(2)
  })

  it('does not clear a session-level refresh failure when the page remounts and reads cached token data', async () => {
    const { props, rerender, read } = setup()
    await screen.findByRole('region', { name: 'Current user' })
    rerender(<AccessCenter {...props} refreshError={new ApiError(401, 'access_account_changed', 'Reload after changing accounts.')} />)
    expect(screen.getByText('Reload after changing accounts.')).toBeTruthy()
    expect(screen.queryByRole('region', { name: 'Current user' })).toBeNull()
    expect(read).toHaveBeenCalledOnce()
  })

  it('rejects a source that conflicts with the configured mode without presenting demo records as live access', async () => {
    const api = new AccessApiClient(async () => null)
    vi.spyOn(api, 'current').mockResolvedValue(demo)
    render(<AccessCenter api={api} fresh mode="live" />)
    expect(await screen.findByText(/The API access source does not match the application mode/)).toBeTruthy()
    expect(screen.queryByText('Synthetic reader')).toBeNull()
    expect(screen.queryByRole('list', { name: 'Reported app roles' })).toBeNull()
  })

  it.each(['demo', 'live'] as const)('handles an absent optional refresh provider without claiming a fresh live token (%s)', async (mode) => {
    const api = new AccessApiClient(async () => null)
    const read = vi.spyOn(api, 'current').mockResolvedValue(mode === 'demo' ? demo : current)
    render(<AccessCenter api={api} fresh mode={mode} />)
    await screen.findByRole('region', { name: 'Current user' })
    const button = screen.getByRole<HTMLButtonElement>('button', { name: 'Refresh permissions' })
    expect(button.disabled).toBe(mode === 'live')
    await userEvent.setup().click(button)
    await waitFor(() => expect(read).toHaveBeenCalledTimes(mode === 'demo' ? 2 : 1))
    if (mode === 'live') expect(screen.getByText(/This session cannot request a fresh API token/)).toBeTruthy()
  })

  it('ignores an old API response after the session client changes', async () => {
    let finishOld!: (value: EffectiveAccess) => void
    const oldApi = new AccessApiClient(async () => null)
    const readOld = vi.spyOn(oldApi, 'current').mockImplementation(() => new Promise((resolve) => { finishOld = resolve }))
    const newApi = new AccessApiClient(async () => null)
    const next = { ...current, current_user: { ...current.current_user, id: 'other-user', display_name: 'Different session' } }
    vi.spyOn(newApi, 'current').mockResolvedValue(next)
    const view = render(<AccessCenter api={oldApi} fresh />)
    view.rerender(<AccessCenter api={newApi} fresh />)
    expect(await screen.findByText('Different session')).toBeTruthy()
    expect(readOld.mock.calls[0]![0]?.aborted).toBe(true)
    await act(async () => finishOld(current))
    expect(screen.queryByText('Casey Example')).toBeNull()
  })

  it('does not reread access after leaving the page during forced renewal', async () => {
    let finishRefresh!: () => void
    const refresh = vi.fn(() => new Promise<void>((resolve) => { finishRefresh = resolve }))
    const { read, unmount } = setup(current, refresh)
    await screen.findByRole('region', { name: 'Current user' })
    await userEvent.setup().click(screen.getByRole('button', { name: 'Refresh permissions' }))
    unmount()
    await act(async () => finishRefresh())
    expect(read).toHaveBeenCalledOnce()
    expect(refresh).toHaveBeenCalledOnce()
  })

  it('renders token names and role descriptions as text, not executable markup', async () => {
    const name = '<img src=x onerror=alert(1)>'
    const { container } = setup({
      ...current, current_user: { ...current.current_user, display_name: name },
      role_catalog: current.role_catalog.map((role) => ({ ...role, description: `<script>${role.id}</script>` })),
    })
    expect(await screen.findByText(name)).toBeTruthy()
    expect(screen.getByText('<script>reader</script>')).toBeTruthy()
    expect(container.querySelector('script, img')).toBeNull()
  })
})
