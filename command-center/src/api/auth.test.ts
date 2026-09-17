import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { InteractionRequiredAuthError } from '@azure/msal-browser'
import { initializeAuth } from './auth'
import { PROFILE_SCOPE } from './profile'
import { approvalFromSearch, approvalFromSignInState, approvalSignInState } from '../approvalLinks'
import { applicationSignInState, incidentFromSignInState } from '../incidentLinks'
import { workspaceFromSearch, workspaceFromSignInState, workspaceSignInState } from '../workspaceNavigation'
import type { AppConfig } from './types'

const sdk = vi.hoisted(() => ({
  construct: vi.fn(),
  initialize: vi.fn(),
  redirect: vi.fn(),
  accounts: vi.fn(),
  activeAccount: vi.fn(),
  setActiveAccount: vi.fn(),
  token: vi.fn(),
  popup: vi.fn(),
  login: vi.fn(),
  logout: vi.fn(),
}))

vi.mock('@azure/msal-browser', () => ({
  BrowserCacheLocation: { SessionStorage: 'sessionStorage' },
  InteractionRequiredAuthError: class extends Error {},
  PublicClientApplication: class {
    constructor(config: unknown) { sdk.construct(config) }
    initialize = sdk.initialize
    handleRedirectPromise = sdk.redirect
    getAllAccounts = sdk.accounts
    getActiveAccount = sdk.activeAccount
    setActiveAccount = sdk.setActiveAccount
    acquireTokenSilent = sdk.token
    acquireTokenPopup = sdk.popup
    loginRedirect = sdk.login
    logoutRedirect = sdk.logout
  },
}))

const config: AppConfig = {
  mode: 'live',
  app_name: 'Test command center',
  auth: {
    enabled: true,
    tenant_id: '11111111-1111-1111-1111-111111111111',
    client_id: '22222222-2222-2222-2222-222222222222',
    scope: 'api://test-command-center/access',
  },
}

beforeEach(() => {
  vi.resetAllMocks()
  sdk.initialize.mockResolvedValue(undefined)
  sdk.redirect.mockResolvedValue(null)
  sdk.accounts.mockReturnValue([])
  sdk.activeAccount.mockReturnValue(null)
  sdk.setActiveAccount.mockImplementation((account: unknown) => sdk.activeAccount.mockReturnValue(account))
  sdk.login.mockResolvedValue(undefined)
  sdk.logout.mockResolvedValue(undefined)
})

const account = {
  homeAccountId: 'home-account-1',
  localAccountId: 'user-1',
  name: 'Casey Example',
  username: 'casey.example@example.test',
}

afterEach(() => window.history.replaceState({}, '', '/'))

describe('MSAL monitoring workspace integration', () => {
  it('round-trips the monitoring deep link with the existing registered redirect URI', async () => {
    window.history.replaceState({}, '', '/?view=monitoring')
    const session = await initializeAuth(config)
    await session.signIn()
    const state = sdk.login.mock.calls[0]![0].state
    expect(workspaceFromSignInState(state)).toBe('monitoring')
    expect(sdk.construct).toHaveBeenCalledWith(expect.objectContaining({
      auth: expect.objectContaining({ redirectUri: `${window.location.origin}/`, navigateToLoginRequestUrl: false }),
    }))
    window.history.replaceState({}, '', '/?existing=kept')
    sdk.redirect.mockResolvedValue({ state, account })
    await initializeAuth(config)
    expect(workspaceFromSearch(window.location.search).section).toBe('monitoring')
    expect(new URLSearchParams(window.location.search).get('existing')).toBe('kept')
  })
  it('keeps an incident link ahead of a workspace restore and ignores unknown destinations', async () => {
    sdk.redirect.mockResolvedValue({ state: workspaceSignInState('?view=monitoring&incident=case%2B1'), account })
    await initializeAuth(config)
    expect(workspaceFromSearch(window.location.search)).toEqual({ section: 'incidents', incidentId: 'case+1' })
    window.history.replaceState({}, '', '/')
    sdk.redirect.mockResolvedValue({ state: 'command_center_workspace=https%3A%2F%2Funsafe.example', account })
    await initializeAuth(config)
    expect(window.location.search).toBe('')
  })
})

describe('MSAL approval-link integration', () => {
  it('carries approval context without changing the registered redirect URI', async () => {
    const id = 'approval/request+1'
    window.history.replaceState({}, '', `/?approval=${encodeURIComponent(id)}`)
    const session = await initializeAuth(config)
    await session.signIn()
    expect(sdk.construct).toHaveBeenCalledWith(expect.objectContaining({
      auth: expect.objectContaining({ redirectUri: `${window.location.origin}/`, navigateToLoginRequestUrl: false }),
    }))
    const request = sdk.login.mock.calls[0]![0] as { scopes: string[]; state: string }
    expect(request.scopes).toEqual([config.auth.scope])
    expect(approvalFromSignInState(request.state)).toBe(id)
    expect(sdk.initialize.mock.invocationCallOrder[0]!).toBeLessThan(sdk.redirect.mock.invocationCallOrder[0]!)
  })

  describe('own-user profile authorization', () => {
    it('exposes the signed-in identity without making a Graph request', async () => {
      sdk.accounts.mockReturnValue([account])
      const session = await initializeAuth(config)
      expect(session.currentUser).toEqual({
        id: 'user-1', display_name: 'Casey Example', username: 'casey.example@example.test',
      })
      expect(sdk.token).not.toHaveBeenCalled()
      expect(sdk.popup).not.toHaveBeenCalled()
      expect(fetch).not.toHaveBeenCalled()
    })
    it('keeps the API scope and Graph scope in separate token requests', async () => {
      sdk.accounts.mockReturnValue([account])
      sdk.token.mockImplementation(async ({ scopes }: { scopes: string[] }) => ({
        account,
        accessToken: scopes[0] === PROFILE_SCOPE ? 'graph-only-token' : 'api-only-token',
      }))
      const session = await initializeAuth(config)
      await expect(session.getToken()).resolves.toBe('api-only-token')
      await expect(session.getProfileToken!()).resolves.toBe('graph-only-token')
      expect(sdk.token.mock.calls.map(([request]) => request)).toEqual([
        { scopes: [config.auth.scope], account },
        { scopes: [PROFILE_SCOPE], account },
      ])
      expect(sdk.popup).not.toHaveBeenCalled()
      expect(sdk.login).not.toHaveBeenCalled()
    })
    it('does not open consent or sign-in on a background photo request', async () => {
      sdk.accounts.mockReturnValue([account])
      sdk.token.mockRejectedValue(new InteractionRequiredAuthError('consent_required'))
      const session = await initializeAuth(config)
      await expect(session.getProfileToken!()).rejects.toMatchObject({ status: 401, code: 'profile_interaction_required' })
      expect(sdk.popup).not.toHaveBeenCalled()
      expect(sdk.login).not.toHaveBeenCalled()
      await expect(session.getToken()).rejects.toMatchObject({ code: 'sign_in_required' })
    })
    it('opens Graph consent only when the caller explicitly requests it', async () => {
      sdk.accounts.mockReturnValue([account])
      sdk.popup.mockResolvedValue({ account, accessToken: 'graph-only-token' })
      const session = await initializeAuth(config)
      await expect(session.getProfileToken!({ interactive: true })).resolves.toBe('graph-only-token')
      expect(sdk.popup).toHaveBeenCalledExactlyOnceWith({ scopes: [PROFILE_SCOPE], account })
      expect(sdk.token).not.toHaveBeenCalled()
      expect(sdk.login).not.toHaveBeenCalled()
    })
    it('surfaces canceled or failed photo authorization without exposing SDK error content', async () => {
      sdk.accounts.mockReturnValue([account])
      sdk.popup.mockRejectedValue(new Error('Untrusted SDK diagnostics'))
      const session = await initializeAuth(config)
      await expect(session.getProfileToken!({ interactive: true })).rejects.toMatchObject({
        code: 'profile_auth_error',
        message: 'Profile photo authorization was not completed. Try again; other commands remain available.',
      })
      expect(sdk.popup).toHaveBeenCalledOnce()
    })
    it('requires a signed-in account and a nonempty Graph token', async () => {
      const signedOut = await initializeAuth(config)
      expect(signedOut.currentUser).toBeNull()
      await expect(signedOut.getProfileToken!()).rejects.toMatchObject({ code: 'profile_sign_in_required' })
      expect(sdk.token).not.toHaveBeenCalled()
      sdk.accounts.mockReturnValue([account])
      sdk.token.mockResolvedValue({ account, accessToken: '' })
      const session = await initializeAuth(config)
      await expect(session.getProfileToken!()).rejects.toMatchObject({ code: 'profile_sign_in_required' })
    })
    it('refuses a token for a different or no-longer-active account', async () => {
      sdk.accounts.mockReturnValue([account])
      sdk.popup.mockResolvedValue({ account: { ...account, homeAccountId: 'other-account' }, accessToken: 'other-token' })
      const session = await initializeAuth(config)
      await expect(session.getProfileToken!({ interactive: true })).rejects.toMatchObject({ code: 'profile_account_changed' })
      sdk.token.mockImplementation(async () => {
        sdk.activeAccount.mockReturnValue(null)
        return { account, accessToken: 'obsolete-token' }
      })
      await expect(session.getProfileToken!()).rejects.toMatchObject({ code: 'profile_account_changed' })
    })
    it('does not load another account photo under the original signed-in identity', async () => {
      sdk.accounts.mockReturnValue([account])
      const session = await initializeAuth(config)
      sdk.activeAccount.mockReturnValue({ ...account, homeAccountId: 'other-account' })
      await expect(session.getProfileToken!()).rejects.toMatchObject({ code: 'profile_account_changed' })
      expect(sdk.token).not.toHaveBeenCalled()
      expect(sdk.popup).not.toHaveBeenCalled()
    })
    it('never offers Graph access in demo or auth-disabled sessions', async () => {
      const offline = await initializeAuth({ ...config, mode: 'demo', auth: { ...config.auth, enabled: false } })
      expect(offline.getProfileToken).toBeUndefined()
      expect(sdk.construct).not.toHaveBeenCalled()
      const demo = await initializeAuth({ ...config, mode: 'demo' })
      expect(demo.getProfileToken).toBeUndefined()
      const local = await initializeAuth({ ...config, auth: { ...config.auth, enabled: false } })
      expect(local.getProfileToken).toBeUndefined()
      expect(fetch).not.toHaveBeenCalled()
    })
    it('signs out the active account and preserves asynchronous sign-out failures', async () => {
      sdk.accounts.mockReturnValue([account])
      const failure = new Error('Sign out was interrupted.')
      sdk.logout.mockRejectedValue(failure)
      const session = await initializeAuth(config)
      await expect(session.signOut()).rejects.toBe(failure)
      expect(sdk.logout).toHaveBeenCalledExactlyOnceWith({ account })
    })
  })
  it('restores the query before the application chooses its initial selection', async () => {
    const id = 'pending+approval'
    window.history.replaceState({}, '', '/?existing=kept')
    sdk.redirect.mockResolvedValue({ state: approvalSignInState(`?approval=${encodeURIComponent(id)}`), account: { homeAccountId: 'test-account' } })
    const session = await initializeAuth(config)
    expect(session.signedIn).toBe(true)
    expect(approvalFromSearch(window.location.search)).toBe(id)
    expect(new URLSearchParams(window.location.search).get('existing')).toBe('kept')
    expect(window.location.pathname).toBe('/')
  })
  it('does not infer authentication from headers or approval query parameters', async () => {
    window.history.replaceState({}, '', '/?approval=request-1')
    const session = await initializeAuth(config)
    expect(session.signedIn).toBe(false)
    await expect(session.getToken()).rejects.toMatchObject({ code: 'sign_in_required' })
    expect(sdk.token).not.toHaveBeenCalled()
  })
})

describe('MSAL incident-link integration', () => {
  it('carries the current incident and approval without changing the registered redirect URI', async () => {
    const incidentId = 'incident/refresh+1 &evidence=2'
    const approvalId = 'approval/request+1'
    window.history.replaceState({}, '', '/?incident=previous-incident')
    const session = await initializeAuth(config)
    window.history.replaceState({}, '', `/?view=incidents&incident=${encodeURIComponent(incidentId)}&approval=${encodeURIComponent(approvalId)}`)
    await session.signIn()
    const request = sdk.login.mock.calls[0]![0] as { scopes: string[]; state: string }
    expect(incidentFromSignInState(request.state)).toBe(incidentId)
    expect(approvalFromSignInState(request.state)).toBe(approvalId)
    expect(request.scopes).toEqual([config.auth.scope])
    expect(sdk.construct).toHaveBeenCalledWith(expect.objectContaining({
      auth: expect.objectContaining({
        redirectUri: `${window.location.origin}/`,
        postLogoutRedirectUri: `${window.location.origin}/`,
        navigateToLoginRequestUrl: false,
      }),
    }))
  })
  it('restores both deep links before returning, retaining unrelated URL and history state', async () => {
    const incidentId = 'incident/refresh+1 &evidence=2'
    const approvalId = 'approval/request+1'
    window.history.replaceState({ navigation: 'kept' }, '', '/?existing=kept&view=runs&incident=old&approval=old-approval#evidence')
    sdk.redirect.mockResolvedValue({
      account,
      state: applicationSignInState(`?incident=${encodeURIComponent(incidentId)}&approval=${encodeURIComponent(approvalId)}`),
    })
    const session = await initializeAuth(config)
    const params = new URLSearchParams(window.location.search)
    expect(session.signedIn).toBe(true)
    expect(params.get('incident')).toBe(incidentId)
    expect(params.get('view')).toBe('incidents')
    expect(params.get('approval')).toBe(approvalId)
    expect(params.get('existing')).toBe('kept')
    expect(window.location.pathname).toBe('/')
    expect(window.location.hash).toBe('#evidence')
    expect(window.history.state).toEqual({ navigation: 'kept' })
  })
  it('restores an incident-only redirect without inventing an approval', async () => {
    sdk.redirect.mockResolvedValue({ account, state: applicationSignInState('?incident=incident-1') })
    await initializeAuth(config)
    const params = new URLSearchParams(window.location.search)
    expect(params.get('incident')).toBe('incident-1')
    expect(params.get('view')).toBe('incidents')
    expect(params.has('approval')).toBe(false)
  })
  it.each([undefined, 'unrelated=value', 'command_center_incident=%20%20'])('leaves the URL unchanged without usable incident state (%s)', async (state) => {
    window.history.replaceState({}, '', '/?existing=kept&view=runs')
    sdk.redirect.mockResolvedValue({ account, state })
    await initializeAuth(config)
    expect(window.location.search).toBe('?existing=kept&view=runs')
  })
  it('does not treat an incident link or its redirect state as authentication', async () => {
    window.history.replaceState({}, '', '/?incident=incident-1&view=incidents')
    sdk.redirect.mockResolvedValue({ state: applicationSignInState(window.location.search) })
    const session = await initializeAuth(config)
    expect(session.signedIn).toBe(false)
    await expect(session.getToken()).rejects.toMatchObject({ code: 'sign_in_required' })
    expect(sdk.token).not.toHaveBeenCalled()
    expect(sdk.login).not.toHaveBeenCalled()
  })
})

describe('MSAL effective-access refresh', () => {
  it('forces a fresh custom API token for the same account, then leaves Graph profile tokens separate', async () => {
    sdk.accounts.mockReturnValue([account])
    let apiToken = 'initial-api-token'
    sdk.token.mockImplementation(async (request: { scopes: string[]; forceRefresh?: boolean }) => {
      if (request.forceRefresh) apiToken = 'renewed-api-token'
      return { account, accessToken: request.scopes[0] === PROFILE_SCOPE ? 'graph-only-token' : apiToken }
    })
    const session = await initializeAuth(config)
    await expect(session.getToken()).resolves.toBe('initial-api-token')
    await expect(session.refreshAccess!()).resolves.toBeUndefined()
    await expect(session.getToken()).resolves.toBe('renewed-api-token')
    await expect(session.getProfileToken!()).resolves.toBe('graph-only-token')
    expect(sdk.token.mock.calls.map(([request]) => request)).toEqual([
      { scopes: [config.auth.scope], account },
      { scopes: [config.auth.scope], account, forceRefresh: true },
      { scopes: [config.auth.scope], account },
      { scopes: [PROFILE_SCOPE], account },
    ])
    expect(sdk.popup).not.toHaveBeenCalled()
    expect(sdk.login).not.toHaveBeenCalled()
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it('does not refresh at initialization or automatically open sign-in or consent after a renewal failure', async () => {
    sdk.accounts.mockReturnValue([account])
    sdk.token.mockRejectedValue(new InteractionRequiredAuthError('interaction_required'))
    const session = await initializeAuth(config)
    expect(sdk.token).not.toHaveBeenCalled()
    await expect(session.refreshAccess!()).rejects.toMatchObject({ status: 401, code: 'sign_in_required' })
    expect(sdk.token).toHaveBeenCalledExactlyOnceWith({ scopes: [config.auth.scope], account, forceRefresh: true })
    expect(sdk.popup).not.toHaveBeenCalled()
    expect(sdk.login).not.toHaveBeenCalled()
  })

  it('preserves an unexpected renewal error without falling back to a cached token', async () => {
    sdk.accounts.mockReturnValue([account])
    const failure = new Error('API token renewal failed.')
    sdk.token.mockRejectedValue(failure)
    const session = await initializeAuth(config)
    await expect(session.refreshAccess!()).rejects.toBe(failure)
    expect(sdk.token).toHaveBeenCalledOnce()
    expect(sdk.popup).not.toHaveBeenCalled()
  })

  it.each(['', '   '])('requires a nonempty token from forced renewal (%j)', async (accessToken) => {
    sdk.accounts.mockReturnValue([account])
    sdk.token.mockResolvedValue({ account, accessToken })
    const session = await initializeAuth(config)
    await expect(session.refreshAccess!()).rejects.toMatchObject({ status: 401, code: 'sign_in_required' })
    expect(sdk.token).toHaveBeenCalledOnce()
  })

  it('requires an unambiguous signed-in account without selecting another cached account', async () => {
    sdk.accounts.mockReturnValue([account, { ...account, homeAccountId: 'other-account' }])
    const session = await initializeAuth(config)
    expect(session.signedIn).toBe(false)
    await expect(session.refreshAccess!()).rejects.toMatchObject({ code: 'sign_in_required' })
    expect(sdk.token).not.toHaveBeenCalled()
    expect(sdk.setActiveAccount).not.toHaveBeenCalled()
    expect(sdk.popup).not.toHaveBeenCalled()
  })

  it.each([
    { ...account, homeAccountId: 'other-account' },
    { ...account, localAccountId: 'other-user' },
    { ...account, tenantId: 'other-tenant' },
  ])('refuses account or tenant changes before any API token request (%j)', async (other) => {
    sdk.accounts.mockReturnValue([account])
    const session = await initializeAuth(config)
    sdk.activeAccount.mockReturnValue(other)
    await expect(session.refreshAccess!()).rejects.toMatchObject({ code: 'access_account_changed' })
    await expect(session.getToken()).rejects.toMatchObject({ code: 'access_account_changed' })
    expect(sdk.token).not.toHaveBeenCalled()
    expect(sdk.popup).not.toHaveBeenCalled()
  })

  it.each([null, { ...account, homeAccountId: 'other-account' }])('refuses a missing or mismatched account in a renewed token (%j)', async (returnedAccount) => {
    sdk.accounts.mockReturnValue([account])
    sdk.token.mockResolvedValue({ account: returnedAccount, accessToken: 'unexpected-account-token' })
    const session = await initializeAuth(config)
    await expect(session.refreshAccess!()).rejects.toMatchObject({ code: 'access_account_changed' })
    expect(sdk.popup).not.toHaveBeenCalled()
  })

  it.each([null, { ...account, homeAccountId: 'other-account' }])('refuses sign-out or an account switch while renewal is pending (%j)', async (active) => {
    sdk.accounts.mockReturnValue([account])
    sdk.token.mockImplementation(async () => {
      sdk.activeAccount.mockReturnValue(active)
      return { account, accessToken: 'obsolete-account-token' }
    })
    const session = await initializeAuth(config)
    await expect(session.refreshAccess!()).rejects.toMatchObject({ code: 'access_account_changed' })
    expect(sdk.token).toHaveBeenCalledOnce()
    expect(sdk.popup).not.toHaveBeenCalled()
  })

  it('applies the same in-flight account check to ordinary API reads', async () => {
    sdk.accounts.mockReturnValue([account])
    sdk.token.mockImplementation(async () => {
      sdk.activeAccount.mockReturnValue(null)
      return { account, accessToken: 'obsolete-account-token' }
    })
    const session = await initializeAuth(config)
    await expect(session.getToken()).rejects.toMatchObject({ code: 'access_account_changed' })
  })

  it.each([false, true])('keeps demo refresh a no-op even when auth is configured (enabled=%s)', async (enabled) => {
    sdk.accounts.mockReturnValue([account])
    const session = await initializeAuth({ ...config, mode: 'demo', auth: { ...config.auth, enabled } })
    expect(session.refreshAccess).toBeTypeOf('function')
    await expect(session.refreshAccess!()).resolves.toBeUndefined()
    expect(sdk.token).not.toHaveBeenCalled()
    expect(sdk.popup).not.toHaveBeenCalled()
    expect(sdk.login).not.toHaveBeenCalled()
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it('does not offer token renewal for an auth-disabled live session', async () => {
    const session = await initializeAuth({ ...config, auth: { ...config.auth, enabled: false } })
    expect(session.refreshAccess).toBeUndefined()
    expect(sdk.construct).not.toHaveBeenCalled()
  })
})
