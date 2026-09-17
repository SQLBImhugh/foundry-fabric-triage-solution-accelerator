import { ApiError } from './errors'
import type { AppConfig } from './types'
import type { TokenProvider } from './client'
import { PROFILE_SCOPE } from './profile'
import type { ProfileTokenProvider } from './profile'
import { approvalFromSignInState } from '../approvalLinks'
import { incidentFromSignInState } from '../incidentLinks'
import { workspaceFromSignInState, workspaceSignInState } from '../workspaceNavigation'

export interface AuthUser {
  id: string
  display_name: string
  username: string
}

export interface AuthSession {
  enabled: boolean
  signedIn: boolean
  getToken: TokenProvider
  currentUser?: AuthUser | null
  getProfileToken?: ProfileTokenProvider
  refreshAccess?: () => Promise<void>
  signIn: () => Promise<void>
  signOut: () => Promise<void>
}

export async function initializeAuth(config: AppConfig): Promise<AuthSession> {
  if (!config.auth.enabled) {
    return {
      enabled: false,
      signedIn: true,
      getToken: async () => null,
      refreshAccess: config.mode === 'demo' ? async () => undefined : undefined,
      signIn: async () => undefined,
      signOut: async () => undefined,
    }
  }
  const { tenant_id, client_id, scope } = config.auth
  const guid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
  if (!guid.test(tenant_id) || !guid.test(client_id) || typeof scope !== 'string' || !scope.trim()) {
    throw new ApiError(0, 'invalid_auth_config', 'Entra sign-in is enabled but its tenant, public client, or API scope is not configured.')
  }
  const { PublicClientApplication, BrowserCacheLocation, InteractionRequiredAuthError } = await import('@azure/msal-browser')
  const redirectUri = `${window.location.origin}${window.location.pathname}`
  const client = new PublicClientApplication({
    auth: {
      clientId: client_id,
      authority: `https://login.microsoftonline.com/${tenant_id}`,
      redirectUri,
      postLogoutRedirectUri: redirectUri,
      navigateToLoginRequestUrl: false,
    },
    cache: { cacheLocation: BrowserCacheLocation.SessionStorage },
  })
  await client.initialize()
  const redirect = await client.handleRedirectPromise()
  const approval = approvalFromSignInState(redirect?.state)
  const incident = incidentFromSignInState(redirect?.state)
  const workspace = workspaceFromSignInState(redirect?.state)
  if (approval || incident || workspace) {
    // Preserve deep links without requiring a distinct SPA redirect URI per request.
    const url = new URL(window.location.href)
    if (workspace) url.searchParams.set('view', workspace)
    if (approval) url.searchParams.set('approval', approval)
    if (incident) {
      url.searchParams.set('incident', incident)
      url.searchParams.set('view', 'incidents')
    }
    window.history.replaceState(window.history.state, '', url)
  }
  const accounts = client.getAllAccounts()
  const account = redirect?.account ?? client.getActiveAccount() ?? (accounts.length === 1 ? accounts[0] : null)
  if (account) client.setActiveAccount(account)
  const scopes = [scope]
  const sameAccount = (candidate: typeof account): boolean => Boolean(candidate && account
    && candidate.homeAccountId === account.homeAccountId
    && candidate.localAccountId === account.localAccountId
    && candidate.tenantId === account.tenantId)
  const acquireApiToken = async (forceRefresh = false): Promise<string> => {
    const activeAccount = client.getActiveAccount()
    if (!activeAccount) throw new ApiError(401, 'sign_in_required', 'Sign in with your work account to continue.')
    if (!sameAccount(activeAccount)) {
      throw new ApiError(401, 'access_account_changed', 'The signed-in account changed. Reload the app before continuing.')
    }
    try {
      const response = await client.acquireTokenSilent({
        scopes, account: activeAccount, ...(forceRefresh ? { forceRefresh: true } : {}),
      })
      if (!response.accessToken?.trim()) throw new ApiError(401, 'sign_in_required', 'Sign in again to obtain access to this API.')
      if (!sameAccount(response.account) || !sameAccount(client.getActiveAccount())) {
        throw new ApiError(401, 'access_account_changed', 'The signed-in account changed. Reload the app before continuing.')
      }
      return response.accessToken
    } catch (error) {
      if (error instanceof InteractionRequiredAuthError) {
        throw new ApiError(401, 'sign_in_required', 'Your session needs attention. Sign in again; no action was submitted.')
      }
      throw error
    }
  }
  return {
    enabled: true,
    signedIn: Boolean(account),
    currentUser: account ? {
      id: account.localAccountId || account.homeAccountId,
      display_name: account.name?.trim() || account.username || '',
      username: account.username || '',
    } : null,
    getToken: () => acquireApiToken(),
    refreshAccess: config.mode === 'demo' ? async () => undefined : async () => { await acquireApiToken(true) },
    getProfileToken: config.mode === 'live' ? async ({ interactive = false } = {}) => {
      const activeAccount = client.getActiveAccount()
      if (!activeAccount) throw new ApiError(401, 'profile_sign_in_required', 'Sign in to load your profile photo.')
      if (activeAccount.homeAccountId !== account?.homeAccountId) {
        throw new ApiError(401, 'profile_account_changed', 'The signed-in account changed. Reload before loading a profile photo.')
      }
      try {
        // The API and Graph have different audiences. Only an explicit menu action may open consent.
        const request = { scopes: [PROFILE_SCOPE], account: activeAccount }
        const response = await (interactive ? client.acquireTokenPopup(request) : client.acquireTokenSilent(request))
        if (!response.accessToken) {
          throw new ApiError(401, 'profile_sign_in_required', 'Microsoft sign-in did not return access to your profile photo.')
        }
        if (response.account?.homeAccountId !== activeAccount.homeAccountId
          || client.getActiveAccount()?.homeAccountId !== activeAccount.homeAccountId) {
          throw new ApiError(401, 'profile_account_changed', 'The signed-in account changed. Reload before loading a profile photo.')
        }
        return response.accessToken
      } catch (error) {
        if (error instanceof ApiError) throw error
        if (error instanceof InteractionRequiredAuthError) {
          throw new ApiError(401, 'profile_interaction_required', 'Your profile photo needs sign-in or consent. Select Connect profile photo to continue.')
        }
        throw new ApiError(0, 'profile_auth_error', 'Profile photo authorization was not completed. Try again; other commands remain available.')
      }
    } : undefined,
    signIn: () => client.loginRedirect({ scopes, prompt: 'select_account', state: workspaceSignInState(window.location.search) }),
    signOut: () => client.logoutRedirect({ account: client.getActiveAccount() }),
  }
}
