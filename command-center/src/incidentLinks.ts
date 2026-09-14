import { approvalSignInState } from './approvalLinks'

const authStateKey = 'command_center_incident'

export function incidentFromSearch(search: string): string | null {
  const value = new URLSearchParams(search).get('incident')
  return value?.trim() ? value : null
}

export function incidentFromSignInState(state: string | undefined): string | null {
  const value = new URLSearchParams(state).get(authStateKey)
  return value?.trim() ? value : null
}

export function applicationSignInState(search: string): string | undefined {
  const state = new URLSearchParams(approvalSignInState(search))
  const incident = incidentFromSearch(search)
  if (incident) state.set(authStateKey, incident)
  return state.size ? state.toString() : undefined
}

export function incidentUrl(id: string | null, current = window.location.href): string {
  const url = new URL(current)
  url.searchParams.delete('approval')
  url.searchParams.set('view', 'incidents')
  if (id) url.searchParams.set('incident', id)
  else url.searchParams.delete('incident')
  return `${url.pathname}${url.search}${url.hash}`
}
