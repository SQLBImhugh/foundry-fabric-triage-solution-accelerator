import { applicationSignInState, incidentFromSearch, incidentUrl } from './incidentLinks'

const sections = ['command', 'incidents', 'runs', 'knowledge', 'validation', 'monitoring', 'access'] as const
export type WorkspaceSection = typeof sections[number]
const signInStateKey = 'command_center_workspace'

export function workspaceFromSearch(search: string): { section: WorkspaceSection; incidentId: string | null } {
  const incidentId = incidentFromSearch(search)
  if (incidentId) return { section: 'incidents', incidentId }
  const view = new URLSearchParams(search).get('view')
  if (view === 'admin') return { section: 'access', incidentId: null }
  return { section: sections.find((section) => section === view) ?? 'command', incidentId: null }
}

export function workspaceUrl(section: WorkspaceSection, current = window.location.href): string {
  if (section === 'incidents') return incidentUrl(null, current)
  const url = new URL(current)
  url.searchParams.delete('approval')
  url.searchParams.delete('incident')
  if (section === 'command') url.searchParams.delete('view')
  else url.searchParams.set('view', section)
  return `${url.pathname}${url.search}${url.hash}`
}

export function workspaceSignInState(search: string): string | undefined {
  const state = new URLSearchParams(applicationSignInState(search))
  const { section } = workspaceFromSearch(search)
  if (section !== 'command') state.set(signInStateKey, section)
  return state.size ? state.toString() : undefined
}

export function workspaceFromSignInState(state: string | undefined): WorkspaceSection | null {
  const value = new URLSearchParams(state).get(signInStateKey)
  return sections.find((section) => section === value) ?? null
}
