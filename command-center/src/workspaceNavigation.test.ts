import { describe, expect, it } from 'vitest'
import { workspaceFromSearch, workspaceFromSignInState, workspaceSignInState, workspaceUrl } from './workspaceNavigation'
import { incidentFromSignInState } from './incidentLinks'
import { approvalFromSignInState } from './approvalLinks'

describe('workspace navigation', () => {
  it('opens incident links directly and preserves opaque IDs', () => {
    expect(workspaceFromSearch('?incident=case%2B1')).toEqual({ section: 'incidents', incidentId: 'case+1' })
    expect(workspaceFromSearch('?view=incidents')).toEqual({ section: 'incidents', incidentId: null })
  })
  it('restores known pages and ignores unrecognized views without changing data', () => {
    expect(workspaceFromSearch('?view=access').section).toBe('access')
    expect(workspaceFromSearch('?view=admin').section).toBe('access')
    expect(workspaceFromSearch('?view=monitoring').section).toBe('monitoring')
    expect(workspaceFromSearch('?view=unrecognized').section).toBe('command')
  })
  it('preserves incident precedence over both current and legacy access links', () => {
    expect(workspaceFromSearch('?view=access&incident=case%2B1')).toEqual({ section: 'incidents', incidentId: 'case+1' })
    expect(workspaceFromSearch('?view=admin&incident=case%2B1')).toEqual({ section: 'incidents', incidentId: 'case+1' })
  })
  it('does not carry a selected incident or approval into another workspace', () => {
    const current = 'https://example.test/?incident=old&approval=old&other=kept#details'
    expect(workspaceUrl('access', current)).toBe('/?other=kept&view=access#details')
    expect(workspaceUrl('monitoring', current)).toBe('/?other=kept&view=monitoring#details')
    expect(workspaceUrl('command', current)).toBe('/?other=kept#details')
    expect(workspaceUrl('incidents', current)).toBe('/?other=kept&view=incidents#details')
  })
  it('writes the canonical read-only access URL when leaving a legacy admin link', () => {
    expect(workspaceUrl('access', 'https://example.test/app?view=admin&other=kept')).toBe('/app?view=access&other=kept')
  })
  it('preserves Monitoring setup through sign-in without encoding an arbitrary return URL', () => {
    const state = workspaceSignInState('?view=monitoring&returnTo=https://unsafe.example/')
    expect(workspaceFromSignInState(state)).toBe('monitoring')
    expect(state).not.toContain('unsafe')
    expect(workspaceFromSignInState('command_center_workspace=https%3A%2F%2Funsafe.example')).toBeNull()
    expect(workspaceFromSignInState('command_center_workspace=unrecognized')).toBeNull()
    expect(workspaceFromSignInState(undefined)).toBeNull()
    expect(workspaceSignInState('?view=unrecognized')).toBeUndefined()
  })
  it('retains incident precedence and existing approval state through workspace sign-in', () => {
    const state = workspaceSignInState('?view=monitoring&incident=case%2B1&approval=request%2F2')
    expect(workspaceFromSignInState(state)).toBe('incidents')
    expect(incidentFromSignInState(state)).toBe('case+1')
    expect(approvalFromSignInState(state)).toBe('request/2')
  })
})
