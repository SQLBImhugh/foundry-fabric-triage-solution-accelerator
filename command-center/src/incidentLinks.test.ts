import { describe, expect, it } from 'vitest'
import { approvalFromSignInState } from './approvalLinks'
import { applicationSignInState, incidentFromSearch, incidentFromSignInState, incidentUrl } from './incidentLinks'

describe('incident links', () => {
  it('round-trips opaque incident IDs through a same-origin query string', () => {
    const id = 'case+id /?=&%20'
    const path = incidentUrl(id, 'https://example.test/?approval=old&other=retained')
    const url = new URL(path, 'https://example.test')
    expect(url.origin).toBe('https://example.test')
    expect(url.searchParams.get('view')).toBe('incidents')
    expect(url.searchParams.has('approval')).toBe(false)
    expect(url.searchParams.get('other')).toBe('retained')
    expect(incidentFromSearch(url.search)).toBe(id)
  })
  it('opens the incident register without retaining a selected incident', () => {
    expect(incidentUrl(null, 'https://example.test/?incident=old')).toBe('/?view=incidents')
    expect(incidentFromSearch('?incident=%20')).toBeNull()
    expect(incidentFromSearch('')).toBeNull()
  })
  it('preserves incident and approval links through namespaced sign-in state', () => {
    const state = applicationSignInState('?incident=case%2B1&approval=request%2B1')
    expect(incidentFromSignInState(state)).toBe('case+1')
    expect(approvalFromSignInState(state)).toBe('request+1')
    expect(incidentFromSignInState('incident=not-application-state')).toBeNull()
    expect(applicationSignInState('')).toBeUndefined()
  })
})
