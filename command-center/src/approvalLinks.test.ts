import { describe, expect, it } from 'vitest'
import { approvalFromSearch, approvalFromSignInState, approvalSignInState } from './approvalLinks'

describe('approval links', () => {
  it('decodes an opaque request ID exactly once', () => {
    const id = 'approval+id /?=&%20'
    expect(approvalFromSearch(`?approval=${encodeURIComponent(id)}`)).toBe(id)
  })
  it('does not select an approval from missing or blank query parameters', () => {
    expect(approvalFromSearch('?other=value')).toBeNull()
    expect(approvalFromSearch('?approval=%20%20')).toBeNull()
    expect(approvalFromSearch('?approval=')).toBeNull()
  })
  it('round-trips an approval through namespaced MSAL application state', () => {
    const id = 'pending/request+1'
    const state = approvalSignInState(`?approval=${encodeURIComponent(id)}`)
    expect(approvalFromSignInState(state)).toBe(id)
    expect(approvalFromSignInState('approval=unrelated')).toBeNull()
    expect(approvalSignInState('')).toBeUndefined()
  })
})
