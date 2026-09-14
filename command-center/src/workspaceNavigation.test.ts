import { describe, expect, it } from 'vitest'
import { workspaceFromSearch, workspaceUrl } from './workspaceNavigation'

describe('workspace navigation', () => {
  it('opens incident links directly and preserves opaque IDs', () => {
    expect(workspaceFromSearch('?incident=case%2B1')).toEqual({ section: 'incidents', incidentId: 'case+1' })
    expect(workspaceFromSearch('?view=incidents')).toEqual({ section: 'incidents', incidentId: null })
  })
  it('restores known pages and ignores unrecognized views without changing data', () => {
    expect(workspaceFromSearch('?view=access').section).toBe('access')
    expect(workspaceFromSearch('?view=admin').section).toBe('access')
    expect(workspaceFromSearch('?view=unrecognized').section).toBe('command')
  })
  it('preserves incident precedence over both current and legacy access links', () => {
    expect(workspaceFromSearch('?view=access&incident=case%2B1')).toEqual({ section: 'incidents', incidentId: 'case+1' })
    expect(workspaceFromSearch('?view=admin&incident=case%2B1')).toEqual({ section: 'incidents', incidentId: 'case+1' })
  })
  it('does not carry a selected incident or approval into another workspace', () => {
    const current = 'https://example.test/?incident=old&approval=old&other=kept#details'
    expect(workspaceUrl('access', current)).toBe('/?other=kept&view=access#details')
    expect(workspaceUrl('command', current)).toBe('/?other=kept#details')
    expect(workspaceUrl('incidents', current)).toBe('/?other=kept&view=incidents#details')
  })
  it('writes the canonical read-only access URL when leaving a legacy admin link', () => {
    expect(workspaceUrl('access', 'https://example.test/app?view=admin&other=kept')).toBe('/app?view=access&other=kept')
  })
})
