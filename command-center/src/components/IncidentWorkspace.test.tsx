import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it, vi } from 'vitest'
import { ApiError, isRecord } from '../api/errors'
import { IncidentApiClient } from '../api/incidents'
import type { IncidentActivity, IncidentCase, IncidentPage } from '../api/incidents'
import { detail, run, workItem } from '../test/fixtures'
import { IncidentWorkspace } from './IncidentWorkspace'
import type { IncidentWorkspaceProps } from './IncidentWorkspace'

function incident(id = 'incident-1'): IncidentCase {
  return {
    detail: {
      ...detail, proposal: null,
      item: { ...workItem, id: `incident:${id}`, kind: 'incident', source_id: id, incident_id: id, title: `Incident ${id}`, status: 'needs_review', can_decide: false },
      incident: { incident_id: id, status: 'open' },
      runs: [{ ...run, id: `run-${id}`, incident_id: id }],
    },
    tracking: { status: 'open', version: 0, source_revision: `evidence-${id}-1`, resolved_at: null, resolved_by: null, resolution_note: null },
    activity: [],
    capabilities: { note: true, resolve: true, ask: true },
  }
}

function activity(id: string, kind: IncidentActivity['kind'], body: string, overrides: Partial<IncidentActivity> = {}): IncidentActivity {
  return {
    id, incident_id: 'incident-1', kind, body, created_at: '2026-01-01T12:30:00Z',
    user_id: 'operator-1', user_name: 'Incident operator', status: 'recorded',
    correlation_id: null, mode: null, ...overrides,
  }
}

function appendNote(value: IncidentCase, body: string): IncidentCase {
  return { ...value, activity: [...value.activity, activity(`note-${value.activity.length + 1}`, 'note', body, { incident_id: value.detail.item.source_id })] }
}

function resolved(value: IncidentCase, reason: string): IncidentCase {
  return {
    ...value,
    tracking: { ...value.tracking, status: 'resolved_by_user', version: value.tracking.version + 1, resolved_at: '2026-01-01T12:30:00Z', resolved_by: 'Incident operator', resolution_note: reason },
    activity: [...value.activity, activity('resolution-1', 'resolution', reason)],
  }
}

function discussion(value: IncidentCase, question: string, mode: 'records' | 'model'): IncidentCase {
  return {
    ...value, activity: [
      ...value.activity,
      activity('question-1', 'question', question, { status: 'completed', mode, correlation_id: 'question-1' }),
      activity('answer-1', 'answer', 'The recorded scan found a transient gateway failure. No repair has been verified.', {
        status: 'completed', mode, user_name: 'Read-only agent', user_id: 'incident-observer', correlation_id: 'question-1',
      }),
    ],
  }
}

function page(items = [incident().detail.item], total = items.length, offset = 0): IncidentPage {
  return { items, total, offset, limit: 25 }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((accept, decline) => { resolve = accept; reject = decline })
  return { promise, resolve, reject }
}

function response(value: IncidentCase): Response {
  return new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } })
}

function submittedKey(options: RequestInit | undefined): string {
  const body: unknown = JSON.parse(String(options?.body))
  if (!isRecord(body) || typeof body.idempotency_key !== 'string') throw new Error('Expected a submission key in the test request.')
  return body.idempotency_key
}

function server(...values: IncidentCase[]) {
  const records = new Map(values.map((value) => [value.detail.item.source_id, value]))
  const api = new IncidentApiClient(async () => null)
  const read = vi.spyOn(api, 'detail').mockImplementation(async (id) => {
    const value = records.get(id)
    if (!value) throw new ApiError(404, 'incident_not_found', 'This incident was not found.')
    return structuredClone(value)
  })
  return { api, records, read }
}

function mount(api: IncidentApiClient, options: Partial<IncidentWorkspaceProps> = {}) {
  const props: IncidentWorkspaceProps = {
    api, selectedId: 'incident-1', onSelect: vi.fn(), onBack: vi.fn(), onOpenRun: vi.fn(),
    onChanged: vi.fn(), fresh: true, ...options,
  }
  return { ...render(<IncidentWorkspace {...props} />), props }
}

async function ready() {
  return screen.findByRole('heading', { name: 'Incident incident-1', level: 1 })
}

describe('incident workspace theme', () => {
  it('inherits the existing font, Onyx palettes, Ink corners, and shadows', () => {
    const styles = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'IncidentWorkspace.css'), 'utf8')
    expect(styles).not.toMatch(/#[\da-f]{3,8}\b|rgba?\s*\(|hsla?\s*\(|font-family\s*:|:root\s*\{/i)
    expect(styles).toContain('border-radius: var(--radius)')
    expect(styles).toContain('box-shadow: var(--shadow)')
    const colors = styles.matchAll(/(?<![\w-])(?:color|background(?:-color)?|border(?:-(?:top|bottom|left|right))?(?:-color)?|accent-color)\s*:\s*([^;]+);/g)
    for (const declaration of colors) expect(declaration[1] === '0' || declaration[1]?.includes('var(--')).toBe(true)
  })
})

describe('readable execution explanations', () => {
  it('renders structured prose, gives runs meaningful labels, and preserves expansion during refresh', async () => {
    const user = userEvent.setup()
    const value = incident()
    value.detail.runs = [
      { ...run, id: 'latest-run', agent_name: 'Incident observer', summary: '## Observed failure\n\n**Refresh failed** for `Sales_Model`.\n\n- Review source access\n- Check refresh history' },
      { ...run, id: 'older-run', agent_name: 'TriageAgent', summary: '**Earlier** recorded context.' },
    ]
    const backend = server(value)
    mount(backend.api)
    await ready()
    const executions = within(screen.getByRole('list', { name: 'Incident executions' }))
    const latest = executions.getByRole('button', { name: 'Open run latest-run' })
    expect(latest.textContent).toContain('Incident observer')
    expect(latest.textContent).not.toContain('latest-run')
    expect(executions.getByRole('heading', { name: 'Observed failure', level: 4 })).toBeTruthy()
    expect(executions.getByText('Sales_Model', { selector: 'code' })).toBeTruthy()
    const older = executions.getByRole('button', { name: 'Open run older-run' }).closest('li')!
    expect(older.querySelector('details')!.open).toBe(false)
    await user.click(within(older).getByText('Run explanation'))
    await waitFor(() => expect(older.querySelector('details')!.open).toBe(true))
    backend.records.set('incident-1', {
      ...value, detail: { ...value.detail, runs: [{ ...run, id: 'newer-run', summary: 'New execution.' }, ...value.detail.runs] },
    })
    await user.click(screen.getByRole('button', { name: 'Refresh incident' }))
    await screen.findByRole('button', { name: 'Open run newer-run' })
    expect(screen.getByRole('button', { name: 'Open run older-run' }).closest('li')!.querySelector('details')!.open).toBe(true)
  })
})

describe('incident directory and record navigation', () => {
  it('opens a full-page incident through the public selection props and exposes recorded run links', async () => {
    const user = userEvent.setup()
    const { api } = server(incident())
    vi.spyOn(api, 'list').mockResolvedValue(page())
    const view = mount(api, { selectedId: null })
    const row = await screen.findByRole('button', { name: 'Open incident incident-1: Incident incident-1' })
    expect(within(row).getByText('Needs investigation')).toBeTruthy()
    expect(screen.queryByRole('complementary')).toBeNull()
    await user.click(row)
    expect(view.props.onSelect).toHaveBeenCalledWith('incident-1')
    view.rerender(<IncidentWorkspace {...view.props} selectedId="incident-1" />)
    await ready()
    expect(screen.getByRole('article', { name: 'Full incident record' })).toBeTruthy()
    expect(screen.queryByRole('complementary')).toBeNull()
    await user.click(screen.getByRole('button', { name: 'Open run run-incident-1' }))
    expect(view.props.onOpenRun).toHaveBeenCalledWith('run-incident-1')
    await user.click(screen.getByRole('button', { name: 'Back to incident list' }))
    expect(view.props.onBack).toHaveBeenCalledOnce()
  })

  it('uses paged server queries, canonical investigation filters, and stable workload choices', async () => {
    const user = userEvent.setup()
    const { api } = server(incident())
    const list = vi.spyOn(api, 'list').mockImplementation(async (query) => page([incident().detail.item], 51, query?.offset ?? 0))
    mount(api, { selectedId: null })
    await screen.findByRole('button', { name: /Open incident incident-1:/ })
    await user.click(screen.getByRole('button', { name: 'Next page' }))
    await waitFor(() => expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 25, limit: 25 }), expect.any(AbortSignal)))
    await user.selectOptions(screen.getByLabelText('Status'), 'needs_investigation')
    await user.selectOptions(screen.getByLabelText('Workload'), 'fabric_pipeline')
    await user.type(screen.getByRole('searchbox', { name: 'Search incidents' }), 'needs_review & gateway')
    await user.click(screen.getByRole('button', { name: 'Search' }))
    await waitFor(() => expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 0, status: 'needs_investigation', workload: 'fabric_pipeline', query: 'needs_review & gateway' }), expect.any(AbortSignal)))
    expect(screen.getByRole('option', { name: 'Power BI' })).toBeTruthy()
    expect(screen.getByRole('option', { name: 'Fabric pipeline' })).toBeTruthy()
    expect(screen.getByRole('option', { name: 'Resolved by user' })).toBeTruthy()
    expect(screen.getByRole('searchbox')).toHaveProperty('maxLength', 200)
    const offeredStatuses = within(screen.getByLabelText('Status')).getAllByRole('option').map((option) => option.getAttribute('value'))
    expect(offeredStatuses).not.toContain('verification_pending')
  })

  it('opens signature-related runs by run ID even when their incident is earlier or unrecorded', async () => {
    const user = userEvent.setup()
    const value = incident()
    value.detail.runs = [
      { ...run, id: 'earlier-run', incident_id: 'earlier-incident' },
      { ...run, id: 'unlinked-run', incident_id: '' },
    ]
    const { api } = server(value)
    const view = mount(api)
    await ready()
    expect(screen.getByText(/History can include earlier incidents with the same failure signature/)).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Open run earlier-run' }))
    await user.click(screen.getByRole('button', { name: 'Open run unlinked-run' }))
    expect(view.props.onOpenRun).toHaveBeenNthCalledWith(1, 'earlier-run')
    expect(view.props.onOpenRun).toHaveBeenNthCalledWith(2, 'unlinked-run')
  })

  it('retains the incident search when returning from a selected record', async () => {
    const user = userEvent.setup()
    const { api } = server(incident())
    vi.spyOn(api, 'list').mockResolvedValue(page())
    const view = mount(api, { selectedId: null })
    await screen.findByRole('button', { name: /Open incident incident-1:/ })
    await user.type(screen.getByRole('searchbox'), 'gateway')
    await user.click(screen.getByRole('button', { name: 'Search' }))
    view.rerender(<IncidentWorkspace {...view.props} selectedId="incident-1" />)
    await ready()
    view.rerender(<IncidentWorkspace {...view.props} selectedId={null} />)
    expect(screen.getByRole('searchbox')).toHaveProperty('value', 'gateway')
  })

  it('labels the combined closed-status filter without conflating manual and verified resolution', async () => {
    const user = userEvent.setup()
    const { api } = server()
    const verified = { ...incident('verified-case').detail.item, status: 'resolved' }
    const manual = { ...incident('manual-case').detail.item, status: 'resolved_by_user' }
    const list = vi.spyOn(api, 'list').mockImplementation(async (query) => page(query?.status === 'resolved' ? [verified, manual] : []))
    mount(api, { selectedId: null })
    await screen.findByRole('heading', { name: 'No incidents match' })
    await user.selectOptions(screen.getByLabelText('Status'), screen.getByRole('option', { name: 'All closed incidents' }))
    const verifiedRow = await screen.findByRole('button', { name: 'Open incident verified-case: Incident verified-case' })
    const manualRow = screen.getByRole('button', { name: 'Open incident manual-case: Incident manual-case' })
    expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ status: 'resolved', offset: 0 }), expect.any(AbortSignal))
    expect(within(verifiedRow).getByText('Resolved')).toBeTruthy()
    expect(within(manualRow).getByText('Resolved by user')).toBeTruthy()
  })

  it('shows an explicit load failure and an honest empty state after refresh', async () => {
    const user = userEvent.setup()
    const { api } = server()
    vi.spyOn(api, 'list').mockRejectedValueOnce(new ApiError(503, 'incident_store_unavailable', 'Incident records are unavailable.')).mockResolvedValue(page([]))
    mount(api, { selectedId: null })
    expect(await screen.findByText('Incident records are unavailable.')).toBeTruthy()
    expect(screen.queryByRole('button', { name: /Open incident / })).toBeNull()
    await user.click(screen.getByRole('button', { name: 'Refresh incidents' }))
    expect(await screen.findByRole('heading', { name: 'No incidents match' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Previous page' })).toHaveProperty('disabled', true)
    expect(screen.getByRole('button', { name: 'Next page' })).toHaveProperty('disabled', true)
    expect(screen.getByRole('option', { name: 'Fabric pipeline' })).toBeTruthy()
  })

  it('allows returning from an empty later page when records shrink', async () => {
    const user = userEvent.setup()
    const { api } = server()
    const list = vi.spyOn(api, 'list').mockResolvedValueOnce(page([incident().detail.item], 26)).mockResolvedValueOnce(page([], 1, 25)).mockResolvedValue(page())
    mount(api, { selectedId: null })
    await screen.findByRole('button', { name: /Open incident incident-1:/ })
    await user.click(screen.getByRole('button', { name: 'Next page' }))
    expect(await screen.findByRole('heading', { name: 'No incidents match' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Previous page' })).toHaveProperty('disabled', false)
    await user.click(screen.getByRole('button', { name: 'Previous page' }))
    await waitFor(() => expect(list).toHaveBeenLastCalledWith(expect.objectContaining({ offset: 0 }), expect.any(AbortSignal)))
  })

  it('does not render collaborative controls for an unavailable record', async () => {
    const user = userEvent.setup()
    const { api, records } = server()
    mount(api)
    expect(await screen.findByText('This incident was not found.')).toBeTruthy()
    expect(screen.queryByRole('textbox')).toBeNull()
    records.set('incident-1', incident())
    await user.click(screen.getByRole('button', { name: 'Refresh incident' }))
    await ready()
  })
})

describe('incident notes and permissions', () => {
  it('saves only on confirmation, excludes duplicate clicks, preserves newer draft edits, and reloads saved notes', async () => {
    const user = userEvent.setup()
    const initial = appendNote(incident(), 'Original operator note.')
    const { api, records } = server(initial)
    const pending = deferred<IncidentCase>()
    const save = vi.spyOn(api, 'addNote').mockReturnValue(pending.promise)
    const view = mount(api)
    await ready()
    const field = screen.getByRole('textbox', { name: 'Add a note' })
    await user.type(field, 'Investigated timing.')
    await user.dblClick(screen.getByRole('button', { name: 'Save note' }))
    expect(save).toHaveBeenCalledOnce()
    expect(field).toHaveProperty('value', 'Investigated timing.')
    expect(within(screen.getByRole('list', { name: 'Incident notes' })).queryByText('Investigated timing.')).toBeNull()
    await user.type(field, ' Added context.')
    const updated = appendNote(initial, 'Investigated timing.')
    records.set('incident-1', updated)
    await act(async () => pending.resolve(updated))
    expect(await screen.findByText('Note saved to the incident record.')).toBeTruthy()
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', 'Investigated timing. Added context.')
    expect(within(screen.getByRole('list', { name: 'Incident notes' })).getByText('Investigated timing.')).toBeTruthy()
    expect(view.props.onChanged).toHaveBeenCalledOnce()
    view.unmount()
    mount(api)
    await ready()
    expect(within(screen.getByRole('list', { name: 'Incident notes' })).getByText('Investigated timing.')).toBeTruthy()
    expect(screen.queryByRole('button', { name: /Delete note|Edit note/ })).toBeNull()
  })

  it('clears an unchanged draft only after a confirmed save', async () => {
    const user = userEvent.setup()
    const { api, records } = server(incident())
    vi.spyOn(api, 'addNote').mockImplementation(async (_id, body) => {
      const value = appendNote(incident(), body)
      records.set('incident-1', value)
      return value
    })
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Add a note' }), 'Saved finding.')
    await user.click(screen.getByRole('button', { name: 'Save note' }))
    await screen.findByText('Note saved to the incident record.')
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', '')
    expect(screen.getByText('Saved finding.')).toBeTruthy()
  })

  it('retains an uncertain note and reuses its key only when the user explicitly retries', async () => {
    const user = userEvent.setup()
    const { api, records } = server(incident())
    const save = vi.spyOn(api, 'addNote').mockRejectedValueOnce(new ApiError(0, 'network_error', 'The submission could not be confirmed.'))
      .mockImplementation(async (_id, body) => {
        const value = appendNote(incident(), body)
        records.set('incident-1', value)
        return value
      })
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Add a note' }), 'Uncertain finding.')
    await user.click(screen.getByRole('button', { name: 'Save note' }))
    await screen.findByText('The submission could not be confirmed.')
    expect(save).toHaveBeenCalledOnce()
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', 'Uncertain finding.')
    const key = save.mock.calls[0]![2]
    await user.click(screen.getByRole('button', { name: 'Refresh incident' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Save note' })).toHaveProperty('disabled', false))
    await user.click(screen.getByRole('button', { name: 'Save note' }))
    await screen.findByText('Note saved to the incident record.')
    expect(save).toHaveBeenCalledTimes(2)
    expect(save.mock.calls[1]![2]).toBe(key)
  })

  it('uses only server capabilities to display note, resolution, and question controls', async () => {
    const value = appendNote(incident(), 'Readable recorded note.')
    value.capabilities = { note: false, resolve: false, ask: false }
    const { api } = server(value)
    mount(api)
    await ready()
    expect(screen.getByText('Readable recorded note.')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Open run run-incident-1' })).toBeTruthy()
    expect(screen.queryByRole('textbox')).toBeNull()
    expect(screen.queryByRole('button', { name: 'Save note' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Review human resolution' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Ask read-only agent' })).toBeNull()
    expect(screen.getByText(/your current role cannot add incident notes/)).toBeTruthy()
  })

  it('disables submissions while the caller says the snapshot is stale', async () => {
    const user = userEvent.setup()
    const { api } = server(incident())
    const save = vi.spyOn(api, 'addNote')
    mount(api, { fresh: false })
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Add a note' }), 'Keep this draft.')
    await user.type(screen.getByRole('textbox', { name: 'Question about this incident' }), 'What happened?')
    await user.type(screen.getByRole('textbox', { name: 'Resolution reason' }), 'Reviewed.')
    expect(screen.getByRole('button', { name: 'Save note' })).toHaveProperty('disabled', true)
    expect(screen.getByRole('button', { name: 'Ask read-only agent' })).toHaveProperty('disabled', true)
    expect(screen.getByRole('button', { name: 'Review human resolution' })).toHaveProperty('disabled', true)
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('maxLength', 4000)
    expect(screen.getByRole('textbox', { name: 'Question about this incident' })).toHaveProperty('maxLength', 2000)
    expect(save).not.toHaveBeenCalled()
  })
})

describe('explicit human resolution', () => {
  it('requires a reason, a separate review, and explicit confirmation without altering automated evidence', async () => {
    const user = userEvent.setup()
    const value = incident()
    const { api, records } = server(value)
    const resolve = vi.spyOn(api, 'resolve').mockImplementation(async (_id, input) => {
      const result = resolved(value, input.reason)
      records.set('incident-1', result)
      return result
    })
    mount(api)
    await ready()
    expect(screen.getByRole('button', { name: 'Review human resolution' })).toHaveProperty('disabled', true)
    await user.type(screen.getByRole('textbox', { name: 'Resolution reason' }), '  External repair reviewed.  ')
    await user.click(screen.getByRole('button', { name: 'Review human resolution' }))
    expect(resolve).not.toHaveBeenCalled()
    const confirmation = screen.getByRole('group', { name: 'Confirm resolved by user' })
    expect(within(confirmation).getByRole('button', { name: 'Confirm resolved by user' })).toHaveProperty('disabled', true)
    await user.click(within(confirmation).getByRole('checkbox'))
    await user.dblClick(within(confirmation).getByRole('button', { name: 'Confirm resolved by user' }))
    await screen.findByText('Human resolution recorded. This does not verify a repair.')
    expect(resolve).toHaveBeenCalledOnce()
    expect(resolve).toHaveBeenCalledWith('incident-1', {
      reason: 'External repair reviewed.', expected_version: 0, source_revision: 'evidence-incident-1-1', idempotency_key: expect.any(String),
    })
    expect(screen.queryByRole('textbox', { name: 'Resolution reason' })).toBeNull()
    expect(screen.getAllByText('Resolved by user').length).toBeGreaterThan(0)
    expect(screen.getByText('The run completed without resolving the incident.')).toBeTruthy()
    expect(screen.getByText(/This human tracking decision does not verify a repair/)).toBeTruthy()
    expect(screen.getByRole('list', { name: 'Human resolution history' })).toBeTruthy()
  })

  it('keeps a resolution draft when the user cancels confirmation', async () => {
    const user = userEvent.setup()
    const { api } = server(incident())
    const resolve = vi.spyOn(api, 'resolve')
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Resolution reason' }), 'Review before closing.')
    await user.click(screen.getByRole('button', { name: 'Review human resolution' }))
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.queryByRole('group', { name: 'Confirm resolved by user' })).toBeNull()
    expect(screen.getByRole('textbox', { name: 'Resolution reason' })).toHaveProperty('value', 'Review before closing.')
    expect(resolve).not.toHaveBeenCalled()
  })

  it('shows a stale 409, retains the reason, and requires refresh and a new review instead of retrying', async () => {
    const user = userEvent.setup()
    const value = incident()
    const { api, records } = server(value)
    const resolve = vi.spyOn(api, 'resolve').mockRejectedValueOnce(new ApiError(409, 'incident_changed', 'New controller evidence requires review.'))
      .mockImplementation(async (_id, input) => resolved({ ...value, tracking: { ...value.tracking, version: 4, source_revision: 'evidence-new' } }, input.reason))
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Resolution reason' }), 'Tracking reviewed.')
    await user.click(screen.getByRole('button', { name: 'Review human resolution' }))
    await user.click(screen.getByRole('checkbox'))
    await user.click(screen.getByRole('button', { name: 'Confirm resolved by user' }))
    await screen.findByText('New controller evidence requires review.')
    expect(resolve).toHaveBeenCalledOnce()
    expect(screen.getByRole('textbox', { name: 'Resolution reason' })).toHaveProperty('value', 'Tracking reviewed.')
    expect(screen.getByRole('button', { name: 'Review human resolution' })).toHaveProperty('disabled', true)
    expect(screen.queryByRole('group', { name: 'Confirm resolved by user' })).toBeNull()
    records.set('incident-1', { ...value, tracking: { ...value.tracking, version: 4, source_revision: 'evidence-new' } })
    await user.click(screen.getByRole('button', { name: 'Refresh incident' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Review human resolution' })).toHaveProperty('disabled', false))
    await user.click(screen.getByRole('button', { name: 'Review human resolution' }))
    await user.click(screen.getByRole('checkbox'))
    await user.click(screen.getByRole('button', { name: 'Confirm resolved by user' }))
    await waitFor(() => expect(resolve).toHaveBeenCalledTimes(2))
    expect(resolve.mock.calls[1]![1]).toMatchObject({ expected_version: 4, source_revision: 'evidence-new' })
    expect(resolve.mock.calls[1]![1].idempotency_key).not.toBe(resolve.mock.calls[0]![1].idempotency_key)
  })

  it('preserves all drafts across polling and refuses a confirmation reviewed against older evidence', async () => {
    vi.useFakeTimers()
    const value = incident()
    const { api, records, read } = server(value)
    const resolve = vi.spyOn(api, 'resolve')
    mount(api)
    await act(async () => { await Promise.resolve() })
    fireEvent.change(screen.getByRole('textbox', { name: 'Add a note' }), { target: { value: 'Draft note.' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'Question about this incident' }), { target: { value: 'Draft question?' } })
    fireEvent.change(screen.getByRole('textbox', { name: 'Resolution reason' }), { target: { value: 'Draft reason.' } })
    fireEvent.click(screen.getByRole('button', { name: 'Review human resolution' }))
    fireEvent.click(screen.getByRole('checkbox'))
    records.set('incident-1', { ...value, tracking: { ...value.tracking, source_revision: 'newer-evidence' } })
    await act(async () => { await vi.advanceTimersByTimeAsync(5000) })
    expect(read).toHaveBeenCalledTimes(2)
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', 'Draft note.')
    expect(screen.getByRole('textbox', { name: 'Question about this incident' })).toHaveProperty('value', 'Draft question?')
    expect(screen.getByRole('textbox', { name: 'Resolution reason' })).toHaveProperty('value', 'Draft reason.')
    expect(screen.getByRole('button', { name: 'Confirm resolved by user' })).toHaveProperty('disabled', true)
    expect(screen.getByText(/Evidence or tracking changed after this review/)).toBeTruthy()
    expect(resolve).not.toHaveBeenCalled()
  })
})

describe('persisted read-only discussion', () => {
  it.each([
    ['failed', 502, 'discussion_failed'],
    ['pending', 409, 'discussion_not_replayed'],
  ] as const)('reloads a persisted %s question after an error without retrying the observer request', async (status, httpStatus, code) => {
    const user = userEvent.setup()
    const value = incident()
    const { api, records, read } = server(value)
    const discuss = vi.spyOn(api, 'discuss').mockImplementation(async (_id, question) => {
      records.set('incident-1', { ...value, activity: [activity('saved-question', 'question', question, { status })] })
      throw new ApiError(httpStatus, code, 'The saved question was not replayed. Review its recorded status.')
    })
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Question about this incident' }), 'Which observation needs review?')
    await user.click(screen.getByRole('button', { name: 'Ask read-only agent' }))
    const history = await screen.findByRole('list', { name: 'Incident discussion' })
    expect(within(history).getByText(status === 'failed' ? 'Failed' : 'Pending')).toBeTruthy()
    expect(screen.getByText('The saved question was not replayed. Review its recorded status.')).toBeTruthy()
    expect(read.mock.calls.length).toBeGreaterThanOrEqual(2)
    expect(discuss).toHaveBeenCalledOnce()
    expect(screen.getByRole('textbox', { name: 'Question about this incident' })).toHaveProperty('value', 'Which observation needs review?')
    await waitFor(() => expect(screen.getByRole('button', { name: 'Ask read-only agent' })).toHaveProperty('disabled', false))
    await user.click(screen.getByRole('button', { name: 'Ask read-only agent' }))
    await waitFor(() => expect(discuss).toHaveBeenCalledTimes(2))
    expect(discuss.mock.calls[1]![2]).toBe(discuss.mock.calls[0]![2])
    expect(screen.queryByText(/The incident changed or the submission conflicted with another record/)).toBeNull()
  })

  it.each([['records', 'Records-based answer'], ['model', 'Model-generated answer']] as const)('persists and labels a %s answer without treating it as an action', async (mode, label) => {
    const user = userEvent.setup()
    const value = incident()
    const { api, records } = server(value)
    const discuss = vi.spyOn(api, 'discuss').mockImplementation(async (_id, question) => {
      const result = discussion(value, question, mode)
      records.set('incident-1', result)
      return result
    })
    const resolve = vi.spyOn(api, 'resolve')
    const view = mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Question about this incident' }), 'Why is the incident still open?')
    await user.click(screen.getByRole('button', { name: 'Ask read-only agent' }))
    expect(await screen.findByText(label)).toBeTruthy()
    expect(discuss).toHaveBeenCalledWith('incident-1', 'Why is the incident still open?', expect.any(String))
    expect(resolve).not.toHaveBeenCalled()
    expect(screen.getByRole('textbox', { name: 'Question about this incident' })).toHaveProperty('value', '')
    expect(screen.getByText(/Asking does not dispatch triage/)).toBeTruthy()
    view.unmount()
    mount(api)
    await ready()
    expect(within(screen.getByRole('list', { name: 'Incident discussion' })).getByText(label)).toBeTruthy()
    expect(screen.getByText('Why is the incident still open?')).toBeTruthy()
  })

  it('shows persisted pending and failed discussion states and only polls for records', async () => {
    vi.useFakeTimers()
    const value = incident()
    const waiting = { ...value, activity: [activity('question-1', 'question', 'Explain the failure.', { status: 'pending', correlation_id: 'question-1' })] }
    const { api, records } = server(waiting)
    const discuss = vi.spyOn(api, 'discuss')
    mount(api)
    await act(async () => { await Promise.resolve() })
    expect(screen.getByText('Pending')).toBeTruthy()
    expect(screen.getByText(/An answer is pending/)).toBeTruthy()
    expect(screen.queryByText('Model-generated answer')).toBeNull()
    records.set('incident-1', { ...value, activity: [
      activity('question-1', 'question', 'Explain the failure.', { status: 'failed', correlation_id: 'question-1' }),
      activity('answer-1', 'answer', 'The read-only answer service failed.', { status: 'failed', correlation_id: 'question-1' }),
    ] })
    await act(async () => { await vi.advanceTimersByTimeAsync(5000) })
    expect(screen.getByText('The read-only answer service failed.')).toBeTruthy()
    expect(screen.getAllByText('Failed')).toHaveLength(2)
    expect(screen.getAllByText(/No answer or action has been inferred/).length).toBeGreaterThan(0)
    expect(discuss).not.toHaveBeenCalled()
  })

  it('retains an unconfirmed question and its idempotency key on an explicit retry', async () => {
    const user = userEvent.setup()
    const value = incident()
    const { api, records } = server(value)
    const discuss = vi.spyOn(api, 'discuss').mockRejectedValueOnce(new ApiError(503, 'discussion_unavailable', 'Discussion could not be confirmed.'))
      .mockImplementation(async (_id, question) => {
        const result = discussion(value, question, 'records')
        records.set('incident-1', result)
        return result
      })
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Question about this incident' }), 'What evidence is missing?')
    await user.click(screen.getByRole('button', { name: 'Ask read-only agent' }))
    await screen.findByText('Discussion could not be confirmed.')
    expect(screen.getByRole('textbox', { name: 'Question about this incident' })).toHaveProperty('value', 'What evidence is missing?')
    expect(discuss).toHaveBeenCalledOnce()
    await user.click(screen.getByRole('button', { name: 'Refresh incident' }))
    await waitFor(() => expect(screen.getByRole('button', { name: 'Ask read-only agent' })).toHaveProperty('disabled', false))
    await user.click(screen.getByRole('button', { name: 'Ask read-only agent' }))
    await screen.findByText('Records-based answer')
    expect(discuss.mock.calls[1]![2]).toBe(discuss.mock.calls[0]![2])
  })
})

describe('confirmed activity receipts', () => {
  it.each([
    ['note', 'Add a note', 'Save note'],
    ['question', 'Question about this incident', 'Ask read-only agent'],
    ['resolution', 'Resolution reason', 'Confirm resolved by user'],
  ] as const)('retains the %s draft when a valid selected case has no matching receipt', async (kind, label, button) => {
    const user = userEvent.setup()
    const api = new IncidentApiClient(async () => null)
    const fetch = vi.mocked(globalThis.fetch).mockImplementation(async () => response(incident()))
    const view = mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: label }), 'Keep the unconfirmed draft.')
    if (kind === 'resolution') {
      await user.click(screen.getByRole('button', { name: 'Review human resolution' }))
      await user.click(screen.getByRole('checkbox'))
    }
    await user.click(screen.getByRole('button', { name: button }))
    await screen.findByText(new RegExp(`The API did not confirm this ${kind} receipt`))
    expect(screen.getByRole('textbox', { name: label })).toHaveProperty('value', 'Keep the unconfirmed draft.')
    expect(view.props.onChanged).not.toHaveBeenCalled()
    expect(fetch.mock.calls.filter((call) => call[1]?.method === 'POST')).toHaveLength(1)
  })

  it('clears a confirmed note using its receipt ID even when the stored body was redacted', async () => {
    const user = userEvent.setup()
    const api = new IncidentApiClient(async () => null)
    let stored = incident()
    vi.mocked(globalThis.fetch).mockImplementation(async (_url, options) => {
      if (options?.method === 'POST') {
        stored = { ...stored, activity: [activity(submittedKey(options), 'note', '[redacted] stored finding')] }
      }
      return response(stored)
    })
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Add a note' }), 'Original finding before redaction.')
    await user.click(screen.getByRole('button', { name: 'Save note' }))
    await screen.findByText('Note saved to the incident record.')
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', '')
    expect(within(screen.getByRole('list', { name: 'Incident notes' })).getByText('[redacted] stored finding')).toBeTruthy()
    expect(screen.queryByText('Original finding before redaction.')).toBeNull()
  })

  it.each([false, true])('does not close reopened tracking on a historical receipt (newer draft=%s)', async (editDraft) => {
    const user = userEvent.setup()
    const api = new IncidentApiClient(async () => null)
    const initial = incident()
    initial.tracking.source_revision = 'a'.repeat(64)
    let stored = initial
    let key: string | undefined
    const pending = deferred<Response>()
    vi.mocked(globalThis.fetch).mockImplementation(async (_url, options) => {
      if (options?.method === 'POST') {
        key = submittedKey(options)
        return pending.promise
      }
      return response(stored)
    })
    const view = mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Resolution reason' }), 'Submitted resolution reason.')
    await user.click(screen.getByRole('button', { name: 'Review human resolution' }))
    await user.click(screen.getByRole('checkbox'))
    await user.click(screen.getByRole('button', { name: 'Confirm resolved by user' }))
    if (editDraft) {
      await user.clear(screen.getByRole('textbox', { name: 'Resolution reason' }))
      await user.type(screen.getByRole('textbox', { name: 'Resolution reason' }), 'Newer unsent reason.')
    }
    if (!key) throw new Error('Expected the reviewed resolution request.')
    stored = {
      ...initial,
      detail: { ...initial.detail, evidence: [{ label: 'Latest scan', value: 'A later occurrence remains unresolved.' }] },
      tracking: { ...initial.tracking, version: 1, source_revision: 'b'.repeat(64) },
      activity: [activity(key, 'resolution', '[redacted] recorded resolution reason')],
    }
    await act(async () => pending.resolve(response(stored)))
    await screen.findByText(/Human resolution decision saved to history. Current tracking is open/)
    expect(screen.getByRole('textbox', { name: 'Resolution reason' })).toHaveProperty('value', editDraft ? 'Newer unsent reason.' : '')
    expect(screen.getByText('A later occurrence remains unresolved.')).toBeTruthy()
    expect(screen.queryByText('Resolved by user')).toBeNull()
    expect(view.props.onChanged).toHaveBeenCalledOnce()
  })
})

describe('incident request isolation', () => {
  it('does not let a polling read from before a save replace its confirmed activity', async () => {
    vi.useFakeTimers()
    const value = incident()
    const { api, read } = server(value)
    const poll = deferred<IncidentCase>()
    const afterSave = deferred<IncidentCase>()
    const updated = appendNote(value, 'Confirmed activity.')
    const save = vi.spyOn(api, 'addNote').mockResolvedValue(updated)
    mount(api)
    await act(async () => { await Promise.resolve() })
    read.mockReturnValueOnce(poll.promise).mockReturnValueOnce(afterSave.promise)
    await act(async () => { await vi.advanceTimersByTimeAsync(5000) })
    const pollSignal = read.mock.calls[1]![1]
    fireEvent.change(screen.getByRole('textbox', { name: 'Add a note' }), { target: { value: 'Confirmed activity.' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save note' }))
    await act(async () => { await Promise.resolve() })
    expect(save).toHaveBeenCalledOnce()
    expect(pollSignal?.aborted).toBe(true)
    expect(screen.getByText('Confirmed activity.')).toBeTruthy()
    await act(async () => poll.resolve(value))
    expect(screen.getByText('Confirmed activity.')).toBeTruthy()
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', '')
    await act(async () => afterSave.resolve(updated))
    expect(screen.getByText('Confirmed activity.')).toBeTruthy()
  })

  it('aborts obsolete detail reads and never renders a late result from another incident', async () => {
    const { api, read } = server(incident(), incident('incident-2'))
    const delayed = deferred<IncidentCase>()
    read.mockReturnValueOnce(delayed.promise)
    const view = mount(api)
    const firstSignal = read.mock.calls[0]![1]
    view.rerender(<IncidentWorkspace {...view.props} selectedId="incident-2" />)
    await screen.findByRole('heading', { name: 'Incident incident-2', level: 1 })
    expect(firstSignal?.aborted).toBe(true)
    await act(async () => delayed.resolve(appendNote(incident(), 'Old incident evidence.')))
    expect(screen.queryByRole('heading', { name: 'Incident incident-1', level: 1 })).toBeNull()
    expect(screen.queryByText('Old incident evidence.')).toBeNull()
  })

  it.each(['confirmed', 'failed'] as const)('does not leak drafts or a late %s write response when switching incidents', async (outcome) => {
    const user = userEvent.setup()
    const { api, records } = server(incident(), incident('incident-2'))
    const delayed = deferred<IncidentCase>()
    vi.spyOn(api, 'addNote').mockReturnValue(delayed.promise)
    const view = mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Add a note' }), 'Incident one private draft.')
    await user.type(screen.getByRole('textbox', { name: 'Question about this incident' }), 'Question for incident one.')
    await user.type(screen.getByRole('textbox', { name: 'Resolution reason' }), 'Reason for incident one.')
    await user.click(screen.getByRole('button', { name: 'Save note' }))
    view.rerender(<IncidentWorkspace {...view.props} selectedId="incident-2" />)
    await screen.findByRole('heading', { name: 'Incident incident-2', level: 1 })
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', '')
    expect(screen.getByRole('textbox', { name: 'Question about this incident' })).toHaveProperty('value', '')
    expect(screen.getByRole('textbox', { name: 'Resolution reason' })).toHaveProperty('value', '')
    await user.type(screen.getByRole('textbox', { name: 'Add a note' }), 'Incident two draft.')
    const updated = appendNote(incident(), 'Incident one private draft.')
    records.set('incident-1', updated)
    await act(async () => {
      if (outcome === 'confirmed') delayed.resolve(updated)
      else delayed.reject(new ApiError(503, 'old_incident_error', 'Old incident request failed.'))
    })
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', 'Incident two draft.')
    expect(screen.queryByText('Incident one private draft.')).toBeNull()
    expect(screen.queryByText('Note saved to the incident record.')).toBeNull()
    expect(screen.queryByText('Old incident request failed.')).toBeNull()
    expect(view.props.onChanged).not.toHaveBeenCalled()
  })

  it('retains the draft if the real client rejects a mismatched mutation response', async () => {
    const user = userEvent.setup()
    const api = new IncidentApiClient(async () => null)
    vi.mocked(globalThis.fetch).mockImplementation(async (_url, options) => new Response(
      JSON.stringify(options?.method === 'POST' ? incident('incident-2') : incident()),
      { headers: { 'Content-Type': 'application/json' } },
    ))
    mount(api)
    await ready()
    await user.type(screen.getByRole('textbox', { name: 'Add a note' }), 'Keep until confirmed.')
    await user.click(screen.getByRole('button', { name: 'Save note' }))
    await screen.findByText(/The API did not confirm the selected incident and complete collaboration records/)
    expect(screen.getByRole('textbox', { name: 'Add a note' })).toHaveProperty('value', 'Keep until confirmed.')
    expect(screen.queryByRole('heading', { name: 'Incident incident-2', level: 1 })).toBeNull()
    expect(screen.queryByText('Note saved to the incident record.')).toBeNull()
  })
})
