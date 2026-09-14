import { describe, expect, it, vi } from 'vitest'
import { detail, run, workItem } from '../test/fixtures'
import { IncidentApiClient, IncidentIntent, incidentQueryLimit, incidentQuestionLimit, incidentTextLimit } from './incidents'
import type { IncidentActivity, IncidentCase, IncidentPage } from './incidents'

type ReceiptKind = 'note' | 'resolution' | 'question'
const keys: Record<ReceiptKind, string> = {
  note: 'a1111111-b111-4111-8111-c11111111111',
  resolution: 'a2222222-b222-4222-8222-c22222222222',
  question: 'a3333333-b333-4333-8333-c33333333333',
}
const answerId = 'a4444444-b444-4444-8444-c44444444444'

function incident(id = 'incident-1'): IncidentCase {
  return {
    detail: {
      ...detail,
      item: { ...workItem, id: `incident:${id}`, kind: 'incident', source_id: id, incident_id: id, status: 'needs_review', can_decide: false },
      incident: { incident_id: id, status: 'open' },
      proposal: null,
      runs: [{ ...run, incident_id: id }],
    },
    tracking: { status: 'open', version: 0, source_revision: 'evidence-1', resolved_at: null, resolved_by: null, resolution_note: null },
    activity: [],
    capabilities: { note: true, resolve: true, ask: true },
  }
}

function activity(overrides: Partial<IncidentActivity> = {}): IncidentActivity {
  return {
    id: 'entry-1', incident_id: 'incident-1', kind: 'note', body: 'Reviewed gateway availability.',
    created_at: '2026-01-01T12:30:00Z', user_id: 'operator-1', user_name: 'Operator',
    status: 'recorded', correlation_id: null, mode: null, ...overrides,
  }
}

function acknowledged(kind: ReceiptKind): IncidentCase {
  const value = incident()
  const key = keys[kind]
  value.activity = [activity({ id: key, kind, body: '[redacted] stored request' })]
  if (kind === 'resolution') {
    value.tracking = {
      ...value.tracking, status: 'resolved_by_user', version: 1, resolved_at: '2026-01-01T12:30:00Z',
      resolved_by: 'Operator', resolution_note: '[redacted] stored request',
    }
  } else if (kind === 'question') {
    value.activity = [
      activity({ id: key, kind, body: '[redacted] stored question', status: 'completed', mode: 'records', correlation_id: key }),
      activity({ id: answerId, kind: 'answer', body: 'Stored answer.', status: 'completed', mode: 'records', correlation_id: key, user_id: 'incident-observer' }),
    ]
  }
  return value
}

function changeActivity(value: IncidentCase, kind: IncidentActivity['kind'], patch: Partial<IncidentActivity>): IncidentCase {
  return { ...value, activity: value.activity.map((entry) => entry.kind === kind ? { ...entry, ...patch } : entry) }
}

function submit(api: IncidentApiClient, kind: ReceiptKind): Promise<IncidentCase> {
  if (kind === 'note') return api.addNote('incident-1', 'Original note before redaction.', keys.note)
  if (kind === 'question') return api.discuss('incident-1', 'Original question before redaction?', keys.question)
  return api.resolve('incident-1', {
    reason: 'Original reason before redaction.', expected_version: 0,
    source_revision: 'evidence-1', idempotency_key: keys.resolution,
  })
}

function page(overrides: Partial<IncidentPage> = {}): IncidentPage {
  return { items: [incident().detail.item], total: 1, offset: 0, limit: 25, ...overrides }
}

function response(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
}

describe('incident API reads', () => {
  it('requests server paging and encoded filters using the shared bearer transport', async () => {
    const result = page({ total: 80, offset: 25 })
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(result))
    const signal = new AbortController().signal
    const api = new IncidentApiClient(async () => 'test-incident-token')
    expect(await api.list({ offset: 25, query: '  model & gateway  ', status: 'needs_investigation', workload: 'fabric_pipeline' }, signal)).toEqual(result)
    const [url, options] = fetch.mock.calls[0]!
    expect(url).toBe('/api/incidents?limit=25&offset=25&query=model+%26+gateway&status=needs_investigation&workload=fabric_pipeline')
    expect(options?.signal).toBe(signal)
    expect(options?.credentials).toBe('omit')
    expect(options?.cache).toBe('no-store')
    expect(new Headers(options?.headers).has('Authorization')).toBe(true)
  })

  it('keeps the actual needs_review wire status and includes closed records', async () => {
    const values = [incident().detail.item, { ...incident('closed').detail.item, status: 'resolved_by_user' }]
    vi.mocked(globalThis.fetch).mockResolvedValue(response(page({ items: values, total: 2 })))
    const result = await new IncidentApiClient(async () => null).list()
    expect(result.items.map((item) => item.status)).toEqual(['needs_review', 'resolved_by_user'])
  })

  it.each([
    { limit: 0 }, { limit: 101 }, { limit: 1.5 }, { offset: -1 }, { offset: Number.NaN },
    { offset: 2_147_483_648 }, { workload: '' }, { workload: 'unsupported_workload' }, { query: 'x'.repeat(incidentQueryLimit + 1) },
  ])('rejects an invalid page before requesting records: %j', async (query) => {
    await expect(new IncidentApiClient(async () => null).list(query)).rejects.toMatchObject({ code: 'invalid_incident_query' })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it.each([
    ['missing items', { total: 0, offset: 0, limit: 25 }],
    ['wrong kind', page({ items: [{ ...workItem, kind: 'approval' }] })],
    ['wrong incident link', page({ items: [{ ...incident().detail.item, incident_id: 'different' }] })],
    ['invalid field', { ...page(), items: [{ ...incident().detail.item, summary: 1 }] }],
    ['duplicate records', page({ items: [incident().detail.item, incident().detail.item], total: 2 })],
    ['wrong page', page({ offset: 25 })],
    ['wrong limit', page({ limit: 50 })],
    ['negative total', page({ total: -1 })],
    ['contradictory total', page({ total: 0 })],
    ['fractional total', page({ total: 1.5 })],
  ])('rejects %s instead of rendering a partial list', async (_label, payload) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(payload))
    await expect(new IncidentApiClient(async () => null).list()).rejects.toMatchObject({ code: 'invalid_incident_page' })
  })

  it('accepts an empty later page after the result set shrinks', async () => {
    const result = page({ items: [], total: 1, offset: 25 })
    vi.mocked(globalThis.fetch).mockResolvedValue(response(result))
    expect(await new IncidentApiClient(async () => null).list({ offset: 25 })).toEqual(result)
  })

  it('encodes the selected incident ID and propagates cancellation', async () => {
    const id = 'case/id?target=other#part'
    const value = incident(id)
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    const signal = new AbortController().signal
    expect(await new IncidentApiClient(async () => null).detail(id, signal)).toEqual(value)
    expect(fetch.mock.calls[0]![0]).toBe('/api/incidents/case%2Fid%3Ftarget%3Dother%23part')
    expect(fetch.mock.calls[0]![1]?.signal).toBe(signal)
  })

  it('rejects an empty incident identifier locally', async () => {
    await expect(new IncidentApiClient(async () => null).detail(' ')).rejects.toMatchObject({ code: 'incident_id_required' })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })
})

describe('incident response validation', () => {
  it.each(['', 'earlier-related-incident'])('keeps signature-related history with incident_id=%j', async (relatedId) => {
    const value = incident()
    value.detail.runs = [{ ...run, id: 'related-run', incident_id: relatedId }]
    vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    const result = await new IncidentApiClient(async () => null).detail('incident-1')
    expect(result.detail.runs).toEqual(value.detail.runs)
    expect(result.detail.item.source_id).toBe('incident-1')
  })

  const corruptions: [string, (value: IncidentCase) => unknown][] = [
    ['different source', (value) => ({ ...value, detail: { ...value.detail, item: { ...value.detail.item, source_id: 'different' } } })],
    ['manual resolution label without closed tracking', (value) => ({ ...value, detail: { ...value.detail, item: { ...value.detail.item, status: 'resolved_by_user' } } })],
    ['different core incident', (value) => ({ ...value, detail: { ...value.detail, incident: { incident_id: 'different' } } })],
    ['incomplete evidence', (value) => ({ ...value, detail: { ...value.detail, evidence: [{ label: 'Scan' }] } })],
    ['invalid run metrics', (value) => ({ ...value, detail: { ...value.detail, runs: [{ ...run, write_actions: -1 }] } })],
    ['incomplete timeline', (value) => ({ ...value, detail: { ...value.detail, timeline: [{ id: 'event-1', sequence: 1 }] } })],
    ['invalid controller notes', (value) => ({ ...value, detail: { ...value.detail, notes: null } })],
    ['incomplete proposal', (value) => ({ ...value, detail: { ...value.detail, proposal: { request_id: 'request-1' } } })],
    ['missing capability', (value) => ({ ...value, capabilities: { note: true, resolve: true } })],
    ['string capability', (value) => ({ ...value, capabilities: { ...value.capabilities, resolve: 'true' } })],
    ['fractional version', (value) => ({ ...value, tracking: { ...value.tracking, version: 0.5 } })],
    ['missing evidence revision', (value) => ({ ...value, tracking: { ...value.tracking, source_revision: '' } })],
    ['unattributed human closure', (value) => ({ ...value, tracking: { ...value.tracking, status: 'resolved_by_user' } })],
    ['unknown tracking status', (value) => ({ ...value, tracking: { ...value.tracking, status: 'repaired' } })],
    ['foreign activity', (value) => ({ ...value, activity: [activity({ incident_id: 'different' })] })],
    ['duplicate activity', (value) => ({ ...value, activity: [activity(), activity()] })],
    ['invalid activity kind', (value) => ({ ...value, activity: [{ ...activity(), kind: 'remediation' }] })],
    ['invalid activity status', (value) => ({ ...value, activity: [{ ...activity(), status: 'success' }] })],
    ['unknown answer source', (value) => ({ ...value, activity: [{ ...activity({ kind: 'answer', status: 'completed' }), mode: 'unknown' }] })],
    ['missing completed answer source', (value) => ({ ...value, activity: [activity({ kind: 'answer', status: 'completed', mode: null })] })],
  ]

  it.each(corruptions)('rejects %s without accepting a successful-looking record', async (_label, corrupt) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(corrupt(incident())))
    await expect(new IncidentApiClient(async () => null).detail('incident-1')).rejects.toMatchObject({ code: 'invalid_incident_case' })
  })

  it('accepts explicit records/model sources and visible pending/failed entries', async () => {
    const value = incident()
    value.activity = [
      activity({ id: 'pending', kind: 'question', status: 'pending', correlation_id: 'question-pending' }),
      activity({ id: 'failed', kind: 'answer', status: 'failed', body: 'Answer service unavailable.', correlation_id: 'question-failed' }),
      activity({ id: 'records', kind: 'answer', status: 'completed', mode: 'records' }),
      activity({ id: 'model', kind: 'answer', status: 'completed', mode: 'model' }),
    ]
    vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    expect(await new IncidentApiClient(async () => null).detail('incident-1')).toEqual(value)
  })

  it('keeps human tracking resolution separate from the original automated evidence', async () => {
    const value = incident()
    value.tracking = { ...value.tracking, status: 'resolved_by_user', version: 1, resolved_at: '2026-01-01T12:30:00Z', resolved_by: 'operator-1', resolution_note: 'Handled through the operations process.' }
    vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    const result = await new IncidentApiClient(async () => null).detail('incident-1')
    expect(result.tracking.status).toBe('resolved_by_user')
    expect(result.detail.item.status).toBe('needs_review')
    expect(result.detail.runs[0]?.outcome).toBe('needs_investigation')
  })
})

describe('incident mutations', () => {
  it('posts a trimmed note to the selected incident without replacing controller notes', async () => {
    const value = acknowledged('note')
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    expect(await new IncidentApiClient(async () => null).addNote('incident-1', '  Reviewed gateway availability.  ', keys.note)).toEqual(value)
    expect(fetch.mock.calls[0]![0]).toBe('/api/incidents/incident-1/notes')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({ body: 'Reviewed gateway availability.', idempotency_key: keys.note })
    expect(fetch.mock.calls[0]![1]?.method).toBe('POST')
  })

  it('posts the reviewed resolution version and evidence revision, not an automated outcome', async () => {
    const value = acknowledged('resolution')
    const sourceRevision = '0123456789abcdef'.repeat(4)
    value.tracking.source_revision = sourceRevision
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    const input = { reason: '  Reviewed externally. ', expected_version: 0, source_revision: sourceRevision, idempotency_key: keys.resolution, outcome: 'resolved' }
    await new IncidentApiClient(async () => null).resolve('incident-1', input)
    expect(fetch.mock.calls[0]![0]).toBe('/api/incidents/incident-1/resolution')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({
      reason: 'Reviewed externally.', expected_version: 0, source_revision: sourceRevision, idempotency_key: keys.resolution,
    })
  })

  it('posts only the incident question and submission identifier to discussion', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(acknowledged('question')))
    await new IncidentApiClient(async () => null).discuss('incident-1', ' Why is it open? ', keys.question)
    expect(fetch.mock.calls[0]![0]).toBe('/api/incidents/incident-1/discussion')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({ question: 'Why is it open?', idempotency_key: keys.question })
  })

  it('rejects blank or oversized drafts, a missing key, or a missing review locally', async () => {
    const api = new IncidentApiClient(async () => null)
    await expect(api.addNote('incident-1', ' ', keys.note)).rejects.toMatchObject({ code: 'invalid_incident_text' })
    await expect(api.addNote('incident-1', 'x'.repeat(incidentTextLimit + 1), keys.note)).rejects.toMatchObject({ code: 'invalid_incident_text' })
    await expect(api.discuss('incident-1', 'x'.repeat(incidentQuestionLimit + 1), keys.question)).rejects.toMatchObject({ code: 'invalid_incident_text' })
    await expect(api.addNote('incident-1', 'Reviewed.', '')).rejects.toMatchObject({ code: 'idempotency_key_required' })
    await expect(api.resolve('incident-1', { reason: 'Reviewed.', expected_version: -1, source_revision: '', idempotency_key: keys.resolution })).rejects.toMatchObject({ code: 'incident_revision_required' })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it.each(['not-a-uuid', 'a'.repeat(31), `${keys.note}extra`])('rejects invalid UUID key %s before sending a request', async (key) => {
    await expect(new IncidentApiClient(async () => null).addNote('incident-1', 'Reviewed.', key)).rejects.toMatchObject({ code: 'invalid_idempotency_key' })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it('rejects a mutation receipt for a different incident', async () => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(incident('another-incident')))
    await expect(new IncidentApiClient(async () => null).addNote('incident-1', 'Reviewed.', keys.note)).rejects.toMatchObject({ code: 'invalid_incident_case' })
  })

  it('surfaces an explicit stale 409 and never retries the resolution', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response({ detail: { code: 'incident_changed', message: 'New evidence was recorded. Refresh the incident.' } }, 409))
    await expect(new IncidentApiClient(async () => null).resolve('incident-1', { reason: 'Reviewed.', expected_version: 0, source_revision: 'evidence-1', idempotency_key: keys.resolution })).rejects.toMatchObject({ status: 409, code: 'incident_changed' })
    expect(fetch).toHaveBeenCalledOnce()
  })

  it('does not retry a write when the server response is lost', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockRejectedValue(new TypeError('Response lost.'))
    await expect(new IncidentApiClient(async () => null).discuss('incident-1', 'What happened?', keys.question)).rejects.toMatchObject({ code: 'network_error' })
    expect(fetch).toHaveBeenCalledOnce()
  })
})

describe('activity-backed mutation receipts', () => {
  it.each(['note', 'resolution', 'question'] as const)('confirms %s by ID and kind without comparing the redacted body', async (kind) => {
    const value = acknowledged(kind)
    vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    expect(await submit(new IncidentApiClient(async () => null), kind)).toEqual(value)
  })

  it.each([keys.note.toUpperCase(), keys.note.replaceAll('-', ''), `{${keys.note}}`, `urn:uuid:${keys.note}`])('canonicalizes UUID key %s before matching the receipt', async (key) => {
    const value = acknowledged('note')
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    expect(await new IncidentApiClient(async () => null).addNote('incident-1', 'Reviewed.', key)).toEqual(value)
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body)).idempotency_key).toBe(keys.note)
  })

  const corruptions: [string, ReceiptKind, (value: IncidentCase) => IncidentCase][] = [
    ['missing note', 'note', (value) => ({ ...value, activity: [] })],
    ['another note UUID', 'note', (value) => changeActivity(value, 'note', { id: answerId })],
    ['wrong note kind', 'note', (value) => changeActivity(value, 'note', { kind: 'resolution' })],
    ['unrecorded note', 'note', (value) => changeActivity(value, 'note', { status: 'pending' })],
    ['note discussion correlation', 'note', (value) => changeActivity(value, 'note', { correlation_id: keys.note })],
    ['note model attribution', 'note', (value) => changeActivity(value, 'note', { mode: 'model' })],
    ['missing resolution despite closed tracking', 'resolution', (value) => ({ ...value, activity: [] })],
    ['wrong resolution kind', 'resolution', (value) => changeActivity(value, 'resolution', { kind: 'note' })],
    ['unrecorded resolution', 'resolution', (value) => changeActivity(value, 'resolution', { status: 'pending' })],
    ['resolution discussion correlation', 'resolution', (value) => changeActivity(value, 'resolution', { correlation_id: keys.resolution })],
    ['missing question', 'question', (value) => ({ ...value, activity: value.activity.filter((entry) => entry.kind !== 'question') })],
    ['pending question', 'question', (value) => changeActivity(value, 'question', { status: 'pending', mode: null })],
    ['failed question', 'question', (value) => changeActivity(value, 'question', { status: 'failed', mode: null })],
    ['wrong question correlation', 'question', (value) => changeActivity(value, 'question', { correlation_id: keys.note })],
    ['missing completed answer', 'question', (value) => ({ ...value, activity: value.activity.filter((entry) => entry.kind !== 'answer') })],
    ['unrelated answer', 'question', (value) => changeActivity(value, 'answer', { correlation_id: keys.note })],
    ['failed answer', 'question', (value) => changeActivity(value, 'answer', { status: 'failed', mode: null })],
    ['inconsistent answer mode', 'question', (value) => changeActivity(value, 'answer', { mode: 'model' })],
    ['multiple correlated answers', 'question', (value) => ({
      ...value, activity: [...value.activity, activity({
        id: keys.note, kind: 'answer', status: 'completed', mode: 'records', correlation_id: keys.question,
      })],
    })],
  ]

  it.each(corruptions)('rejects a successful-looking case with %s', async (_label, kind, corrupt) => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(corrupt(acknowledged(kind))))
    await expect(submit(new IncidentApiClient(async () => null), kind)).rejects.toMatchObject({ code: 'unconfirmed_incident_submission' })
    expect(fetch).toHaveBeenCalledOnce()
  })

  it.each(['note', 'resolution', 'question'] as const)('cannot accept a %s receipt attached to another incident', async (kind) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(changeActivity(acknowledged(kind), kind, { incident_id: 'another-incident' })))
    await expect(submit(new IncidentApiClient(async () => null), kind)).rejects.toMatchObject({ code: 'invalid_incident_case' })
  })

  it('confirms a historical resolution without treating reopened tracking as currently closed', async () => {
    const value = acknowledged('resolution')
    value.tracking = { ...incident().tracking, version: 1, source_revision: 'b'.repeat(64) }
    vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    const result = await submit(new IncidentApiClient(async () => null), 'resolution')
    expect(result.tracking.status).toBe('open')
    expect(result.activity[0]?.id).toBe(keys.resolution)
  })
})

describe('incident submission identity', () => {
  it('uses a stable key for unchanged retries and a new key after an edit or confirmed save', () => {
    const intent = new IncidentIntent()
    const first = intent.keyFor('Reviewed.')
    expect(intent.keyFor('Reviewed.')).toBe(first)
    const second = intent.keyFor('Different finding.')
    expect(second).not.toBe(first)
    expect(intent.keyFor('Different finding.')).toBe(second)
    intent.clear()
    expect(intent.keyFor('Different finding.')).not.toBe(second)
  })

  it('does not share a submission key between incidents or activities', () => {
    expect(new IncidentIntent().keyFor('Reviewed.')).not.toBe(new IncidentIntent().keyFor('Reviewed.'))
  })
})
