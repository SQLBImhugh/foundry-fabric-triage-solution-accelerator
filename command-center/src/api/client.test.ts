import { describe, expect, it, vi } from 'vitest'
import { ApiClient, loadConfig, requestJson } from './client'
import { normalizeApiError } from './errors'
import { config, detail, proposal, snapshot } from '../test/fixtures'

function jsonResponse(value: unknown, status = 200) {
  return new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
}

describe('same-origin API transport', () => {
  it('loads config without a credential and never uses cookies', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse(config))
    expect(await loadConfig()).toEqual(config)
    expect(fetch).toHaveBeenCalledOnce()
    const [url, options] = fetch.mock.calls[0]!
    expect(url).toBe('/api/config')
    expect(options?.credentials).toBe('omit')
    expect(new Headers(options?.headers).has('Authorization')).toBe(false)
  })
  it('attaches a bearer token only when the provider returns one', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse(snapshot))
    await new ApiClient(async () => 'test-access-token').snapshot()
    expect(new Headers(fetch.mock.calls[0]![1]?.headers).get('Authorization')).toBe('Bearer test-access-token')
    fetch.mockResolvedValueOnce(jsonResponse(snapshot))
    await new ApiClient(async () => null).snapshot()
    expect(new Headers(fetch.mock.calls[1]![1]?.headers).has('Authorization')).toBe(false)
  })
  it('never automatically retries a mutation after a transport failure', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockRejectedValue(new TypeError('Connection lost'))
    const api = new ApiClient(async () => null)
    await expect(api.decide({ request_id: proposal.request_id, decision: 'deny', fingerprint: proposal.fingerprint, reason: 'Unsafe target.' })).rejects.toMatchObject({ code: 'network_error', status: 0 })
    expect(fetch).toHaveBeenCalledOnce()
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({ request_id: proposal.request_id, decision: 'deny', fingerprint: proposal.fingerprint, reason: 'Unsafe target.' })
  })
  it('normalizes a FastAPI conflict without returning a success-shaped value', async () => {
    vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ detail: { code: 'proposal_expired', message: 'The request has expired.' } }, 409))
    await expect(requestJson('/decisions')).rejects.toMatchObject({ status: 409, code: 'proposal_expired', message: 'The request has expired.' })
  })
  it('rejects HTML in a successful API response', async () => {
    vi.mocked(globalThis.fetch).mockResolvedValue(new Response('<html>SPA fallback</html>'))
    await expect(loadConfig()).rejects.toMatchObject({ code: 'invalid_response' })
  })
  it('rejects unsupported snapshots instead of supplying fake counters', async () => {
    vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ ...snapshot, schema_version: 2 }))
    await expect(new ApiClient(async () => null).snapshot()).rejects.toMatchObject({ code: 'unsupported_snapshot' })
  })
  it('cannot show evidence returned for a different selected request', async () => {
    vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ ...detail, item: { ...detail.item, source_id: 'different-request' } }))
    await expect(new ApiClient(async () => null).detail({ kind: 'approval', source_id: 'approval-1' })).rejects.toMatchObject({ code: 'invalid_detail' })
  })
  it('encodes source IDs and validates the decision receipt identity', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ ...detail, item: { ...detail.item, kind: 'incident', source_id: 'id&other=bad' } }))
    await new ApiClient(async () => null).detail({ kind: 'incident', source_id: 'id&other=bad' })
    expect(fetch.mock.calls[0]![0]).toBe('/api/detail?kind=incident&id=id%26other%3Dbad')
    fetch.mockResolvedValueOnce(jsonResponse({ status: 'decision_recorded', request: { ...proposal, request_id: 'another' } }))
    await expect(new ApiClient(async () => null).decide({ request_id: 'approval-1', decision: 'approve', fingerprint: 'fp', reason: 'Reviewed' })).rejects.toMatchObject({ code: 'unconfirmed_decision' })
  })
  it('keeps the command payload snake_case with an explicit idempotency key', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ command_id: 'command-1', status: 'queued' }))
    await new ApiClient(async () => null).command({ kind: 'pipeline_sweep', target_id: 'target-1', subject: 'Observed failure', body: 'Investigate.' }, 'uuid-1')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({ kind: 'pipeline_sweep', target_id: 'target-1', subject: 'Observed failure', body: 'Investigate.', idempotency_key: 'uuid-1' })
  })
})

describe('error normalization', () => {
  it('supports detail strings and FastAPI validation arrays', () => {
    expect(normalizeApiError(403, { detail: 'Permission denied' }).message).toBe('Permission denied')
    expect(normalizeApiError(422, { detail: [{ msg: 'Field required' }, { msg: 'Invalid target' }] }).message).toBe('Field required; Invalid target')
    expect(normalizeApiError(500, '<html>private trace</html>').message).toBe('The API rejected this request (HTTP 500).')
  })
})

describe('scenario validation endpoints', () => {
  const result = { scenario: 'test-case', provider: 'foundry', passed: true, failures: [], run_id: 'run-1', outcome: 'resolved', duration_ms: 35 }
  it('uses the encoded scenario path and explicit provider', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse(result))
    const api = new ApiClient(async () => null)
    expect(await api.validateScenario('test-case', 'foundry')).toEqual(result)
    expect(fetch.mock.calls[0]![0]).toBe('/api/validation/scenarios/test-case')
    expect(fetch.mock.calls[0]![1]?.method).toBe('POST')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({ provider: 'foundry' })
  })
  it('does not accept a result for another case or contradictory passing assertions', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(jsonResponse({ ...result, scenario: 'other-case' }))
      .mockResolvedValueOnce(jsonResponse({ ...result, passed: true, failures: ['Expected assertion failed.'] }))
    const api = new ApiClient(async () => null)
    await expect(api.validateScenario('test-case', 'foundry')).rejects.toMatchObject({ code: 'unconfirmed_validation' })
    await expect(api.validateScenario('test-case', 'foundry')).rejects.toMatchObject({ code: 'unconfirmed_validation' })
    expect(fetch).toHaveBeenCalledTimes(2)
  })
  it('validates the provider catalog and recorded result shapes', async () => {
    vi.mocked(globalThis.fetch)
      .mockResolvedValueOnce(jsonResponse({ items: [{ name: 'case', title: 'Case', description: 'Case' }], providers: ['unknown'] }))
      .mockResolvedValueOnce(jsonResponse({ items: [{ ...result, passed: 'true' }] }))
    const api = new ApiClient(async () => null)
    await expect(api.validationScenarios()).rejects.toMatchObject({ code: 'invalid_scenario_catalog' })
    await expect(api.validationResults()).rejects.toMatchObject({ code: 'invalid_validation_results' })
  })
})

describe('read-only Ask response contract', () => {
  it('sends approval context in incident_id and keeps the records mode explicit', async () => {
    const answer = { answer: 'The approval is pending.', mode: 'records', references: [], question_id: 'question-1' }
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse(answer))
    expect(await new ApiClient(async () => null).ask('approval-1', 'What is pending?')).toEqual(answer)
    expect(fetch.mock.calls[0]![0]).toBe('/api/ask')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({ incident_id: 'approval-1', question: 'What is pending?' })
  })
  it('does not infer a records/model mode from an unspecified response', async () => {
    vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ answer: 'Unknown source.', mode: 'unknown', references: [], question_id: 'question-1' }))
    await expect(new ApiClient(async () => null).ask('approval-1', 'Explain.')).rejects.toMatchObject({ code: 'invalid_ask_response' })
  })
})

describe('human command reconciliation endpoint', () => {
  it('posts only a reason to the selected command reconciliation endpoint', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ status: 'reconciled', command_id: 'command/id+1' }))
    const result = await new ApiClient(async () => null).reconcileCommand('command/id+1', ' External job history reviewed. ')
    expect(result.status).toBe('reconciled')
    expect(fetch.mock.calls[0]![0]).toBe('/api/commands/command%2Fid%2B1/reconcile')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual({ reason: 'External job history reviewed.' })
    expect(fetch).toHaveBeenCalledOnce()
  })
  it('rejects a blank reason before any request and rejects a mismatched receipt', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(jsonResponse({ status: 'reconciled', command_id: 'different-command' }))
    const api = new ApiClient(async () => null)
    await expect(api.reconcileCommand('command-1', ' ')).rejects.toMatchObject({ code: 'reconciliation_reason_required' })
    expect(fetch).not.toHaveBeenCalled()
    await expect(api.reconcileCommand('command-1', 'Reviewed.')).rejects.toMatchObject({ code: 'unconfirmed_reconciliation' })
    expect(fetch).toHaveBeenCalledOnce()
  })
  it('does not retry when the reconciliation response is lost', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockRejectedValue(new TypeError('Connection lost'))
    await expect(new ApiClient(async () => null).reconcileCommand('command-1', 'Evidence checked.')).rejects.toMatchObject({ code: 'network_error' })
    expect(fetch).toHaveBeenCalledOnce()
  })
})
