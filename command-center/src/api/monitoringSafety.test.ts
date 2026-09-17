import { webcrypto } from 'node:crypto'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  configurationFingerprint, isActionParameters, isReviewParameters, MonitoringApiClient, parseSafetyParameters,
} from './monitoring'
import type { MonitoringAction, ReviewParameters, SafetyReviewIntent, SafetyReviewRequest } from './monitoring'
import { ApiError } from './errors'
import {
  ids, monitoringId, monitoringPage, monitoringSafetyIntent, monitoringSafetyReview, monitoringTarget, monitoringVersion,
  acceptedSafetyReviewResponse, publishedSafetyReview, reviewParameterHash, safetyReviewOperationReceipt, safetyReviewResponse,
} from '../test/monitoringFixtures'

beforeEach(() => vi.stubGlobal('crypto', webcrypto))
const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
const client = () => new MonitoringApiClient(async () => null)
function request(updates: Partial<SafetyReviewIntent> = {}): SafetyReviewRequest {
  return {
    request_id: ids.submission, expected: monitoringVersion, expected_review_revision: 0,
    review: { ...monitoringSafetyIntent, parameter_hash: null, ...updates },
  }
}

describe('safety review wire contract', () => {
  it('reads exactly the requested review with the current token and abort signal', async () => {
    const token = vi.fn().mockResolvedValue('synthetic-test-token')
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(monitoringSafetyReview))
    const signal = new AbortController().signal
    expect(await new MonitoringApiClient(token).safetyReview(ids.review, signal)).toEqual(monitoringSafetyReview)
    expect(fetch).toHaveBeenCalledOnce()
    expect(fetch.mock.calls[0]![0]).toBe(`/api/monitoring/safety-reviews/${ids.review}`)
    expect(fetch.mock.calls[0]![1]).toMatchObject({ signal, credentials: 'omit', cache: 'no-store' })
    expect(new Headers(fetch.mock.calls[0]![1]?.headers).has('Authorization')).toBe(true)
  })

  it('looks up the original operation by request ID and checks the correlated receipt rather than the current review', async () => {
    const input = request()
    const receipt = safetyReviewOperationReceipt(input)
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(receipt))
    const token = vi.fn().mockResolvedValue('synthetic-test-token')
    const signal = new AbortController().signal
    expect(await new MonitoringApiClient(token).safetyReviewOperation(input.request_id, signal)).toEqual(receipt)
    expect(fetch.mock.calls[0]![0]).toBe(`/api/monitoring/safety-review-operations/${input.request_id}`)
    expect(fetch.mock.calls[0]![1]).toMatchObject({ signal, cache: 'no-store', credentials: 'omit' })
    expect(fetch.mock.calls[0]![1]?.method ?? 'GET').toBe('GET')
    expect(fetch).toHaveBeenCalledOnce()
  })

  it('accepts the backend operation metadata while retaining the original request and review projection', async () => {
    const input = request()
    const receipt = safetyReviewOperationReceipt(input)
    vi.mocked(globalThis.fetch).mockResolvedValue(response({
      ...receipt, target: receipt.review.target, action: receipt.review.action,
      expected: input.expected, expected_review_revision: input.expected_review_revision,
      new_review_revision: receipt.review.revision, fingerprint: 'a'.repeat(64),
      recorded_at: receipt.review.reviewed_at,
    }))
    expect(await client().safetyReviewOperation(input.request_id)).toEqual(receipt)
    expect(globalThis.fetch).toHaveBeenCalledOnce()
  })

  it.each([
    monitoringSafetyReview,
    { request_id: ids.work, review: monitoringSafetyReview },
    { request_id: ids.submission, review: null },
    { request_id: ids.submission, review: { ...monitoringSafetyReview, revision: 0 } },
  ])('does not accept an uncorrelated, bare-current or invalid operation receipt: %j', async (value) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response(value))
    await expect(client().safetyReviewOperation(ids.submission)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
    expect(globalThis.fetch).toHaveBeenCalledOnce()
  })

  it.each([404, 503])('keeps missing or unavailable operation lookup explicit without a current-review fallback (%s)', async (status) => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response({ detail: { code: 'operation_unavailable', message: 'The original operation receipt is unavailable.' } }, status))
    await expect(client().safetyReviewOperation(ids.submission)).rejects.toMatchObject({ status, code: 'operation_unavailable' })
    expect(fetch).toHaveBeenCalledOnce()
    expect(fetch.mock.calls[0]![0]).toBe(`/api/monitoring/safety-review-operations/${ids.submission}`)
  })

  it('posts the exact request/review IDs, registry version and next review revision, accepting server reviewer identity', async () => {
    const input = request({ reviewer_id: monitoringId(90) })
    const returned = safetyReviewResponse(input)
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(returned))
    expect(await client().recordSafetyReview(input)).toEqual(returned)
    expect(fetch.mock.calls[0]![0]).toBe('/api/monitoring/safety-reviews')
    expect(fetch.mock.calls[0]![1]?.method).toBe('POST')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body))).toEqual(input)
    expect(returned.reviewer_id).not.toBe(input.review.reviewer_id)
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body)).review).not.toHaveProperty('requested_state')
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body)).review).not.toHaveProperty('publication_status')
  })

  it.each(['pending', 'verified', 'revoked', 'unverifiable'] as const)('accepts committed %s intent without claiming its requested state is published', async (state) => {
    const input = request({
      state, exact_correlation_verified: state === 'verified',
      revoked_at: state === 'revoked' ? '2026-09-16T12:00:00Z' : null,
    })
    const accepted = acceptedSafetyReviewResponse(input, { reviewed_at: '2026-09-16T12:00:00Z' })
    vi.mocked(globalThis.fetch).mockResolvedValue(response(accepted))
    const result = await client().recordSafetyReview(input)
    expect(result).toMatchObject({
      state: 'pending', requested_state: state, publication_status: 'pending_validation',
      exact_correlation_verified: false, revoked_at: null,
    })
    expect(globalThis.fetch).toHaveBeenCalledOnce()
  })

  it('accepts an expired revocation intent acknowledgement and its immutable receipt before publication', async () => {
    const input = request({
      state: 'revoked', reviewed_at: '2026-09-16T12:00:00Z', expires_at: '2026-09-15T13:00:00Z',
      revoked_at: '2026-09-16T12:00:00Z', revision: 2,
    })
    input.expected_review_revision = 1
    const accepted = acceptedSafetyReviewResponse(input)
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(response(accepted))
      .mockResolvedValueOnce(response(safetyReviewOperationReceipt(input, accepted)))
    expect(await client().recordSafetyReview(input)).toEqual(accepted)
    expect((await client().safetyReviewOperation(input.request_id)).review).toEqual(accepted)
    expect(accepted.revoked_at).toBeNull()
    expect(Date.parse(accepted.expires_at)).toBeLessThan(Date.parse(accepted.reviewed_at))
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it('keeps the operation acceptance immutable when the current review is published with a newer policy revision', async () => {
    const input = request({ state: 'verified', exact_correlation_verified: true })
    const accepted = acceptedSafetyReviewResponse(input)
    const published = publishedSafetyReview(accepted, 'unverifiable', monitoringVersion.revision + 1)
    const operation = safetyReviewOperationReceipt(input, accepted)
    vi.mocked(globalThis.fetch).mockResolvedValueOnce(response(operation)).mockResolvedValueOnce(response(published))
    expect(await client().safetyReviewOperation(input.request_id)).toEqual(operation)
    expect(await client().safetyReview(input.review.review_id)).toMatchObject({
      state: 'unverifiable', requested_state: 'verified', publication_status: 'published', policy_revision: 4,
    })
    expect(operation.review.publication_status).toBe('pending_validation')
  })

  it.each([
    { requested_state: undefined }, { publication_status: undefined },
    { requested_state: 'approved' }, { publication_status: 'complete' },
  ])('rejects missing or unknown publication metadata instead of defaulting to an older wire shape: %j', async (updates) => {
    const invalid = { ...monitoringSafetyReview, ...updates }
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValueOnce(response(invalid))
      .mockResolvedValueOnce(response({ request_id: ids.submission, review: invalid }))
    await expect(client().safetyReview(ids.review)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
    await expect(client().safetyReviewOperation(ids.submission)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
    expect(fetch).toHaveBeenCalledTimes(2)
  })

  it.each([
    { state: 'verified', exact_correlation_verified: true },
    { state: 'revoked', revoked_at: '2026-09-16T12:00:00Z' },
    { requested_state: null }, { exact_correlation_verified: true },
    { revoked_at: '2026-09-16T12:00:00Z' },
    { reviewed_at: '2026-09-16T12:00:00Z', expires_at: '2026-09-15T13:00:00Z' },
  ])('refuses malformed pending validation or non-revocation expiry: %j', async (updates) => {
    const input = request({ state: 'verified', exact_correlation_verified: true })
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...acceptedSafetyReviewResponse(input), ...updates }))
    await expect(client().recordSafetyReview(input)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each(['pending_validation', 'published'] as const)('does not confirm a different requested state in a %s result', async (publication_status) => {
    const input = request({
      state: 'revoked', revoked_at: '2026-09-16T12:00:00Z',
    })
    const unrelated = {
      ...acceptedSafetyReviewResponse(input), publication_status,
      requested_state: 'verified', state: 'pending',
    }
    vi.mocked(globalThis.fetch).mockResolvedValue(response(unrelated))
    await expect(client().recordSafetyReview(input)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('requires explicit removal of response-only metadata before posting a new human intent', async () => {
    const current = publishedSafetyReview(
      acceptedSafetyReviewResponse(request({ state: 'verified', exact_correlation_verified: true })), 'unverifiable', 4,
    )
    const input = { ...request(), review: { ...current, revision: 2, policy_revision: 3 }, expected_review_revision: 1 }
    await expect(client().recordSafetyReview(input)).rejects.toMatchObject({ status: 400, code: 'invalid_safety_review' })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it.each([
    { review_id: ids.epoch }, { action: 'pipeline_rerun' },
    { revision: 2 }, { policy_revision: 4 },
    { target: { ...monitoringSafetyReview.target, item_id: ids.otherModel } },
    { target: { ...monitoringSafetyReview.target, tenant_id: ids.epoch } },
    { expires_at: '2098-01-01T00:00:00Z' },
    { definition_hash: 'a'.repeat(64) },
    { parameters: { unexpected: true }, parameter_hash: reviewParameterHash({ unexpected: true }) },
    { state: 'verified', exact_correlation_verified: true },
  ])('does not accept a POST reply for different intent or revision: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...monitoringSafetyReview, ...updates }))
    await expect(client().recordSafetyReview(request())).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
    expect(globalThis.fetch).toHaveBeenCalledOnce()
  })

  it.each([
    { review_id: undefined }, { target: undefined }, { revision: 0 }, { policy_revision: -1 },
    { state: 'active' }, { reviewer_id: 'configured-user' }, { parameters: undefined },
    { parameter_hash: null }, { parameters_redacted: undefined },
    { exact_correlation_verified: undefined }, { detail: '' },
    { revoked_at: '2026-09-15T13:00:00Z' }, { state: 'revoked' },
    { expires_at: monitoringSafetyReview.reviewed_at },
    { action: 'disable_monitoring' },
    { parameters: { Batch: 1, batch: 2 }, parameter_hash: 'a'.repeat(64) },
  ])('fails loudly for missing or inconsistent returned review fields: %j', async (updates) => {
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...monitoringSafetyReview, ...updates }))
    await expect(client().safetyReview(ids.review)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each(['pending', 'unverifiable'] as const)('shows a verified-state request returned as %s without inferring verification', async (state) => {
    const input = request({ state: 'verified', exact_correlation_verified: true, parameters: { window: 'daily' } })
    const returned = safetyReviewResponse(input, { state })
    vi.mocked(globalThis.fetch).mockResolvedValue(response(returned))
    expect((await client().recordSafetyReview(input)).state).toBe(state)
  })

  it('accepts explicit redaction as unavailable parameters, never as executable replacement text', async () => {
    const input = request({ state: 'verified', exact_correlation_verified: true, parameters: { batch: 'daily' } })
    const returned = safetyReviewResponse(input, {
      state: 'unverifiable', parameters: null, parameters_redacted: true, parameter_hash: reviewParameterHash(input.review.parameters),
    })
    vi.mocked(globalThis.fetch).mockResolvedValue(response(returned))
    expect(await client().recordSafetyReview(input)).toEqual(returned)
    vi.mocked(globalThis.fetch).mockResolvedValue(response({ ...returned, state: 'verified' }))
    await expect(client().safetyReview(ids.review)).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it('requires a known definition, explicit parameter object and replay attestation for a verified pipeline', async () => {
    const pipeline = request({
      action: 'pipeline_rerun', target: { ...monitoringTarget.identity, workload: 'fabric_pipeline', item_id: ids.pipeline },
      state: 'verified', definition_hash: 'a'.repeat(64), parameters: {}, replay_safe: true, exact_correlation_verified: true,
    })
    vi.mocked(globalThis.fetch).mockResolvedValue(response(safetyReviewResponse(pipeline)))
    expect((await client().recordSafetyReview(pipeline)).parameters).toEqual({})
    vi.mocked(globalThis.fetch).mockClear()
    for (const updates of [
      { parameters: null }, { definition_hash: null }, { replay_safe: false }, { exact_correlation_verified: false },
    ]) {
      await expect(client().recordSafetyReview({ ...pipeline, review: { ...pipeline.review, ...updates } }))
        .rejects.toMatchObject({ status: 400 })
    }
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it('supports explicit revocation of an expired review without inventing a new expiry', async () => {
    const input = request({
      state: 'revoked', expires_at: '2026-09-15T13:00:00Z', revoked_at: '2026-09-15T14:00:00Z',
      revision: 2, detail: 'Revoke the previous action profile.',
    })
    input.expected_review_revision = 1
    vi.mocked(globalThis.fetch).mockResolvedValue(response(safetyReviewResponse(input)))
    expect((await client().recordSafetyReview(input)).state).toBe('revoked')
  })

  it.each(['rebind_dataset_gateway', 'reenable_refresh_schedule'] as const)('sends %s as configuration intent, never a forged REST proof', async (action) => {
    const parameters: ReviewParameters = action === 'rebind_dataset_gateway'
      ? { gateway_id: ids.gateway, datasource_ids: [ids.datasource] } : { enabled: true }
    const hash = await configurationFingerprint(action, parameters)
    expect(hash).toBe(reviewParameterHash(parameters))
    const input = request({ action, state: 'verified', parameters, configuration_hash: hash, exact_correlation_verified: false })
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response(safetyReviewResponse(input)))
    expect((await client().recordSafetyReview(input)).configuration_hash).toBe(hash)
    const body = JSON.parse(String(fetch.mock.calls[0]![1]?.body))
    expect(body.review.parameters).toEqual(parameters)
    expect(body.review).not.toHaveProperty('configuration_verification')
    expect(body.review).not.toHaveProperty('authority')
    expect(body.review).not.toHaveProperty('observed_at')
  })

  it('requires canonical sorted distinct gateway IDs and exactly enabled:true for schedule intent', async () => {
    for (const parameters of [
      { gateway_id: ids.gateway, datasource_ids: [] },
      { gateway_id: ids.gateway, datasource_ids: [ids.datasource, ids.datasource] },
      { gateway_id: ids.gateway, datasource_ids: [ids.datasource, ids.gateway] },
      { gateway_id: 'HTTPS://example.test', datasource_ids: [ids.datasource] },
      { gateway_id: monitoringId(26).toUpperCase(), datasource_ids: [ids.datasource] },
      { gateway_id: ids.gateway, datasource_ids: [ids.datasource], other: true },
    ]) expect(isActionParameters('rebind_dataset_gateway', parameters)).toBe(false)
    expect(isActionParameters('reenable_refresh_schedule', { enabled: false })).toBe(false)
    expect(isActionParameters('reenable_refresh_schedule', { enabled: true, days: ['Monday'] })).toBe(false)
    expect(isActionParameters('reenable_refresh_schedule', { enabled: true })).toBe(true)
    const input = request({ action: 'reenable_refresh_schedule', parameters: { enabled: true }, configuration_hash: 'f'.repeat(64), state: 'verified' })
    await expect(client().recordSafetyReview(input)).rejects.toMatchObject({ status: 400, code: 'invalid_configuration_hash' })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })

  it('recognizes the expanded action kinds on target records without relaxing workload binding', async () => {
    const fetch = vi.mocked(globalThis.fetch)
    for (const action of ['powerbi_refresh', 'rebind_dataset_gateway', 'reenable_refresh_schedule'] as const) {
      const target = { ...monitoringTarget, action: { enabled: true, action, review_id: ids.review, review_revision: 1 } }
      fetch.mockResolvedValueOnce(response(monitoringPage([target])))
      expect((await client().targets()).items[0]!.action.action).toBe(action)
    }
    fetch.mockResolvedValueOnce(response(monitoringPage([{
      ...monitoringTarget, identity: { ...monitoringTarget.identity, workload: 'fabric_pipeline' },
      action: { enabled: true, action: 'rebind_dataset_gateway', review_id: ids.review, review_revision: 1 },
    }])))
    await expect(client().targets()).rejects.toMatchObject({ code: 'invalid_monitoring_response' })
  })

  it.each([401, 403, 404, 409, 422, 429, 503])('preserves a rejected or unavailable transition (%s) without retrying', async (status) => {
    const fetch = vi.mocked(globalThis.fetch).mockResolvedValue(response({ detail: { code: 'review_transition_unavailable', message: 'The store cannot accept this transition.' } }, status))
    await expect(client().recordSafetyReview(request())).rejects.toMatchObject({ status, code: 'review_transition_unavailable' })
    expect(fetch).toHaveBeenCalledOnce()
  })

  it('reports lost writes and missing lookups without fabricating success or a new request ID', async () => {
    const fetch = vi.mocked(globalThis.fetch).mockRejectedValueOnce(new TypeError('Response lost'))
      .mockResolvedValueOnce(response({ detail: { code: 'not_found', message: 'Safety review not found.' } }, 404))
    await expect(client().recordSafetyReview(request())).rejects.toMatchObject({ status: 0, code: 'network_error' })
    await expect(client().safetyReview(ids.review)).rejects.toMatchObject({ status: 404 })
    expect(fetch).toHaveBeenCalledTimes(2)
    expect(JSON.parse(String(fetch.mock.calls[0]![1]?.body)).request_id).toBe(ids.submission)
  })

  it('does not fall back to an unauthenticated write or submit stale review revisions', async () => {
    const failure = new ApiError(401, 'sign_in_required', 'Sign in again.')
    await expect(new MonitoringApiClient(async () => { throw failure }).recordSafetyReview(request())).rejects.toBe(failure)
    await expect(client().recordSafetyReview({ ...request(), expected_review_revision: 4 })).rejects.toMatchObject({ status: 400 })
    await expect(client().recordSafetyReview(request({ policy_revision: 2 }))).rejects.toMatchObject({ status: 400 })
    expect(globalThis.fetch).not.toHaveBeenCalled()
  })
})

describe('review parameter input boundary', () => {
  it('distinguishes a missing pipeline parameter set from an explicitly reviewed empty object', () => {
    expect(parseSafetyParameters('pipeline_rerun', '{}')).toEqual({})
    for (const value of ['', 'null', '[]', 'true', '{"Batch":1,"batch":2}']) {
      expect(() => parseSafetyParameters('pipeline_rerun', value)).toThrow(ApiError)
    }
    expect(parseSafetyParameters('powerbi_refresh', '')).toBeNull()
    expect(parseSafetyParameters('powerbi_refresh', 'null')).toBeNull()
    expect(parseSafetyParameters('powerbi_refresh', '{"batch":"daily"}')).toEqual({ batch: 'daily' })
  })
  it('enforces the shared JSON bounds and distinct parameter names', () => {
    expect(isReviewParameters({ batch: 'daily', nested: { count: 2 }, dates: [true, null] })).toBe(true)
    expect(isReviewParameters({ Batch: 1, batch: 2 })).toBe(false)
    expect(isReviewParameters({ ' ': 1 })).toBe(false)
    expect(isReviewParameters({ count: Number.POSITIVE_INFINITY })).toBe(false)
    expect(isReviewParameters({ text: 'x'.repeat(65536) })).toBe(false)
    expect(isReviewParameters({ values: Array.from({ length: 4100 }, () => 1) })).toBe(false)
  })
  it.each(['pipeline_rerun', 'powerbi_refresh', 'rebind_dataset_gateway', 'reenable_refresh_schedule'] as MonitoringAction[])('never accepts primitive parameters for %s', (action) => {
    expect(isActionParameters(action, 'parameters')).toBe(false)
    expect(isActionParameters(action, ['parameters'])).toBe(false)
  })
})
