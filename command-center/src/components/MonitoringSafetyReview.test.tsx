import { webcrypto } from 'node:crypto'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { MonitoringSafetyReview, SAFETY_REVIEW_RECOVERY_KEY } from './MonitoringSafetyReview'
import type { MonitoringSafetyReviewProps } from './MonitoringSafetyReview'
import { ApiError } from '../api/errors'
import { MonitoringApiClient, monitoringTargetKey } from '../api/monitoring'
import type { MonitoringTarget, SafetyReview, SafetyReviewOperationReceipt, SafetyReviewRequest } from '../api/monitoring'
import {
  ids, monitoringFixtureApi, monitoringId, monitoringInventory, monitoringSafetyIntent, monitoringSafetyReview, monitoringTarget,
  monitoringVersion, acceptedSafetyReviewResponse, publishedSafetyReview, reviewParameterHash,
  safetyReviewOperationReceipt, safetyReviewResponse,
} from '../test/monitoringFixtures'

beforeEach(() => vi.stubGlobal('crypto', webcrypto))
afterEach(() => window.sessionStorage.removeItem(SAFETY_REVIEW_RECOVERY_KEY))
function referencedTarget(review: SafetyReview, enabled = false): MonitoringTarget {
  return { ...monitoringTarget, identity: review.target, policy_revision: review.policy_revision, action: {
    enabled, action: review.action, review_id: review.review_id, review_revision: review.revision,
  } }
}
function setup(options: Partial<MonitoringSafetyReviewProps> & { review?: SafetyReview } = {}) {
  const { review, ...overrides } = options
  const api = overrides.api ?? monitoringFixtureApi()
  if (review) vi.mocked(api.safetyReview).mockResolvedValue(review)
  const target = overrides.target ?? (review ? referencedTarget(review) : monitoringTarget)
  const props: MonitoringSafetyReviewProps = {
    api, selection: { identity: target.identity, name: target.name, workspaceName: 'Operations workspace' },
    target, inventory: monitoringInventory[0]!, expected: monitoringVersion, userId: ids.reviewer,
    admin: true, allowed: true, permissionKey: '0:current', active: true, onChanged: vi.fn(),
    onSelect: vi.fn(), onClose: vi.fn(), onBusyChange: vi.fn(), ...overrides,
  }
  return { ...render(<MonitoringSafetyReview {...props} />), props, api, user: userEvent.setup() }
}
function setParameters(value: string) {
  fireEvent.change(screen.getByRole('textbox', { name: 'Reviewed parameters (JSON)' }), { target: { value } })
}
async function complete(user: ReturnType<typeof userEvent.setup>, action = 'powerbi_refresh', state = 'pending') {
  const select = await screen.findByRole<HTMLSelectElement>('combobox', { name: 'Action profile' })
  if (!select.disabled) await user.selectOptions(select, action)
  await user.selectOptions(screen.getByRole('combobox', { name: 'Requested review state' }), state)
  if (state !== 'revoked') fireEvent.change(screen.getByLabelText('Review expiry (your local time)'), { target: { value: '2098-12-15T12:00' } })
  await user.type(screen.getByRole('textbox', { name: 'Reason for this review change' }), 'Reviewed this specific target and intended action.')
}
const pipeline: MonitoringTarget = {
  ...monitoringTarget, name: 'Scheduled ingestion',
  identity: { ...monitoringTarget.identity, workload: 'fabric_pipeline', item_id: ids.pipeline },
}

describe('explicit target safety review', () => {
  it('requires an explicit action, future expiry and reason and never changes observation', async () => {
    const { api, user, props } = setup()
    expect((await screen.findByRole<HTMLSelectElement>('combobox', { name: 'Action profile' })).value).toBe('')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
    await complete(user)
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as pending, revision 1/)
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    expect(input).toMatchObject({
      expected: monitoringVersion, expected_review_revision: 0,
      review: { target: monitoringTarget.identity, revision: 1, policy_revision: 3, action: 'powerbi_refresh',
        state: 'pending', reviewer_id: ids.reviewer, parameters: null, exact_correlation_verified: false, replay_safe: false },
    })
    expect(input.review).not.toHaveProperty('observation')
    expect(props.onChanged).toHaveBeenCalledOnce()
    expect(api.activate).not.toHaveBeenCalled()
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()
    expect(screen.getByText(/not the human approval for an individual remediation/)).toBeTruthy()
  })

  it.each([false, true])('keeps Reader and stale Admin pages from submitting reviews (admin=%s)', async (admin) => {
    const { api } = setup({ admin, allowed: false, review: monitoringSafetyReview })
    await screen.findByRole('heading', { name: 'Returned review record' })
    if (admin) expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    else expect(screen.queryByRole('combobox', { name: 'Action profile' })).toBeNull()
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
    expect(screen.getByText(/Prior page snapshots cannot restore permission/)).toBeTruthy()
  })

  it('does not invent a canonical reviewer identity when the validated User ID is unavailable', async () => {
    const { api } = setup({ userId: 'demo-user' })
    await screen.findByRole('combobox', { name: 'Action profile' })
    expect(screen.getByText(/No replacement reviewer identity will be invented/)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })

  it.each(['review_required', 'paused', 'removed'] as const)('reports unsupported review/revoke transitions for a %s target', async (state) => {
    const target = { ...referencedTarget(monitoringSafetyReview), state }
    const { api } = setup({ target, review: monitoringSafetyReview })
    await screen.findByRole('heading', { name: 'Returned review record' })
    expect(screen.getByText(/including revocation, only for currently admitted targets/)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })

  it('requires the current pipeline definition, an explicit parameter object and fresh replay attestation', async () => {
    const { api, user } = setup({
      target: pipeline, selection: { identity: pipeline.identity, name: pipeline.name, workspaceName: 'Operations workspace' },
      inventory: { ...monitoringInventory[1]!, definition_hash: 'a'.repeat(64) },
    })
    const options = within(await screen.findByRole('combobox', { name: 'Action profile' }))
    expect(options.getByRole('option', { name: 'Full pipeline rerun' })).toBeTruthy()
    expect(options.queryByRole('option', { name: 'Power BI refresh' })).toBeNull()
    await complete(user, 'pipeline_rerun', 'verified')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    setParameters('null')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    setParameters('{}')
    const attestation = screen.getByRole<HTMLInputElement>('checkbox', { name: /full-pipeline replay safety/ })
    await user.click(attestation)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(false)
    setParameters('{"batch":"daily"}')
    expect(attestation.checked).toBe(false)
    await user.click(attestation)
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as verified/)
    expect(vi.mocked(api.recordSafetyReview).mock.calls[0]![0].review).toMatchObject({
      action: 'pipeline_rerun', state: 'verified', definition_hash: 'a'.repeat(64), parameters: { batch: 'daily' },
      replay_safe: true, exact_correlation_verified: true,
    })
    expect(screen.getByText(/not a live capability probe/)).toBeTruthy()
  })

  it.each([null, 'stale-generation'] as const)('does not enable a pipeline profile with missing or stale inventory hash (%s)', async (source) => {
    const inventory = source === null ? { ...monitoringInventory[1]!, definition_hash: null }
      : { ...monitoringInventory[1]!, generation_id: ids.epoch, definition_hash: 'a'.repeat(64) }
    const { api, user } = setup({ target: pipeline, inventory })
    await complete(user, 'pipeline_rerun', 'verified')
    setParameters('{}')
    await user.click(screen.getByRole('checkbox', { name: /full-pipeline replay safety/ }))
    expect(screen.getByText('Not available from current inventory')).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })

  it('normalizes the explicitly reviewed gateway intent without copying credentials or unrelated parameters', async () => {
    const { api, user } = setup()
    await complete(user, 'rebind_dataset_gateway', 'verified')
    await user.type(screen.getByRole('textbox', { name: 'Reviewed gateway ID' }), ids.gateway)
    fireEvent.change(screen.getByRole('textbox', { name: 'Reviewed datasource IDs' }),
      { target: { value: `${ids.datasource}\n${ids.ownership}\n${ids.datasource}` } })
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as verified/)
    const parameters = { gateway_id: ids.gateway, datasource_ids: [ids.ownership, ids.datasource].sort() }
    expect(vi.mocked(api.recordSafetyReview).mock.calls[0]![0].review).toMatchObject({
      action: 'rebind_dataset_gateway', parameters, configuration_hash: reviewParameterHash(parameters),
      exact_correlation_verified: false,
    })
  })

  it('records exactly enabled:true as an explicit schedule profile intent, not an immediate schedule operation', async () => {
    const { api, user } = setup()
    await complete(user, 'reenable_refresh_schedule', 'verified')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    await user.click(screen.getByRole('checkbox', { name: 'Reviewed intent: enable the refresh schedule' }))
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as verified/)
    expect(vi.mocked(api.recordSafetyReview).mock.calls[0]![0].review).toMatchObject({
      action: 'reenable_refresh_schedule', parameters: { enabled: true },
      configuration_hash: reviewParameterHash({ enabled: true }), exact_correlation_verified: false,
    })
    expect(api.activate).not.toHaveBeenCalled()
  })

  it('uses a new explicit review identity when replacing an action profile and copies no previous intent', async () => {
    const oldReview: SafetyReview = {
      ...monitoringSafetyReview, action: 'pipeline_rerun', target: pipeline.identity,
      state: 'verified', definition_hash: 'a'.repeat(64), parameters: { batch: 'previous' },
      parameter_hash: reviewParameterHash({ batch: 'previous' }), exact_correlation_verified: true, replay_safe: true,
    }
    const { api, user } = setup({ review: oldReview, inventory: { ...monitoringInventory[1]!, definition_hash: 'a'.repeat(64) } })
    await user.click(await screen.findByRole('button', { name: 'Create a different action profile' }))
    expect(screen.getByRole<HTMLSelectElement>('combobox', { name: 'Action profile' }).value).toBe('')
    await complete(user, 'pipeline_rerun', 'verified')
    expect(screen.getByRole<HTMLTextAreaElement>('textbox', { name: 'Reviewed parameters (JSON)' }).value).toBe('')
    expect(screen.getByRole<HTMLInputElement>('checkbox', { name: /full-pipeline replay safety/ }).checked).toBe(false)
    setParameters('{}')
    await user.click(screen.getByRole('checkbox', { name: /full-pipeline replay safety/ }))
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as verified/)
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    expect(input.expected_review_revision).toBe(0)
    expect(input.review.review_id).not.toBe(oldReview.review_id)
    expect(input.review.parameters).toEqual({})
  })

  it('updates the existing revision to pending to disable action admission without changing observation', async () => {
    const oldReview = { ...monitoringSafetyReview, state: 'verified' as const, exact_correlation_verified: true }
    const { api, user } = setup({ review: oldReview })
    await complete(user, 'powerbi_refresh', 'pending')
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as pending, revision 2/)
    expect(vi.mocked(api.recordSafetyReview).mock.calls[0]![0]).toMatchObject({
      expected_review_revision: 1, review: { review_id: ids.review, revision: 2, state: 'pending', exact_correlation_verified: false },
    })
  })

  it('revokes an expired review using its original dates and an explicit new reason', async () => {
    const oldReview = { ...monitoringSafetyReview, state: 'verified' as const, exact_correlation_verified: true, expires_at: '2026-09-15T13:00:00Z' }
    const { api, user } = setup({ review: oldReview })
    await complete(user, 'powerbi_refresh', 'revoked')
    expect(screen.queryByLabelText('Review expiry (your local time)')).toBeNull()
    await user.click(screen.getByRole('button', { name: 'Revoke safety review' }))
    await screen.findByText(/Safety review recorded as revoked, revision 2/)
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    expect(input.review.reviewed_at).toBe(oldReview.reviewed_at)
    expect(input.review.expires_at).toBe(oldReview.expires_at)
    expect(input.review.revoked_at).not.toBeNull()
    expect(input.review.detail).toBe('Reviewed this specific target and intended action.')
  })

  it('shows an unverifiable/redacted result and never restores action readiness from the requested state', async () => {
    const { api, user, props, rerender } = setup()
    let returned!: SafetyReview
    vi.mocked(api.recordSafetyReview).mockImplementation(async (input) => {
      returned = safetyReviewResponse(input, { state: 'unverifiable', parameters: null, parameters_redacted: true })
      vi.mocked(api.safetyReview).mockResolvedValue(returned)
      return returned
    })
    await complete(user, 'powerbi_refresh', 'verified')
    setParameters('{"batch":"daily"}')
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as unverifiable/)
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(returned)} />)
    await screen.findByText(/current server target keeps this action disabled/)
    expect(screen.getByText('Unverifiable', { selector: '.badge' })).toBeTruthy()
    expect(screen.queryByText('Verified', { selector: '.badge' })).toBeNull()
    expect(screen.getByText(/Reviewed parameters are unavailable after store redaction/)).toBeTruthy()
  })
})

describe('accepted safety intent and publication', () => {
  it.each(['verified', 'pending', 'unverifiable'] as const)('shows accepted %s intent as pending validation, not a published review or repair', async (state) => {
    const { api, props, user, rerender } = setup({ review: monitoringSafetyReview })
    let accepted!: SafetyReview
    vi.mocked(api.recordSafetyReview).mockImplementation(async (input) => {
      accepted = acceptedSafetyReviewResponse(input)
      vi.mocked(api.safetyReview).mockResolvedValue(accepted)
      return accepted
    })
    await complete(user, 'powerbi_refresh', state)
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(new RegExp(`Safety-review intent accepted: ${state}, revision 2`))
    await screen.findByText('Pending validation', { selector: '.badge' })
    expect(screen.getByText('Recorded state').parentElement?.querySelector('dd')?.textContent).toBe('Pending')
    expect(screen.getByText('Requested state').parentElement?.querySelector('dd')?.textContent).toBe(
      state[0]!.toUpperCase() + state.slice(1))
    expect(screen.queryByText('Verified', { selector: '.badge' })).toBeNull()
    expect(screen.queryByText(/Safety-review outcome is unconfirmed/)).toBeNull()
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()
    const posted = vi.mocked(api.recordSafetyReview).mock.calls[0]![0].review
    expect(posted).not.toHaveProperty('requested_state')
    expect(posted).not.toHaveProperty('publication_status')
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(accepted, true)} />)
    await screen.findByText(/Action readiness is not established by this pending intent/)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
  })

  it.each([false, true])('confirms a revocation intent with no published revoked_at, including expired reviews (expired=%s)', async (expired) => {
    const prior: SafetyReview = {
      ...monitoringSafetyReview, state: 'verified', exact_correlation_verified: true,
      expires_at: expired ? '2026-09-15T13:00:00Z' : monitoringSafetyReview.expires_at,
    }
    const { api, user } = setup({ review: prior, target: referencedTarget(prior, true) })
    let accepted!: SafetyReview
    vi.mocked(api.recordSafetyReview).mockImplementation(async (input) => {
      accepted = acceptedSafetyReviewResponse(input, { reviewed_at: '2026-09-16T12:00:00Z' })
      vi.mocked(api.safetyReview).mockResolvedValue(accepted)
      return accepted
    })
    await complete(user, 'powerbi_refresh', 'revoked')
    await user.click(screen.getByRole('button', { name: 'Revoke safety review' }))
    await screen.findByText(/Safety-review intent accepted: revoked, revision 2/)
    await screen.findByText('Revocation intent committed; publication pending')
    expect(screen.getByText(/fences new action reservations/)).toBeTruthy()
    expect(screen.getByText(/fences new action reservations/).textContent).toContain('does not cancel actions already reserved')
    expect(screen.getByText('Revoked', { selector: 'dt' }).parentElement?.querySelector('dd')?.textContent).toBe('Not reported')
    expect(accepted.revoked_at).toBeNull()
    if (expired) expect(Date.parse(accepted.expires_at)).toBeLessThan(Date.parse(accepted.reviewed_at))
    expect(screen.queryByText('Safety-review outcome is unconfirmed')).toBeNull()
    expect(api.safetyReviewOperation).not.toHaveBeenCalled()
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()
  })

  it('reads publication while target projection is lagging, preserves requested state and waits for fresh matching admission', async () => {
    const { api, props, user, rerender } = setup({ review: monitoringSafetyReview })
    let accepted!: SafetyReview
    vi.mocked(api.recordSafetyReview).mockImplementation(async (input) => {
      accepted = acceptedSafetyReviewResponse(input)
      vi.mocked(api.safetyReview).mockResolvedValue(accepted)
      return accepted
    })
    await complete(user, 'powerbi_refresh', 'verified')
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety-review intent accepted: verified/)
    await waitFor(() => expect(vi.mocked(api.safetyReview).mock.calls.some(([id]) => id === accepted.review_id)).toBe(true))
    const version = { ...monitoringVersion, revision: 4 }
    rerender(<MonitoringSafetyReview {...props} expected={version} />)
    await screen.findByText(/target policy revision is not current/)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)

    const published = publishedSafetyReview(accepted, 'unverifiable', 4)
    vi.mocked(api.safetyReview).mockResolvedValue(published)
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText('Unverifiable', { selector: '.badge' })
    expect(screen.getByText('Requested state').parentElement?.querySelector('dd')?.textContent).toBe('Verified')
    expect(screen.getByText('Publication').parentElement?.querySelector('dd')?.textContent).toBe('Published')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    rerender(<MonitoringSafetyReview {...props} expected={version} target={referencedTarget(published)} />)
    await screen.findByText(/current server target keeps this action disabled/)
    expect(screen.getByRole<HTMLSelectElement>('combobox', { name: 'Requested review state' }).value).toBe('unverifiable')
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
  })

  it('does not replace published state with an older pending-validation GET at the same review revision', async () => {
    const accepted = acceptedSafetyReviewResponse({
      request_id: ids.submission, expected: monitoringVersion, expected_review_revision: 0,
      review: { ...monitoringSafetyIntent, state: 'verified', exact_correlation_verified: true },
    })
    const published = publishedSafetyReview(accepted, 'unverifiable', 4)
    const { api, user } = setup({ review: published, expected: { ...monitoringVersion, revision: 4 } })
    await screen.findByText('Unverifiable', { selector: '.badge' })
    vi.mocked(api.safetyReview).mockResolvedValue(accepted)
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText(/publication is older than the latest observed state/)
    expect(screen.getByText('Unverifiable', { selector: '.badge' })).toBeTruthy()
    expect(screen.getByText('Publication').parentElement?.querySelector('dd')?.textContent).toBe('Published')
    expect(screen.queryByText('Pending validation', { selector: '.badge' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })

  it('accepts a newer pending intent but rejects an older published revision racing its current lookup', async () => {
    const accepted = acceptedSafetyReviewResponse({
      request_id: ids.submission, expected: monitoringVersion, expected_review_revision: 1,
      review: { ...monitoringSafetyIntent, revision: 2, state: 'verified', exact_correlation_verified: true },
    })
    const published = publishedSafetyReview(accepted, 'verified', 4)
    const { api, props, user, rerender } = setup({ review: published, expected: { ...monitoringVersion, revision: 4 } })
    await screen.findByText('Verified', { selector: '.badge' })
    const next = { ...accepted, revision: 3, policy_revision: 4, requested_state: 'revoked' as const }
    vi.mocked(api.safetyReview).mockResolvedValue(next)
    rerender(<MonitoringSafetyReview {...props} expected={{ ...monitoringVersion, revision: 5 }} />)
    await screen.findByText('Revocation intent committed; publication pending')
    expect(screen.getByText('Requested state').parentElement?.querySelector('dd')?.textContent).toBe('Revoked')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Revoke safety review' }).disabled).toBe(true)
    vi.mocked(api.safetyReview).mockResolvedValue(published)
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText(/older than the latest recorded revision/)
    expect(screen.getByText('Pending validation', { selector: '.badge' })).toBeTruthy()
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })

  it.each([2, 3])('reconciles the original pending receipt after remount and loads current published revision %s without a new POST', async (latestRevision) => {
    const prior: SafetyReview = { ...monitoringSafetyReview, state: 'verified', exact_correlation_verified: true }
    const { api, props, user, unmount } = setup({ review: prior, target: referencedTarget(prior, true) })
    vi.mocked(api.recordSafetyReview).mockRejectedValueOnce(new ApiError(503, 'commit_uncertain', 'The acknowledgement was lost.'))
    await complete(user, 'powerbi_refresh', 'revoked')
    await user.click(screen.getByRole('button', { name: 'Revoke safety review' }))
    await screen.findByText('Safety-review outcome is unconfirmed')
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    const originalAcceptance = acceptedSafetyReviewResponse(input, { reviewed_at: '2026-09-16T12:00:00Z' })
    const current = latestRevision === 2
      ? publishedSafetyReview(originalAcceptance, 'revoked', 4)
      : publishedSafetyReview({
        ...originalAcceptance, revision: 3, policy_revision: 4, requested_state: 'verified',
      }, 'verified', 5)
    const pointer = JSON.parse(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)!)
    expect(pointer.binding).toMatchObject({ state: 'revoked', revision: 2, policyRevision: 3 })
    unmount()
    const immutableReceipt = safetyReviewOperationReceipt(input, originalAcceptance)
    vi.mocked(api.safetyReviewOperation).mockResolvedValue(immutableReceipt)
    vi.mocked(api.safetyReview).mockClear().mockResolvedValue(current)
    render(<MonitoringSafetyReview {...props} target={referencedTarget(current, latestRevision === 3)}
      expected={{ ...monitoringVersion, revision: current.policy_revision }} />)
    expect(api.safetyReview).not.toHaveBeenCalled()
    await user.click(await screen.findByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText(/Original safety-review intent acceptance confirmed: revoked, revision 2/)
    await screen.findByText(current.state === 'verified' ? 'Verified' : 'Revoked', { selector: '.badge' })
    expect(screen.getByText('Publication').parentElement?.querySelector('dd')?.textContent).toBe('Published')
    expect(screen.queryByText('Pending validation', { selector: '.badge' })).toBeNull()
    expect(immutableReceipt.review.publication_status).toBe('pending_validation')
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(vi.mocked(api.safetyReviewOperation).mock.calls.at(-1)![0]).toBe(input.request_id)
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()

    vi.mocked(api.safetyReview).mockResolvedValue(originalAcceptance)
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText(latestRevision === 2 ? /publication is older/ : /older than the latest recorded revision/)
    expect(screen.getByText('Publication').parentElement?.querySelector('dd')?.textContent).toBe('Published')
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
  })

  it('allows Readers to inspect pending validation and reconcile acceptance without restoring mutation rights', async () => {
    const accepted = { ...monitoringSafetyReview, state: 'pending' as const, requested_state: 'revoked' as const,
      publication_status: 'pending_validation' as const, exact_correlation_verified: false }
    const { api, props, rerender } = setup({ review: accepted, admin: false, allowed: false })
    await screen.findByText('Revocation intent committed; publication pending')
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Revoke safety review' })).toBeNull()
    const published = publishedSafetyReview(accepted, 'revoked', 4)
    vi.mocked(api.safetyReview).mockResolvedValue(published)
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(published)}
      expected={{ ...monitoringVersion, revision: 4 }} permissionKey="1:reader" />)
    await screen.findByText('Revoked', { selector: '.badge' })
    expect(screen.queryByRole('combobox', { name: 'Action profile' })).toBeNull()
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })
})

describe('safety review read and write generations', () => {
  it('does not replace a confirmed write with an older GET result', async () => {
    const { api, props, user, rerender } = setup({ review: monitoringSafetyReview })
    await complete(user)
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as pending, revision 2/)
    const returned = safetyReviewResponse(vi.mocked(api.recordSafetyReview).mock.calls[0]![0])
    vi.mocked(api.safetyReview).mockResolvedValue(monitoringSafetyReview)
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(returned)} />)
    await screen.findByText(/older than the latest recorded revision/)
    expect(screen.getByText(/Safety review recorded as pending, revision 2/)).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
  })

  it('cancels a different target read and never clears or replaces the new target draft with a late result', async () => {
    const api = monitoringFixtureApi()
    let finish!: (value: SafetyReview) => void
    vi.mocked(api.safetyReview).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    const { props, user, rerender } = setup({ api, target: referencedTarget(monitoringSafetyReview) })
    await waitFor(() => expect(api.safetyReview).toHaveBeenCalledOnce())
    const signal = vi.mocked(api.safetyReview).mock.calls[0]![1]
    const otherTarget = { ...monitoringTarget, name: 'Planning model', identity: { ...monitoringTarget.identity, workspace_id: ids.otherWorkspace, item_id: ids.otherModel } }
    rerender(<MonitoringSafetyReview {...props} target={otherTarget}
      selection={{ identity: otherTarget.identity, name: otherTarget.name, workspaceName: 'Planning workspace' }} inventory={monitoringInventory[3]!} />)
    await complete(user)
    await act(async () => finish({ ...monitoringSafetyReview, detail: 'Late other-target detail.' }))
    expect(signal?.aborted).toBe(true)
    expect(screen.queryByText('Late other-target detail.')).toBeNull()
    expect(screen.getByRole<HTMLTextAreaElement>('textbox', { name: 'Reason for this review change' }).value).toBe('Reviewed this specific target and intended action.')
  })

  it('clears volatile parameter drafts on permission renewal and holds the fresh-snapshot lock', async () => {
    const { api, props, user, rerender } = setup()
    await complete(user, 'powerbi_refresh', 'verified')
    setParameters('{"batch":"not-persisted"}')
    rerender(<MonitoringSafetyReview {...props} allowed={false} permissionKey="1:locked" />)
    await screen.findByRole('combobox', { name: 'Action profile' })
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    expect(screen.queryByDisplayValue('{"batch":"not-persisted"}')).toBeNull()
    rerender(<MonitoringSafetyReview {...props} admin={false} allowed={false} permissionKey="1:reader" />)
    await screen.findByText(/Safety-review records are read-only/)
    expect(screen.queryByRole('combobox', { name: 'Action profile' })).toBeNull()
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
  })

  it('refuses a review record bound to a different target instead of opening a replacement editor', async () => {
    const api = monitoringFixtureApi()
    vi.mocked(api.safetyReview).mockResolvedValue({ ...monitoringSafetyReview, target: { ...monitoringSafetyReview.target, item_id: ids.otherModel } })
    setup({ api, target: referencedTarget(monitoringSafetyReview) })
    expect(await screen.findByText(/belongs to another target or action/)).toBeTruthy()
    expect(screen.queryByRole('combobox', { name: 'Action profile' })).toBeNull()
  })
})

describe('same-target assignment supersession through the HTTP client', () => {
  const response = (body: unknown, status = 200) => new Response(JSON.stringify(body), {
    status, headers: { 'Content-Type': 'application/json' },
  })
  function httpSetup() {
    const initialA: SafetyReview = {
      ...monitoringSafetyReview, state: 'verified', exact_correlation_verified: true,
    }
    const profileB: SafetyReview = {
      ...monitoringSafetyReview, review_id: monitoringId(80), action: 'reenable_refresh_schedule',
      state: 'verified', requested_state: 'verified', publication_status: 'published', policy_revision: 5,
      parameters: { enabled: true }, parameter_hash: reviewParameterHash({ enabled: true }),
      configuration_hash: reviewParameterHash({ enabled: true }),
    }
    const server: {
      currentA: SafetyReview
      currentB: SafetyReview
      operationA: SafetyReviewOperationReceipt | null
      requests: SafetyReviewRequest[]
      reads: { id: string; signal: AbortSignal | null | undefined }[]
      readB?: (signal: AbortSignal | null | undefined) => Promise<Response>
    } = { currentA: initialA, currentB: profileB, operationA: null, requests: [], reads: [] }
    const getToken = vi.fn().mockResolvedValue('synthetic-review-token')
    const api = new MonitoringApiClient(getToken)
    const fetch = vi.mocked(globalThis.fetch).mockImplementation(async (url, options) => {
      const path = String(url)
      if (path === '/api/monitoring/safety-reviews' && options?.method === 'POST') {
        const input: SafetyReviewRequest = JSON.parse(String(options.body))
        server.requests.push(input)
        const accepted = acceptedSafetyReviewResponse(input)
        server.currentA = accepted
        server.operationA = safetyReviewOperationReceipt(input, accepted)
        return response(accepted)
      }
      if (path === `/api/monitoring/safety-reviews/${initialA.review_id}`) {
        server.reads.push({ id: initialA.review_id, signal: options?.signal })
        return response(server.currentA)
      }
      if (path === `/api/monitoring/safety-reviews/${profileB.review_id}`) {
        server.reads.push({ id: profileB.review_id, signal: options?.signal })
        return server.readB ? server.readB(options?.signal) : response(server.currentB)
      }
      if (server.operationA && path === `/api/monitoring/safety-review-operations/${server.operationA.request_id}`) {
        return response(server.operationA)
      }
      throw new Error(`Unexpected HTTP request: ${path}`)
    })
    const rendered = setup({ api, target: referencedTarget(initialA, true) })
    async function acceptRevocation() {
      await complete(rendered.user, 'powerbi_refresh', 'revoked')
      await rendered.user.click(screen.getByRole('button', { name: 'Revoke safety review' }))
      await screen.findByText(/Safety-review intent accepted: revoked, revision 2/)
      await screen.findByText('Pending validation', { selector: '.badge' })
      if (!server.operationA) throw new Error('The revocation POST was not recorded')
      return server.operationA
    }
    async function publishA(observeAssignment = true) {
      server.currentA = publishedSafetyReview(server.currentA, 'revoked', 4)
      if (observeAssignment) {
        rendered.rerender(<MonitoringSafetyReview {...rendered.props}
          target={referencedTarget(server.currentA)} expected={{ ...monitoringVersion, revision: 4 }} />)
      } else {
        await rendered.user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
      }
      await screen.findByText('Revoked', { selector: '.badge' })
      expect(screen.getByText('Publication').parentElement?.querySelector('dd')?.textContent).toBe('Published')
    }
    return { ...rendered, server, api, fetch, getToken, initialA, profileB, acceptRevocation, publishA }
  }

  it.each(['automatic', 'manual'] as const)('follows published profile B for the same target on %s refresh and leaves receipt A immutable', async (refresh) => {
    const { api, server, props, rerender, user, initialA, profileB, acceptRevocation, publishA } = httpSetup()
    const operation = await acceptRevocation()
    const immutableA = structuredClone(operation)
    // The manual case misses A's target projection, so supersession must also
    // work without first observing the intermediate A assignment.
    await publishA(refresh === 'automatic')
    let finishOldB: ((value: Response) => void) | undefined
    if (refresh === 'manual') {
      server.readB = async () => {
        server.readB = undefined
        return new Promise((resolve) => { finishOldB = resolve })
      }
    }
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(profileB, true)}
      expected={{ ...monitoringVersion, revision: 5 }} />)
    if (refresh === 'manual') {
      await waitFor(() => expect(server.reads.some((read) => read.id === profileB.review_id)).toBe(true))
      const oldSignal = server.reads.find((read) => read.id === profileB.review_id)!.signal
      await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
      await screen.findByText(profileB.review_id, { selector: 'code' })
      expect(oldSignal?.aborted).toBe(true)
      if (!finishOldB) throw new Error('The superseded B read did not start')
      await act(async () => finishOldB!(response({ ...profileB, policy_revision: 3 })))
    }
    await screen.findByText(profileB.review_id, { selector: 'code' })
    expect(screen.queryByText(initialA.review_id, { selector: 'code' })).toBeNull()
    expect(screen.getByRole<HTMLSelectElement>('combobox', { name: 'Action profile' }).value).toBe('reenable_refresh_schedule')
    await user.type(screen.getByRole('textbox', { name: 'Reason for this review change' }), 'Review the current B profile.')
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(false)
    const aReads = server.reads.filter((read) => read.id === initialA.review_id).length
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText(profileB.review_id, { selector: 'code' })
    expect(server.reads.at(-1)?.id).toBe(profileB.review_id)
    expect(server.reads.filter((read) => read.id === initialA.review_id)).toHaveLength(aReads)
    expect(await api.safetyReviewOperation(immutableA.request_id)).toEqual(immutableA)
    expect(server.operationA).toEqual(immutableA)
    expect(server.operationA?.review.publication_status).toBe('pending_validation')
    expect(server.requests).toHaveLength(1)
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()
    expect(screen.getByText(profileB.review_id, { selector: 'code' })).toBeTruthy()
  })

  it.each([3, 4])('keeps pending A pinned when old B is returned by a stale assignment snapshot (context=%s)', async (revision) => {
    const { server, props, rerender, user, initialA, profileB, acceptRevocation } = httpSetup()
    const operation = await acceptRevocation()
    const oldB = { ...profileB, policy_revision: 3 }
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(oldB, true)}
      expected={{ ...monitoringVersion, revision }} />)
    await screen.findByText('Pending validation', { selector: '.badge' })
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText('Pending validation', { selector: '.badge' })
    expect(server.reads.at(-1)?.id).toBe(initialA.review_id)
    expect(server.reads.filter((read) => read.id === profileB.review_id)).toHaveLength(0)
    expect(screen.getByText(initialA.review_id, { selector: 'code' })).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Revoke safety review' }).disabled).toBe(true)
    expect(server.requests).toHaveLength(1)
    expect(server.operationA).toEqual(operation)
  })

  it('does not retire the pin for an unconfirmed B publication and adopts B only after its current published response', async () => {
    const { server, props, rerender, user, initialA, profileB, acceptRevocation, publishA } = httpSetup()
    await acceptRevocation()
    await publishA(false)
    server.currentB = { ...profileB, state: 'pending', publication_status: 'pending_validation', exact_correlation_verified: false }
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(profileB, true)}
      expected={{ ...monitoringVersion, revision: 5 }} />)
    await screen.findByText(/newly assigned profile has not been confirmed/)
    expect(screen.getByText(initialA.review_id, { selector: 'code' })).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    server.currentB = profileB
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText(profileB.review_id, { selector: 'code' })
    expect(server.reads.at(-1)?.id).toBe(profileB.review_id)
    expect(server.requests).toHaveLength(1)
  })

  it('cancels an in-flight same-target B replacement on permission renewal and keeps Reader inspection mutation-free', async () => {
    const { server, props, rerender, user, profileB, acceptRevocation, publishA } = httpSetup()
    await acceptRevocation()
    await publishA(false)
    let finish!: (value: Response) => void
    server.readB = async () => {
      server.readB = undefined
      return new Promise((resolve) => { finish = resolve })
    }
    const assigned = { ...props, target: referencedTarget(profileB, true), expected: { ...monitoringVersion, revision: 5 } }
    rerender(<MonitoringSafetyReview {...assigned} />)
    await waitFor(() => expect(server.reads.some((read) => read.id === profileB.review_id)).toBe(true))
    const oldSignal = server.reads.find((read) => read.id === profileB.review_id)!.signal
    rerender(<MonitoringSafetyReview {...assigned} allowed={false} permissionKey="1:locked" />)
    await screen.findByText(profileB.review_id, { selector: 'code' })
    await act(async () => finish(response({ ...profileB, detail: 'Late pre-renewal B response.' })))
    expect(oldSignal?.aborted).toBe(true)
    expect(screen.queryByText('Late pre-renewal B response.')).toBeNull()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    rerender(<MonitoringSafetyReview {...assigned} admin={false} allowed={false} permissionKey="1:reader" />)
    await user.click(screen.getByRole('button', { name: 'Refresh safety review' }))
    await screen.findByText(profileB.review_id, { selector: 'code' })
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    expect(server.requests).toHaveLength(1)
  })

  it('does not apply a late B assignment response after changing the selected target', async () => {
    const { server, props, rerender, user, fetch, profileB, acceptRevocation, publishA } = httpSetup()
    await acceptRevocation()
    await publishA(false)
    let finish!: (value: Response) => void
    server.readB = async () => new Promise((resolve) => { finish = resolve })
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(profileB, true)}
      expected={{ ...monitoringVersion, revision: 5 }} />)
    await waitFor(() => expect(server.reads.some((read) => read.id === profileB.review_id)).toBe(true))
    const oldSignal = server.reads.find((read) => read.id === profileB.review_id)!.signal
    const other = { ...monitoringTarget, name: 'Other model',
      identity: { ...monitoringTarget.identity, item_id: ids.otherModel, workspace_id: ids.otherWorkspace } }
    rerender(<MonitoringSafetyReview {...props} target={other} inventory={monitoringInventory[3]!}
      selection={{ identity: other.identity, name: other.name, workspaceName: 'Planning workspace' }} />)
    await complete(user)
    const currentCalls = fetch.mock.calls.length
    await act(async () => finish(response({ ...profileB, detail: 'Late response for the previous target.' })))
    expect(oldSignal?.aborted).toBe(true)
    expect(screen.queryByText('Late response for the previous target.')).toBeNull()
    expect(screen.getByRole<HTMLTextAreaElement>('textbox', { name: 'Reason for this review change' }).value)
      .toBe('Reviewed this specific target and intended action.')
    expect(fetch).toHaveBeenCalledTimes(currentCalls)
    expect(server.requests).toHaveLength(1)
  })
})

describe('safety review rejection and uncertain writes', () => {
  it('retains the nonsecret submission binding after a lost write and requires the original operation receipt', async () => {
    const { api, props, user, rerender } = setup()
    vi.mocked(api.recordSafetyReview).mockRejectedValueOnce(new ApiError(0, 'network_error', 'The review response was lost.'))
    vi.mocked(api.safetyReview).mockRejectedValue(new ApiError(404, 'not_found', 'No review record yet.'))
    await complete(user, 'powerbi_refresh', 'verified')
    setParameters('{"batch":"not-persisted"}')
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText('Safety-review outcome is unconfirmed')
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    expect(JSON.parse(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)!)).toEqual({
      request_id: input.request_id, review_id: input.review.review_id,
      binding: { targetKey: monitoringTargetKey(input.review.target), action: input.review.action,
        revision: input.review.revision, policyRevision: input.expected.revision, state: input.review.state },
    })
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).not.toContain('not-persisted')
    expect(screen.queryByDisplayValue('{"batch":"not-persisted"}')).toBeNull()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    const returned = safetyReviewResponse(input, { state: 'pending' })
    vi.mocked(api.safetyReview).mockResolvedValue(returned)
    vi.mocked(api.safetyReviewOperation).mockResolvedValue(safetyReviewOperationReceipt(input, returned))
    await user.click(screen.getByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText(/Original safety-review operation confirmed: pending/)
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(vi.mocked(api.safetyReviewOperation).mock.calls.every(([requestId]) => requestId === input.request_id)).toBe(true)
    rerender(<MonitoringSafetyReview {...props} target={referencedTarget(returned)} />)
    await screen.findByText(/current server target keeps this action disabled/)
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()
  })

  it('keeps an old GET revision unresolved after an uncertain update and never sends a new blind ID', async () => {
    const { api, user } = setup({ review: monitoringSafetyReview })
    vi.mocked(api.recordSafetyReview).mockRejectedValueOnce(new ApiError(503, 'commit_uncertain', 'The server cannot confirm the review write.'))
    vi.mocked(api.safetyReviewOperation).mockImplementation(async (requestId) => ({
      request_id: requestId, review: monitoringSafetyReview,
    }))
    await complete(user)
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText('Safety-review outcome is unconfirmed')
    expect(screen.getByText(/older than the latest recorded revision/)).toBeTruthy()
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toContain(vi.mocked(api.recordSafetyReview).mock.calls[0]![0].request_id)
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
  })

  it('surfaces an unsupported or rejected transition and keeps the returned current record authoritative', async () => {
    const { api, user } = setup({ review: monitoringSafetyReview })
    vi.mocked(api.recordSafetyReview).mockRejectedValueOnce(new ApiError(409, 'review_transition_rejected', 'The store rejected this transition.'))
    await complete(user)
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText('The store rejected this transition.')
    await waitFor(() => expect(vi.mocked(api.safetyReview).mock.calls.length).toBeGreaterThanOrEqual(2))
    await screen.findByRole('combobox', { name: 'Action profile' })
    expect(screen.getByText('The store rejected this transition.')).toBeTruthy()
    expect(screen.queryByText(/Safety review recorded as/)).toBeNull()
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()
  })

  it('retains IDs but does not adopt a late successful response after permissions change', async () => {
    const { api, props, user, rerender } = setup()
    let finish!: (value: SafetyReview) => void
    vi.mocked(api.recordSafetyReview).mockImplementationOnce(() => new Promise((resolve) => { finish = resolve }))
    await complete(user, 'powerbi_refresh', 'verified')
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText('Saving safety review')
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    rerender(<MonitoringSafetyReview {...props} admin={false} allowed={false} permissionKey="1:reader" />)
    const accepted = acceptedSafetyReviewResponse(input)
    await act(async () => finish(accepted))
    await screen.findByText('Safety-review outcome is unconfirmed')
    expect(props.onChanged).not.toHaveBeenCalled()
    expect(screen.queryByText(/Safety review recorded as verified/)).toBeNull()
    expect(screen.queryByText(/Safety-review intent accepted/)).toBeNull()
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toContain(input.request_id)
    vi.mocked(api.safetyReviewOperation).mockResolvedValue(safetyReviewOperationReceipt(input, accepted))
    vi.mocked(api.safetyReview).mockResolvedValue(accepted)
    await user.click(screen.getByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText(/Original safety-review intent acceptance confirmed: verified/)
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
  })

  it('recovers the submitted binding after remount without retaining parameter text or automatically posting', async () => {
    const { api, props, user, unmount } = setup()
    vi.mocked(api.recordSafetyReview).mockRejectedValueOnce(new ApiError(0, 'network_error', 'The review response was lost.'))
    vi.mocked(api.safetyReview).mockRejectedValue(new ApiError(404, 'not_found', 'No review yet.'))
    await complete(user)
    setParameters('{"batch":"not-persisted"}')
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText('Safety-review outcome is unconfirmed')
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    unmount()
    render(<MonitoringSafetyReview {...props} />)
    await screen.findByText('Safety-review outcome is unconfirmed')
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(screen.queryByDisplayValue('{"batch":"not-persisted"}')).toBeNull()
    const returned = safetyReviewResponse(input)
    vi.mocked(api.safetyReview).mockResolvedValue(returned)
    vi.mocked(api.safetyReviewOperation).mockResolvedValue(safetyReviewOperationReceipt(input, returned))
    await user.click(screen.getByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText(/Original safety-review operation confirmed/)
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
  })

  it('never converts a pre-revocation review into confirmation after remount or posts another revision 2 with a new ID', async () => {
    const prior: SafetyReview = { ...monitoringSafetyReview, state: 'verified', exact_correlation_verified: true }
    const { api, props, user, unmount } = setup({ review: prior, target: referencedTarget(prior, true) })
    vi.mocked(api.recordSafetyReview).mockRejectedValueOnce(new ApiError(503, 'commit_uncertain', 'Revocation was not confirmed.'))
    vi.mocked(api.safetyReviewOperation).mockImplementation(async (requestId) => ({ request_id: requestId, review: prior }))
    await complete(user, 'powerbi_refresh', 'revoked')
    await user.click(screen.getByRole('button', { name: 'Revoke safety review' }))
    await screen.findByText('Safety-review outcome is unconfirmed')
    const input = vi.mocked(api.recordSafetyReview).mock.calls[0]![0]
    expect(input.review.revision).toBe(2)
    expect(input.review.state).toBe('revoked')
    const stored = window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)
    expect(JSON.parse(stored!).binding).toEqual({
      targetKey: monitoringTargetKey(prior.target), action: prior.action, revision: 2, policyRevision: 3, state: 'revoked',
    })
    unmount()
    vi.mocked(api.safetyReview).mockClear().mockResolvedValue(prior)
    const restored = render(<MonitoringSafetyReview {...props} />)
    await user.click(await screen.findByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText(/older than the latest recorded revision/)
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBe(stored)
    expect(api.safetyReview).not.toHaveBeenCalled()
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Revoke safety review' })).toBeNull()
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()

    const revoked = safetyReviewResponse(input)
    vi.mocked(api.safetyReviewOperation).mockResolvedValue({ request_id: ids.work, review: revoked })
    await user.click(screen.getByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText(/receipt belongs to a different request/)
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBe(stored)
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(vi.mocked(api.safetyReviewOperation).mock.calls.every(([requestId]) => requestId === input.request_id)).toBe(true)

    vi.mocked(api.safetyReviewOperation).mockResolvedValue(safetyReviewOperationReceipt(input, revoked))
    vi.mocked(api.safetyReview).mockResolvedValue(revoked)
    await user.click(screen.getByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText(/Original safety-review operation confirmed: revoked, revision 2/)
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBeNull()
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Revoke safety review' }).disabled).toBe(true)
    restored.rerender(<MonitoringSafetyReview {...props} target={referencedTarget(revoked)} />)
    await screen.findByText(/current server target keeps this action disabled/)
    await complete(user, 'powerbi_refresh', 'pending')
    await user.click(screen.getByRole('button', { name: 'Save safety review' }))
    await screen.findByText(/Safety review recorded as pending, revision 3/)
    const next = vi.mocked(api.recordSafetyReview).mock.calls[1]![0]
    expect(next.expected_review_revision).toBe(2)
    expect(next.review.revision).toBe(3)
    expect(next.request_id).not.toBe(input.request_id)
    expect(vi.mocked(api.recordSafetyReview).mock.calls.filter(([request]) => request.review.revision === 2
      && request.request_id !== input.request_id)).toHaveLength(0)
  })

  it('does not fall back to a current review when the operation endpoint or receipt is unavailable after remount', async () => {
    const { api, props, user, unmount } = setup({ review: monitoringSafetyReview })
    vi.mocked(api.recordSafetyReview).mockRejectedValueOnce(new ApiError(0, 'network_error', 'The write response was lost.'))
    await complete(user, 'powerbi_refresh', 'revoked')
    await user.click(screen.getByRole('button', { name: 'Revoke safety review' }))
    await screen.findByText('Safety-review outcome is unconfirmed')
    const stored = window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)
    unmount()
    vi.mocked(api.safetyReview).mockClear().mockResolvedValue(monitoringSafetyReview)
    render(<MonitoringSafetyReview {...props} />)
    await user.click(await screen.findByRole('button', { name: 'Check safety-review operation' }))
    await screen.findByText('Safety-review operation receipt not found.')
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).toBe(stored)
    expect(api.safetyReview).not.toHaveBeenCalled()
    expect(api.recordSafetyReview).toHaveBeenCalledOnce()
    expect(screen.queryByRole('button', { name: 'Save safety review' })).toBeNull()
  })

  it('fails closed for an incomplete saved binding instead of treating the current review as a receipt', async () => {
    window.sessionStorage.setItem(SAFETY_REVIEW_RECOVERY_KEY, JSON.stringify({ request_id: ids.submission, review_id: ids.review }))
    const { api } = setup({ review: monitoringSafetyReview })
    await screen.findByRole('heading', { name: 'Returned review record' })
    expect(screen.getByText(/could not read the original safety-review submission binding/)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Save safety review' }).disabled).toBe(true)
    expect(api.recordSafetyReview).not.toHaveBeenCalled()
    expect(window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)).not.toBeNull()
  })

  it('does not submit if the browser cannot preserve the nonsecret recovery IDs', async () => {
    const { api, user } = setup()
    await complete(user)
    const storage = vi.spyOn(window, 'sessionStorage', 'get').mockImplementation(() => { throw new Error('Storage unavailable') })
    try {
      await user.click(screen.getByRole('button', { name: 'Save safety review' }))
      await screen.findByText(/could not preserve the safety-review IDs/)
      expect(api.recordSafetyReview).not.toHaveBeenCalled()
    } finally { storage.mockRestore() }
  })
})
