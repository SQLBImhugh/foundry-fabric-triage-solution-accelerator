import { describe, expect, it } from 'vitest'
import { approvalBlockReason, CommandIntent, effectiveStatus, filterWork, humanize, needsCommandReconciliation, safeSourceUrl, statusInfo, workloadOptions } from './domain'
import { proposal, workItem } from './test/fixtures'

describe('approval policy display', () => {
  const now = Date.parse('2026-01-01T12:10:00Z')
  it('requires explicit server permission, valid expiry and fingerprint', () => {
    expect(approvalBlockReason(proposal, true, now)).toBeNull()
    expect(approvalBlockReason(proposal, false, now)).toMatch(/role/)
    expect(approvalBlockReason({ ...proposal, can_decide: false }, true, now)).toMatch(/server has locked/)
    expect(approvalBlockReason({ ...proposal, expires_at: 'invalid' }, true, now)).toMatch(/valid expiry/)
    expect(approvalBlockReason({ ...proposal, fingerprint: '' }, true, now)).toMatch(/fingerprint/)
  })
  it('locks at expiry, not after it', () => {
    expect(approvalBlockReason({ ...proposal, expires_at: '2026-01-01T12:10:00Z' }, true, now)).toMatch(/expired/)
  })
  it('never offers a second decision', () => {
    expect(approvalBlockReason({ ...proposal, decision: 'deny' }, true, now)).toMatch(/already been recorded/)
    expect(approvalBlockReason({ ...proposal, status: 'decision_recorded' }, true, now)).toMatch(/already been recorded/)
  })
  it('expires only pending approvals, not incident evidence', () => {
    const expired = { ...workItem, expires_at: '2026-01-01T12:00:00Z' }
    expect(effectiveStatus(expired, now)).toBe('expired')
    expect(effectiveStatus({ ...expired, kind: 'incident', status: 'resolved' }, now)).toBe('resolved')
    expect(effectiveStatus({ ...expired, status: 'decision_recorded' }, now)).toBe('decision_recorded')
  })
})

describe('state distinctions and filtering', () => {
  it('includes the API needs_review status in the investigation group with the same label', () => {
    const review = { ...workItem, kind: 'incident' as const, status: 'needs_review', can_decide: false }
    expect(filterWork([review], '', 'investigation', 'all')).toEqual([review])
    expect(statusInfo('needs_review')).toEqual({ label: 'Needs investigation', tone: 'danger' })
    expect(statusInfo('needs_investigation').label).toBe('Needs investigation')
    expect(review.status).toBe('needs_review')
  })
  it('does not conflate decisions, submission, verification and completion', () => {
    expect(statusInfo('decision_recorded')).toEqual({ label: 'Decision recorded', tone: 'info' })
    expect(statusInfo('submitted')).toEqual({ label: 'Submitted', tone: 'info' })
    expect(statusInfo('verification_pending')).toEqual({ label: 'Verifying', tone: 'warning' })
    expect(statusInfo('completed').label).toBe('Complete')
    expect(statusInfo('resolved').label).toBe('Resolved')
    expect(statusInfo('unrecognized_state').tone).toBe('neutral')
  })
  it('keeps a user resolution distinct from verified remediation', () => {
    const item = { ...workItem, kind: 'incident' as const, status: 'resolved_by_user' }
    expect(statusInfo(item.status)).toEqual({ label: 'Resolved by user', tone: 'info' })
    expect(filterWork([item], '', 'resolved', 'all')).toEqual([item])
    expect(filterWork([item], '', 'investigation', 'all')).toEqual([])
  })
  it('searches IDs, target and agent without mutating server records', () => {
    const source = [workItem, { ...workItem, id: 'other', source_id: 'other', kind: 'incident' as const, status: 'resolved' }]
    expect(filterWork(source, 'OPERATIONS', 'approval', 'powerbi')).toEqual([workItem])
    expect(filterWork(source, 'incident-1', 'resolved', 'all', true)).toEqual([source[1]])
    expect(source[0]?.status).toBe('pending_approval')
  })
  it('allows only HTTPS documentation links', () => {
    expect(safeSourceUrl('https://learn.microsoft.com/example')).toBe('https://learn.microsoft.com/example')
    expect(safeSourceUrl('javascript:alert(1)')).toBeNull()
    expect(safeSourceUrl('http://example.com')).toBeNull()
  })
  it('treats uncertain commands as needing investigation, not as successful or retryable', () => {
    const command = { ...workItem, kind: 'command' as const, status: 'uncertain' }
    expect(needsCommandReconciliation(command)).toBe(true)
    expect(needsCommandReconciliation({ ...command, status: 'interrupted' })).toBe(true)
    expect(needsCommandReconciliation({ ...command, kind: 'incident' })).toBe(false)
    expect(filterWork([command], '', 'investigation', 'all')).toEqual([command])
    expect(filterWork([command], '', 'resolved', 'all')).toEqual([])
    expect(statusInfo('uncertain').tone).toBe('danger')
    expect(statusInfo('reconciled').tone).toBe('info')
  })
})

describe('workload choices', () => {
  it('keeps supported workloads available and preserves unique observed workloads', () => {
    expect(workloadOptions([])).toEqual(['powerbi', 'fabric_pipeline'])
    expect(workloadOptions(['powerbi', 'fabric_pipeline', 'custom_job', 'custom_job', '', ' ', 'all']
      .map((workload) => ({ workload })))).toEqual(['powerbi', 'fabric_pipeline', 'custom_job'])
  })
  it('uses the Power BI product name across workload and target labels', () => {
    expect(humanize('powerbi')).toBe('Power BI')
    expect(humanize('powerbi_triage')).toBe('Power BI triage')
    expect(humanize('fabric_pipeline')).toBe('Fabric pipeline')
  })
})

describe('command idempotency', () => {
  it('reuses a UUID for unchanged retries and changes it for a new intent', () => {
    const intent = new CommandIntent()
    const input = { kind: 'powerbi_triage' as const, target_id: 'target-1', subject: 'Investigate failure', body: 'Recorded failure at 12:00.' }
    const first = intent.keyFor(input)
    expect(first).toMatch(/^[a-f0-9-]{36}$/)
    expect(intent.keyFor({ ...input })).toBe(first)
    expect(intent.keyFor({ ...input, body: 'Different investigation request.' })).not.toBe(first)
  })
})
