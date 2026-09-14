import type { CommandInput, Proposal, WorkItem } from './api/types'

export type Tone = 'neutral' | 'info' | 'warning' | 'danger' | 'success'
export type QueueFilter = 'all' | 'approval' | 'investigation' | 'running' | 'verifying' | 'resolved'

export const queueFilterLabels: Record<QueueFilter, string> = {
  all: 'All work',
  approval: 'Awaiting decision',
  investigation: 'Needs investigation',
  running: 'In progress',
  verifying: 'Verifying',
  resolved: 'Complete / resolved',
}

export function humanize(value: string): string {
  if (value === 'powerbi') return 'Power BI'
  if (value === 'powerbi_triage') return 'Power BI triage'
  const text = value.replace(/[_-]+/g, ' ').trim()
  return text ? text[0]!.toUpperCase() + text.slice(1) : 'Not recorded'
}

function canonicalStatus(status: string): string {
  const value = status.toLowerCase()
  return value === 'needs_review' ? 'needs_investigation' : value
}

export function workloadOptions(items: readonly Pick<WorkItem, 'workload'>[]): string[] {
  const observed = items.map((item) => item.workload).filter((value) => value.trim() && value !== 'all').sort()
  return [...new Set(['powerbi', 'fabric_pipeline', ...observed])]
}

export function statusInfo(status: string): { label: string; tone: Tone } {
  switch (canonicalStatus(status)) {
    case 'pending': case 'pending_approval': case 'awaiting_approval': case 'awaiting_decision':
      return { label: 'Awaiting decision', tone: 'warning' }
    case 'decision_recorded': case 'approved':
      return { label: 'Decision recorded', tone: 'info' }
    case 'reconciled': return { label: 'Reconciled', tone: 'info' }
    case 'queued': return { label: 'Queued', tone: 'neutral' }
    case 'submitted': case 'remediation_submitted': return { label: 'Submitted', tone: 'info' }
    case 'running': case 'investigating': case 'in_progress': return { label: humanize(status), tone: 'info' }
    case 'verification_pending': case 'pending_verification': case 'verifying':
      return { label: 'Verifying', tone: 'warning' }
    case 'expired': return { label: 'Expired', tone: 'danger' }
    case 'denied': case 'refused': return { label: humanize(status), tone: 'danger' }
    case 'needs_investigation': return { label: queueFilterLabels.investigation, tone: 'danger' }
    case 'resolved_by_user': return { label: 'Resolved by user', tone: 'info' }
    case 'error': case 'failed': case 'blocked': case 'escalated': case 'interrupted': case 'uncertain':
      return { label: humanize(status), tone: 'danger' }
    case 'resolved': case 'verified': return { label: humanize(status), tone: 'success' }
    case 'complete': case 'completed': case 'succeeded': return { label: 'Complete', tone: 'success' }
    default: return { label: humanize(status), tone: 'neutral' }
  }
}

export function effectiveStatus(item: WorkItem, now = Date.now()): string {
  const awaiting = ['pending', 'pending_approval', 'awaiting_approval', 'awaiting_decision']
  if (item.kind === 'approval' && awaiting.includes(item.status) && item.expires_at && Date.parse(item.expires_at) <= now) return 'expired'
  return item.status
}

export function needsCommandReconciliation(item: WorkItem): boolean {
  return item.kind === 'command' && (item.status === 'interrupted' || item.status === 'uncertain')
}

export function approvalBlockReason(proposal: Proposal, allowed: boolean, now = Date.now()): string | null {
  if (!allowed) return 'Your current role cannot decide on this request.'
  if (proposal.decision || ['decision_recorded', 'approved', 'denied', 'consumed'].includes(proposal.status)) return 'A decision has already been recorded. This request cannot be used again.'
  if (!proposal.expires_at || !Number.isFinite(Date.parse(proposal.expires_at))) return 'No valid expiry is recorded. This request cannot be approved.'
  if (Date.parse(proposal.expires_at) <= now || proposal.status === 'expired') return 'This request has expired. A new proposal is required.'
  if (!proposal.fingerprint.trim()) return 'The proposal fingerprint is missing. Decisions are unavailable.'
  if (!proposal.can_decide) return 'The server has locked this request. Refresh to review its current policy status.'
  return null
}

export function filterWork(items: WorkItem[], query: string, filter: QueueFilter, workload: string, incidentsOnly = false): WorkItem[] {
  const needle = query.toLowerCase().trim()
  return items.filter((item) => {
    if (incidentsOnly && item.kind !== 'incident') return false
    if (workload !== 'all' && item.workload !== workload) return false
    if (needle && ![item.title, item.target, item.summary, item.agent, item.id, item.source_id, item.incident_id ?? '']
      .some((text) => text.toLowerCase().includes(needle))) return false
    const status = canonicalStatus(effectiveStatus(item))
    if (filter === 'approval') return item.kind === 'approval' && ['pending', 'pending_approval', 'awaiting_approval', 'awaiting_decision'].includes(status)
    if (filter === 'investigation') return ['open', 'needs_investigation', 'escalated', 'failed', 'blocked', 'investigating', 'interrupted', 'uncertain'].includes(status)
    if (filter === 'running') return ['queued', 'running', 'submitted', 'remediation_submitted', 'in_progress'].includes(status)
    if (filter === 'verifying') return ['verification_pending', 'pending_verification', 'verifying'].includes(status)
    if (filter === 'resolved') return ['resolved', 'resolved_by_user', 'verified', 'complete', 'completed', 'succeeded'].includes(status)
    return true
  })
}

export function formatDate(value: string | null): string {
  if (!value) return 'Not recorded'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? 'Invalid timestamp' : date.toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

export function relativeTime(value: string, now = Date.now()): string {
  const ms = now - Date.parse(value)
  if (!Number.isFinite(ms)) return 'Unknown'
  if (ms < 0) return 'Just now'
  if (ms < 60_000) return 'Just now'
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m ago`
  if (ms < 86_400_000) return `${Math.floor(ms / 3_600_000)}h ago`
  return `${Math.floor(ms / 86_400_000)}d ago`
}

export function timeUntil(value: string, now: number): string {
  const ms = Date.parse(value) - now
  if (!Number.isFinite(ms)) return 'Expiry not available'
  if (ms <= 0) return 'Expired'
  if (ms < 60_000) return `Expires in ${Math.ceil(ms / 1000)}s`
  if (ms < 3_600_000) return `Expires in ${Math.ceil(ms / 60_000)}m`
  return `Expires in ${Math.floor(ms / 3_600_000)}h ${Math.ceil((ms % 3_600_000) / 60_000)}m`
}

export function duration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`
}

export function safeSourceUrl(value: string): string | null {
  try {
    const url = new URL(value)
    return url.protocol === 'https:' ? url.href : null
  } catch {
    return null
  }
}

export class CommandIntent {
  private body = ''
  private key = ''

  keyFor(input: CommandInput): string {
    const body = JSON.stringify(input)
    if (body !== this.body) {
      this.body = body
      this.key = crypto.randomUUID()
    }
    return this.key
  }
}
