import { requestJson } from './client'
import type { TokenProvider } from './client'
import { ApiError, isRecord } from './errors'
import type { Proposal, RunSummary, TimelineEvent, WorkDetail, WorkItem } from './types'

export type IncidentTrackingStatus = 'open' | 'resolved' | 'resolved_by_user'
export type IncidentListStatus = 'all' | 'open' | 'needs_investigation' | 'needs_review' | 'investigating' | 'wont_fix' | 'resolved' | 'resolved_by_user'
export type IncidentActivityKind = 'note' | 'resolution' | 'question' | 'answer'

export interface IncidentTracking {
  status: IncidentTrackingStatus
  version: number
  source_revision: string
  resolved_at: string | null
  resolved_by: string | null
  resolution_note: string | null
}

export interface IncidentActivity {
  id: string
  incident_id: string
  kind: IncidentActivityKind
  body: string
  created_at: string
  user_id: string
  user_name: string
  status: 'recorded' | 'pending' | 'completed' | 'failed'
  correlation_id: string | null
  mode: 'records' | 'model' | null
}

export interface IncidentCase {
  detail: WorkDetail
  tracking: IncidentTracking
  activity: IncidentActivity[]
  capabilities: { note: boolean; resolve: boolean; ask: boolean }
}

export interface IncidentPage {
  items: WorkItem[]
  total: number
  offset: number
  limit: number
}

export interface IncidentListQuery {
  limit?: number
  offset?: number
  query?: string
  status?: IncidentListStatus
  workload?: string
}

export interface IncidentResolutionInput {
  reason: string
  expected_version: number
  source_revision: string
  idempotency_key: string
}

type MutationBody = { body: string } | { question: string } | Omit<IncidentResolutionInput, 'idempotency_key'>

export const incidentTextLimit = 4000
export const incidentQuestionLimit = 2000
export const incidentQueryLimit = 200

const listStatuses: readonly IncidentListStatus[] = [
  'all', 'open', 'needs_investigation', 'needs_review', 'investigating', 'wont_fix', 'resolved', 'resolved_by_user',
]

function nonempty(value: unknown): value is string {
  return typeof value === 'string' && Boolean(value.trim())
}

function nullableString(value: unknown): value is string | null {
  return value === null || typeof value === 'string'
}

function timestamp(value: unknown): value is string {
  return typeof value === 'string' && Number.isFinite(Date.parse(value))
}

function nonnegative(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
}

function integer(value: unknown): value is number {
  return nonnegative(value) && Number.isSafeInteger(value)
}

function strings(value: Record<string, unknown>, fields: readonly string[]): boolean {
  return fields.every((field) => typeof value[field] === 'string')
}

function choice(value: unknown, options: readonly string[]): boolean {
  return typeof value === 'string' && options.includes(value)
}

function isIncidentItem(value: unknown): value is WorkItem {
  return isRecord(value) && value.kind === 'incident'
    && nonempty(value.id) && nonempty(value.source_id) && value.incident_id === value.source_id
    && strings(value, ['title', 'target', 'workload', 'agent', 'status', 'summary'])
    && choice(value.severity, ['low', 'medium', 'high'])
    && timestamp(value.created_at) && timestamp(value.updated_at)
    && (value.expires_at === null || timestamp(value.expires_at))
    && typeof value.can_decide === 'boolean'
}

function isProposal(value: unknown): value is Proposal {
  return isRecord(value)
    && strings(value, ['request_id', 'action', 'justification', 'impact', 'fingerprint', 'status'])
    && timestamp(value.requested_at) && timestamp(value.expires_at)
    && nullableString(value.decision) && nullableString(value.responder) && nullableString(value.reason)
    && typeof value.can_decide === 'boolean' && isRecord(value.arguments)
}

function isRun(value: unknown): value is RunSummary {
  return isRecord(value) && nonempty(value.id)
    && strings(value, ['request_id', 'incident_id', 'signature', 'target', 'workload', 'agent_name', 'state', 'outcome', 'summary'])
    && timestamp(value.started_at) && (value.finished_at === null || timestamp(value.finished_at))
    && nonnegative(value.duration_ms) && integer(value.tool_calls) && integer(value.tokens_used) && integer(value.write_actions)
}

function isEvent(value: unknown): value is TimelineEvent {
  return isRecord(value) && nonempty(value.id) && integer(value.sequence) && timestamp(value.timestamp)
    && strings(value, ['kind', 'label', 'status', 'detail', 'tool_name'])
}

function isTracking(value: unknown): value is IncidentTracking {
  if (!isRecord(value) || !choice(value.status, ['open', 'resolved', 'resolved_by_user'])
    || !integer(value.version) || !nonempty(value.source_revision)
    || !(value.resolved_at === null || timestamp(value.resolved_at))
    || !nullableString(value.resolved_by) || !nullableString(value.resolution_note)) return false
  return value.status !== 'resolved_by_user'
    || (timestamp(value.resolved_at) && nonempty(value.resolved_by) && nonempty(value.resolution_note))
}

function isActivity(value: unknown): value is IncidentActivity {
  return isRecord(value) && nonempty(value.id) && nonempty(value.incident_id)
    && choice(value.kind, ['note', 'resolution', 'question', 'answer'])
    && strings(value, ['body', 'user_id', 'user_name']) && timestamp(value.created_at)
    && choice(value.status, ['recorded', 'pending', 'completed', 'failed'])
    && nullableString(value.correlation_id) && (value.mode === null || value.mode === 'records' || value.mode === 'model')
    && (value.kind !== 'answer' || value.status !== 'completed'
      || (nonempty(value.body) && (value.mode === 'records' || value.mode === 'model')))
}

function uniqueIds(values: readonly { id: string }[]): boolean {
  return new Set(values.map((value) => value.id)).size === values.length
}

function isIncidentCase(value: unknown, id: string): value is IncidentCase {
  if (!isRecord(value) || !isRecord(value.detail) || !isTracking(value.tracking)
    || !isRecord(value.capabilities)) return false
  const detail = value.detail
  const capabilities = value.capabilities
  return isIncidentItem(detail.item) && detail.item.source_id === id
    && (detail.item.status !== 'resolved_by_user' || value.tracking.status === 'resolved_by_user')
    && (detail.proposal === null || isProposal(detail.proposal))
    && (detail.incident === null || (isRecord(detail.incident)
      && (detail.incident.incident_id === undefined || detail.incident.incident_id === id)))
    && Array.isArray(detail.evidence) && detail.evidence.every((entry: unknown) =>
      isRecord(entry) && strings(entry, ['label', 'value']))
    && Array.isArray(detail.runs) && detail.runs.every(isRun) && uniqueIds(detail.runs)
    && Array.isArray(detail.timeline) && detail.timeline.every(isEvent) && uniqueIds(detail.timeline)
    && typeof detail.notes === 'string'
    && Array.isArray(value.activity) && value.activity.every(isActivity)
    && value.activity.every((entry) => entry.incident_id === id) && uniqueIds(value.activity)
    && ['note', 'resolve', 'ask'].every((key) => typeof capabilities[key] === 'boolean')
}

function requireId(id: string): void {
  if (!nonempty(id)) throw new ApiError(400, 'incident_id_required', 'Select an incident before requesting its records.')
}

function requireText(value: string, label: string, limit = incidentTextLimit): string {
  const text = value.trim()
  if (!text || text.length > limit) {
    throw new ApiError(400, 'invalid_incident_text', `${label} must contain between 1 and ${limit} characters.`)
  }
  return text
}

function canonicalKey(key: string): string {
  if (!nonempty(key)) throw new ApiError(400, 'idempotency_key_required', 'A submission identifier is required. No request was sent.')
  const value = key.toLowerCase().replace(/^urn:uuid:/, '').replace(/^\{(.*)\}$/, '$1')
  const match = /^([a-f0-9]{8})-?([a-f0-9]{4})-?([a-f0-9]{4})-?([a-f0-9]{4})-?([a-f0-9]{12})$/.exec(value)
  if (!match) throw new ApiError(400, 'invalid_idempotency_key', 'A valid UUID submission identifier is required. No request was sent.')
  return match.slice(1).join('-')
}

function hasReceipt(value: IncidentCase, id: string, key: string, kind: 'note' | 'resolution' | 'question'): boolean {
  const receipt = value.activity.find((entry) => entry.id === key && entry.kind === kind && entry.incident_id === id)
  if (!receipt) return false
  if (kind !== 'question') return receipt.status === 'recorded' && receipt.correlation_id === null && receipt.mode === null
  const answers = value.activity.filter((entry) => entry.kind === 'answer' && entry.incident_id === id && entry.correlation_id === key)
  return receipt.status === 'completed' && receipt.correlation_id === key
    && (receipt.mode === 'records' || receipt.mode === 'model')
    && answers.length === 1 && answers[0]?.status === 'completed' && answers[0]?.mode === receipt.mode
}

export class IncidentIntent {
  private body = ''
  private key = ''

  keyFor(input: string): string {
    if (!this.key || this.body !== input) {
      this.body = input
      this.key = crypto.randomUUID()
    }
    return this.key
  }

  clear(): void {
    this.body = ''
    this.key = ''
  }
}

export class IncidentApiClient {
  constructor(private readonly getToken: TokenProvider) {}

  async list(input: IncidentListQuery = {}, signal?: AbortSignal): Promise<IncidentPage> {
    const { limit = 25, offset = 0, query = '', status = 'all', workload = 'all' } = input
    if (!integer(limit) || limit < 1 || limit > 100 || !integer(offset) || offset > 2_147_483_647
      || !listStatuses.includes(status) || !['all', 'powerbi', 'fabric_pipeline'].includes(workload)
      || query.trim().length > incidentQueryLimit) {
      throw new ApiError(400, 'invalid_incident_query', 'The incident page or filter is invalid. No records were requested.')
    }
    const params = new URLSearchParams({ limit: String(limit), offset: String(offset), query: query.trim(), status, workload })
    const value = await requestJson<unknown>(`/incidents?${params}`, { signal }, this.getToken)
    if (!isRecord(value) || !Array.isArray(value.items) || !value.items.every(isIncidentItem)
      || !uniqueIds(value.items) || !integer(value.total) || value.offset !== offset || value.limit !== limit
      || value.items.length > limit || (value.items.length > 0 && offset + value.items.length > value.total)) {
      throw new ApiError(200, 'invalid_incident_page', 'The API returned incomplete incidents or inconsistent pagination. No replacement records have been inferred.')
    }
    return { items: value.items, total: value.total, offset, limit }
  }

  async detail(id: string, signal?: AbortSignal): Promise<IncidentCase> {
    requireId(id)
    const value = await requestJson<unknown>(`/incidents/${encodeURIComponent(id)}`, { signal }, this.getToken)
    return this.confirmCase(value, id)
  }

  async addNote(id: string, body: string, idempotencyKey: string): Promise<IncidentCase> {
    return this.post(id, 'notes', { body: requireText(body, 'A note') }, idempotencyKey)
  }

  async resolve(id: string, input: IncidentResolutionInput): Promise<IncidentCase> {
    if (!integer(input.expected_version) || input.expected_version >= Number.MAX_SAFE_INTEGER || !nonempty(input.source_revision)) {
      throw new ApiError(400, 'incident_revision_required', 'Refresh the incident and review its current evidence before resolving it.')
    }
    return this.post(id, 'resolution', {
      reason: requireText(input.reason, 'A resolution reason'),
      expected_version: input.expected_version, source_revision: input.source_revision,
    }, input.idempotency_key)
  }

  async discuss(id: string, question: string, idempotencyKey: string): Promise<IncidentCase> {
    return this.post(id, 'discussion', { question: requireText(question, 'A question', incidentQuestionLimit) }, idempotencyKey)
  }

  private async post(id: string, action: 'notes' | 'resolution' | 'discussion', body: MutationBody, key: string): Promise<IncidentCase> {
    requireId(id)
    const submissionKey = canonicalKey(key)
    const value = await requestJson<unknown>(`/incidents/${encodeURIComponent(id)}/${action}`, {
      method: 'POST', body: JSON.stringify({ ...body, idempotency_key: submissionKey }),
    }, this.getToken)
    const result = this.confirmCase(value, id)
    const kind = action === 'notes' ? 'note' : action === 'resolution' ? 'resolution' : 'question'
    // Stored bodies are redacted; the server binds this ID to the original request.
    if (!hasReceipt(result, id, submissionKey, kind)) {
      throw new ApiError(200, 'unconfirmed_incident_submission', `The API did not confirm this ${kind} receipt. It may have been saved; refresh the incident before retrying.`)
    }
    return result
  }

  private confirmCase(value: unknown, id: string): IncidentCase {
    if (!isIncidentCase(value, id)) {
      throw new ApiError(200, 'invalid_incident_case', 'The API did not confirm the selected incident and complete collaboration records. Refresh before repeating a submission.')
    }
    return value
  }
}
