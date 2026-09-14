import { ApiError, isAborted, isRecord, normalizeApiError } from './errors'
import type {
  AppConfig, AskAnswer, CommandInput, Decision, Playbook, Proposal,
  RunDetail, RunSummary, ScenarioCatalog, ScenarioValidationResult, Snapshot,
  ValidationProvider, WorkDetail, WorkSelection,
} from './types'

export type TokenProvider = () => Promise<string | null>

function isValidationResult(value: unknown): value is ScenarioValidationResult {
  return isRecord(value) && typeof value.scenario === 'string' && Boolean(value.scenario)
    && (value.provider === 'mock' || value.provider === 'foundry')
    && typeof value.passed === 'boolean' && Array.isArray(value.failures)
    && value.failures.every((failure: unknown) => typeof failure === 'string')
    && (!value.passed || value.failures.length === 0)
    && typeof value.run_id === 'string' && typeof value.outcome === 'string'
    && typeof value.duration_ms === 'number' && Number.isFinite(value.duration_ms) && value.duration_ms >= 0
}

export async function requestJson<T>(
  path: string,
  options: RequestInit = {},
  getToken?: TokenProvider,
): Promise<T> {
  const headers = new Headers(options.headers)
  headers.set('Accept', 'application/json')
  if (options.body !== undefined) headers.set('Content-Type', 'application/json')
  const token = await getToken?.()
  if (token) headers.set('Authorization', `Bearer ${token}`)
  let response: Response
  try {
    response = await fetch(`/api${path}`, { ...options, headers, credentials: 'omit', cache: 'no-store' })
  } catch (error) {
    if (isAborted(error)) throw error
    throw new ApiError(0, 'network_error', options.method === 'POST'
      ? 'The API could not confirm this submission. It may have reached the server. Refresh the records before retrying.'
      : 'Cannot reach the API. Check the connection and try again. No demo data has been substituted.')
  }
  const text = await response.text()
  let payload: unknown
  try {
    payload = text ? JSON.parse(text) : null
  } catch {
    if (!response.ok) throw normalizeApiError(response.status, null)
    throw new ApiError(response.status, 'invalid_response', 'The API returned non-JSON content. Check the same-origin /api configuration.')
  }
  if (!response.ok) throw normalizeApiError(response.status, payload)
  if (!isRecord(payload)) throw new ApiError(response.status, 'invalid_response', 'The API returned an invalid response object.')
  return payload as T
}

export async function loadConfig(signal?: AbortSignal): Promise<AppConfig> {
  const config = await requestJson<AppConfig>('/config', { signal })
  if (
    !['live', 'demo'].includes(config.mode) || !isRecord(config.auth)
    || typeof config.auth.enabled !== 'boolean' || typeof config.app_name !== 'string'
  ) {
    throw new ApiError(200, 'invalid_config', 'The API configuration is incomplete. The command center cannot start safely.')
  }
  return config
}

export class ApiClient {
  constructor(private readonly getToken: TokenProvider) {}

  private get<T>(path: string, signal?: AbortSignal): Promise<T> {
    return requestJson<T>(path, { signal }, this.getToken)
  }

  private post<T>(path: string, body: unknown): Promise<T> {
    // A transport error is not evidence that a write did not happen.
    return requestJson<T>(path, { method: 'POST', body: JSON.stringify(body) }, this.getToken)
  }

  async snapshot(signal?: AbortSignal): Promise<Snapshot> {
    const value = await this.get<Snapshot>('/snapshot', signal)
    if (
      value.schema_version !== 1 || !['demo', 'live'].includes(value.mode)
      || !Array.isArray(value.work_items) || !Array.isArray(value.recent_runs)
      || !Array.isArray(value.targets) || !Array.isArray(value.agents)
      || !Array.isArray(value.health) || !isRecord(value.counts)
      || !isRecord(value.capabilities) || !isRecord(value.actor)
      || typeof value.actor.display_name !== 'string' || !Array.isArray(value.actor.roles)
      || typeof value.as_of !== 'string'
      || !['approve', 'ask', 'request_triage'].every((key) => typeof value.capabilities[key as keyof Snapshot['capabilities']] === 'boolean')
      || !['pending_approvals', 'needs_investigation', 'running', 'verification_pending', 'resolved']
        .every((key) => Number.isFinite(value.counts[key as keyof Snapshot['counts']]) && value.counts[key as keyof Snapshot['counts']] >= 0)
    ) {
      throw new ApiError(200, 'unsupported_snapshot', 'The API snapshot is incomplete or uses an unsupported schema. Actions are unavailable.')
    }
    return value
  }

  async detail(selection: WorkSelection, signal?: AbortSignal): Promise<WorkDetail> {
    const query = new URLSearchParams({ kind: selection.kind, id: selection.source_id })
    const detail = await this.get<WorkDetail>(`/detail?${query}`, signal)
    if (
      !isRecord(detail.item) || detail.item.kind !== selection.kind || detail.item.source_id !== selection.source_id
      || !Array.isArray(detail.evidence) || !Array.isArray(detail.runs) || !Array.isArray(detail.timeline)
      || (selection.kind === 'approval' && detail.proposal?.request_id !== selection.source_id)
    ) {
      throw new ApiError(200, 'invalid_detail', 'The API returned incomplete evidence or a different request. No decision can be made from these records.')
    }
    return detail
  }

  runs(offset: number, signal?: AbortSignal): Promise<{ items: RunSummary[]; total: number }> {
    return this.get(`/runs?limit=50&offset=${offset}`, signal)
  }

  async run(id: string, signal?: AbortSignal): Promise<RunDetail> {
    const result = await this.get<RunDetail>(`/runs/${encodeURIComponent(id)}`, signal)
    if (!isRecord(result.run) || result.run.id !== id || !Array.isArray(result.events)) {
      throw new ApiError(200, 'invalid_run', 'The API did not return the selected run and its recorded events.')
    }
    return result
  }

  knowledge(signal?: AbortSignal): Promise<{ items: Playbook[] }> {
    return this.get('/knowledge', signal)
  }

  async validationScenarios(signal?: AbortSignal): Promise<ScenarioCatalog> {
    const catalog = await this.get<ScenarioCatalog>('/validation/scenarios', signal)
    if (
      !Array.isArray(catalog.items) || !Array.isArray(catalog.providers)
      || !catalog.items.every((item) => isRecord(item) && typeof item.name === 'string' && item.name
        && typeof item.title === 'string' && typeof item.description === 'string')
      || new Set(catalog.items.map((item) => item.name)).size !== catalog.items.length
      || !catalog.providers.every((provider) => provider === 'mock' || provider === 'foundry')
    ) {
      throw new ApiError(200, 'invalid_scenario_catalog', 'The validation service returned an invalid scenario catalog. No scenarios were started.')
    }
    return catalog
  }

  async validationResults(signal?: AbortSignal): Promise<{ items: ScenarioValidationResult[] }> {
    const results = await this.get<{ items: ScenarioValidationResult[] }>('/validation/results', signal)
    if (!Array.isArray(results.items) || !results.items.every(isValidationResult)) {
      throw new ApiError(200, 'invalid_validation_results', 'The validation service returned incomplete or inconsistent results.')
    }
    return results
  }

  async validateScenario(name: string, provider: ValidationProvider): Promise<ScenarioValidationResult> {
    const result = await this.post<ScenarioValidationResult>(`/validation/scenarios/${encodeURIComponent(name)}`, { provider })
    if (!isValidationResult(result) || result.scenario !== name || result.provider !== provider) {
      throw new ApiError(200, 'unconfirmed_validation', 'The service did not confirm the selected scenario and provider. Refresh recorded results before running it again.')
    }
    return result
  }

  async decide(request: { request_id: string; decision: Decision; fingerprint: string; reason: string }): Promise<{ status: 'decision_recorded'; request: Proposal }> {
    const result = await this.post<{ status: 'decision_recorded'; request: Proposal }>('/decisions', request)
    if (result.status !== 'decision_recorded' || !isRecord(result.request) || result.request.request_id !== request.request_id) {
      throw new ApiError(200, 'unconfirmed_decision', 'The API did not confirm this decision. Refresh the request before taking another action.')
    }
    return result
  }

  async ask(incidentId: string, question: string): Promise<AskAnswer> {
    const answer = await this.post<AskAnswer>('/ask', { incident_id: incidentId, question })
    if (
      typeof answer.answer !== 'string' || (answer.mode !== 'records' && answer.mode !== 'model')
      || typeof answer.question_id !== 'string' || !Array.isArray(answer.references)
      || !answer.references.every((reference) => isRecord(reference) && typeof reference.id === 'string'
        && typeof reference.label === 'string' && typeof reference.kind === 'string')
    ) {
      throw new ApiError(200, 'invalid_ask_response', 'The API did not identify a valid answer source. No answer or action has been inferred.')
    }
    return answer
  }

  async command(input: CommandInput, idempotencyKey: string): Promise<{ command_id: string; status: 'queued' }> {
    const result = await this.post<{ command_id: string; status: 'queued' }>('/commands', { ...input, idempotency_key: idempotencyKey })
    if (result.status !== 'queued' || typeof result.command_id !== 'string' || !result.command_id) {
      throw new ApiError(200, 'unconfirmed_command', 'The API did not confirm the queued command. Refresh before retrying this request.')
    }
    return result
  }

  async reconcileCommand(commandId: string, reason: string): Promise<{ status: 'reconciled'; command_id: string }> {
    if (!reason.trim()) throw new ApiError(400, 'reconciliation_reason_required', 'A human reconciliation reason is required.')
    const result = await this.post<{ status: 'reconciled'; command_id: string }>(`/commands/${encodeURIComponent(commandId)}/reconcile`, { reason: reason.trim() })
    if (result.status !== 'reconciled' || result.command_id !== commandId) {
      throw new ApiError(200, 'unconfirmed_reconciliation', 'The API did not confirm reconciliation of this command. Refresh its records before trying again.')
    }
    return result
  }
}
