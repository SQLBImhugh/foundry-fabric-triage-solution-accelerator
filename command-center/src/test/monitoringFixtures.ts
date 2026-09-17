import { vi } from 'vitest'
import { createHash } from 'node:crypto'
import { MonitoringApiClient } from '../api/monitoring'
import { ApiError } from '../api/errors'
import type {
  ActivationReceipt, ActivationRequest, DomainMetadata, InventoryItem, MonitoringBootstrap,
  MonitoringPlan, MonitoringSnapshot, MonitoringTarget, OwnedConnectorManifest, RecordPage,
  RegistryVersion, ScopeDefinition, ScopePolicy, ScopePreviewRequest, WorkspaceMetadata,
  ReviewJsonValue, SafetyReview, SafetyReviewIntent, SafetyReviewOperationReceipt, SafetyReviewRequest, SafetyReviewState,
} from '../api/monitoring'
import type { AppRole } from '../api/access'

export const monitoringId = (value: number) => `${value.toString(16).padStart(8, '0')}-1111-4111-8111-111111111111`
export const ids = {
  tenant: monitoringId(1), epoch: monitoringId(2), generation: monitoringId(3),
  workspace: monitoringId(4), otherWorkspace: monitoringId(5),
  domain: monitoringId(6), childDomain: monitoringId(7),
  model: monitoringId(8), pipeline: monitoringId(9), notebook: monitoringId(10), otherModel: monitoringId(11),
  scope: monitoringId(12), rule: monitoringId(13), capability: monitoringId(14),
  connector: monitoringId(15), stream: monitoringId(16), ownership: monitoringId(17),
  plan: monitoringId(18), submission: monitoringId(19), work: monitoringId(20),
  review: monitoringId(21), reviewer: monitoringId(22), gateway: monitoringId(23), datasource: monitoringId(24),
}
export const monitoringTime = '2026-09-15T12:00:00Z'
export const monitoringVersion: RegistryVersion = { tenant_id: ids.tenant, epoch: ids.epoch, revision: 3 }
export const monitoringSnapshot: MonitoringSnapshot = {
  control: {
    ...monitoringVersion, schema_version: 1, activation_cutoff: monitoringTime,
    maintenance: false, updated_at: monitoringTime,
  },
  coverage: {
    ...monitoringVersion, as_of: monitoringTime, inventory_completeness: 'partial',
    capability_completeness: 'partial', scope_item_count: null, discovered_count: 4,
    access_verified_count: 1, admitted_count: 2, current_count: 1, action_enabled_count: 0,
    unsupported_count: 1, backlog_count: 2, checkpoint_lag_seconds: null,
    last_inventory_completed_at: null, last_poll_window_end: null, last_receiver_activity_at: null, next_due_at: null,
    gaps: [{ code: 'workspace_access_denied', detail: 'The collector cannot enumerate a workspace. Known inventory is retained.', workspace_id: ids.otherWorkspace }],
  },
  can_admin: true, user: { id: 'operator-1', display_name: 'Example User', roles: ['reader', 'admin'] },
}
export const monitoringBootstrap: MonitoringBootstrap = {
  status: 'ready', expected_tenant_id: ids.tenant, found_schema_version: 1,
  control: monitoringSnapshot.control, detail: 'Monitoring schema and deployment tenant match.',
}
export const monitoringWrongTenantBootstrap: MonitoringBootstrap = {
  status: 'wrong_tenant', expected_tenant_id: ids.tenant, found_schema_version: null, control: null,
  detail: "Monitoring bootstrap does not belong to the server's deployment tenant.",
}
export const monitoringScope: ScopePolicy = {
  ...monitoringVersion, scope_id: ids.scope, name: 'Operations scope', enabled: true, updated_at: monitoringTime,
  cadence: { poll_seconds: 300, reconciliation_seconds: 900 },
  rules: [{
    rule_id: ids.rule, selector: {
      tenant_id: ids.tenant, kind: 'workspace', workspace_id: ids.workspace,
      domain_id: null, item_id: null, include_descendants: false,
    },
    effect: 'include', workloads: ['powerbi', 'fabric_pipeline'], auto_enrol_detection_only: false,
  }],
}
export const monitoringWorkspaces: WorkspaceMetadata[] = [
  {
    tenant_id: ids.tenant, epoch: ids.epoch, workspace_id: ids.workspace, name: 'Operations workspace',
    domain_id: ids.domain, capacity_id: null, state: 'present', observed_at: monitoringTime, generation_id: ids.generation,
  },
  {
    tenant_id: ids.tenant, epoch: ids.epoch, workspace_id: ids.otherWorkspace, name: 'Planning workspace',
    domain_id: ids.childDomain, capacity_id: null, state: 'present', observed_at: monitoringTime, generation_id: ids.generation,
  },
]
export const monitoringDomains: DomainMetadata[] = [
  { tenant_id: ids.tenant, epoch: ids.epoch, domain_id: ids.domain, name: 'Operations domain', parent_domain_id: null, state: 'present', observed_at: monitoringTime, generation_id: ids.generation },
  { tenant_id: ids.tenant, epoch: ids.epoch, domain_id: ids.childDomain, name: 'Planning domain', parent_domain_id: ids.domain, state: 'present', observed_at: monitoringTime, generation_id: ids.generation },
]
export const monitoringInventory: InventoryItem[] = [
  {
    tenant_id: ids.tenant, epoch: ids.epoch, generation_id: ids.generation, workspace_id: ids.workspace,
    item_id: ids.model, name: 'Operations model', item_type: 'SemanticModel', workload: 'powerbi',
    unsupported_reason: null, domain_ids: [ids.domain], state: 'present', observed_at: monitoringTime, definition_hash: null,
  },
  {
    tenant_id: ids.tenant, epoch: ids.epoch, generation_id: ids.generation, workspace_id: ids.workspace,
    item_id: ids.pipeline, name: 'Scheduled ingestion', item_type: 'DataPipeline', workload: 'fabric_pipeline',
    unsupported_reason: null, domain_ids: [ids.domain], state: 'present', observed_at: monitoringTime, definition_hash: null,
  },
  {
    tenant_id: ids.tenant, epoch: ids.epoch, generation_id: ids.generation, workspace_id: ids.workspace,
    item_id: ids.notebook, name: 'Notebook analysis', item_type: 'Notebook', workload: null,
    unsupported_reason: 'Standalone notebooks have no monitoring detector contract.',
    domain_ids: [ids.domain], state: 'present', observed_at: monitoringTime, definition_hash: null,
  },
  {
    tenant_id: ids.tenant, epoch: ids.epoch, generation_id: ids.generation, workspace_id: ids.otherWorkspace,
    item_id: ids.otherModel, name: 'Operations model', item_type: 'SemanticModel', workload: 'powerbi',
    unsupported_reason: null, domain_ids: [ids.childDomain], state: 'present', observed_at: monitoringTime, definition_hash: null,
  },
]
export const monitoringTarget: MonitoringTarget = {
  identity: { tenant_id: ids.tenant, epoch: ids.epoch, workload: 'powerbi', workspace_id: ids.workspace, item_id: ids.model },
  name: 'Operations model', scope_ids: [ids.scope], admitted_rule_ids: [ids.rule],
  inventory_generation: ids.generation, capability_id: ids.capability, policy_revision: 3,
  admitted_at: monitoringTime, state: 'current', admission_basis: 'reviewed', reason: 'Explicit scope; detection only.',
  observation: { enabled: true, events_enabled: false, cadence: { poll_seconds: 300, reconciliation_seconds: 900 } },
  action: { enabled: false, action: null, review_id: null, review_revision: null }, next_poll_at: null,
}
export const monitoringTargets: MonitoringTarget[] = [
  monitoringTarget,
  {
    ...monitoringTarget, identity: { ...monitoringTarget.identity, workload: 'fabric_pipeline', item_id: ids.pipeline },
    name: 'Scheduled ingestion', state: 'review_required', admission_basis: 'pending_review', reason: 'Newly discovered item requires review.',
    observation: { ...monitoringTarget.observation, enabled: false },
  },
]
function canonicalJson(value: ReviewJsonValue): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key]!)}`).join(',')}}`
}
export function reviewParameterHash(value: ReviewJsonValue): string {
  return createHash('sha256').update(canonicalJson(value)).digest('hex')
}
export const monitoringSafetyIntent: SafetyReviewIntent = {
  review_id: ids.review, target: monitoringTarget.identity, revision: 1, policy_revision: monitoringVersion.revision,
  action: 'powerbi_refresh', state: 'pending', reviewer_id: ids.reviewer, reviewed_at: monitoringTime,
  expires_at: '2099-01-01T00:00:00Z', revoked_at: null, definition_hash: null, configuration_hash: null,
  parameters: null, parameter_hash: reviewParameterHash(null), parameters_redacted: false,
  replay_safe: false, exact_correlation_verified: false, detail: 'Pending explicit target review.',
}
export const monitoringSafetyReview: SafetyReview = {
  ...monitoringSafetyIntent, requested_state: null, publication_status: 'published',
}
export function safetyReviewResponse(input: SafetyReviewRequest, updates: Partial<SafetyReview> = {}): SafetyReview {
  return {
    ...input.review, reviewer_id: ids.reviewer,
    requested_state: null, publication_status: 'published',
    parameter_hash: input.review.parameter_hash ?? reviewParameterHash(input.review.parameters), ...updates,
  }
}
export function acceptedSafetyReviewResponse(input: SafetyReviewRequest, updates: Partial<SafetyReview> = {}): SafetyReview {
  return safetyReviewResponse(input, {
    state: 'pending', requested_state: input.review.state, publication_status: 'pending_validation',
    revoked_at: null, exact_correlation_verified: false, ...updates,
  })
}
export function publishedSafetyReview(review: SafetyReview, state: SafetyReviewState, policyRevision: number): SafetyReview {
  return {
    ...review, state, publication_status: 'published', policy_revision: policyRevision,
    revoked_at: state === 'revoked' ? review.reviewed_at : null,
    exact_correlation_verified: state === 'verified' && (review.action === 'powerbi_refresh' || review.action === 'pipeline_rerun'),
  }
}
export function safetyReviewOperationReceipt(input: SafetyReviewRequest, review = safetyReviewResponse(input)): SafetyReviewOperationReceipt {
  return { request_id: input.request_id, review }
}
export const monitoringConnector: OwnedConnectorManifest = {
  tenant_id: ids.tenant, epoch: ids.epoch, connector_id: ids.connector, ownership_id: ids.ownership,
  revision: 1, policy_revision: 3, workspace_id: ids.workspace, eventstream_id: ids.stream,
  destination_id: 'owned-destination', name: 'Pipeline event connector',
  sources: [{
    source_id: 'pipeline-source', target: { ...monitoringTarget.identity, workload: 'fabric_pipeline', item_id: ids.pipeline },
    event_types: ['Microsoft.Fabric.ItemJobFailed'],
  }],
  desired_definition: { sources: [{ id: 'pipeline-source' }], destinations: [{ id: 'owned-destination' }] },
  observed_definition: null, endpoint: null, operation_id: 'provisioning-operation',
  state: 'provisioning', updated_at: monitoringTime, identity_verified_at: null, delivery_verified_at: null,
  last_receiver_activity_at: null, gaps: [],
}
export function monitoringPage<T>(items: T[], next_cursor: string | null = null, version = monitoringVersion): RecordPage<T> {
  return { version, as_of: monitoringTime, items, next_cursor }
}
export function monitoringPreview(input: ScopePreviewRequest): MonitoringPlan {
  return {
    ...input, plan_id: ids.plan, created_at: monitoringTime, expires_at: '2099-01-01T00:00:00Z',
    inventory_generations: [ids.generation], inventory_completeness: 'partial', status: 'ready',
    changes: [{ identity: monitoringTarget.identity, change: 'admit', reason: 'Included by the selected scope.', basis: 'explicit_policy' }],
    required_permissions: ['Collector workspace access', 'Semantic-model Write for refresh history'],
    poll_count_delta: 1, subscription_count_delta: 0,
    gaps: [{ code: 'inventory_partial', detail: 'Future inventory pages still require reconciliation.' }],
  }
}
export function monitoringReceipt(planId: string, input: ActivationRequest, scope: ScopeDefinition): ActivationReceipt {
  const version = { ...input.expected, revision: input.expected.revision + 1 }
  return {
    plan_id: planId, idempotency_id: input.idempotency_id, version,
    scope: { ...scope, ...version, updated_at: monitoringTime }, activated_at: monitoringTime, state: 'configuring',
    queued_work_ids: [ids.work],
  }
}
export function monitoringFixtureApi(roles: AppRole[] = ['reader', 'admin']) {
  const api = new MonitoringApiClient(async () => null)
  let previewedScope: ScopeDefinition = monitoringScope
  let targetRows = monitoringTargets
  const reviews = new Map<string, SafetyReview>()
  const reviewOperations = new Map<string, SafetyReviewOperationReceipt>()
  vi.spyOn(api, 'bootstrap').mockResolvedValue(monitoringBootstrap)
  vi.spyOn(api, 'snapshot').mockResolvedValue({
    ...monitoringSnapshot, can_admin: roles.includes('admin'), user: { ...monitoringSnapshot.user, roles },
  })
  vi.spyOn(api, 'scopes').mockResolvedValue(monitoringPage([monitoringScope]))
  vi.spyOn(api, 'inventory').mockResolvedValue(monitoringPage(monitoringInventory))
  vi.spyOn(api, 'targets').mockImplementation(async () => monitoringPage(targetRows))
  vi.spyOn(api, 'workspaces').mockResolvedValue(monitoringPage(monitoringWorkspaces))
  vi.spyOn(api, 'domains').mockResolvedValue(monitoringPage(monitoringDomains))
  vi.spyOn(api, 'connectors').mockResolvedValue(monitoringPage([monitoringConnector]))
  vi.spyOn(api, 'preview').mockImplementation(async (input) => { previewedScope = input.scope; return monitoringPreview(input) })
  vi.spyOn(api, 'activate').mockImplementation(async (planId, input) => monitoringReceipt(planId, input, previewedScope))
  vi.spyOn(api, 'activation').mockRejectedValue(new Error('No receipt lookup was expected.'))
  vi.spyOn(api, 'refreshInventory').mockResolvedValue({ work_id: ids.work, status: 'queued' })
  vi.spyOn(api, 'safetyReview').mockImplementation(async (id) => {
    const review = reviews.get(id)
    if (!review) throw new ApiError(404, 'not_found', 'Safety review not found.')
    return review
  })
  vi.spyOn(api, 'safetyReviewOperation').mockImplementation(async (requestId) => {
    const receipt = reviewOperations.get(requestId)
    if (!receipt) throw new ApiError(404, 'not_found', 'Safety-review operation receipt not found.')
    return receipt
  })
  vi.spyOn(api, 'recordSafetyReview').mockImplementation(async (input) => {
    const prior = reviewOperations.get(input.request_id)
    if (prior) return prior.review
    const review = safetyReviewResponse(input)
    reviews.set(review.review_id, review)
    reviewOperations.set(input.request_id, safetyReviewOperationReceipt(input, review))
    targetRows = targetRows.map((target) => target.identity.workspace_id === review.target.workspace_id && target.identity.item_id === review.target.item_id
      ? { ...target, action: { enabled: false, action: review.action, review_id: review.review_id, review_revision: review.revision } } : target)
    return review
  })
  return api
}
