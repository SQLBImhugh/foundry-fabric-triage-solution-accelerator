import { APP_ROLES } from './access'
import type { AppRole } from './access'
import { requestJson } from './client'
import type { TokenProvider } from './client'
import { ApiError, isRecord } from './errors'

export type MonitoringWorkload = 'powerbi' | 'fabric_pipeline'
export const MONITORING_ACTIONS = ['powerbi_refresh', 'pipeline_rerun', 'rebind_dataset_gateway', 'reenable_refresh_schedule'] as const
export type MonitoringAction = typeof MONITORING_ACTIONS[number]
export type ConfigurationAction = 'rebind_dataset_gateway' | 'reenable_refresh_schedule'
export type SafetyReviewState = 'pending' | 'verified' | 'revoked' | 'unverifiable'
export type ReviewPublicationStatus = 'pending_validation' | 'published'
export type ReviewJsonValue = string | number | boolean | null | ReviewJsonValue[] | { [key: string]: ReviewJsonValue }
export type ReviewParameters = Record<string, ReviewJsonValue>
export type Completeness = 'complete' | 'partial' | 'unknown' | 'blocked'
export interface MonitoringContext { tenant_id: string; epoch: string }
export interface RegistryVersion extends MonitoringContext { revision: number }
export interface MonitoringControl extends RegistryVersion {
  schema_version: 1
  activation_cutoff: string
  maintenance: boolean
  updated_at: string
}
export interface MonitoringBootstrap {
  status: 'ready' | 'maintenance' | 'missing' | 'incompatible' | 'wrong_tenant'
  expected_tenant_id: string
  found_schema_version?: number | null
  control?: MonitoringControl | null
  detail: string
}
export interface CoverageGap {
  code: string
  detail: string
  workspace_id?: string | null
  item_id?: string | null
  retry_at?: string | null
}
export interface MonitoringCoverage extends RegistryVersion {
  as_of: string
  inventory_completeness: Completeness
  capability_completeness: Completeness
  scope_item_count: number | null
  discovered_count: number
  access_verified_count: number
  admitted_count: number
  current_count: number
  action_enabled_count: number
  unsupported_count: number
  backlog_count: number
  last_inventory_completed_at?: string | null
  last_poll_window_end?: string | null
  last_receiver_activity_at?: string | null
  checkpoint_lag_seconds?: number | null
  next_due_at?: string | null
  gaps: CoverageGap[]
}
export interface MonitoringSnapshot {
  control: MonitoringControl
  coverage: MonitoringCoverage
  can_admin: boolean
  user: { id: string; display_name: string; roles: AppRole[] }
}
export interface RecordPage<T> {
  version: RegistryVersion
  as_of: string
  items: T[]
  next_cursor: string | null
}
export interface ScopeSelector {
  tenant_id: string
  kind: 'tenant' | 'domain' | 'workspace' | 'item'
  domain_id?: string | null
  workspace_id?: string | null
  item_id?: string | null
  include_descendants: boolean
}
export interface PollCadence { poll_seconds: number; reconciliation_seconds: number }
export interface ScopeRule {
  rule_id: string
  selector: ScopeSelector
  effect: 'include' | 'exclude'
  workloads: MonitoringWorkload[]
  auto_enrol_detection_only: boolean
}
export interface ScopeDefinition extends MonitoringContext {
  scope_id: string
  name: string
  enabled: boolean
  rules: ScopeRule[]
  cadence: PollCadence
}
export interface ScopePolicy extends ScopeDefinition { revision: number; updated_at: string | null }
export interface InventoryItem extends MonitoringContext {
  generation_id: string
  workspace_id: string
  item_id: string
  name: string
  item_type: string
  workload: MonitoringWorkload | null
  unsupported_reason?: string | null
  domain_ids: string[]
  state: 'present' | 'deleted' | 'unknown'
  observed_at: string
  definition_hash?: string | null
}
export interface WorkspaceMetadata extends MonitoringContext {
  workspace_id: string
  name: string
  domain_id?: string | null
  capacity_id?: string | null
  state: string
  observed_at: string
  generation_id: string
}
export interface DomainMetadata extends MonitoringContext {
  domain_id: string
  name: string
  parent_domain_id?: string | null
  state: string
  observed_at: string
  generation_id: string
}
export interface TargetIdentity extends MonitoringContext {
  workload: MonitoringWorkload
  workspace_id: string
  item_id: string
}
export interface MonitoringTarget {
  identity: TargetIdentity
  name: string
  scope_ids: string[]
  admitted_rule_ids: string[]
  inventory_generation: string
  capability_id: string
  policy_revision: number
  admitted_at: string
  state: 'current' | 'review_required' | 'paused' | 'removed'
  admission_basis: 'reviewed' | 'auto_detection_only' | 'pending_review'
  reason: string
  observation: { enabled: boolean; events_enabled: boolean; cadence: PollCadence }
  action: {
    enabled: boolean
    action?: MonitoringAction | null
    review_id?: string | null
    review_revision?: number | null
  }
  next_poll_at?: string | null
}
export interface OwnedConnectorManifest extends MonitoringContext {
  connector_id: string
  ownership_id: string
  revision: number
  policy_revision: number
  workspace_id: string | null
  eventstream_id: string | null
  destination_id: string | null
  name: string
  sources: { source_id: string; target: TargetIdentity; event_types: string[] }[]
  desired_definition: Record<string, unknown>
  observed_definition?: Record<string, unknown> | null
  endpoint?: { namespace: string; entity: string; consumer_group: string } | null
  operation_id?: string | null
  state: 'planned' | 'provisioning' | 'ready' | 'degraded' | 'blocked' | 'deleting' | 'deleted'
  updated_at: string
  identity_verified_at?: string | null
  delivery_verified_at?: string | null
  last_receiver_activity_at?: string | null
  gaps: CoverageGap[]
}
export interface ScopePreviewRequest {
  expected: RegistryVersion
  idempotency_id: string
  scope: ScopeDefinition
}
export interface MonitoringPlan extends ScopePreviewRequest {
  plan_id: string
  created_at: string
  expires_at: string
  inventory_generations: string[]
  inventory_completeness: Completeness
  status: 'ready' | 'blocked'
  changes: {
    identity: TargetIdentity
    change: 'admit' | 'pause' | 'remove' | 'retain'
    reason: string
    basis: 'explicit_policy' | 'completed_inventory' | 'uncertain_inventory'
  }[]
  required_permissions: string[]
  poll_count_delta: number
  subscription_count_delta: number
  gaps: CoverageGap[]
}
export interface ActivationRequest { expected: RegistryVersion; idempotency_id: string }
export interface ActivationReceipt {
  plan_id: string
  idempotency_id: string
  version: RegistryVersion
  scope: ScopePolicy
  activated_at: string
  state: 'configuring' | 'active'
  queued_work_ids: string[]
}
export interface InventoryRefreshRequest extends ActivationRequest { selector: ScopeSelector }
export interface ConfigurationVerification {
  target: TargetIdentity
  action: ConfigurationAction
  expected_hash: string
  configuration: ReviewParameters
  observed_at: string
  authority: 'rest' | 'fixture'
}
export interface SafetyReviewIntent {
  review_id: string
  target: TargetIdentity
  revision: number
  policy_revision: number
  action: MonitoringAction
  state: SafetyReviewState
  reviewer_id: string
  reviewed_at: string
  expires_at: string
  revoked_at: string | null
  definition_hash: string | null
  configuration_hash: string | null
  parameters: ReviewParameters | null
  parameter_hash: string | null
  parameters_redacted: boolean
  replay_safe: boolean
  exact_correlation_verified: boolean
  detail: string
}
export interface SafetyReview extends SafetyReviewIntent {
  requested_state: SafetyReviewState | null
  publication_status: ReviewPublicationStatus
}
export interface SafetyReviewRequest {
  request_id: string
  expected: RegistryVersion
  expected_review_revision: number
  review: SafetyReviewIntent
}
export interface SafetyReviewOperationReceipt {
  request_id: string
  review: SafetyReview
}
export interface PageOptions { limit?: number; cursor?: string }
export interface InventoryOptions extends PageOptions { workload?: MonitoringWorkload; workspace_id?: string }
export interface TargetOptions extends InventoryOptions { include_inactive?: boolean }

const workloads = ['powerbi', 'fabric_pipeline'] as const
const completeness = ['complete', 'partial', 'unknown', 'blocked'] as const
const text = (value: unknown): value is string => typeof value === 'string' && Boolean(value.trim())
const id = (value: unknown): value is string => typeof value === 'string'
  && /^[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(value)
  && value !== '00000000-0000-0000-0000-000000000000'
const integer = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value)
const count = (value: unknown): value is number => integer(value) && value >= 0
const instant = (value: unknown): value is string => typeof value === 'string'
  && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
  && Number.isFinite(Date.parse(value))
const fingerprint = (value: unknown): value is string => typeof value === 'string' && /^[a-f0-9]{64}$/i.test(value)

function optional<T>(value: unknown, guard: (value: unknown) => value is T): value is T | null | undefined {
  return value === undefined || value === null || guard(value)
}
function oneOf<const T extends readonly string[]>(value: unknown, values: T): value is T[number] {
  return values.some((entry) => entry === value)
}
function arrayOf<T>(value: unknown, guard: (value: unknown) => value is T): value is T[] {
  return Array.isArray(value) && value.every(guard)
}
function unique(values: readonly string[]): boolean { return new Set(values).size === values.length }
function context(value: unknown): value is MonitoringContext {
  return isRecord(value) && id(value.tenant_id) && id(value.epoch)
}
function version(value: unknown): value is RegistryVersion {
  return isRecord(value) && context(value) && count(value.revision)
}
export { id as isMonitoringId, version as isMonitoringVersion }
export function sameMonitoringContext(left: MonitoringContext, right: MonitoringContext): boolean {
  return left.tenant_id === right.tenant_id && left.epoch === right.epoch
}
export function sameMonitoringVersion(left: RegistryVersion, right: RegistryVersion): boolean {
  return sameMonitoringContext(left, right) && left.revision === right.revision
}
export function monitoringTargetKey(value: TargetIdentity): string {
  return [value.tenant_id, value.epoch, value.workload, value.workspace_id, value.item_id].join(':')
}
function control(value: unknown): value is MonitoringControl {
  return isRecord(value) && version(value) && value.schema_version === 1
    && instant(value.activation_cutoff) && typeof value.maintenance === 'boolean' && instant(value.updated_at)
}
function bootstrap(value: unknown): value is MonitoringBootstrap {
  if (!isRecord(value) || !oneOf(value.status, ['ready', 'maintenance', 'missing', 'incompatible', 'wrong_tenant'])
    || !id(value.expected_tenant_id) || !text(value.detail) || !optional(value.found_schema_version, count)
    || !optional(value.control, control)) return false
  if (value.status === 'ready' || value.status === 'maintenance') {
    return control(value.control) && value.found_schema_version === 1
      && value.control.tenant_id === value.expected_tenant_id
      && value.control.maintenance === (value.status === 'maintenance')
  }
  if (value.status === 'missing') return value.control == null && value.found_schema_version == null
  if (value.status === 'wrong_tenant') return value.control === null && value.found_schema_version === null
  return count(value.found_schema_version) && value.found_schema_version !== 1
}
function gap(value: unknown): value is CoverageGap {
  return isRecord(value) && text(value.code) && text(value.detail) && optional(value.workspace_id, id)
    && optional(value.item_id, id) && (!value.item_id || Boolean(value.workspace_id)) && optional(value.retry_at, instant)
}
function coverage(value: unknown): value is MonitoringCoverage {
  return isRecord(value) && version(value) && instant(value.as_of)
    && oneOf(value.inventory_completeness, completeness) && oneOf(value.capability_completeness, completeness)
    && (value.scope_item_count === null || count(value.scope_item_count))
    && count(value.discovered_count) && count(value.access_verified_count) && count(value.admitted_count)
    && count(value.current_count) && count(value.action_enabled_count) && count(value.unsupported_count)
    && count(value.backlog_count) && optional(value.last_inventory_completed_at, instant)
    && optional(value.last_poll_window_end, instant) && optional(value.last_receiver_activity_at, instant)
    && optional(value.checkpoint_lag_seconds, count) && optional(value.next_due_at, instant) && arrayOf(value.gaps, gap)
    && value.action_enabled_count <= value.current_count && value.current_count <= value.admitted_count
    && value.admitted_count + value.unsupported_count <= value.discovered_count
    && value.access_verified_count <= value.discovered_count
    && (value.scope_item_count === null || value.discovered_count <= value.scope_item_count)
    && (value.inventory_completeness !== 'complete' || value.scope_item_count !== null)
    && ((value.inventory_completeness === 'complete' && value.capability_completeness === 'complete') || value.gaps.length > 0)
}
function snapshot(value: unknown): value is MonitoringSnapshot {
  return isRecord(value) && control(value.control) && coverage(value.coverage)
    && sameMonitoringVersion(value.control, value.coverage) && typeof value.can_admin === 'boolean'
    && isRecord(value.user) && text(value.user.id) && text(value.user.display_name)
    && arrayOf(value.user.roles, (entry): entry is AppRole => oneOf(entry, APP_ROLES))
    && value.user.roles.length > 0 && unique(value.user.roles) && value.can_admin === value.user.roles.includes('admin')
}
function selector(value: unknown): value is ScopeSelector {
  if (!isRecord(value) || !id(value.tenant_id) || !oneOf(value.kind, ['tenant', 'domain', 'workspace', 'item'])
    || !optional(value.domain_id, id) || !optional(value.workspace_id, id) || !optional(value.item_id, id)
    || typeof value.include_descendants !== 'boolean' || (value.include_descendants && value.kind !== 'domain')) return false
  if (value.kind === 'tenant') return value.domain_id == null && value.workspace_id == null && value.item_id == null
  if (value.kind === 'domain') return id(value.domain_id) && value.workspace_id == null && value.item_id == null
  return value.domain_id == null && id(value.workspace_id)
    && (value.kind === 'item' ? id(value.item_id) : value.item_id == null)
}
function cadence(value: unknown): value is PollCadence {
  return isRecord(value) && integer(value.poll_seconds) && value.poll_seconds >= 15 && value.poll_seconds <= 86400
    && integer(value.reconciliation_seconds) && value.reconciliation_seconds >= 15 && value.reconciliation_seconds <= 86400
}
function rule(value: unknown): value is ScopeRule {
  return isRecord(value) && id(value.rule_id) && selector(value.selector) && oneOf(value.effect, ['include', 'exclude'])
    && arrayOf(value.workloads, (entry): entry is MonitoringWorkload => oneOf(entry, workloads))
    && value.workloads.length > 0 && unique(value.workloads) && typeof value.auto_enrol_detection_only === 'boolean'
    && (value.effect !== 'exclude' || !value.auto_enrol_detection_only)
}
function scope(value: unknown): value is ScopeDefinition {
  return isRecord(value) && context(value) && id(value.scope_id) && text(value.name) && value.name.length <= 200
    && typeof value.enabled === 'boolean' && arrayOf(value.rules, rule) && cadence(value.cadence)
    && unique(value.rules.map((entry) => entry.rule_id)) && value.rules.every((entry) => entry.selector.tenant_id === value.tenant_id)
}
function policy(value: unknown): value is ScopePolicy {
  return isRecord(value) && scope(value) && count(value.revision)
    && (value.updated_at === null || instant(value.updated_at))
}
function inventoryItem(value: unknown): value is InventoryItem {
  return isRecord(value) && context(value) && id(value.generation_id) && id(value.workspace_id)
    && id(value.item_id) && text(value.name) && text(value.item_type)
    && arrayOf(value.domain_ids, id) && unique(value.domain_ids) && oneOf(value.state, ['present', 'deleted', 'unknown'])
    && instant(value.observed_at) && optional(value.definition_hash, fingerprint)
    && (value.workload === null ? text(value.unsupported_reason)
      : oneOf(value.workload, workloads) && value.unsupported_reason == null
        && (value.workload === 'powerbi' ? ['SemanticModel', 'Dataset'].includes(value.item_type) : value.item_type === 'DataPipeline'))
}
function metadata(value: unknown): value is MonitoringContext & { name: string; state: string; observed_at: string; generation_id: string } {
  return isRecord(value) && context(value) && text(value.name) && text(value.state) && instant(value.observed_at) && id(value.generation_id)
}
function workspace(value: unknown): value is WorkspaceMetadata {
  return isRecord(value) && metadata(value) && id(value.workspace_id) && optional(value.domain_id, id) && optional(value.capacity_id, id)
}
function domain(value: unknown): value is DomainMetadata {
  return isRecord(value) && metadata(value) && id(value.domain_id) && optional(value.parent_domain_id, id)
}
function targetIdentity(value: unknown): value is TargetIdentity {
  return isRecord(value) && context(value) && oneOf(value.workload, workloads) && id(value.workspace_id) && id(value.item_id)
}
export function actionMatchesWorkload(action: MonitoringAction, workload: MonitoringWorkload): boolean {
  return (action === 'pipeline_rerun') === (workload === 'fabric_pipeline')
}
function target(value: unknown): value is MonitoringTarget {
  if (!isRecord(value) || !targetIdentity(value.identity) || !text(value.name)
    || !arrayOf(value.scope_ids, id) || !value.scope_ids.length || !unique(value.scope_ids)
    || !arrayOf(value.admitted_rule_ids, id) || !value.admitted_rule_ids.length || !unique(value.admitted_rule_ids)
    || !id(value.inventory_generation) || !id(value.capability_id) || !count(value.policy_revision) || !instant(value.admitted_at)
    || !oneOf(value.state, ['current', 'review_required', 'paused', 'removed'])
    || !oneOf(value.admission_basis, ['reviewed', 'auto_detection_only', 'pending_review']) || !text(value.reason)
    || !isRecord(value.observation) || typeof value.observation.enabled !== 'boolean'
    || typeof value.observation.events_enabled !== 'boolean' || !cadence(value.observation.cadence)
    || !isRecord(value.action) || typeof value.action.enabled !== 'boolean'
    || !optional(value.action.action, (entry): entry is MonitoringAction => oneOf(entry, MONITORING_ACTIONS))
    || !optional(value.action.review_id, id) || !optional(value.action.review_revision, (entry): entry is number => count(entry) && entry > 0)
    || !optional(value.next_poll_at, instant)) return false
  return (value.state === 'current' || (!value.observation.enabled && !value.action.enabled))
    && (value.admission_basis !== 'pending_review' || value.state !== 'current')
    && (!value.observation.events_enabled || value.observation.enabled)
    && ((value.action.review_id == null) === (value.action.review_revision == null))
    && (value.action.action == null || actionMatchesWorkload(value.action.action, value.identity.workload))
    && (!value.action.enabled || (value.observation.enabled && value.admission_basis === 'reviewed'
      && Boolean(value.action.action) && Boolean(value.action.review_id)))
}
export function monitoringDefinitionsMatch(left: unknown, right: unknown): boolean {
  if (left === right) return true
  if (Array.isArray(left) && Array.isArray(right)) return left.length === right.length
    && left.every((entry: unknown, index) => monitoringDefinitionsMatch(entry, right[index]))
  if (!isRecord(left) || !isRecord(right)) return false
  return Object.keys(left).length === Object.keys(right).length
    && Object.keys(left).every((key) => Object.hasOwn(right, key) && monitoringDefinitionsMatch(left[key], right[key]))
}
export function isReviewParameters(value: unknown): value is ReviewParameters {
  if (!isRecord(value)) return false
  let remaining = 4096
  function visit(node: unknown, depth: number): boolean {
    remaining -= 1
    if (remaining < 0 || depth > 16) return false
    if (node === null || typeof node === 'boolean' || typeof node === 'string') return true
    if (typeof node === 'number') return Number.isFinite(node)
    if (Array.isArray(node)) return node.every((child: unknown) => visit(child, depth + 1))
    return isRecord(node) && Object.entries(node).every(([key, child]) =>
      Boolean(key.trim()) && key.length <= 256 && visit(child, depth + 1))
  }
  return unique(Object.keys(value).map((key) => key.toLowerCase())) && visit(value, 0)
    && new TextEncoder().encode(JSON.stringify(value)).length <= 65536
}
export function isActionParameters(action: MonitoringAction, value: unknown): value is ReviewParameters | null {
  if (action === 'powerbi_refresh' && value === null) return true
  if (!isReviewParameters(value)) return false
  if (action === 'rebind_dataset_gateway') {
    if (Object.keys(value).length !== 2 || !id(value.gateway_id) || value.gateway_id !== value.gateway_id.toLowerCase()
      || !arrayOf(value.datasource_ids, id) || !value.datasource_ids.length) return false
    const sources = value.datasource_ids
    return sources.every((entry) => entry === entry.toLowerCase()) && unique(sources)
      && sources.every((entry, index) => index === 0 || entry > sources[index - 1]!)
  }
  if (action === 'reenable_refresh_schedule') return Object.keys(value).length === 1 && value.enabled === true
  return true
}
export function parseSafetyParameters(action: 'powerbi_refresh' | 'pipeline_rerun', value: string): ReviewParameters | null {
  if (!value.trim() && action === 'powerbi_refresh') return null
  let parsed: unknown
  try { parsed = JSON.parse(value) } catch {
    throw new ApiError(400, 'invalid_review_parameters', 'Enter a JSON parameter object. For a pipeline with no parameters, explicitly enter {}.')
  }
  if (!isActionParameters(action, parsed)) {
    throw new ApiError(400, 'invalid_review_parameters',
      'Parameters must be a bounded JSON object with nonblank, case-insensitively distinct names. Pipeline parameters cannot be missing or null.')
  }
  return parsed
}
export async function configurationFingerprint(action: ConfigurationAction, parameters: ReviewParameters): Promise<string> {
  if (!isActionParameters(action, parameters)) {
    throw new ApiError(400, 'invalid_configuration_intent', 'Specify the exact canonical gateway binding or exactly {enabled: true} for the schedule intent.')
  }
  if (!globalThis.crypto?.subtle) {
    throw new ApiError(0, 'review_hash_unavailable', 'A secure browser context is required to fingerprint the reviewed configuration. Nothing was submitted.')
  }
  // Configuration intents contain only canonical GUIDs or a boolean, so this
  // sorted compact JSON has the same bytes as the server's configuration digest.
  const canonical = action === 'rebind_dataset_gateway'
    ? { datasource_ids: parameters.datasource_ids, gateway_id: parameters.gateway_id }
    : { enabled: true }
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(JSON.stringify(canonical)))
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('')
}
function safetyReviewFields(value: unknown): value is SafetyReviewIntent {
  if (!isRecord(value) || !id(value.review_id) || !targetIdentity(value.target)
    || !count(value.revision) || value.revision < 1 || !count(value.policy_revision)
    || !oneOf(value.action, MONITORING_ACTIONS) || !actionMatchesWorkload(value.action, value.target.workload)
    || !oneOf(value.state, ['pending', 'verified', 'revoked', 'unverifiable'])
    || !id(value.reviewer_id) || !instant(value.reviewed_at) || !instant(value.expires_at)
    || !(value.revoked_at === null || instant(value.revoked_at))
    || ((value.state === 'revoked') !== (value.revoked_at !== null))
    || (value.revoked_at !== null && Date.parse(value.revoked_at) < Date.parse(value.reviewed_at))
    || !(value.definition_hash === null || fingerprint(value.definition_hash))
    || !(value.configuration_hash === null || fingerprint(value.configuration_hash))
    || !(value.parameters === null || isReviewParameters(value.parameters))
    || !(value.parameter_hash === null || fingerprint(value.parameter_hash))
    || typeof value.parameters_redacted !== 'boolean' || typeof value.replay_safe !== 'boolean'
    || typeof value.exact_correlation_verified !== 'boolean' || !text(value.detail) || value.detail.length > 4096) return false
  if (value.parameters_redacted && (value.parameters !== null || value.parameter_hash === null || value.state === 'verified')) return false
  if (value.parameters !== null && !isActionParameters(value.action, value.parameters)) return false
  if (value.state !== 'verified') return true
  if (value.action === 'pipeline_rerun') return value.definition_hash !== null && value.parameters !== null
    && value.replay_safe && value.exact_correlation_verified
  if (value.action === 'powerbi_refresh') return value.exact_correlation_verified
  return value.configuration_hash !== null && value.parameters !== null
}
function safetyReviewIntent(value: unknown): value is SafetyReviewIntent {
  return isRecord(value) && safetyReviewFields(value)
    && !Object.hasOwn(value, 'requested_state') && !Object.hasOwn(value, 'publication_status')
    && (value.state === 'revoked' || Date.parse(value.expires_at) > Date.parse(value.reviewed_at))
}
function safetyReview(value: unknown): value is SafetyReview {
  if (!isRecord(value) || !safetyReviewFields(value)
    || !oneOf(value.publication_status, ['pending_validation', 'published'])
    || !(value.requested_state === null || oneOf(value.requested_state, ['pending', 'verified', 'revoked', 'unverifiable']))) return false
  const awaitingPublication = value.publication_status === 'pending_validation'
  if (awaitingPublication && (value.state !== 'pending' || value.requested_state === null || value.exact_correlation_verified)) return false
  const revocation = value.state === 'revoked' || (awaitingPublication && value.requested_state === 'revoked')
  return revocation || Date.parse(value.expires_at) > Date.parse(value.reviewed_at)
}
async function checkedReviewHash<T extends SafetyReviewIntent>(review: T, response: boolean): Promise<T> {
  if (response && review.parameter_hash === null) throw invalidResponse()
  if ((review.action === 'rebind_dataset_gateway' || review.action === 'reenable_refresh_schedule')
    && review.configuration_hash !== null && review.parameters !== null
    && await configurationFingerprint(review.action, review.parameters) !== review.configuration_hash.toLowerCase()) {
    throw response ? invalidResponse() : new ApiError(400, 'invalid_configuration_hash', 'The configuration fingerprint does not match the explicit reviewed intent.')
  }
  return review
}
async function checkedSafetyReview(value: unknown): Promise<SafetyReview> {
  return checkedReviewHash(checked(value, safetyReview), true)
}
function safetyReviewRequest(value: unknown): value is SafetyReviewRequest {
  return isRecord(value) && id(value.request_id) && version(value.expected) && count(value.expected_review_revision)
    && safetyReviewIntent(value.review) && sameMonitoringContext(value.expected, value.review.target)
    && value.review.policy_revision === value.expected.revision
    && value.review.revision === value.expected_review_revision + 1
    && (value.review.action !== 'pipeline_rerun' || value.review.state === 'revoked' || value.review.parameters_redacted
      || (value.review.parameters !== null && value.review.definition_hash !== null))
}
export function reviewStateMatches(requested: SafetyReviewState, returned: SafetyReview): boolean {
  if (returned.publication_status === 'pending_validation') {
    return returned.state === 'pending' && returned.requested_state === requested
      && !returned.exact_correlation_verified && returned.revoked_at === null
  }
  if (returned.requested_state !== null && returned.requested_state !== requested) return false
  return requested === 'verified' ? returned.state !== 'revoked'
    : requested === 'revoked' ? returned.state === 'revoked' : returned.state === requested || returned.state === 'unverifiable'
}
function connector(value: unknown): value is OwnedConnectorManifest {
  if (!isRecord(value) || !context(value) || !id(value.connector_id) || !id(value.ownership_id)
    || !count(value.revision) || !count(value.policy_revision)
    || !(value.workspace_id === null || id(value.workspace_id)) || !(value.eventstream_id === null || id(value.eventstream_id))
    || !(value.destination_id === null || text(value.destination_id)) || !text(value.name) || !isRecord(value.desired_definition)
    || !optional(value.observed_definition, isRecord) || !optional(value.operation_id, text)
    || !Array.isArray(value.sources) || !value.sources.every((entry: unknown) => isRecord(entry) && text(entry.source_id)
      && targetIdentity(entry.target) && sameMonitoringContext(entry.target, value)
      && arrayOf(entry.event_types, text) && entry.event_types.length > 0 && unique(entry.event_types))
    || !oneOf(value.state, ['planned', 'provisioning', 'ready', 'degraded', 'blocked', 'deleting', 'deleted'])
    || !instant(value.updated_at) || !optional(value.identity_verified_at, instant) || !optional(value.delivery_verified_at, instant)
    || !optional(value.last_receiver_activity_at, instant) || !arrayOf(value.gaps, gap)) return false
  if (value.endpoint != null && (!isRecord(value.endpoint) || !text(value.endpoint.namespace)
    || !/^[a-z0-9-]+(?:\.[a-z0-9-]+)+$/i.test(value.endpoint.namespace)
    || !text(value.endpoint.entity) || !text(value.endpoint.consumer_group))) return false
  return ((value.state !== 'blocked' && value.state !== 'degraded') || value.gaps.length > 0)
    && (value.state !== 'ready' || (value.workspace_id !== null && value.eventstream_id !== null && value.destination_id !== null
      && value.sources.length > 0 && value.endpoint != null && value.observed_definition != null
      && monitoringDefinitionsMatch(value.desired_definition, value.observed_definition)
      && value.identity_verified_at != null && value.delivery_verified_at != null))
}
function previewRequest(value: unknown): value is ScopePreviewRequest {
  return isRecord(value) && version(value.expected) && id(value.idempotency_id) && scope(value.scope)
    && sameMonitoringContext(value.expected, value.scope)
}
function plan(value: unknown): value is MonitoringPlan {
  return isRecord(value) && previewRequest(value) && id(value.plan_id) && instant(value.created_at) && instant(value.expires_at)
    && Date.parse(value.expires_at) > Date.parse(value.created_at)
    && arrayOf(value.inventory_generations, id) && unique(value.inventory_generations)
    && oneOf(value.inventory_completeness, completeness) && oneOf(value.status, ['ready', 'blocked'])
    && Array.isArray(value.changes) && value.changes.every((entry: unknown) => isRecord(entry)
      && targetIdentity(entry.identity) && sameMonitoringContext(entry.identity, value.expected)
      && oneOf(entry.change, ['admit', 'pause', 'remove', 'retain']) && text(entry.reason)
      && oneOf(entry.basis, ['explicit_policy', 'completed_inventory', 'uncertain_inventory'])
      && (entry.change !== 'remove' || entry.basis !== 'uncertain_inventory'))
    && arrayOf(value.required_permissions, text) && integer(value.poll_count_delta)
    && integer(value.subscription_count_delta) && arrayOf(value.gaps, gap)
    && ((value.status === 'ready' && value.inventory_completeness === 'complete') || value.gaps.length > 0)
}
function receipt(value: unknown): value is ActivationReceipt {
  return isRecord(value) && id(value.plan_id) && id(value.idempotency_id) && version(value.version)
    && policy(value.scope) && sameMonitoringVersion(value.version, value.scope) && instant(value.activated_at)
    && oneOf(value.state, ['configuring', 'active']) && arrayOf(value.queued_work_ids, id) && unique(value.queued_work_ids)
}
function invalidResponse(): ApiError {
  return new ApiError(200, 'invalid_monitoring_response',
    'The monitoring API returned incomplete, inconsistent or unsupported records. No coverage or action permission has been inferred.')
}
function checked<T>(value: unknown, guard: (value: unknown) => value is T): T {
  if (!guard(value)) throw invalidResponse()
  return value
}
function normalizedScope(value: ScopeDefinition): unknown {
  return { ...value, rules: value.rules.map((entry) => ({ ...entry, selector: {
    ...entry.selector, domain_id: entry.selector.domain_id ?? null,
    workspace_id: entry.selector.workspace_id ?? null, item_id: entry.selector.item_id ?? null,
  } })) }
}
function queryString(options: TargetOptions): string {
  if (options.limit !== undefined && (!integer(options.limit) || options.limit < 1 || options.limit > 1000)
    || options.cursor !== undefined && !text(options.cursor)
    || options.workspace_id !== undefined && !id(options.workspace_id)
    || options.workload !== undefined && !oneOf(options.workload, workloads)) {
    throw new ApiError(400, 'invalid_monitoring_query', 'The monitoring page selection is invalid.')
  }
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(options)) if (value !== undefined) query.set(key, String(value))
  return query.size ? `?${query}` : ''
}

export async function readMonitoringPages<T>(
  read: (cursor?: string) => Promise<RecordPage<T>>, expected: RegistryVersion,
  signal: AbortSignal, key: (item: T) => string,
): Promise<T[]> {
  const items: T[] = []
  const cursors = new Set<string>()
  const keys = new Set<string>()
  let cursor: string | undefined
  do {
    signal.throwIfAborted()
    const page = await read(cursor)
    signal.throwIfAborted()
    if (!sameMonitoringVersion(page.version, expected)) {
      throw new ApiError(409, 'monitoring_revision_changed', 'Monitoring changed while these pages were loading. Refresh before previewing or activating a scope.')
    }
    for (const item of page.items) {
      const identity = key(item)
      if (keys.has(identity)) throw invalidResponse()
      keys.add(identity)
      items.push(item)
    }
    cursor = page.next_cursor ?? undefined
    if (cursor !== undefined && cursors.has(cursor)) throw invalidResponse()
    if (cursor !== undefined) cursors.add(cursor)
  } while (cursor !== undefined)
  return items
}

export class MonitoringApiClient {
  constructor(private readonly getToken: TokenProvider) {}

  private async page<T>(
    path: string, options: TargetOptions, guard: (value: unknown) => value is T,
    itemContext: (item: T) => MonitoringContext, signal?: AbortSignal,
  ): Promise<RecordPage<T>> {
    const value = await requestJson<unknown>(`/monitoring/${path}${queryString(options)}`, { signal }, this.getToken)
    if (!isRecord(value) || !version(value.version) || !instant(value.as_of)
      || !arrayOf(value.items, guard) || !(value.next_cursor === null || text(value.next_cursor))) throw invalidResponse()
    const pageVersion = value.version
    if (!value.items.every((item) => sameMonitoringContext(itemContext(item), pageVersion))) throw invalidResponse()
    return { version: pageVersion, as_of: value.as_of, items: value.items, next_cursor: value.next_cursor }
  }

  async bootstrap(signal?: AbortSignal): Promise<MonitoringBootstrap> {
    return checked(await requestJson<unknown>('/monitoring/bootstrap', { signal }, this.getToken), bootstrap)
  }
  async snapshot(signal?: AbortSignal): Promise<MonitoringSnapshot> {
    return checked(await requestJson<unknown>('/monitoring/snapshot', { signal }, this.getToken), snapshot)
  }
  scopes(options: PageOptions = {}, signal?: AbortSignal): Promise<RecordPage<ScopePolicy>> {
    return this.page('scopes', options, policy, (item) => item, signal)
  }
  inventory(options: InventoryOptions = {}, signal?: AbortSignal): Promise<RecordPage<InventoryItem>> {
    return this.page('inventory', options, inventoryItem, (item) => item, signal)
  }
  targets(options: TargetOptions = {}, signal?: AbortSignal): Promise<RecordPage<MonitoringTarget>> {
    return this.page('targets', options, target, (item) => item.identity, signal)
  }
  workspaces(options: PageOptions = {}, signal?: AbortSignal): Promise<RecordPage<WorkspaceMetadata>> {
    return this.page('workspaces', options, workspace, (item) => item, signal)
  }
  domains(options: PageOptions = {}, signal?: AbortSignal): Promise<RecordPage<DomainMetadata>> {
    return this.page('domains', options, domain, (item) => item, signal)
  }
  connectors(options: PageOptions = {}, signal?: AbortSignal): Promise<RecordPage<OwnedConnectorManifest>> {
    return this.page('connectors', options, connector, (item) => item, signal)
  }
  async refreshInventory(input: InventoryRefreshRequest): Promise<{ work_id: string; status: 'queued' }> {
    if (!version(input.expected) || !id(input.idempotency_id) || !selector(input.selector)
      || input.selector.tenant_id !== input.expected.tenant_id) {
      throw new ApiError(400, 'invalid_monitoring_request', 'Choose a valid discovery scope in the deployment tenant.')
    }
    const value = await requestJson<unknown>('/monitoring/inventory/refresh', { method: 'POST', body: JSON.stringify(input) }, this.getToken)
    if (!isRecord(value) || !id(value.work_id) || value.status !== 'queued') throw invalidResponse()
    return { work_id: value.work_id, status: value.status }
  }
  async preview(input: ScopePreviewRequest, signal?: AbortSignal): Promise<MonitoringPlan> {
    if (!previewRequest(input)) throw new ApiError(400, 'invalid_monitoring_request', 'Complete the scope, supported workloads and cadence before previewing.')
    const value = checked(await requestJson<unknown>('/monitoring/scopes/preview',
      { method: 'POST', body: JSON.stringify(input), signal }, this.getToken), plan)
    if (!sameMonitoringVersion(value.expected, input.expected) || value.idempotency_id !== input.idempotency_id
      || !monitoringDefinitionsMatch(normalizedScope(value.scope), normalizedScope(input.scope))) throw invalidResponse()
    return value
  }
  async plan(planId: string, signal?: AbortSignal): Promise<MonitoringPlan> {
    const value = checked(await requestJson<unknown>(`/monitoring/plans/${encodeURIComponent(planId)}`, { signal }, this.getToken), plan)
    if (value.plan_id !== planId) throw invalidResponse()
    return value
  }
  async activate(planId: string, input: ActivationRequest): Promise<ActivationReceipt> {
    if (!id(planId) || !version(input.expected) || !id(input.idempotency_id)) {
      throw new ApiError(400, 'invalid_monitoring_request', 'A current preview and activation submission identity are required.')
    }
    const value = checked(await requestJson<unknown>(`/monitoring/plans/${encodeURIComponent(planId)}/activate`,
      { method: 'POST', body: JSON.stringify(input) }, this.getToken), receipt)
    if (value.plan_id !== planId || value.idempotency_id !== input.idempotency_id
      || !sameMonitoringContext(value.version, input.expected) || value.version.revision <= input.expected.revision) throw invalidResponse()
    return value
  }
  async activation(idempotencyId: string, signal?: AbortSignal): Promise<ActivationReceipt> {
    const value = checked(await requestJson<unknown>(`/monitoring/activations/${encodeURIComponent(idempotencyId)}`, { signal }, this.getToken), receipt)
    if (value.idempotency_id !== idempotencyId) throw invalidResponse()
    return value
  }
  async safetyReview(reviewId: string, signal?: AbortSignal): Promise<SafetyReview> {
    if (!id(reviewId)) throw new ApiError(400, 'invalid_review_id', 'A canonical safety-review ID is required.')
    const value = await checkedSafetyReview(await requestJson<unknown>(
      `/monitoring/safety-reviews/${encodeURIComponent(reviewId)}`, { signal }, this.getToken))
    if (value.review_id !== reviewId) throw invalidResponse()
    return value
  }
  async safetyReviewOperation(requestId: string, signal?: AbortSignal): Promise<SafetyReviewOperationReceipt> {
    if (!id(requestId)) throw new ApiError(400, 'invalid_review_request_id', 'The original safety-review request ID is required.')
    const value = await requestJson<unknown>(
      `/monitoring/safety-review-operations/${encodeURIComponent(requestId)}`, { signal }, this.getToken)
    if (!isRecord(value) || value.request_id !== requestId) throw invalidResponse()
    return { request_id: requestId, review: await checkedSafetyReview(value.review) }
  }
  async recordSafetyReview(input: SafetyReviewRequest): Promise<SafetyReview> {
    if (!safetyReviewRequest(input)) {
      throw new ApiError(400, 'invalid_safety_review', 'The safety review must bind one supported target/action to the current policy and the next review revision.')
    }
    await checkedReviewHash(input.review, false)
    const value = await checkedSafetyReview(await requestJson<unknown>('/monitoring/safety-reviews',
      { method: 'POST', body: JSON.stringify(input) }, this.getToken))
    const requested = input.review
    const allowedState = reviewStateMatches(requested.state, value)
    if (value.review_id !== requested.review_id || monitoringTargetKey(value.target) !== monitoringTargetKey(requested.target)
      || value.action !== requested.action || value.revision !== requested.revision
      || value.policy_revision !== input.expected.revision || !allowedState
      || value.definition_hash !== requested.definition_hash || value.configuration_hash !== requested.configuration_hash
      || Date.parse(value.expires_at) !== Date.parse(requested.expires_at)
      || (!value.parameters_redacted && !monitoringDefinitionsMatch(value.parameters, requested.parameters))
      || (requested.parameters_redacted && value.parameter_hash !== requested.parameter_hash)) throw invalidResponse()
    return value
  }
}
