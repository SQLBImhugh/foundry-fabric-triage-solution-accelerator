import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { Eye, Plus, Radar, RefreshCw, ShieldCheck, X } from 'lucide-react'
import { ApiError, asApiError, isAborted, isRecord } from '../api/errors'
import {
  isMonitoringId, isMonitoringVersion, monitoringDefinitionsMatch, monitoringTargetKey, readMonitoringPages,
  sameMonitoringContext, sameMonitoringVersion,
} from '../api/monitoring'
import type {
  ActivationReceipt, ActivationRequest, CoverageGap, DomainMetadata, InventoryItem,
  InventoryRefreshRequest, MonitoringApiClient, MonitoringBootstrap, MonitoringPlan,
  MonitoringSnapshot, MonitoringTarget, MonitoringWorkload, OwnedConnectorManifest,
  RegistryVersion, ScopeDefinition, ScopePolicy, ScopeRule, ScopeSelector, WorkspaceMetadata,
  TargetIdentity,
} from '../api/monitoring'
import { formatDate, humanize } from '../domain'
import type { Tone } from '../domain'
import { useResource } from '../hooks/useResource'
import { Badge, ErrorNotice, LoadingState } from './shared'
import { MonitoringSafetyReview } from './MonitoringSafetyReview'
import type { SafetyReviewSelection } from './MonitoringSafetyReview'
import './MonitoringWorkspace.css'

interface MonitoringCatalog {
  scopes: ScopePolicy[]
  inventory: InventoryItem[]
  targets: MonitoringTarget[]
  workspaces: WorkspaceMetadata[]
  domains: DomainMetadata[]
  connectors: OwnedConnectorManifest[]
}
interface PendingActivation {
  planId: string
  scopeId: string
  userId: string
  request: ActivationRequest
  state: 'submitting' | 'checking' | 'reviewing' | 'uncertain'
  restored?: boolean
}
export const MONITORING_ACTIVATION_STORAGE_KEY = 'command-center.monitoring.activation.v1'

function activationRecovery(): { pending: PendingActivation | null; error: ApiError | null } {
  try {
    const raw = window.sessionStorage.getItem(MONITORING_ACTIVATION_STORAGE_KEY)
    if (raw === null) return { pending: null, error: null }
    const value: unknown = raw.length <= 4096 ? JSON.parse(raw) : null
    if (!isRecord(value) || value.schema_version !== 1 || !isMonitoringId(value.planId) || !isMonitoringId(value.scopeId)
      || typeof value.userId !== 'string' || !value.userId.trim()
      || !isRecord(value.request) || !isMonitoringVersion(value.request.expected) || !isMonitoringId(value.request.idempotency_id)) {
      throw new Error('The saved activation pointer is incomplete or incompatible.')
    }
    return { pending: {
      planId: value.planId, scopeId: value.scopeId, userId: value.userId,
      request: { expected: value.request.expected, idempotency_id: value.request.idempotency_id },
      state: 'uncertain', restored: true,
    }, error: null }
  } catch {
    return { pending: null, error: new ApiError(0, 'monitoring_recovery_unavailable',
      'The browser could not read the unresolved activation pointer. Setup is locked; restore browser session storage before reloading. No activation has been retried.') }
  }
}
export interface MonitoringWorkspaceProps {
  api: MonitoringApiClient
  roles: readonly string[]
  userId: string | null
  fresh: boolean
  permissionRevision: number
  active?: boolean
  onChanged: () => void
}
const supportedWorkloads: { id: MonitoringWorkload; label: string }[] = [
  { id: 'powerbi', label: 'Power BI semantic models' },
  { id: 'fabric_pipeline', label: 'Scheduled Fabric pipelines' },
]
const workloadName = (value: MonitoringWorkload) => value === 'powerbi' ? 'Power BI' : 'Fabric pipeline'
const date = (value?: string | null) => value ? <time dateTime={value}>{formatDate(value)}</time> : 'Not reported'
const contextKey = (value: RegistryVersion) => `${value.tenant_id}:${value.epoch}:${value.revision}`
const inventoryKey = (value: InventoryItem) => `${value.workspace_id}:${value.item_id}`
const workspaceName = (catalog: MonitoringCatalog, id: string) =>
  catalog.workspaces.find((item) => item.workspace_id === id)?.name ?? `Workspace ${id} (metadata unavailable)`
const itemName = (catalog: MonitoringCatalog, workspaceId: string, itemId: string) =>
  catalog.inventory.find((item) => item.workspace_id === workspaceId && item.item_id === itemId)?.name
  ?? catalog.targets.find((item) => item.identity.workspace_id === workspaceId && item.identity.item_id === itemId)?.name
  ?? `Item ${itemId} (metadata unavailable)`

function domainName(catalog: MonitoringCatalog, domainId: string): string {
  const names: string[] = []
  const seen = new Set<string>()
  let current: string | null | undefined = domainId
  while (current && !seen.has(current)) {
    seen.add(current)
    const entry = catalog.domains.find((item) => item.domain_id === current)
    names.unshift(entry?.name ?? `Domain ${current} (metadata unavailable)`)
    current = entry?.parent_domain_id
  }
  return names.join(' / ')
}
function selectorName(value: ScopeSelector, catalog: MonitoringCatalog): string {
  if (value.kind === 'tenant') return 'Deployment tenant'
  if (value.kind === 'domain') return `${domainName(catalog, value.domain_id ?? '')}${value.include_descendants ? ' and descendants' : ''}`
  const workspace = workspaceName(catalog, value.workspace_id ?? '')
  return value.kind === 'item' ? `${workspace} / ${itemName(catalog, value.workspace_id ?? '', value.item_id ?? '')}` : workspace
}
function emptySelector(tenant: string, kind: ScopeSelector['kind'] = 'workspace'): ScopeSelector {
  return { tenant_id: tenant, kind, domain_id: null, workspace_id: null, item_id: null, include_descendants: false }
}
function newScope(expected: RegistryVersion): ScopeDefinition {
  return {
    tenant_id: expected.tenant_id, epoch: expected.epoch, scope_id: crypto.randomUUID(), name: '', enabled: true,
    rules: [{
      rule_id: crypto.randomUUID(), selector: emptySelector(expected.tenant_id),
      effect: 'include', workloads: ['powerbi', 'fabric_pipeline'], auto_enrol_detection_only: false,
    }],
    cadence: { poll_seconds: 300, reconciliation_seconds: 900 },
  }
}
function selectorComplete(value: ScopeSelector, catalog: MonitoringCatalog, workloads: MonitoringWorkload[]): boolean {
  if (value.kind === 'tenant') return true
  if (value.kind === 'domain') return catalog.domains.some((item) => item.domain_id === value.domain_id && item.state === 'present')
  if (!catalog.workspaces.some((item) => item.workspace_id === value.workspace_id && item.state === 'present')) return false
  return value.kind === 'workspace' || catalog.inventory.some((item) => item.workspace_id === value.workspace_id
    && item.item_id === value.item_id && item.state === 'present' && item.workload !== null && workloads.includes(item.workload))
}
function rejectedBeforeCommit(error: ApiError): boolean {
  return error.status >= 400 && error.status < 500 && error.status !== 408 && error.status !== 429
}

function GapList({ gaps, catalog }: { gaps: CoverageGap[]; catalog?: MonitoringCatalog }) {
  return gaps.length ? <ul className="monitoring-gaps">{gaps.map((gap, index) => <li key={`${gap.code}:${gap.workspace_id}:${gap.item_id}:${index}`}>
    <strong>{humanize(gap.code)}</strong><p>{gap.detail}</p>
    {gap.workspace_id && <p className="small">{catalog ? workspaceName(catalog, gap.workspace_id) : `Workspace ${gap.workspace_id}`}
      {gap.item_id && ` / ${catalog ? itemName(catalog, gap.workspace_id, gap.item_id) : `Item ${gap.item_id}`}`}</p>}
    {gap.retry_at && <p className="small">Retry after {date(gap.retry_at)}</p>}
  </li>)}</ul> : <p className="monitoring-help">No gaps reported by this endpoint. This is not independent proof of service access or delivery.</p>
}
function MonitoringTable<T>({
  items, columns, caption, row, empty,
}: { items: T[]; columns: string[]; caption: string; row: (item: T) => ReactNode; empty: string }) {
  const [page, setPage] = useState(0)
  const pageIndex = Math.min(page, Math.max(0, Math.ceil(items.length / 25) - 1))
  const start = pageIndex * 25
  return <>
    <div className="monitoring-table-scroll" role="region" aria-label={caption} tabIndex={0}>
      <table className="monitoring-table">
        <caption className="sr-only">{caption}</caption>
        <thead><tr>{columns.map((column) => <th key={column} scope="col">{column}</th>)}</tr></thead>
        <tbody>{items.length ? items.slice(start, start + 25).map(row) : <tr><td colSpan={columns.length}>{empty}</td></tr>}</tbody>
      </table>
    </div>
    {items.length > 25 && <div className="monitoring-pagination" aria-label={`${caption} pages`}>
      <span>{start + 1}-{Math.min(start + 25, items.length)} of {items.length} loaded records</span>
      <button type="button" className="button secondary" disabled={pageIndex === 0} onClick={() => setPage(pageIndex - 1)}>Previous</button>
      <button type="button" className="button secondary" disabled={start + 25 >= items.length} onClick={() => setPage(pageIndex + 1)}>Next</button>
    </div>}
  </>
}
function CoverageOverview({ snapshot, catalog }: { snapshot: MonitoringSnapshot; catalog: MonitoringCatalog | null }) {
  const id = useId()
  const value = snapshot.coverage
  let state: { label: string; tone: Tone } = { label: 'Coverage reported', tone: 'info' }
  if (value.inventory_completeness === 'blocked' || value.capability_completeness === 'blocked'
    || catalog?.connectors.some((item) => item.state === 'blocked')) state = { label: 'Blocked', tone: 'danger' }
  else if (catalog?.connectors.some((item) => item.state === 'planned' || item.state === 'provisioning')) state = { label: 'Configuring', tone: 'warning' }
  else if (value.inventory_completeness !== 'complete' || value.capability_completeness !== 'complete' || value.gaps.length
    || value.current_count < value.admitted_count || catalog?.connectors.some((item) => item.state === 'degraded')
    || (value.current_count > 0 && (!value.last_poll_window_end || !catalog?.connectors.some((item) => item.state === 'ready')))) {
    state = { label: 'Partial', tone: 'warning' }
  } else if (catalog && !catalog.scopes.some((scope) => scope.enabled)) state = { label: 'Not configured', tone: 'neutral' }
  return <section className="monitoring-panel" aria-labelledby={`${id}-coverage`}>
    <div className="panel-heading"><div><h2 id={`${id}-coverage`}>Monitoring coverage</h2>
      <p>Configuration, collection access, observation and action admission are separate records.</p></div>
      <Badge tone={state.tone}>{state.label}</Badge></div>
    <dl className="monitoring-counts">
      {([
        ['Discovered', value.discovered_count], ['Access verified', value.access_verified_count],
        ['Admitted', value.admitted_count], ['Current', value.current_count],
        ['Action enabled', value.action_enabled_count], ['Unsupported', value.unsupported_count],
      ] as const).map(([label, count]) => <div key={label}><dt>{label}</dt><dd>{count}</dd></div>)}
    </dl>
    <div className="monitoring-panel-body">
      <p><strong>Inventory total: {value.scope_item_count === null ? 'Unknown' : value.scope_item_count}</strong>.
        {' '}Inventory {humanize(value.inventory_completeness).toLowerCase()}; capability checks {humanize(value.capability_completeness).toLowerCase()}.
        {value.scope_item_count === null && ' Discovered items are not a complete tenant count.'}</p>
      <dl className="monitoring-facts">
        <div><dt>Coverage as of</dt><dd>{date(value.as_of)}</dd></div>
        <div><dt>Last completed inventory</dt><dd>{date(value.last_inventory_completed_at)}</dd></div>
        <div><dt>Last completed poll window</dt><dd>{date(value.last_poll_window_end)}</dd></div>
        <div><dt>Receiver last activity</dt><dd>{date(value.last_receiver_activity_at)}</dd></div>
        <div><dt>Checkpoint lag</dt><dd>{value.checkpoint_lag_seconds == null ? 'Not reported' : `${value.checkpoint_lag_seconds} seconds`}</dd></div>
        <div><dt>Work backlog</dt><dd>{value.backlog_count}</dd></div>
        <div><dt>Next due work</dt><dd>{date(value.next_due_at)}</dd></div>
      </dl>
      <GapList gaps={value.gaps} catalog={catalog ?? undefined} />
      <p className="monitoring-help">An enabled scope is not proof of current polling or event delivery. A receiver heartbeat alone
        does not prove failure detection. Action-enabled targets still require the controller&apos;s policy and any applicable approval.</p>
    </div>
  </section>
}
function RuleFields({ value, catalog, label, onChange }: {
  value: ScopeRule; catalog: MonitoringCatalog; label: string; onChange: (value: ScopeRule) => void
}) {
  const selector = value.selector
  const setSelector = (next: ScopeSelector) => onChange({ ...value, selector: next })
  const selectedItems = catalog.inventory.filter((item) => item.workspace_id === selector.workspace_id)
  return <div className="monitoring-rule-fields">
    <label>{label} scope
      <select value={selector.kind} onChange={(event) => {
        const kind = event.target.value
        if (kind === 'tenant' || kind === 'domain' || kind === 'workspace' || kind === 'item') setSelector(emptySelector(selector.tenant_id, kind))
      }}>
        <option value="tenant">Deployment tenant</option><option value="domain">Domain</option>
        <option value="workspace">Workspace</option><option value="item">Item</option>
      </select>
    </label>
    {selector.kind === 'tenant' && <p className="monitoring-help">All supported items in the pinned deployment tenant are candidates.
      An incomplete or caller-visible inventory is not complete tenant coverage.</p>}
    {selector.kind === 'domain' && <>
      <label>{label} domain<select value={selector.domain_id ?? ''} onChange={(event) => setSelector({ ...selector, domain_id: event.target.value || null })}>
        <option value="">Choose a domain</option>
        {catalog.domains.map((domain) => <option key={domain.domain_id} value={domain.domain_id} disabled={domain.state !== 'present'}>
          {domainName(catalog, domain.domain_id)} ({domain.domain_id.slice(0, 8)}){domain.state !== 'present' ? ` - ${humanize(domain.state)}` : ''}
        </option>)}
      </select></label>
      {!catalog.domains.length && <p className="monitoring-help">No domain metadata was returned. Queue inventory discovery and inspect its gaps; no domain membership is inferred.</p>}
      <label className="monitoring-check"><input type="checkbox" checked={selector.include_descendants}
        onChange={(event) => setSelector({ ...selector, include_descendants: event.target.checked })} />Include descendant domains for {label.toLowerCase()}</label>
    </>}
    {(selector.kind === 'workspace' || selector.kind === 'item') && <>
      <label>{label} workspace<select value={selector.workspace_id ?? ''} onChange={(event) => setSelector({ ...selector, workspace_id: event.target.value || null, item_id: null })}>
        <option value="">Choose a workspace</option>
        {catalog.workspaces.map((workspace) => <option key={workspace.workspace_id} value={workspace.workspace_id} disabled={workspace.state !== 'present'}>
          {workspace.name}{workspace.domain_id ? ` / ${domainName(catalog, workspace.domain_id)}` : ''} ({workspace.workspace_id.slice(0, 8)}){workspace.state !== 'present' ? ` - ${humanize(workspace.state)}` : ''}
        </option>)}
      </select></label>
      {!catalog.workspaces.length && <p className="monitoring-help">No workspace metadata was returned. Queue inventory discovery before choosing a workspace.</p>}
    </>}
    {selector.kind === 'item' && <label>{label} item<select value={selector.item_id ?? ''} disabled={!selector.workspace_id}
      onChange={(event) => setSelector({ ...selector, item_id: event.target.value || null })}>
      <option value="">Choose an item</option>
      {selectedItems.map((item) => <option key={inventoryKey(item)} value={item.item_id}
        disabled={item.state !== 'present' || item.workload === null || !value.workloads.includes(item.workload)}>
        {item.name} / {item.item_type} ({item.item_id.slice(0, 8)})
        {item.workload === null ? ` - Unsupported: ${item.unsupported_reason}` : !value.workloads.includes(item.workload) ? ' - Workload not selected' : item.state !== 'present' ? ` - ${humanize(item.state)}` : ''}
      </option>)}
    </select></label>}
    <fieldset className="monitoring-workloads"><legend>{label} workloads</legend>
      {supportedWorkloads.map((workload) => <label className="monitoring-check" key={workload.id}><input type="checkbox"
        checked={value.workloads.includes(workload.id)} onChange={(event) => {
          const next = event.target.checked ? [...value.workloads, workload.id] : value.workloads.filter((id) => id !== workload.id)
          const item = selectedItems.find((item) => item.item_id === selector.item_id)
          onChange({ ...value, workloads: next, selector: selector.kind === 'item' && item?.workload && !next.includes(item.workload)
            ? { ...selector, item_id: null } : selector })
        }} />{workload.label}</label>)}
    </fieldset>
    {value.effect === 'include' && <label className="monitoring-check"><input type="checkbox" checked={value.auto_enrol_detection_only}
      onChange={(event) => onChange({ ...value, auto_enrol_detection_only: event.target.checked })} />Automatically enroll future items for detection only</label>}
  </div>
}
function PreviewPanel({ plan, catalog, disabled, expired, onActivate }: {
  plan: MonitoringPlan; catalog: MonitoringCatalog; disabled: boolean; expired: boolean; onActivate: () => void
}) {
  const id = useId()
  const [clock, setClock] = useState(Date.now)
  const isExpired = expired || Date.parse(plan.expires_at) <= Math.max(clock, Date.now())
  useEffect(() => {
    const delay = Date.parse(plan.expires_at) - Date.now()
    if (delay <= 0) return
    const timer = setTimeout(() => setClock(Date.now()), Math.min(delay + 1, 2_000_000_000))
    return () => clearTimeout(timer)
  }, [plan.expires_at, clock])
  return <section className="monitoring-preview" aria-labelledby={`${id}-preview`}>
    <div className="panel-heading"><div><h3 id={`${id}-preview`}>Scope preview</h3>
      <p>Revision {plan.expected.revision}. Expires {date(plan.expires_at)}.</p></div>
      <Badge tone={isExpired || plan.status === 'blocked' ? 'danger' : 'info'}>{isExpired ? 'Expired' : plan.status === 'blocked' ? 'Blocked' : 'Ready to activate configuration'}</Badge></div>
    <div className="monitoring-panel-body">
      <p><strong>{plan.scope.name}</strong> / {plan.scope.enabled ? 'Enabled' : 'Paused'}.
        {' '}Inventory {humanize(plan.inventory_completeness).toLowerCase()} across {plan.inventory_generations.length} recorded generations.</p>
      <p>Poll count change: <strong>{plan.poll_count_delta > 0 ? '+' : ''}{plan.poll_count_delta}</strong>.
        {' '}Event subscription change: <strong>{plan.subscription_count_delta > 0 ? '+' : ''}{plan.subscription_count_delta}</strong>.</p>
      <h4>Required service permissions</h4>
      {plan.required_permissions.length ? <ul className="monitoring-list">{plan.required_permissions.map((permission) => <li key={permission}>{permission}</li>)}</ul>
        : <p>No additional permission requirements were reported. This does not establish verified service access.</p>}
      <p className="monitoring-help">These are collection/execution identity requirements, not the signed-in User&apos;s app roles. This page grants no permissions.</p>
      <GapList gaps={plan.gaps} catalog={catalog} />
    </div>
    <MonitoringTable items={plan.changes} columns={['Target', 'Change', 'Reason']} caption="Planned target changes"
      empty="The preview reports no target changes." row={(change) => <tr key={monitoringTargetKey(change.identity)}>
        <th scope="row">{itemName(catalog, change.identity.workspace_id, change.identity.item_id)}<span className="monitoring-cell-detail">{workspaceName(catalog, change.identity.workspace_id)} / {workloadName(change.identity.workload)}</span></th>
        <td>{humanize(change.change)}</td><td>{change.reason}<span className="monitoring-cell-detail">{humanize(change.basis)}</span></td>
      </tr>} />
    <div className="monitoring-panel-body monitoring-activation">
      <p>Existing application Readers can inspect admitted incident records. Activation may broaden that dataset.
        Monitoring activation does not approve a refresh or pipeline rerun. Newly admitted items start with detection-only authority.</p>
      <button type="button" className="button primary" disabled={disabled || isExpired || plan.status !== 'ready'} onClick={onActivate}>
        Activate current preview
      </button>
    </div>
  </section>
}
function ScopeEditor({ api, expected, catalog, allowed, pending, onActivate, onRefresh }: {
  api: MonitoringApiClient; expected: RegistryVersion; catalog: MonitoringCatalog; allowed: boolean; pending: boolean
  onActivate: (plan: MonitoringPlan) => void; onRefresh: () => void
}) {
  const id = useId()
  const [draft, setDraft] = useState(() => newScope(expected))
  const [editing, setEditing] = useState(false)
  const [plan, setPlan] = useState<MonitoringPlan | null>(null)
  const [error, setError] = useState<ApiError | null>(null)
  const [previewing, setPreviewing] = useState(false)
  const previewRead = useRef<AbortController | null>(null)
  const generation = useRef(0)
  const unlocked = allowed && !pending
  const savedScope = editing ? catalog.scopes.find((scope) => scope.scope_id === draft.scope_id) : undefined
  const pausingSavedRules = Boolean(savedScope && !draft.enabled
    && sameMonitoringContext(savedScope, draft) && monitoringDefinitionsMatch(savedScope.rules, draft.rules))
  const complete = Boolean(draft.name.trim()) && draft.name.trim().length <= 200
    && draft.rules.some((rule) => rule.effect === 'include')
    && draft.rules.every((rule) => rule.workloads.length > 0
      && (pausingSavedRules || selectorComplete(rule.selector, catalog, rule.workloads)))
    && [draft.cadence.poll_seconds, draft.cadence.reconciliation_seconds].every((seconds) => Number.isInteger(seconds) && seconds >= 15 && seconds <= 86400)
  const expired = Boolean(plan && Date.parse(plan.expires_at) <= Date.now())

  useEffect(() => () => { generation.current += 1; previewRead.current?.abort() }, [])

  function change(next: ScopeDefinition) {
    generation.current += 1
    previewRead.current?.abort()
    setPreviewing(false)
    setPlan(null)
    setError(null)
    setDraft(next)
  }
  function edit(scope: ScopePolicy) {
    change({
      tenant_id: scope.tenant_id, epoch: scope.epoch, scope_id: scope.scope_id, name: scope.name,
      enabled: scope.enabled, cadence: { ...scope.cadence },
      rules: scope.rules.map((rule) => ({ ...rule, selector: { ...rule.selector }, workloads: [...rule.workloads] })),
    })
    setEditing(true)
  }
  async function preview() {
    if (!unlocked || !complete || previewing) return
    const requestGeneration = ++generation.current
    previewRead.current?.abort()
    const controller = new AbortController()
    previewRead.current = controller
    setPreviewing(true)
    setPlan(null)
    setError(null)
    try {
      const result = await api.preview({ expected, idempotency_id: crypto.randomUUID(), scope: { ...draft, name: draft.name.trim() } }, controller.signal)
      if (!controller.signal.aborted && requestGeneration === generation.current) setPlan(result)
    } catch (caught) {
      if (!controller.signal.aborted && requestGeneration === generation.current && !isAborted(caught)) {
        const failure = asApiError(caught)
        setError(failure)
        if (failure.status === 409) onRefresh()
      }
    } finally {
      if (!controller.signal.aborted && requestGeneration === generation.current) setPreviewing(false)
    }
  }
  return <section className="monitoring-panel" aria-labelledby={`${id}-setup`}>
    <div className="panel-heading"><div><h2 id={`${id}-setup`}>Scope configuration</h2>
      <p>One positive include rule with explicit exclusions. Exclusions win over every inclusion.</p></div>
      <Badge tone="info">Admin setup</Badge></div>
    {catalog.scopes.length > 0 && <div className="monitoring-saved-scopes">
      {catalog.scopes.map((scope) => <div key={scope.scope_id}><span><strong>{scope.name}</strong> / revision {scope.revision}</span>
        <button type="button" className="button secondary" disabled={!unlocked || scope.rules.filter((rule) => rule.effect === 'include').length !== 1}
          onClick={() => edit(scope)}>Edit {scope.name}</button>
        {scope.rules.filter((rule) => rule.effect === 'include').length !== 1
          && <p className="monitoring-help">This form requires one positive include rule. This scope is shown read-only; no rules have been discarded.</p>}
      </div>)}
    </div>}
    <form className="monitoring-scope-form" onSubmit={(event) => { event.preventDefault(); void preview() }}>
      <fieldset disabled={!unlocked} className="monitoring-form-body">
        <legend>{editing ? 'Edit monitoring scope' : 'New monitoring scope'}</legend>
        <label>Scope name<input value={draft.name} maxLength={200} required onChange={(event) => change({ ...draft, name: event.target.value })} /></label>
        <label className="monitoring-check"><input type="checkbox" checked={draft.enabled}
          onChange={(event) => change({ ...draft, enabled: event.target.checked })} />Scope enabled</label>
        {pausingSavedRules && <p className="monitoring-help">This pauses the existing policy with its saved include/exclude rules unchanged.
          Fresh workspace or item metadata is not required to stop observation. New rules or expansion still require current metadata.</p>}
        {draft.rules.map((rule, index) => <fieldset className="monitoring-rule" key={rule.rule_id}>
          <legend>{rule.effect === 'include' ? 'Include' : `Exclusion ${index}`}</legend>
          <RuleFields value={rule} catalog={catalog} label={rule.effect === 'include' ? 'Include' : `Exclusion ${index}`}
            onChange={(next) => change({ ...draft, rules: draft.rules.map((entry) => entry.rule_id === next.rule_id ? next : entry) })} />
          {rule.effect === 'exclude' && <button type="button" className="text-button" onClick={() => change({ ...draft, rules: draft.rules.filter((entry) => entry.rule_id !== rule.rule_id) })}>
            <X size={14} aria-hidden="true" />Remove exclusion {index}
          </button>}
        </fieldset>)}
        <button type="button" className="button secondary" onClick={() => change({ ...draft, rules: [...draft.rules, {
          rule_id: crypto.randomUUID(), selector: emptySelector(expected.tenant_id), effect: 'exclude',
          workloads: ['powerbi', 'fabric_pipeline'], auto_enrol_detection_only: false,
        }] })}><Plus size={14} aria-hidden="true" />Add exclusion</button>
        <p className="monitoring-help">Future items require review unless detection-only enrollment is explicitly selected.
          A domain does not grant service access. Standalone notebooks and other unsupported types are never admitted by these rules.</p>
        <div className="monitoring-cadence">
          <label>Polling interval (seconds)<input type="number" min={15} max={86400} step={1} required value={draft.cadence.poll_seconds || ''}
            onChange={(event) => change({ ...draft, cadence: { ...draft.cadence, poll_seconds: Number(event.target.value) } })} /></label>
          <label>Event reconciliation interval (seconds)<input type="number" min={15} max={86400} step={1} required value={draft.cadence.reconciliation_seconds || ''}
            onChange={(event) => change({ ...draft, cadence: { ...draft.cadence, reconciliation_seconds: Number(event.target.value) } })} /></label>
        </div>
        <p className="monitoring-help">Intervals must be whole seconds from 15 to 86400. Event-enabled targets still require REST reconciliation.
          Pausing observation does not retract a workload action already committed to execution.</p>
        <div className="monitoring-form-actions">
          <button className="button primary" type="submit" disabled={!unlocked || !complete || previewing}>
            <Eye size={15} aria-hidden="true" />{previewing ? 'Preparing preview' : 'Preview scope changes'}
          </button>
          {editing && <button className="button secondary" type="button" onClick={() => { change(newScope(expected)); setEditing(false) }}>Start new scope</button>}
        </div>
        {!complete && <p className="monitoring-help">Provide a name, an available resource for each rule, at least one supported workload per rule, and valid intervals.</p>}
      </fieldset>
    </form>
    {error && <div className="monitoring-panel-body"><ErrorNotice error={error} /></div>}
    {plan && <PreviewPanel plan={plan} catalog={catalog} disabled={!unlocked || previewing} expired={expired}
      onActivate={() => {
        if (!unlocked || plan.status !== 'ready' || Date.parse(plan.expires_at) <= Date.now()) return
        setPlan(null)
        onActivate(plan)
      }} />}
  </section>
}
function TargetDetails({ target, catalog, onReview, reviewAllowed }: {
  target: MonitoringTarget; catalog: MonitoringCatalog; onReview?: () => void; reviewAllowed: boolean
}) {
  return <details className="monitoring-target-details"><summary>Inspect {target.name}</summary>
    <dl className="monitoring-facts">
      <div><dt>Item ID</dt><dd><code>{target.identity.item_id}</code></dd></div>
      <div><dt>Workspace</dt><dd>{workspaceName(catalog, target.identity.workspace_id)}<code className="block-code">{target.identity.workspace_id}</code></dd></div>
      <div><dt>Admission reason</dt><dd>{target.reason}</dd></div>
      <div><dt>Admission basis</dt><dd>{humanize(target.admission_basis)}</dd></div>
      <div><dt>Scope policies</dt><dd>{target.scope_ids.map((id) => catalog.scopes.find((scope) => scope.scope_id === id)?.name ?? id).join(', ')}</dd></div>
      <div><dt>Admitting rule IDs</dt><dd>{target.admitted_rule_ids.join(', ')}</dd></div>
      <div><dt>Inventory generation</dt><dd><code>{target.inventory_generation}</code></dd></div>
      <div><dt>Capability evidence ID</dt><dd><code>{target.capability_id}</code></dd></div>
      <div><dt>Policy revision</dt><dd>{target.policy_revision}</dd></div>
      <div><dt>Admission recorded</dt><dd>{date(target.admitted_at)}</dd></div>
      <div><dt>Next poll due</dt><dd>{date(target.next_poll_at)}</dd></div>
      <div><dt>Remediation review</dt><dd>{target.action.review_id
        ? <>{target.action.review_id} / revision {target.action.review_revision}</> : 'No review reference reported'}</dd></div>
    </dl>
    <p className="monitoring-help">A capability reference is not a fresh probe result. Per-target probe dates and completed poll windows are not
      returned by this endpoint; inspect aggregate coverage and gaps above. Remediation review is separate from monitoring setup and any required per-action approval.</p>
    {onReview && <button type="button" className="button secondary" onClick={onReview} disabled={!reviewAllowed}>
      Safety review for {target.name}
    </button>}
  </details>
}
function ConnectorPanel({ catalog }: { catalog: MonitoringCatalog }) {
  const id = useId()
  return <section className="monitoring-panel" aria-labelledby={`${id}-connectors`}>
    <div className="panel-heading"><div><h2 id={`${id}-connectors`}>Event connectors and proof</h2>
      <p>App-owned topology, managed-identity proof, delivery evidence and reported gaps.</p></div></div>
    <div className="monitoring-panel-body">
      <p className="monitoring-help">The Eventstream custom endpoint uses public outbound TLS with Entra authentication and a managed-identity consumer.
        No public inbound worker endpoint is required. Power BI polling remains required; event setup is not universal item monitoring.</p>
      {!catalog.connectors.length && <div className="notice tone-warning"><div><strong>Event layer not configured</strong>
        <p>No owned connector manifests were returned. Polling configuration alone is not verified hybrid coverage.</p></div></div>}
      {catalog.connectors.map((connector) => <section className="monitoring-connector" key={connector.connector_id} aria-label={connector.name}>
        <div className="split-line"><h3>{connector.name}</h3><Badge tone={connector.state === 'ready' ? 'success' : connector.state === 'blocked' ? 'danger' : 'warning'}>
          {connector.state === 'planned' || connector.state === 'provisioning' ? `Configuring / ${humanize(connector.state)}` : humanize(connector.state)}
        </Badge></div>
        <p>{connector.workspace_id ? workspaceName(catalog, connector.workspace_id) : 'Monitoring workspace identity not assigned'} / {connector.sources.length} owned source subscriptions</p>
        <dl className="monitoring-facts">
          <div><dt>Topology round trip</dt><dd>{connector.observed_definition == null ? 'Not observed'
            : monitoringDefinitionsMatch(connector.desired_definition, connector.observed_definition) ? 'Desired and observed definitions match' : 'Definition drift recorded'}</dd></div>
          <div><dt>Identity proof recorded</dt><dd>{date(connector.identity_verified_at)}</dd></div>
          <div><dt>Delivery proof recorded</dt><dd>{date(connector.delivery_verified_at)}</dd></div>
          <div><dt>Receiver last activity</dt><dd>{date(connector.last_receiver_activity_at)}</dd></div>
          <div><dt>Manifest updated</dt><dd>{date(connector.updated_at)}</dd></div>
          <div><dt>Endpoint namespace</dt><dd>{connector.endpoint?.namespace ?? 'Not reported'}</dd></div>
          <div><dt>Endpoint entity</dt><dd>{connector.endpoint?.entity ?? 'Not reported'}</dd></div>
          <div><dt>Consumer group</dt><dd>{connector.endpoint?.consumer_group ?? 'Not reported'}</dd></div>
        </dl>
        <details><summary>Topology identities and subscriptions</summary>
          <dl className="monitoring-facts">
            <div><dt>Connector ID</dt><dd><code>{connector.connector_id}</code></dd></div>
            <div><dt>Ownership ID</dt><dd><code>{connector.ownership_id}</code></dd></div>
            <div><dt>Eventstream ID</dt><dd><code>{connector.eventstream_id ?? 'Not assigned'}</code></dd></div>
            <div><dt>Destination ID</dt><dd><code>{connector.destination_id ?? 'Not assigned'}</code></dd></div>
            <div><dt>Provisioning operation</dt><dd>{connector.operation_id ?? 'Not reported'}</dd></div>
          </dl>
          <ul className="monitoring-list">{connector.sources.map((source) => <li key={source.source_id}>
            {itemName(catalog, source.target.workspace_id, source.target.item_id)} / {workspaceName(catalog, source.target.workspace_id)}
            <span className="monitoring-cell-detail">{source.event_types.join(', ')}</span>
          </li>)}</ul>
        </details>
        <GapList gaps={connector.gaps} catalog={catalog} />
      </section>)}
    </div>
  </section>
}

export function MonitoringWorkspace({ api, roles, userId, fresh, permissionRevision, active = true, onChanged }: MonitoringWorkspaceProps) {
  const id = useId()
  const [refreshRevision, setRefreshRevision] = useState(0)
  const [inventoryWorkspace, setInventoryWorkspace] = useState('')
  const [inventoryWorkload, setInventoryWorkload] = useState('all')
  const [safetySelection, setSafetySelection] = useState<SafetyReviewSelection | null>(null)
  const [safetyBusy, setSafetyBusy] = useState(false)
  const [recovery] = useState(activationRecovery)
  const [recoveryError, setRecoveryError] = useState<ApiError | null>(recovery.error)
  const [pending, setPending] = useState<PendingActivation | null>(recovery.pending)
  const pendingRef = useRef<PendingActivation | null>(recovery.pending)
  const [recoveredPlan, setRecoveredPlan] = useState<MonitoringPlan | null>(null)
  const recoveryRead = useRef<AbortController | null>(null)
  const [receipt, setReceipt] = useState<ActivationReceipt | null>(null)
  const [actionError, setActionError] = useState<ApiError | null>(null)
  const [reconciliationError, setReconciliationError] = useState<ApiError | null>(null)
  const [inventoryRequest, setInventoryRequest] = useState<InventoryRefreshRequest | null>(null)
  const inventoryRequestRef = useRef<InventoryRefreshRequest | null>(null)
  const [inventoryBusy, setInventoryBusy] = useState(false)
  const inventoryBusyRef = useRef(false)
  const [inventoryError, setInventoryError] = useState<ApiError | null>(null)
  const [discoveryNotice, setDiscoveryNotice] = useState<string | null>(null)
  const securityKey = `${permissionRevision}:${fresh ? 'current' : 'locked'}:${userId ?? ''}:${roles.join(',')}`
  const load = useCallback(async (signal: AbortSignal): Promise<{ bootstrap: MonitoringBootstrap; snapshot: MonitoringSnapshot | null }> => {
    const bootstrap = await api.bootstrap(signal)
    if (bootstrap.status !== 'ready') return { bootstrap, snapshot: null }
    const snapshot = await api.snapshot(signal)
    if (!bootstrap.control || !sameMonitoringVersion(bootstrap.control, snapshot.control) || snapshot.control.maintenance) {
      throw new ApiError(409, 'monitoring_context_changed', 'Monitoring control changed during this read. Refresh the deployment state.')
    }
    return { bootstrap, snapshot }
  }, [api])
  const records = useResource(`monitoring:${securityKey}:${refreshRevision}`, load, 15000, active)
  const current = records.data?.snapshot ?? null
  const tenantId = current?.control.tenant_id
  const epoch = current?.control.epoch
  const revision = current?.control.revision
  const expected = useMemo<RegistryVersion | null>(() => tenantId && epoch && revision !== undefined ? {
    tenant_id: tenantId, epoch, revision,
  } : null, [tenantId, epoch, revision])
  const loadCatalog = useCallback(async (signal: AbortSignal): Promise<MonitoringCatalog> => {
    if (!expected) throw new ApiError(409, 'monitoring_control_required', 'A current monitoring control record is required.')
    const batch = new AbortController()
    const batchSignal = AbortSignal.any([signal, batch.signal])
    try {
      const [scopes, inventory, targets, workspaces, domains, connectors] = await Promise.all([
        readMonitoringPages((cursor) => api.scopes({ limit: 100, cursor }, batchSignal), expected, batchSignal, (item) => item.scope_id),
        readMonitoringPages((cursor) => api.inventory({ limit: 100, cursor }, batchSignal), expected, batchSignal, inventoryKey),
        readMonitoringPages((cursor) => api.targets({ limit: 100, cursor, include_inactive: true }, batchSignal), expected, batchSignal, (item) => monitoringTargetKey(item.identity)),
        readMonitoringPages((cursor) => api.workspaces({ limit: 100, cursor }, batchSignal), expected, batchSignal, (item) => item.workspace_id),
        readMonitoringPages((cursor) => api.domains({ limit: 100, cursor }, batchSignal), expected, batchSignal, (item) => item.domain_id),
        readMonitoringPages((cursor) => api.connectors({ limit: 100, cursor }, batchSignal), expected, batchSignal, (item) => item.connector_id),
      ])
      return { scopes, inventory, targets, workspaces, domains, connectors }
    } catch (error) {
      // Promise.all rejects before sibling reads finish. Leaving their pages
      // running lets each failed poll add another estate-wide catalogue scan.
      batch.abort()
      throw error
    }
  }, [api, expected])
  const catalog = useResource(`monitoring-catalog:${securityKey}:${refreshRevision}:${expected ? contextKey(expected) : 'unavailable'}`,
    loadCatalog, 15000, active && Boolean(expected) && !records.error)
  const data = catalog.data
  // Same-generation background reads keep the last accepted snapshot usable.
  // Permission/context key changes hide that data immediately in useResource.
  const baseAdmin = Boolean(active && fresh && roles.includes('admin') && current?.can_admin && current.user.id === userId
    && expected && !current.control.maintenance && !records.error && data && !catalog.error && !recoveryError)
  const generations = data ? [...new Set([
    ...data.inventory.map((item) => item.generation_id), ...data.workspaces.map((item) => item.generation_id),
    ...data.domains.map((item) => item.generation_id),
  ])].sort().join(':') : ''
  const editorKey = `${securityKey}:${refreshRevision}:${expected ? contextKey(expected) : 'unavailable'}:${generations}`

  useEffect(() => {
    setRecoveredPlan(null)
    recoveryRead.current?.abort()
    return () => recoveryRead.current?.abort()
  }, [editorKey])

  function refresh() {
    setRefreshRevision((value) => value + 1)
  }
  function selectSafetyReview(identity: TargetIdentity) {
    const target = data?.targets.find((target) => monitoringTargetKey(target.identity) === monitoringTargetKey(identity))
    setSafetySelection({
      identity, name: target?.name ?? 'Previously reviewed target',
      workspaceName: data ? workspaceName(data, identity.workspace_id) : `Workspace ${identity.workspace_id}`,
    })
  }
  function rememberPending(value: PendingActivation | null) {
    if (value === null) {
      try {
        window.sessionStorage.removeItem(MONITORING_ACTIVATION_STORAGE_KEY)
        setRecoveryError(null)
      } catch {
        setRecoveryError(new ApiError(0, 'monitoring_recovery_not_cleared',
          'The activation was reconciled, but the browser could not clear its recovery pointer. Reload and check the saved receipt before submitting another change.'))
      }
      setRecoveredPlan(null)
    }
    pendingRef.current = value
    setPending(value)
  }
  function acceptReceipt(value: ActivationReceipt, submission: PendingActivation) {
    if (value.plan_id !== submission.planId || value.idempotency_id !== submission.request.idempotency_id
      || value.scope.scope_id !== submission.scopeId || !sameMonitoringContext(value.version, submission.request.expected)
      || value.version.revision <= submission.request.expected.revision) {
      throw new ApiError(200, 'unconfirmed_monitoring_activation', 'The activation receipt does not match this submission. It remains unresolved.')
    }
    rememberPending(null)
    setReceipt(value)
    setActionError(null)
    setReconciliationError(null)
    refresh()
    onChanged()
  }
  async function checkReceipt(submission: PendingActivation, originalError?: ApiError) {
    rememberPending({ ...submission, state: 'checking' })
    setReconciliationError(null)
    try {
      acceptReceipt(await api.activation(submission.request.idempotency_id), submission)
    } catch (caught) {
      const failure = asApiError(caught)
      if (failure.status === 404 && originalError && rejectedBeforeCommit(originalError)) {
        rememberPending(null)
        setActionError(originalError)
        refresh()
      } else {
        rememberPending({ ...submission, state: 'uncertain' })
        setReconciliationError(failure)
      }
    }
  }
  async function submitActivation(submission: PendingActivation) {
    rememberPending({ ...submission, state: 'submitting' })
    setActionError(null)
    setReconciliationError(null)
    try {
      acceptReceipt(await api.activate(submission.planId, submission.request), submission)
    } catch (caught) {
      const failure = asApiError(caught)
      setActionError(failure)
      await checkReceipt(submission, failure)
    }
  }
  async function loadOriginalPreview(submission: PendingActivation) {
    recoveryRead.current?.abort()
    const controller = new AbortController()
    recoveryRead.current = controller
    rememberPending({ ...submission, state: 'reviewing' })
    setReconciliationError(null)
    try {
      const plan = await api.plan(submission.planId, controller.signal)
      if (controller.signal.aborted) return
      if (plan.scope.scope_id !== submission.scopeId || !sameMonitoringVersion(plan.expected, submission.request.expected)) {
        throw new ApiError(409, 'monitoring_recovery_plan_mismatch', 'The saved activation pointer does not match the server plan. No activation was retried.')
      }
      setRecoveredPlan(plan)
    } catch (caught) {
      if (!controller.signal.aborted) setReconciliationError(asApiError(caught))
    } finally {
      rememberPending({ ...submission, state: 'uncertain' })
    }
  }
  function retryActivation() {
    const submission = pendingRef.current
    if (!baseAdmin || !submission || submission.state !== 'uncertain' || submission.userId !== userId
      || !expected || !sameMonitoringVersion(expected, submission.request.expected)
      || (submission.restored && (!recoveredPlan || recoveredPlan.status !== 'ready' || Date.parse(recoveredPlan.expires_at) <= Date.now()))) return
    void submitActivation(submission)
  }
  function activate(plan: MonitoringPlan) {
    if (!baseAdmin || safetyBusy || pendingRef.current || !expected || !userId || !sameMonitoringVersion(expected, plan.expected)
      || plan.status !== 'ready' || Date.parse(plan.expires_at) <= Date.now()) {
      setActionError(new ApiError(409, 'monitoring_preview_stale', 'The preview or current permissions changed. Refresh and preview again; nothing was submitted.'))
      return
    }
    setReceipt(null)
    const submission: PendingActivation = {
      planId: plan.plan_id, scopeId: plan.scope.scope_id, userId,
      request: { expected: plan.expected, idempotency_id: crypto.randomUUID() }, state: 'submitting',
    }
    try {
      // Only a receipt pointer is stored in the browser; server state remains authoritative.
      window.sessionStorage.setItem(MONITORING_ACTIVATION_STORAGE_KEY, JSON.stringify({
        schema_version: 1, planId: submission.planId, scopeId: submission.scopeId, userId,
        request: submission.request,
      }))
    } catch {
      setRecoveryError(new ApiError(0, 'monitoring_recovery_unavailable',
        'The browser could not preserve the activation submission ID for recovery. Nothing was submitted. Restore browser session storage and reload before activating.'))
      return
    }
    void submitActivation(submission)
  }
  async function refreshInventory() {
    if (!baseAdmin || safetyBusy || !expected || inventoryBusyRef.current || pendingRef.current) return
    const request = inventoryRequestRef.current ?? {
      expected, idempotency_id: crypto.randomUUID(),
      selector: inventoryWorkspace ? { ...emptySelector(expected.tenant_id), workspace_id: inventoryWorkspace }
        : emptySelector(expected.tenant_id, 'tenant'),
    }
    if (!sameMonitoringContext(request.expected, expected)) {
      setInventoryError(new ApiError(409, 'monitoring_discovery_context_changed', 'The unresolved inventory request belongs to another tenant or epoch. No new request was sent.'))
      return
    }
    inventoryRequestRef.current = request
    setInventoryRequest(request)
    inventoryBusyRef.current = true
    setInventoryBusy(true)
    setInventoryError(null)
    setDiscoveryNotice(null)
    try {
      const queued = await api.refreshInventory(request)
      inventoryRequestRef.current = null
      setInventoryRequest(null)
      setDiscoveryNotice(`Inventory discovery queued (${queued.work_id}). This is not a completed scan or verified coverage. Refresh the records after the worker completes discovery.`)
      refresh()
    } catch (caught) {
      const failure = asApiError(caught)
      setInventoryError(failure)
      if (rejectedBeforeCommit(failure)) {
        inventoryRequestRef.current = null
        setInventoryRequest(null)
        if (failure.status === 409) refresh()
      }
    } finally {
      inventoryBusyRef.current = false
      setInventoryBusy(false)
    }
  }
  const bootstrap = records.data?.bootstrap
  const stale = !fresh || Boolean(records.error) || Boolean(catalog.error)
  return <div className="monitoring-workspace">
    <header className="page-heading"><div><span className="eyebrow">Collection and coverage</span><h1>Monitoring setup</h1>
      <p>Discover supported resources, preview scope changes and inspect collection evidence.</p></div>
      <div className="page-actions"><button type="button" className="button secondary" disabled={records.loading || catalog.loading && Boolean(expected)}
        onClick={refresh}><RefreshCw size={15} aria-hidden="true" />Refresh monitoring records</button></div></header>
    <div className="notice tone-info"><ShieldCheck size={18} aria-hidden="true" /><div><strong>Monitoring does not authorize remediation</strong>
      <p>Only Admin can change monitoring configuration. Reader, Operator and Approver can inspect coverage.
        Collection uses the service identity, not the signed-in User&apos;s resource permissions. No grants or workload actions are submitted from this page.</p></div></div>
    {!fresh && <div className="notice tone-warning" role="status">Monitoring setup is locked until the current permission-refresh generation
      has a successful command-center snapshot. Previously reported Admin roles cannot unlock setup.</div>}
    {records.error && <ErrorNotice error={records.error} retry={refresh} />}
    {records.loading && !records.data && <LoadingState label="Loading monitoring deployment state" />}
    {bootstrap && bootstrap.status !== 'ready' && <section className="monitoring-panel" aria-label="Monitoring deployment blocked">
      <div className="panel-heading"><h2>{bootstrap.status === 'maintenance' ? 'Monitoring in maintenance'
        : bootstrap.status === 'missing' ? 'Monitoring is not initialized'
          : bootstrap.status === 'wrong_tenant' ? 'Wrong monitoring tenant' : 'Monitoring schema is incompatible'}</h2><Badge tone="danger">Blocked</Badge></div>
      <div className="monitoring-panel-body"><p>{bootstrap.detail}</p><p>No setup can be activated. Deployment bootstrap and prototype reset are operator procedures, not browser actions.
        No legacy targets or sample records have been substituted.</p></div>
    </section>}
    {current && <>
      <div className="monitoring-context"><span><strong>User:</strong> {current.user.display_name}</span>
        <span>App roles: {current.user.roles.map(humanize).join(', ')}</span>
        <details><summary>Deployment tenant and epoch</summary><dl className="monitoring-facts">
          <div><dt>Tenant</dt><dd><code>{current.control.tenant_id}</code></dd></div>
          <div><dt>Epoch</dt><dd><code>{current.control.epoch}</code></dd></div>
          <div><dt>Registry revision</dt><dd>{current.control.revision}</dd></div>
          <div><dt>Activation cutoff</dt><dd>{date(current.control.activation_cutoff)}</dd></div>
        </dl></details>
      </div>
      {stale && <p className="notice tone-warning" role="status">Monitoring records are stale or incomplete. Setup and activation are locked.</p>}
      <CoverageOverview snapshot={current} catalog={data} />
      {catalog.error && <ErrorNotice error={catalog.error} retry={refresh}><p>Resource metadata, target rows and setup could not be refreshed.
        Missing data is not an empty or complete inventory.</p></ErrorNotice>}
      {catalog.loading && !data && <LoadingState label="Loading all monitoring inventory and configuration pages" />}
    </>}
    {actionError && <ErrorNotice error={actionError} />}
    {recoveryError && <ErrorNotice error={recoveryError} />}
    {pending && <section className="notice tone-warning" aria-label="Activation submission">
      <Radar size={18} aria-hidden="true" /><div><strong>{pending.state === 'uncertain' ? 'Activation outcome is unconfirmed'
        : pending.state === 'checking' ? 'Checking activation receipt' : pending.state === 'reviewing' ? 'Loading original activation preview' : 'Submitting monitoring configuration'}</strong>
        <p>Submission ID <code>{pending.request.idempotency_id}</code>. No new submission ID will be generated until this request is reconciled.
          A lost response is not evidence of rollback.</p>
        {pending.restored && <p>The unresolved submission was restored from this browser session. Read its server receipt or load its original preview;
          no activation is retried automatically.</p>}
        {pending.userId !== userId && <p>This submission belongs to a different signed-in User. Only receipt inspection is available in this session.</p>}
        {reconciliationError && <p>{reconciliationError.status === 404 ? 'No receipt is available yet. This does not prove that activation failed.' : reconciliationError.message}</p>}
        {pending.state === 'uncertain' && <div className="monitoring-form-actions">
          <button type="button" className="button secondary" onClick={() => {
            const submission = pendingRef.current
            if (submission?.state === 'uncertain') void checkReceipt(submission)
          }}>Check activation receipt</button>
          {pending.restored ? <button type="button" className="button secondary" onClick={() => {
            const submission = pendingRef.current
            if (submission?.state === 'uncertain') void loadOriginalPreview(submission)
          }}>Load original preview</button> : <button type="button" className="button secondary"
            disabled={!baseAdmin || pending.userId !== userId || !expected || !sameMonitoringVersion(pending.request.expected, expected)}
            onClick={retryActivation}>Retry same activation</button>}
        </div>}
      </div>
    </section>}
    {pending?.restored && recoveredPlan && data && <PreviewPanel plan={recoveredPlan} catalog={data}
      disabled={!baseAdmin || pending.state !== 'uncertain' || pending.userId !== userId
        || !expected || !sameMonitoringVersion(pending.request.expected, expected)}
      expired={Date.parse(recoveredPlan.expires_at) <= Date.now()} onActivate={retryActivation} />}
    {receipt && <div className="notice tone-info" role="status"><div>
      <strong>{receipt.state === 'configuring' ? 'Configuring' : 'Configuration activated'}: {receipt.scope.name}</strong>
      <p>Configuration committed at revision {receipt.version.revision}; {receipt.queued_work_ids.length} work items queued.
        Receipt recorded {date(receipt.activated_at)}. Consult current coverage and connector proof before treating monitoring as ready.
        No remediation was approved by this activation.</p>
    </div></div>}
    {inventoryError && <ErrorNotice error={inventoryError} />}
    {inventoryRequest && !inventoryBusy && <p className="notice tone-warning" role="status">Inventory submission {inventoryRequest.idempotency_id} is unconfirmed.
      Retrying uses that same ID and original selector, not the current filter.</p>}
    {discoveryNotice && <p className="notice tone-info" role="status">{discoveryNotice}</p>}
    {current && expected && data && <>
      <section className="monitoring-panel" aria-labelledby={`${id}-scopes`}>
        <div className="panel-heading"><div><h2 id={`${id}-scopes`}>Configured scopes</h2><p>Policies describe intended monitoring, not verified readiness.</p></div></div>
        <MonitoringTable items={data.scopes} columns={['Scope', 'Rules', 'Cadence']} caption="Configured monitoring scopes"
          empty="No scope policies were returned for this registry revision." row={(scope) => <tr key={scope.scope_id}>
            <th scope="row">{scope.name}<span className="monitoring-cell-detail">{scope.enabled ? 'Enabled' : 'Paused'} / revision {scope.revision}</span></th>
            <td><ul className="monitoring-list">{scope.rules.map((rule) => <li key={rule.rule_id}>{humanize(rule.effect)}: {selectorName(rule.selector, data)}
              <span className="monitoring-cell-detail">{rule.workloads.map(workloadName).join(', ')}{rule.effect === 'include'
                ? rule.auto_enrol_detection_only ? ' / Future items: detection only' : ' / Future items: review required' : ''}</span></li>)}</ul></td>
            <td>{scope.cadence.poll_seconds}s polling / {scope.cadence.reconciliation_seconds}s reconciliation</td>
          </tr>} />
      </section>
      {roles.includes('admin') && current.can_admin ? <ScopeEditor key={editorKey} api={api} expected={expected} catalog={data}
        allowed={baseAdmin && !safetyBusy} pending={Boolean(pending)} onActivate={activate} onRefresh={refresh} />
        : <p className="monitoring-help">Monitoring configuration is read-only for your current app roles. Setup changes require Admin; they are not Operator or Approver actions.</p>}
      <section className="monitoring-panel" aria-labelledby={`${id}-inventory`}>
        <div className="panel-heading"><div><h2 id={`${id}-inventory`}>Discovered inventory</h2><p>Supported workloads are selectable. Unsupported items remain visible with their reasons.</p></div>
          {roles.includes('admin') && current.can_admin && <button type="button" className="button secondary" disabled={!baseAdmin || safetyBusy || inventoryBusy || Boolean(pending)}
            onClick={() => void refreshInventory()}><RefreshCw size={15} aria-hidden="true" />
            {inventoryBusy ? 'Queueing discovery' : inventoryRequest ? 'Retry same inventory request' : 'Queue inventory refresh'}</button>}</div>
        <div className="monitoring-filters">
          <label>Inventory workspace<select value={inventoryWorkspace} onChange={(event) => setInventoryWorkspace(event.target.value)}>
            <option value="">All returned workspaces</option>{data.workspaces.map((item) => <option key={item.workspace_id} value={item.workspace_id}>{item.name} ({item.workspace_id.slice(0, 8)})</option>)}
          </select></label>
          <label>Inventory workload<select value={inventoryWorkload} onChange={(event) => setInventoryWorkload(event.target.value)}>
            <option value="all">All workloads and unsupported items</option><option value="powerbi">Power BI and unsupported items</option>
            <option value="fabric_pipeline">Fabric pipelines and unsupported items</option>
          </select></label>
        </div>
        <p className="monitoring-help monitoring-inset">Inventory refresh queues discovery for the selected workspace, or the deployment tenant when all workspaces are selected.
          Names come from server inventory; no resource IDs need to be copied into setup. A filtered empty list does not prove complete discovery.</p>
        <MonitoringTable key={`${editorKey}:${inventoryWorkspace}:${inventoryWorkload}`} items={data.inventory.filter((item) => (!inventoryWorkspace || item.workspace_id === inventoryWorkspace)
          && (inventoryWorkload === 'all' || item.workload === null || item.workload === inventoryWorkload))}
          columns={['Item', 'Workspace', 'Support', 'Last observed']} caption="Discovered monitoring inventory"
          empty={current.coverage.inventory_completeness === 'complete' ? 'No returned inventory items match these filters.' : 'No matching items have been returned. Inventory coverage is incomplete; this is not proof of an empty scope.'}
          row={(item) => <tr key={inventoryKey(item)}><th scope="row">{item.name}<span className="monitoring-cell-detail">{item.item_type} / {humanize(item.state)}</span></th>
            <td>{workspaceName(data, item.workspace_id)}</td><td>{item.workload === null
              ? <><Badge tone="warning">Unsupported</Badge><p>{item.unsupported_reason}</p></> : workloadName(item.workload)}</td><td>{date(item.observed_at)}</td></tr>} />
      </section>
      <section className="monitoring-panel" aria-labelledby={`${id}-targets`}>
        <div className="panel-heading"><div><h2 id={`${id}-targets`}>Admitted targets</h2><p>Current, review-required, paused and removed records are shown separately by their recorded state.</p></div></div>
        <MonitoringTable items={data.targets} columns={['Target', 'Admission state', 'Observation policy', 'Action admission']} caption="Admitted monitoring targets"
          empty="No admitted target records were returned. Inspect inventory completeness and gaps before concluding that coverage is empty."
          row={(target) => <tr key={monitoringTargetKey(target.identity)}><th scope="row">{target.name}<span className="monitoring-cell-detail">
            {workspaceName(data, target.identity.workspace_id)} / {workloadName(target.identity.workload)}</span>
            <TargetDetails target={target} catalog={data}
              onReview={target.action.review_id || (roles.includes('admin') && current.can_admin) ? () => selectSafetyReview(target.identity) : undefined}
              reviewAllowed={target.action.review_id ? active && !safetyBusy : baseAdmin && !safetyBusy && !pending && !inventoryBusy} /></th>
            <td><Badge tone={target.state === 'current' ? 'info' : 'warning'}>{humanize(target.state)}</Badge><p>{target.reason}</p></td>
            <td>{target.observation.enabled ? 'Polling enabled' : 'Observation disabled'}<span className="monitoring-cell-detail">
              Events {target.observation.events_enabled ? 'enabled by policy' : 'not enabled'} / {target.observation.cadence.poll_seconds}s poll / {target.observation.cadence.reconciliation_seconds}s reconciliation</span></td>
            <td>{target.action.enabled ? <><Badge tone="warning">Policy gated</Badge><p>{humanize(target.action.action ?? '')}</p></> : 'Detection only / actions disabled'}</td>
          </tr>} />
      </section>
      <ConnectorPanel catalog={data} />
    </>}
    <MonitoringSafetyReview api={api} selection={safetySelection}
      target={data?.targets.find((target) => safetySelection && monitoringTargetKey(target.identity) === monitoringTargetKey(safetySelection.identity)) ?? null}
      inventory={data?.inventory.find((item) => safetySelection && item.workspace_id === safetySelection.identity.workspace_id
        && item.item_id === safetySelection.identity.item_id) ?? null}
      expected={expected} userId={userId} admin={Boolean(roles.includes('admin') && current?.can_admin)}
      allowed={baseAdmin && !pending && !inventoryBusy} permissionKey={securityKey} active={active}
      onChanged={() => { refresh(); onChanged() }} onSelect={selectSafetyReview}
      onClose={() => setSafetySelection(null)} onBusyChange={setSafetyBusy} />
  </div>
}
