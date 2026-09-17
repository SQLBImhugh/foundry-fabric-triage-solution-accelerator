import { useEffect, useId, useRef, useState } from 'react'
import { RefreshCw, ShieldCheck, X } from 'lucide-react'
import { ApiError, asApiError, isRecord } from '../api/errors'
import {
  actionMatchesWorkload, configurationFingerprint, isActionParameters, isMonitoringId,
  MONITORING_ACTIONS, monitoringTargetKey, parseSafetyParameters, reviewStateMatches, sameMonitoringContext,
} from '../api/monitoring'
import type {
  InventoryItem, MonitoringAction, MonitoringApiClient, MonitoringTarget, RegistryVersion,
  ReviewParameters, SafetyReview, SafetyReviewRequest, SafetyReviewState, TargetIdentity,
} from '../api/monitoring'
import { formatDate, humanize } from '../domain'
import { Badge, ErrorNotice, LoadingState } from './shared'

export const SAFETY_REVIEW_RECOVERY_KEY = 'command-center.monitoring.safety-review.v1'
const actionLabels: Record<MonitoringAction, string> = {
  powerbi_refresh: 'Power BI refresh',
  pipeline_rerun: 'Full pipeline rerun',
  rebind_dataset_gateway: 'Rebind dataset gateway',
  reenable_refresh_schedule: 'Re-enable refresh schedule',
}
interface ReviewPointer {
  request_id: string
  review_id: string
  binding: { targetKey: string; action: MonitoringAction; revision: number; policyRevision: number; state: SafetyReviewState }
}
interface PendingReview extends ReviewPointer {
  phase: 'saving' | 'checking' | 'uncertain'
}
export interface SafetyReviewSelection { identity: TargetIdentity; name: string; workspaceName: string }
export interface MonitoringSafetyReviewProps {
  api: MonitoringApiClient
  selection: SafetyReviewSelection | null
  target: MonitoringTarget | null
  inventory: InventoryItem | null
  expected: RegistryVersion | null
  userId: string | null
  admin: boolean
  allowed: boolean
  permissionKey: string
  active: boolean
  onChanged: () => void
  onSelect: (identity: TargetIdentity) => void
  onClose: () => void
  onBusyChange: (busy: boolean) => void
}
interface ReviewRecord {
  targetKey: string
  context: string
  review: SafetyReview | null
  loading: boolean
  error: ApiError | null
}
type ReviewProgress = Pick<SafetyReview, 'revision' | 'policy_revision' | 'publication_status'>
interface ReviewIntent {
  action: MonitoringAction
  state: SafetyReviewState
  expiresAt: string
  parameters: ReviewParameters | null
  preserveRedacted: boolean
  replaySafe: boolean
  detail: string
  replace: boolean
}

function readRecovery(): { pointer: PendingReview | null; error: ApiError | null } {
  try {
    const raw = window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)
    if (raw === null) return { pointer: null, error: null }
    const value: unknown = raw.length <= 1024 ? JSON.parse(raw) : null
    if (!isRecord(value) || Object.keys(value).length !== 3 || !isMonitoringId(value.request_id)
      || !isMonitoringId(value.review_id) || !isRecord(value.binding) || Object.keys(value.binding).length !== 5) {
      throw new Error('Invalid safety-review recovery pointer.')
    }
    const binding = value.binding
    const action = MONITORING_ACTIONS.find((action) => action === binding.action)
    const state = (['pending', 'verified', 'revoked', 'unverifiable'] as const).find((state) => state === binding.state)
    const parts = typeof binding.targetKey === 'string' ? binding.targetKey.split(':') : []
    if (!action || !state || typeof binding.targetKey !== 'string' || parts.length !== 5
      || !isMonitoringId(parts[0]) || !isMonitoringId(parts[1]) || !isMonitoringId(parts[3]) || !isMonitoringId(parts[4])
      || (parts[2] !== 'powerbi' && parts[2] !== 'fabric_pipeline') || !actionMatchesWorkload(action, parts[2])
      || typeof binding.revision !== 'number' || !Number.isSafeInteger(binding.revision) || binding.revision < 1
      || typeof binding.policyRevision !== 'number' || !Number.isSafeInteger(binding.policyRevision) || binding.policyRevision < 0) {
      throw new Error('Invalid safety-review submission binding.')
    }
    return { pointer: {
      request_id: value.request_id, review_id: value.review_id, phase: 'uncertain',
      binding: { targetKey: binding.targetKey, action, revision: binding.revision, policyRevision: binding.policyRevision, state },
    }, error: null }
  } catch {
    return { pointer: null, error: new ApiError(0, 'safety_review_recovery_unavailable',
      'The browser could not read the original safety-review submission binding. Review changes are locked. Do not discard the unresolved request; restore its session-storage binding before reloading.') }
  }
}
function localDate(value: string): string {
  const date = new Date(value)
  const part = (value: number) => String(value).padStart(2, '0')
  return `${date.getFullYear()}-${part(date.getMonth() + 1)}-${part(date.getDate())}T${part(date.getHours())}:${part(date.getMinutes())}`
}
function futureExpiry(value: string): string | null {
  const date = new Date(value)
  return Number.isFinite(date.getTime()) && date.getTime() > Date.now() && localDate(date.toISOString()) === value
    ? date.toISOString() : null
}
function time(value: string | null) {
  return value ? <time dateTime={value}>{formatDate(value)}</time> : 'Not reported'
}
function currentDefinitionHash(target: MonitoringTarget, inventory: InventoryItem | null): string | null {
  return inventory?.state === 'present' && inventory.generation_id === target.inventory_generation
    && inventory.tenant_id === target.identity.tenant_id && inventory.epoch === target.identity.epoch
    && inventory.workspace_id === target.identity.workspace_id && inventory.item_id === target.identity.item_id
    && inventory.workload === target.identity.workload ? inventory.definition_hash ?? null : null
}
const revisionKey = (review: SafetyReview) => `${monitoringTargetKey(review.target)}:${review.review_id}`
function compareReviewProgress(left: ReviewProgress, right: ReviewProgress): number {
  return left.revision - right.revision
    || Number(left.publication_status === 'published') - Number(right.publication_status === 'published')
    || left.policy_revision - right.policy_revision
}
function progressOf(review: SafetyReview): ReviewProgress {
  return { revision: review.revision, policy_revision: review.policy_revision, publication_status: review.publication_status }
}
function reviewBinding(review: SafetyReview, selected: TargetIdentity, action?: MonitoringAction | null, minimumRevision = 0) {
  if (monitoringTargetKey(review.target) !== monitoringTargetKey(selected) || (action && action !== review.action)) {
    throw new ApiError(409, 'safety_review_target_mismatch', 'The returned review belongs to another target or action. No review has been adopted.')
  }
  if (review.revision < minimumRevision) {
    throw new ApiError(409, 'stale_safety_review', 'The returned review is older than the latest recorded revision. It cannot replace the current review.')
  }
}
function definitiveRejection(error: ApiError): boolean {
  return error.status >= 400 && error.status < 500 && error.status !== 408 && error.status !== 429
}

function ReviewForm({ review, target, inventory, allowed, onSubmit }: {
  review: SafetyReview | null
  target: MonitoringTarget
  inventory: InventoryItem | null
  allowed: boolean
  onSubmit: (intent: ReviewIntent) => void
}) {
  const id = useId()
  const [replace, setReplace] = useState(false)
  const [action, setAction] = useState<MonitoringAction | ''>(review?.action ?? '')
  const [state, setState] = useState<SafetyReviewState>(review?.publication_status === 'pending_validation'
    ? review.requested_state ?? 'pending' : review?.state ?? 'pending')
  const [expiry, setExpiry] = useState(review ? localDate(review.expires_at) : '')
  const [parameters, setParameters] = useState(review?.parameters && !review.parameters_redacted
    ? JSON.stringify(review.parameters, null, 2) : '')
  const [gateway, setGateway] = useState(typeof review?.parameters?.gateway_id === 'string' ? review.parameters.gateway_id : '')
  const [datasources, setDatasources] = useState(Array.isArray(review?.parameters?.datasource_ids)
    ? review.parameters.datasource_ids.filter((value): value is string => typeof value === 'string').join('\n') : '')
  const [schedule, setSchedule] = useState(review?.parameters?.enabled === true)
  const [replaySafe, setReplaySafe] = useState(false)
  const [detail, setDetail] = useState('')
  const original = replace ? null : review
  const revoking = state === 'revoked' && original !== null
  const expiryInstant = revoking ? original.expires_at : futureExpiry(expiry)
  const currentDefinition = currentDefinitionHash(target, inventory)
  const preserveRedacted = Boolean(original?.parameters_redacted && state !== 'verified' && !replace
    && !parameters.trim() && !gateway.trim() && !datasources.trim() && !schedule)
  let parsed: ReviewParameters | null = null
  let parameterError = ''
  try {
    if (revoking) parsed = original.parameters
    else if (preserveRedacted) parsed = null
    else if (action === 'powerbi_refresh' || action === 'pipeline_rerun') parsed = parseSafetyParameters(action, parameters)
    else if (action === 'rebind_dataset_gateway') {
      parsed = {
        gateway_id: gateway.trim().toLowerCase(),
        datasource_ids: [...new Set(datasources.split(/[\s,]+/).filter(Boolean).map((id) => id.toLowerCase()))].sort(),
      }
      if (!isActionParameters(action, parsed)) parameterError = 'Enter a canonical gateway GUID and at least one valid datasource GUID.'
    } else if (action === 'reenable_refresh_schedule') {
      parsed = { enabled: true }
      if (!schedule) parameterError = 'Explicitly select the reviewed intent to enable the refresh schedule.'
    }
  } catch (caught) { parameterError = asApiError(caught).message }
  const pipelineBlocked = action === 'pipeline_rerun' && !revoking && !preserveRedacted
    && (!currentDefinition || parsed === null || (state === 'verified' && !replaySafe))
  const valid = Boolean(action && expiryInstant && detail.trim() && !parameterError && !pipelineBlocked
    && (state !== 'revoked' || original))

  function chooseAction(next: MonitoringAction | '') {
    setAction(next)
    setParameters('')
    setGateway('')
    setDatasources('')
    setSchedule(false)
    setReplaySafe(false)
  }
  return <form className="monitoring-safety-form" onSubmit={(event) => {
    event.preventDefault()
    if (!allowed || !valid || !action || !expiryInstant) return
    onSubmit({ action, state, expiresAt: expiryInstant, parameters: parsed, preserveRedacted, replaySafe, detail: detail.trim(), replace })
  }}>
    <fieldset className="monitoring-form-body" disabled={!allowed}>
      <legend>{original ? 'Revise the selected action profile' : 'Create one explicit action profile'}</legend>
      <label>Action profile<select value={action} disabled={Boolean(original)} onChange={(event) => {
        const value = event.target.value
        if (value === '' || MONITORING_ACTIONS.some((action) => action === value)) {
          chooseAction(MONITORING_ACTIONS.find((action) => action === value) ?? '')
        }
      }}>
        <option value="">Choose a supported action</option>
        {MONITORING_ACTIONS.filter((action) => actionMatchesWorkload(action, target.identity.workload))
          .map((action) => <option value={action} key={action}>{actionLabels[action]}</option>)}
      </select></label>
      {original && <button type="button" className="button secondary" onClick={() => {
        setReplace(true)
        chooseAction('')
        setState('pending')
        setExpiry('')
        setDetail('')
      }}>Create a different action profile</button>}
      {replace && <p className="notice tone-warning">This creates a new review identity for the same target and replaces its assigned action profile.
        No parameters, safety attestation or proof are copied. This cannot retract an action already committed to execution.</p>}
      <label>Requested review state<select value={state} onChange={(event) => {
        const value = event.target.value
        if (value === 'pending' || value === 'verified' || value === 'revoked' || value === 'unverifiable') setState(value)
      }}>
        <option value="pending">Pending - keep action disabled</option>
        <option value="verified">Request a verified action profile</option>
        <option value="unverifiable">Unverifiable - keep action disabled</option>
        <option value="revoked" disabled={!original}>Revoke this action profile</option>
      </select></label>
      <p className="monitoring-help">A verified-state request asks the server to evaluate this profile. It does not prove capability, correlation or definition safety.
        The returned review state and freshly loaded target admission determine what was accepted. Pending, unverifiable and revoked profiles do not enable actions.</p>
      {revoking ? <p className="monitoring-help">Revocation retains the reviewed intent and expiry, including an expired review.
        The server records acceptance separately from publication and supplies the published revocation timestamp.
        A committed revocation intent fences new actions; it does not cancel actions already reserved.</p>
        : <label>Review expiry (your local time)<input type="datetime-local" value={expiry}
          onChange={(event) => setExpiry(event.target.value)} required /></label>}
      {action === 'pipeline_rerun' && <div className="monitoring-safety-definition">
        <strong>Current inventory definition hash</strong><code className="block-code">{currentDefinition ?? 'Not available from current inventory'}</code>
        {!currentDefinition && <p className="monitoring-help">A new or verified pipeline profile cannot be submitted without the current definition hash.
          Inventory refresh and service verification must establish it; this form cannot supply a hash.</p>}
        {original?.definition_hash && currentDefinition && original.definition_hash !== currentDefinition
          && <p className="notice tone-warning">The pipeline definition changed since the recorded review. Review the current definition and attest again.</p>}
      </div>}
      {!revoking && <>
        {(action === 'pipeline_rerun' || action === 'powerbi_refresh') && <><label>Reviewed parameters (JSON)
          <textarea value={parameters} spellCheck={false} maxLength={65536} rows={5} aria-describedby={`${id}-parameters-help`}
            onChange={(event) => { setParameters(event.target.value); setReplaySafe(false) }} /></label>
          <p className="monitoring-help" id={`${id}-parameters-help`}>{action === 'pipeline_rerun'
            ? 'An explicit parameter object is required. Enter {} when the reviewed pipeline needs no parameters; blank or null is not an empty object.'
            : 'Optional JSON object. Blank or null means no reviewed refresh parameters.'}
            {' '}Do not enter credentials. Parameter text is never written to browser local or session storage.</p>
        </>}
        {action === 'rebind_dataset_gateway' && <>
          <label>Reviewed gateway ID<input value={gateway} spellCheck={false} autoComplete="off"
            onChange={(event) => setGateway(event.target.value)} /></label>
          <label>Reviewed datasource IDs<textarea value={datasources} spellCheck={false} rows={3}
            onChange={(event) => setDatasources(event.target.value)} /></label>
          <p className="monitoring-help">Enter the approved gateway and datasource GUIDs, not credentials. Datasource IDs may be comma-separated or one per line;
            they are lowercased, deduplicated and sorted before submission. The intent contains only gateway_id and datasource_ids.</p>
        </>}
        {action === 'reenable_refresh_schedule' && <label className="monitoring-check">
          <input type="checkbox" checked={schedule} onChange={(event) => setSchedule(event.target.checked)} />
          Reviewed intent: enable the refresh schedule
        </label>}
        {action === 'reenable_refresh_schedule' && <p className="monitoring-help">The reviewed intent is exactly {'{ "enabled": true }'}.
          This checkbox does not change the schedule or observation settings.</p>}
        {action === 'pipeline_rerun' && <label className="monitoring-check"><input type="checkbox" checked={replaySafe}
          onChange={(event) => setReplaySafe(event.target.checked)} />
          I reviewed these parameters and the current definition for full-pipeline replay safety
        </label>}
        {original?.parameters_redacted && <p className="notice tone-warning">The store removed replay parameter values. The recorded fingerprint is not executable input.
          Re-enter safe values for verified review; redaction placeholders are never used as parameters.</p>}
      </>}
      <label>Reason for this review change<textarea value={detail} required maxLength={4096} rows={3}
        onChange={(event) => setDetail(event.target.value)} /></label>
      {parameterError && <p className="monitoring-help">{parameterError}</p>}
      {!expiryInstant && <p className="monitoring-help">Choose an expiry in the future using your browser&apos;s local time.</p>}
      <p className="monitoring-help">This changes action admission only, not observation. It is not the individual human approval to execute a remediation.
        The controller must still apply its allowlist, policy, approval and durable reservation checks.</p>
      <button type="submit" className="button primary" disabled={!allowed || !valid}>{state === 'revoked' ? 'Revoke safety review' : 'Save safety review'}</button>
    </fieldset>
  </form>
}

export function MonitoringSafetyReview({
  api, selection, target, inventory, expected, userId, admin, allowed, permissionKey,
  active, onChanged, onSelect, onClose, onBusyChange,
}: MonitoringSafetyReviewProps) {
  const headingId = useId()
  const root = useRef<HTMLElement>(null)
  const [recovery] = useState(readRecovery)
  const [pending, setPending] = useState<PendingReview | null>(recovery.pointer)
  const pendingRef = useRef<PendingReview | null>(recovery.pointer)
  const [recoveryError, setRecoveryError] = useState<ApiError | null>(recovery.error)
  const [record, setRecord] = useState<ReviewRecord | null>(null)
  const [error, setError] = useState<ApiError | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [reload, setReload] = useState(0)
  const [formRevision, setFormRevision] = useState(0)
  const [computing, setComputing] = useState(false)
  const readController = useRef<AbortController | null>(null)
  const generation = useRef(0)
  const mounted = useRef(true)
  const saving = useRef(false)
  const highestRevision = useRef(new Map<string, ReviewProgress>())
  const writeLookupPin = useRef<{ targetKey: string; reviewId: string; revision: number; action: MonitoringAction } | null>(null)
  const targetKey = selection ? monitoringTargetKey(selection.identity) : ''
  const expectedKey = expected ? `${expected.tenant_id}:${expected.epoch}:${expected.revision}` : 'unavailable'
  const context = `${targetKey}:${permissionKey}:${expectedKey}`
  const liveContext = useRef(context)
  liveContext.current = context
  const assignedId = target?.action.review_id ?? null
  const assignedRevision = target?.action.review_revision ?? 0
  const assignedAction = target?.action.action ?? null
  const review = record?.targetKey === targetKey ? record.review : null
  const pendingBusy = Boolean(pending) || computing
  const recentWrite = writeLookupPin.current?.targetKey === targetKey ? writeLookupPin.current : null
  const pinnedProgress = recentWrite ? highestRevision.current.get(`${targetKey}:${recentWrite.reviewId}`) : undefined
  const newerAssignment = Boolean(recentWrite && assignedId && assignedId !== recentWrite.reviewId && assignedAction
    && target && expected && target.policy_revision === expected.revision
    && pinnedProgress && target.policy_revision > pinnedProgress.policy_revision)
  const lookupId = newerAssignment ? assignedId : recentWrite?.reviewId ?? assignedId
  const lookupAction = newerAssignment ? assignedAction : recentWrite?.action ?? assignedAction
  const minimumRevision = Math.max(newerAssignment ? 0 : recentWrite?.revision ?? 0, lookupId === assignedId ? assignedRevision : 0)
  const readKey = `${context}:${lookupId}:${assignedId}:${assignedRevision}:${reload}`
  const targetConfirmed = review ? assignedId === review.review_id && assignedRevision === review.revision : !assignedId && !recentWrite
  const recordFresh = record?.context === readKey && !record.error && !record.loading
  const canEdit = Boolean(active && allowed && admin && isMonitoringId(userId) && target?.state === 'current'
    && expected && selection && sameMonitoringContext(expected, selection.identity)
    && monitoringTargetKey(target.identity) === targetKey && target.policy_revision === expected.revision && recordFresh
    && targetConfirmed && (!review || review.publication_status === 'published')
    && !pending && !recoveryError && !saving.current)

  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false; generation.current += 1; readController.current?.abort() }
  }, [])
  useEffect(() => { onBusyChange(pendingBusy); return () => onBusyChange(false) }, [pendingBusy, onBusyChange])
  useEffect(() => {
    if (selection) root.current?.scrollIntoView?.({ block: 'nearest' })
  }, [targetKey, selection])
  useEffect(() => {
    setError(null)
    setNotice(null)
    if (writeLookupPin.current?.targetKey !== targetKey) writeLookupPin.current = null
  }, [targetKey])
  useEffect(() => {
    const readGeneration = ++generation.current
    readController.current?.abort()
    if (!active || !selection || !target || !expected || pendingRef.current || saving.current) return
    const controller = new AbortController()
    readController.current = controller
    if (!lookupId) {
      setRecord({ targetKey, context: readKey, review: null, loading: false, error: null })
      return () => controller.abort()
    }
    setRecord((previous) => ({ targetKey, context: readKey,
      review: previous?.targetKey === targetKey ? previous.review : null, loading: true, error: null }))
    void api.safetyReview(lookupId, controller.signal).then((value) => {
      if (controller.signal.aborted || readGeneration !== generation.current) return
      const observed = highestRevision.current.get(revisionKey(value))
      reviewBinding(value, selection.identity, lookupAction, Math.max(minimumRevision, observed?.revision ?? 0))
      if (observed && compareReviewProgress(value, observed) < 0) {
        throw new ApiError(409, 'stale_safety_review_publication',
          'The returned review publication is older than the latest observed state. It cannot replace the current review.')
      }
      const publishedAssignment = value.publication_status === 'published' && value.review_id === assignedId
        && value.revision === assignedRevision && value.policy_revision === expected.revision
        && target.policy_revision === expected.revision
      if (newerAssignment && !publishedAssignment) {
        throw new ApiError(409, 'safety_review_assignment_unconfirmed',
          'The newly assigned profile has not been confirmed as the current published review. The existing write lookup remains pinned.')
      }
      // This pin bridges publication lag, not the lifetime of the original receipt.
      // Never retire it using an old assignment or a merely accepted review.
      if (publishedAssignment) writeLookupPin.current = null
      if (newerAssignment) setNotice(null)
      highestRevision.current.set(revisionKey(value), progressOf(value))
      setRecord({ targetKey, context: readKey, review: value, loading: false, error: null })
    }).catch((caught: unknown) => {
      if (controller.signal.aborted || readGeneration !== generation.current) return
      const failure = asApiError(caught)
      setRecord((previous) => ({ targetKey, context: readKey,
        review: previous?.targetKey === targetKey ? previous.review : null, loading: false, error: failure }))
    })
    return () => controller.abort()
  }, [api, active, readKey, targetKey, selection, target, expected, lookupId, lookupAction, minimumRevision,
    assignedId, assignedRevision, newerAssignment])

  function remember(value: PendingReview | null) {
    pendingRef.current = value
    setPending(value)
  }
  function currentPending(): PendingReview | null { return pendingRef.current }
  function clearPointer(pointer: ReviewPointer) {
    try {
      const raw = window.sessionStorage.getItem(SAFETY_REVIEW_RECOVERY_KEY)
      const stored: unknown = raw === null ? null : JSON.parse(raw)
      if (isRecord(stored) && stored.request_id === pointer.request_id && stored.review_id === pointer.review_id) {
        window.sessionStorage.removeItem(SAFETY_REVIEW_RECOVERY_KEY)
      } else if (stored !== null) {
        throw new Error('A different unresolved safety-review submission is stored.')
      }
      setRecoveryError(null)
      remember(null)
    } catch {
      setRecoveryError(new ApiError(0, 'safety_review_recovery_not_cleared',
        'The server review was read, but its browser recovery IDs could not be cleared. Review changes remain locked; restore session storage and reload.'))
    }
  }
  function accept(value: SafetyReview, pointer: PendingReview, source: 'response' | 'operation') {
    if (monitoringTargetKey(value.target) !== pointer.binding.targetKey || value.action !== pointer.binding.action
      || value.revision !== pointer.binding.revision || value.policy_revision !== pointer.binding.policyRevision
      || !reviewStateMatches(pointer.binding.state, value)) {
      throw new ApiError(409, 'safety_review_unconfirmed', 'The operation result does not match the submitted target, action, state and review revision. The original request remains unresolved.')
    }
    if (value.review_id !== pointer.review_id) throw new ApiError(409, 'safety_review_unconfirmed', 'The service returned a different review ID.')
    const observed = highestRevision.current.get(revisionKey(value))
    if (!observed || compareReviewProgress(value, observed) > 0) highestRevision.current.set(revisionKey(value), progressOf(value))
    const key = monitoringTargetKey(value.target)
    const previous = writeLookupPin.current
    writeLookupPin.current = {
      targetKey: key, reviewId: value.review_id, action: value.action,
      revision: previous?.targetKey === key && previous.reviewId === value.review_id ? Math.max(previous.revision, value.revision) : value.revision,
    }
    // An operation receipt proves that write, not the latest review state.
    setRecord((current) => ({
      targetKey: key, context: `confirmed:${pointer.request_id}`, loading: false, error: null,
      review: current?.targetKey === key && current.review?.review_id === value.review_id
        && compareReviewProgress(current.review, value) > 0 ? current.review : value,
    }))
    clearPointer(pointer)
    setError(null)
    setNotice(value.publication_status === 'pending_validation'
      ? source === 'response'
        ? `Safety-review intent accepted: ${value.requested_state}, revision ${value.revision}. Technical validation and publication are pending; this is not a verified result or repair.`
        : `Original safety-review intent acceptance confirmed: ${value.requested_state}, revision ${value.revision}. This immutable receipt records pending validation, not the current publication result. Reload the current review before another change.`
      : source === 'response'
        ? `Safety review recorded as ${value.state}, revision ${value.revision}. Current target admission must be refreshed before another change.`
        : `Original safety-review operation confirmed: ${value.state}, revision ${value.revision}. The current review and target admission must be reloaded before another change.`)
    setReload((value) => value + 1)
    if (!selection) onSelect(value.target)
    onChanged()
  }
  async function reconcile(pointer: PendingReview, rejected?: ApiError) {
    const startedContext = liveContext.current
    const readGeneration = ++generation.current
    readController.current?.abort()
    const controller = new AbortController()
    readController.current = controller
    remember({ ...pointer, phase: 'checking' })
    setError(null)
    try {
      const receipt = await api.safetyReviewOperation(pointer.request_id, controller.signal)
      if (!mounted.current || controller.signal.aborted || readGeneration !== generation.current || liveContext.current !== startedContext) return
      if (receipt.request_id !== pointer.request_id) {
        throw new ApiError(409, 'safety_review_operation_mismatch', 'The receipt belongs to a different request. The original safety-review submission remains unresolved.')
      }
      const result = receipt.review
      if (expected && !sameMonitoringContext(result.target, expected)) {
        throw new ApiError(409, 'safety_review_context_mismatch', 'The review belongs to another tenant or epoch. No record was adopted.')
      }
      if (selection) reviewBinding(result, selection.identity, pointer.binding.action, pointer.binding.revision)
      accept(result, pointer, 'operation')
    } catch (caught) {
      if (!mounted.current || controller.signal.aborted || readGeneration !== generation.current || liveContext.current !== startedContext) return
      const failure = asApiError(caught)
      if (rejected && definitiveRejection(rejected) && failure.status === 404) {
        clearPointer(pointer)
        setError(rejected)
        setReload((value) => value + 1)
        onChanged()
      } else {
        remember({ ...pointer, phase: 'uncertain' })
        setError(failure)
      }
    } finally {
      if (mounted.current && pendingRef.current?.request_id === pointer.request_id && pendingRef.current.phase === 'checking') {
        remember({ ...pointer, phase: 'uncertain' })
      }
    }
  }
  async function submit(intent: ReviewIntent) {
    if (!canEdit || !selection || !target || !expected || !isMonitoringId(userId) || saving.current || currentPending()) return
    const startedContext = context
    const original = intent.replace ? null : review
    const reviewId = original?.review_id ?? crypto.randomUUID()
    const priorRevision = original?.revision ?? 0
    const revoking = intent.state === 'revoked' && original !== null
    const now = new Date().toISOString()
    saving.current = true
    setComputing(true)
    generation.current += 1
    readController.current?.abort()
    setError(null)
    setNotice(null)
    setFormRevision((value) => value + 1)
    const pointer: PendingReview = {
      request_id: crypto.randomUUID(), review_id: reviewId, phase: 'saving',
      binding: { targetKey, action: intent.action, revision: priorRevision + 1, policyRevision: expected.revision, state: intent.state },
    }
    let submitted = false
    try {
      const parameters = revoking ? original.parameters : intent.parameters
      const redacted = revoking ? original.parameters_redacted : intent.preserveRedacted
      const configurationHash = revoking || redacted ? original?.configuration_hash ?? null
        : (intent.action === 'rebind_dataset_gateway' || intent.action === 'reenable_refresh_schedule') && parameters
          ? await configurationFingerprint(intent.action, parameters) : null
      if (!mounted.current || liveContext.current !== startedContext) return
      const input: SafetyReviewRequest = {
        request_id: pointer.request_id, expected, expected_review_revision: priorRevision,
        review: {
          review_id: reviewId, target: selection.identity, revision: priorRevision + 1, policy_revision: expected.revision,
          action: intent.action, state: intent.state, reviewer_id: userId.toLowerCase(),
          reviewed_at: revoking ? original.reviewed_at : now, expires_at: intent.expiresAt, revoked_at: revoking ? now : null,
          definition_hash: revoking ? original.definition_hash : currentDefinitionHash(target, inventory),
          configuration_hash: configurationHash, parameters, parameter_hash: redacted ? original?.parameter_hash ?? null : null,
          parameters_redacted: redacted, replay_safe: revoking ? original.replay_safe : intent.replaySafe,
          // The full review DTO requires this flag for a verified-state job request.
          // It expresses the requested condition, never independent capability proof.
          exact_correlation_verified: revoking ? original.exact_correlation_verified
            : intent.state === 'verified' && (intent.action === 'powerbi_refresh' || intent.action === 'pipeline_rerun'),
          detail: intent.detail,
        },
      }
      window.sessionStorage.setItem(SAFETY_REVIEW_RECOVERY_KEY, JSON.stringify({
        request_id: pointer.request_id, review_id: pointer.review_id, binding: pointer.binding,
      }))
      remember(pointer)
      submitted = true
      const result = await api.recordSafetyReview(input)
      if (!mounted.current || liveContext.current !== startedContext) return
      accept(result, pointer, 'response')
    } catch (caught) {
      if (!mounted.current || liveContext.current !== startedContext) return
      const failure = asApiError(caught)
      if (submitted) {
        remember({ ...pointer, phase: 'uncertain' })
        setError(failure)
        await reconcile(pointer, failure)
      } else {
        setError(new ApiError(failure.status, failure.code === 'unexpected_error' ? 'safety_review_not_submitted' : failure.code,
          failure.code === 'unexpected_error' ? 'The browser could not preserve the safety-review IDs. Nothing was submitted; parameter text has not been saved.' : failure.message))
      }
    } finally {
      saving.current = false
      if (mounted.current) setComputing(false)
      if (mounted.current && pendingRef.current?.request_id === pointer.request_id && pendingRef.current.phase === 'saving') {
        remember({ ...pointer, phase: 'uncertain' })
      }
    }
  }

  if (!selection && !pending && !recoveryError) return null
  const errorForSelection = record?.targetKey === targetKey ? record.error : null
  const loadingForSelection = record?.targetKey === targetKey && record.loading
  const expired = review !== null && Date.parse(review.expires_at) <= Date.now()
  const awaitingPublication = review?.publication_status === 'pending_validation'
  const admissionCurrent = Boolean(!pendingBusy && !awaitingPublication && allowed && target?.state === 'current' && review && target.action.review_id === review.review_id
    && target.action.review_revision === review.revision && expected && review.policy_revision === expected.revision
    && target.policy_revision === expected.revision)
  return <section className="monitoring-panel monitoring-safety" ref={root} aria-labelledby={headingId}>
    <div className="panel-heading"><div><h2 id={headingId}>{selection ? `Safety review: ${selection.name}` : 'Safety-review submission recovery'}</h2>
      <p>{selection?.workspaceName} / Action admission, separate from observation and individual execution approval.</p></div>
      {selection && <button type="button" className="button secondary" disabled={pendingBusy || !active || !target || !expected}
        onClick={() => setReload((value) => value + 1)}><RefreshCw size={15} aria-hidden="true" />Refresh safety review</button>}
      {selection && <button type="button" className="icon-button" aria-label="Close safety review"
        disabled={pending?.phase === 'saving' || pending?.phase === 'checking'} onClick={onClose}><X size={17} /></button>}
    </div>
    <div className="monitoring-panel-body">
      <div className="notice tone-info"><ShieldCheck size={18} aria-hidden="true" /><div><strong>No remediation is executed here</strong>
        <p>This records an Admin&apos;s action profile for one target. Service identity, current definition and technical correlation/configuration proof remain server-owned.
          Configuration is not the human approval for an individual remediation.</p></div></div>
      {!allowed && <p className="notice tone-warning" role="status">Safety-review changes are locked until the current permission generation,
        monitoring records and target admission have been refreshed. Prior page snapshots cannot restore permission.</p>}
      {admin && userId && !isMonitoringId(userId) && <p className="notice tone-warning">The validated User ID is not a canonical GUID required by the safety-review contract.
        No replacement reviewer identity will be invented.</p>}
      {target && target.state !== 'current' && <p className="notice tone-warning">This target is {humanize(target.state).toLowerCase()}.
        The current store supports safety-review transitions, including revocation, only for currently admitted targets. No visual-only enable or revoke switch is available.</p>}
      {selection && !target && <p className="notice tone-warning">Current target admission is unavailable. Previously loaded review records are inspection-only.</p>}
      {target && expected && target.policy_revision !== expected.revision && <p className="notice tone-warning">
        The target policy revision is not current. Refresh target admission before changing its safety review.
      </p>}
      {recoveryError && <ErrorNotice error={recoveryError} />}
      {error && <ErrorNotice error={error} />}
      {errorForSelection && <ErrorNotice error={errorForSelection} retry={() => setReload((value) => value + 1)}>
        <p>The assigned review could not be read. A missing or stale record is not permission to create a replacement silently.</p>
      </ErrorNotice>}
      {pending && <div className="notice tone-warning" aria-label="Safety-review submission">
        <div><strong>{pending.phase === 'saving' ? 'Saving safety review' : pending.phase === 'checking' ? 'Checking safety-review operation' : 'Safety-review outcome is unconfirmed'}</strong>
          <p>Request <code>{pending.request_id}</code>; review <code>{pending.review_id}</code>.
            The original target, action, state and submitted revision binding are retained for browser recovery, never parameter text.
            A current-review GET, timeout or 404 does not confirm this operation or prove rollback.</p>
          {pending.phase === 'uncertain' && <button type="button" className="button secondary" disabled={!active}
            onClick={() => {
              const pointer = pendingRef.current
              if (pointer?.phase === 'uncertain') void reconcile(pointer)
            }}><RefreshCw size={15} aria-hidden="true" />Check safety-review operation</button>}
          <p>No blind POST retry is available. The receipt for the original request must be confirmed before a new revision can be submitted.</p>
        </div>
      </div>}
      {notice && <p className="notice tone-info" role="status">{notice}</p>}
      {loadingForSelection && <LoadingState label="Loading the assigned safety review" />}
      {review && <>
        <div className="split-line"><h3>Returned review record</h3>
          <Badge tone={!awaitingPublication && review.state === 'verified' && !expired ? 'info' : 'warning'}>
            {awaitingPublication ? 'Pending validation' : expired ? `Expired / ${humanize(review.state)}` : humanize(review.state)}
          </Badge></div>
        <dl className="monitoring-facts">
          <div><dt>Review ID / revision</dt><dd><code>{review.review_id}</code> / {review.revision}</dd></div>
          <div><dt>Requested state</dt><dd>{review.requested_state === null ? 'Not recorded' : humanize(review.requested_state)}</dd></div>
          <div><dt>Recorded state</dt><dd>{humanize(review.state)}</dd></div>
          <div><dt>Publication</dt><dd>{humanize(review.publication_status)}</dd></div>
          <div><dt>Action</dt><dd>{actionLabels[review.action]}</dd></div>
          <div><dt>Policy revision</dt><dd>{review.policy_revision}</dd></div>
          <div><dt>Reviewer User ID</dt><dd><code>{review.reviewer_id}</code></dd></div>
          <div><dt>Reviewed / expires</dt><dd>{time(review.reviewed_at)} / {time(review.expires_at)}</dd></div>
          <div><dt>Revoked</dt><dd>{time(review.revoked_at)}</dd></div>
          <div><dt>Definition fingerprint</dt><dd><code>{review.definition_hash ?? 'Not reported'}</code></dd></div>
          <div><dt>Configuration intent fingerprint</dt><dd><code>{review.configuration_hash ?? 'Not reported'}</code></dd></div>
          <div><dt>Parameter fingerprint</dt><dd><code>{review.parameter_hash ?? 'Not reported'}</code></dd></div>
          <div><dt>Replay attestation recorded</dt><dd>{review.replay_safe ? 'Yes' : 'No'}</dd></div>
          <div><dt>Correlation field in review</dt><dd>{review.exact_correlation_verified ? 'True' : 'False'} - not a live capability probe</dd></div>
          <div><dt>Reason</dt><dd>{review.detail}</dd></div>
        </dl>
        {awaitingPublication && <div className="notice tone-warning" role="status"><div>
          <strong>{review.requested_state === 'revoked' ? 'Revocation intent committed; publication pending' : 'Intent accepted; controller validation pending'}</strong>
          <p>{review.requested_state === 'revoked'
            ? 'The committed revocation intent fences new action reservations. It does not cancel actions already reserved; those retain their submission and verification obligations.'
            : 'Acceptance records the requested configuration, not technical proof or action admission. Wait for the controller to publish the validated current review.'}</p>
          <p>The original operation receipt stays an immutable acceptance record after publication. Refresh the current review to inspect the published result.</p>
        </div></div>}
        {review.parameters_redacted ? <p className="notice tone-warning">Reviewed parameters are unavailable after store redaction. This profile is not verified for replay.</p>
          : <details><summary>Returned reviewed parameters</summary><pre>{JSON.stringify(review.parameters, null, 2)}</pre></details>}
        <p className={`notice ${admissionCurrent && target?.action.enabled && review.state === 'verified' && !expired ? 'tone-info' : 'tone-warning'}`}>
          {awaitingPublication ? 'Action readiness is not established by this pending intent, even if an earlier target projection was action-enabled.'
            : !admissionCurrent ? 'Current action admission has not yet been confirmed against this returned review. Refreshing records is not proof of admission.'
            : !target?.action.enabled ? 'The current server target keeps this action disabled. A stored verified review alone does not establish technical readiness or action admission.'
              : review.state === 'verified' && !expired ? 'The current server target admits this action profile, subject to controller policy and any required individual approval.'
                : 'The target admission and returned review state disagree. No current action readiness has been inferred.'}
        </p>
      </>}
      {(!targetConfirmed || awaitingPublication) && <p className="monitoring-help">Review changes remain locked until publication and a fresh target snapshot reference the current review revision.</p>}
      {selection && target && record?.targetKey === targetKey && !loadingForSelection && !errorForSelection && admin
        && <ReviewForm key={`${context}:${review?.review_id ?? 'new'}:${review?.revision ?? 0}:${review?.publication_status ?? 'new'}:${review?.state ?? 'new'}:${review?.requested_state ?? 'none'}:${inventory?.definition_hash ?? 'unknown'}:${formRevision}`}
          review={review} target={target} inventory={inventory} allowed={canEdit} onSubmit={(intent) => void submit(intent)} />}
      {selection && !admin && <p className="monitoring-help">Safety-review records are read-only for your current app roles. Changing a profile requires Admin.</p>}
    </div>
  </section>
}
