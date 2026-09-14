import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { ArrowLeft, ArrowUpRight, CheckCircle2, FileText, MessageSquare, RefreshCw, Search, ShieldCheck } from 'lucide-react'
import { IncidentIntent, incidentQueryLimit, incidentQuestionLimit, incidentTextLimit } from '../api/incidents'
import type {
  IncidentActivity, IncidentApiClient, IncidentCase, IncidentListQuery,
  IncidentListStatus, IncidentResolutionInput,
} from '../api/incidents'
import { asApiError } from '../api/errors'
import type { ApiError } from '../api/errors'
import { duration, formatDate, humanize, workloadOptions } from '../domain'
import { useResource } from '../hooks/useResource'
import { Badge, EmptyState, ErrorNotice, Field, LoadingState, StatusBadge, StructuredData } from './shared'
import { Timeline } from './Timeline'
import { MarkdownText } from './MarkdownText'
import './IncidentWorkspace.css'

export interface IncidentWorkspaceProps {
  api: IncidentApiClient
  selectedId: string | null
  onSelect: (id: string) => void
  onBack: () => void
  onOpenRun: (id: string) => void
  fresh: boolean
  onChanged?: () => void
}

const statusOptions: { value: IncidentListStatus; label: string }[] = [
  { value: 'all', label: 'All statuses' },
  { value: 'open', label: 'Open incidents' },
  { value: 'needs_investigation', label: 'Needs investigation' },
  { value: 'investigating', label: 'Investigating' },
  { value: 'wont_fix', label: "Won't fix" },
  { value: 'resolved', label: 'All closed incidents' },
  { value: 'resolved_by_user', label: 'Resolved by user' },
]

type ListQuery = Required<IncidentListQuery>
type Mutation = 'note' | 'resolution' | 'discussion'
type ResolutionReview = Omit<IncidentResolutionInput, 'idempotency_key'>
const noErrors: Record<Mutation, ApiError | null> = { note: null, resolution: null, discussion: null }

export function IncidentWorkspace(props: IncidentWorkspaceProps) {
  const [query, setQuery] = useState<ListQuery>({ limit: 25, offset: 0, query: '', status: 'all', workload: 'all' })
  const [searchDraft, setSearchDraft] = useState('')
  if (props.selectedId !== null) {
    return <IncidentRecord key={props.selectedId} {...props} id={props.selectedId} />
  }
  return <IncidentIndex {...props} query={query} setQuery={setQuery} searchDraft={searchDraft} setSearchDraft={setSearchDraft} />
}

function IncidentIndex({ api, onSelect, query, setQuery, searchDraft, setSearchDraft }: IncidentWorkspaceProps & {
  query: ListQuery
  setQuery: (query: ListQuery) => void
  searchDraft: string
  setSearchDraft: (query: string) => void
}) {
  const load = useCallback((signal: AbortSignal) => api.list(query, signal), [api, query])
  const records = useResource(`incidents:${JSON.stringify(query)}`, load, 10_000)
  const data = records.data
  const workloads = workloadOptions([])

  function search(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setQuery({ ...query, query: searchDraft.trim(), offset: 0 })
  }

  return <section className="incident-workspace" aria-label="Incident directory">
    <header className="page-heading">
      <div><span className="eyebrow">Incident records</span><h1>Incidents</h1><p>Open a full incident record to review evidence, add notes, record a human resolution, or discuss it with the read-only agent.</p></div>
      <button type="button" className="button secondary" onClick={records.refresh} disabled={records.loading}><RefreshCw size={15} className={records.loading ? 'spin' : ''} aria-hidden="true" />Refresh incidents</button>
    </header>
    <section className="incident-panel" aria-label="Find incidents">
      <form className="incident-filters" onSubmit={search}>
        <label className="incident-search"><span>Search incidents</span><span className="search-field"><Search size={16} aria-hidden="true" /><input type="search" value={searchDraft} onChange={(event) => setSearchDraft(event.target.value)} placeholder="Title, target, identifier, or recorded text" maxLength={incidentQueryLimit} /></span></label>
        <label><span>Status</span><select value={query.status} onChange={(event) => {
          const option = statusOptions.find((entry) => entry.value === event.target.value)
          if (option) setQuery({ ...query, status: option.value, offset: 0 })
        }}>{statusOptions.map((entry) => <option key={entry.value} value={entry.value}>{entry.label}</option>)}</select></label>
        <label><span>Workload</span><select value={query.workload} onChange={(event) => setQuery({ ...query, workload: event.target.value, offset: 0 })}><option value="all">All workloads</option>{workloads.map((value) => <option key={value} value={value}>{humanize(value)}</option>)}</select></label>
        <button type="submit" className="button secondary">Search</button>
      </form>
      <div className="incident-catalog-caption"><span>Closed incidents remain readable and searchable.</span>{data && <span>{data.total.toLocaleString()} incident{data.total === 1 ? '' : 's'}</span>}</div>
      {records.error && <div className="incident-panel-body"><ErrorNotice error={records.error} retry={records.refresh} /></div>}
      {!data ? records.loading ? <LoadingState label="Loading incidents" /> : <EmptyState title="Incidents unavailable">The API could not return incident records. Refresh to try loading them again.</EmptyState>
        : data.items.length ? <ul className="incident-catalog" aria-label="Incident records">{data.items.map((item) =>
          <li key={item.id}><button type="button" className="incident-catalog-record" onClick={() => onSelect(item.source_id)} aria-label={`Open incident ${item.source_id}: ${item.title}`}>
            <span className="incident-catalog-summary"><strong>{item.title}</strong><span className="muted">{item.summary}</span><code>{item.source_id}</code></span>
            <span className="incident-catalog-target"><span>{item.target}</span><small>{humanize(item.workload)} / {item.agent}</small></span>
            <span className="incident-catalog-state"><StatusBadge status={item.status} /><time dateTime={item.updated_at}>{formatDate(item.updated_at)}</time></span>
            <ArrowUpRight size={17} aria-hidden="true" />
          </button></li>,
        )}</ul> : <EmptyState title="No incidents match">{query.offset > 0 ? 'No records remain on this page. Go to the previous page or change the filters.' : 'Try a different search or filter. No incident records have been substituted.'}</EmptyState>}
      <nav className="incident-pagination" aria-label="Incident pages">
        <span>{data && data.items.length > 0 ? `${data.offset + 1}-${data.offset + data.items.length} of ${data.total}` : data ? '0 records on this page' : 'Waiting for records'}</span>
        <div><button type="button" className="button secondary" disabled={records.loading || query.offset === 0} onClick={() => setQuery({ ...query, offset: Math.max(0, query.offset - query.limit) })}>Previous page</button>
          <button type="button" className="button secondary" disabled={records.loading || !data || query.offset + query.limit >= data.total} onClick={() => setQuery({ ...query, offset: query.offset + query.limit })}>Next page</button></div>
      </nav>
    </section>
  </section>
}

function IncidentRecord({ api, id, onBack, onOpenRun, fresh, onChanged }: IncidentWorkspaceProps & { id: string }) {
  const [mutation, setMutation] = useState<Mutation | null>(null)
  const [errors, setErrors] = useState(noErrors)
  const [feedback, setFeedback] = useState<{ kind: Mutation; text: string } | null>(null)
  const [note, setNote] = useState('')
  const [question, setQuestion] = useState('')
  const [reason, setReason] = useState('')
  const [review, setReview] = useState<ResolutionReview | null>(null)
  const [confirmed, setConfirmed] = useState(false)
  const [conflict, setConflict] = useState(false)
  const [refreshVersion, setRefreshVersion] = useState(0)
  const [receipt, setReceipt] = useState<{ version: number; value: IncidentCase } | null>(null)
  const receiptVersion = receipt?.version ?? 0
  const busy = useRef(false)
  const alive = useRef(false)
  const heading = useRef<HTMLHeadingElement>(null)
  const focused = useRef(false)
  const noteIntent = useRef(new IncidentIntent())
  const questionIntent = useRef(new IncidentIntent())
  const resolutionIntent = useRef(new IncidentIntent())

  useEffect(() => {
    alive.current = true
    return () => { alive.current = false }
  }, [])

  const load = useCallback(async (signal: AbortSignal) => ({
    value: await api.detail(id, signal), receiptVersion, refreshVersion,
  }), [api, id, receiptVersion, refreshVersion])
  const record = useResource(`incident:${id}`, load, 5000, mutation === null)
  // A GET started before a write cannot replace the confirmed write response.
  const data = receipt && (record.data?.receiptVersion ?? -1) < receipt.version
    ? receipt.value : record.data?.value
  useEffect(() => {
    if (data && !focused.current) {
      heading.current?.focus()
      focused.current = true
    }
  }, [data])
  const current = fresh && Boolean(data) && !record.error
    && record.data?.refreshVersion === refreshVersion && record.data?.receiptVersion === receiptVersion
  const ready = current && mutation === null
  const reviewChanged = Boolean(review && data && (review.expected_version !== data.tracking.version
    || review.source_revision !== data.tracking.source_revision || data.tracking.status !== 'open'))

  function refresh() {
    setRefreshVersion((value) => value + 1)
    setErrors(noErrors)
    setConflict(false)
    setReview(null)
    setConfirmed(false)
  }

  async function submit(kind: Mutation, request: () => Promise<IncidentCase>, saved: (value: IncidentCase) => void) {
    if (busy.current || !ready) return
    busy.current = true
    setMutation(kind)
    setErrors((previous) => ({ ...previous, [kind]: null }))
    setFeedback(null)
    try {
      const result = await request()
      if (!alive.current) return
      setReceipt((previous) => ({ version: (previous?.version ?? 0) + 1, value: result }))
      saved(result)
      onChanged?.()
    } catch (error) {
      if (!alive.current) return
      const failure = asApiError(error)
      setErrors((previous) => ({ ...previous, [kind]: failure }))
      // An observer error can still leave a durable pending or failed question.
      if (kind === 'discussion') setRefreshVersion((value) => value + 1)
      if (kind === 'resolution' && failure.status === 409) {
        setConflict(true)
        setReview(null)
        setConfirmed(false)
      }
    } finally {
      if (alive.current) {
        busy.current = false
        setMutation(null)
      }
    }
  }

  function saveNote(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!data?.capabilities.note) return
    const body = note.trim()
    void submit('note', () => api.addNote(id, body, noteIntent.current.keyFor(body)), () => {
      setNote((draft) => draft.trim() === body ? '' : draft)
      noteIntent.current.clear()
      setFeedback({ kind: 'note', text: 'Note saved to the incident record.' })
    })
  }

  function ask(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!data?.capabilities.ask) return
    const body = question.trim()
    void submit('discussion', () => api.discuss(id, body, questionIntent.current.keyFor(body)), () => {
      setQuestion((draft) => draft.trim() === body ? '' : draft)
      questionIntent.current.clear()
      setFeedback({ kind: 'discussion', text: 'Question saved. The recorded answer status is shown in the discussion.' })
    })
  }

  function reviewResolution(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!data?.capabilities.resolve || !ready || conflict || data.tracking.status !== 'open' || !reason.trim()) return
    setReview({ reason: reason.trim(), expected_version: data.tracking.version, source_revision: data.tracking.source_revision })
    setConfirmed(false)
  }

  function resolve() {
    if (!review || !confirmed || reviewChanged || conflict || !data?.capabilities.resolve) return
    const reviewed = review
    void submit('resolution', () => api.resolve(id, { ...reviewed, idempotency_key: resolutionIntent.current.keyFor(JSON.stringify(reviewed)) }), (result) => {
      setReason((draft) => draft.trim() === reviewed.reason ? '' : draft)
      resolutionIntent.current.clear()
      setReview(null)
      setConfirmed(false)
      setFeedback({
        kind: 'resolution',
        text: result.tracking.status === 'resolved_by_user'
          ? 'Human resolution recorded. This does not verify a repair.'
          : `Human resolution decision saved to history. Current tracking is ${humanize(result.tracking.status).toLowerCase()}. This does not verify a repair.`,
      })
    })
  }

  return <article className="incident-workspace incident-record" aria-label="Full incident record">
    <div className="incident-record-navigation"><button type="button" className="text-button" onClick={onBack}><ArrowLeft size={16} aria-hidden="true" />Back to incident list</button>
      <button type="button" className="button secondary" onClick={refresh} disabled={mutation !== null}><RefreshCw size={15} className={record.loading ? 'spin' : ''} aria-hidden="true" />Refresh incident</button></div>
    {record.error && <ErrorNotice error={record.error} retry={refresh} />}
    {!data ? record.loading ? <LoadingState label="Loading full incident record" /> : <EmptyState title="Incident unavailable">The selected record could not be loaded. It may have moved or become inaccessible.</EmptyState> : <>
      <header className="incident-record-heading">
        <div><span className="eyebrow">Incident / <code>{id}</code></span><h1 ref={heading} tabIndex={-1}>{data.detail.item.title}</h1><p>{data.detail.item.summary}</p></div>
        <div className="incident-record-badges"><StatusBadge status={data.tracking.status === 'open' ? data.detail.item.status : data.tracking.status} /><Badge tone={data.detail.item.severity === 'high' ? 'danger' : data.detail.item.severity === 'medium' ? 'warning' : 'neutral'}>{humanize(data.detail.item.severity)} severity</Badge></div>
      </header>
      {!current && <div className="notice tone-warning" role="status"><div className="notice-body"><strong>Updates are temporarily unavailable</strong><p>Wait for fresh records or refresh the incident. Existing notes and drafts remain visible; no submission is retried automatically.</p></div></div>}
      <dl className="incident-summary">
        <Field label="Target">{data.detail.item.target}</Field>
        <Field label="Workload">{humanize(data.detail.item.workload)}</Field>
        <Field label="Agent">{data.detail.item.agent}</Field>
        <Field label="Tracking status">{humanize(data.tracking.status)}</Field>
        <Field label="First recorded">{formatDate(data.detail.item.created_at)}</Field>
        <Field label="Record updated">{formatDate(data.detail.item.updated_at)}</Field>
      </dl>
      {data.tracking.status === 'resolved_by_user' && <div className="notice tone-info"><CheckCircle2 size={18} aria-hidden="true" /><div className="notice-body"><strong>Resolved by user</strong><p className="preserve-lines">{data.tracking.resolution_note}</p><p>{data.tracking.resolved_by} / {formatDate(data.tracking.resolved_at)}</p><p>This human tracking decision does not verify a repair or authorize remediation. New controller evidence can reopen the incident.</p></div></div>}
      <nav className="incident-section-links" aria-label="Incident sections"><a href="#incident-evidence">Evidence</a><a href="#incident-executions">Execution history</a><a href="#incident-notes">Notes</a><a href="#incident-discussion">Discussion</a><a href="#incident-resolution">Human resolution</a></nav>
      <div className="incident-record-grid">
        <section className="incident-panel" aria-labelledby="incident-evidence">
          <div className="incident-section-heading"><h2 id="incident-evidence"><ShieldCheck size={18} aria-hidden="true" />Evidence and summary</h2><p>Controller records are authoritative. Model output and human tracking decisions do not override deterministic findings.</p></div>
          <div className="incident-panel-body">
            {data.detail.evidence.length ? <dl className="incident-evidence">{data.detail.evidence.map((entry, index) => <Field key={`${entry.label}:${index}`} label={entry.label}><span className="preserve-lines">{entry.value}</span></Field>)}</dl> : <p className="muted small">No deterministic evidence has been recorded.</p>}
            {data.detail.notes && <div className="incident-controller-notes"><h3>Controller notes</h3><p className="preserve-lines">{data.detail.notes}</p></div>}
            {data.detail.proposal && <details className="raw-details"><summary>Recorded proposal (read-only)</summary><p className="preserve-lines">{data.detail.proposal.justification}</p><StructuredData data={data.detail.proposal.arguments} label="Recorded proposal arguments" /><p className="small muted">This record does not approve or dispatch the proposed action.</p></details>}
            {data.detail.incident && <details className="raw-details"><summary>Structured incident record</summary><StructuredData data={data.detail.incident} label="Structured incident record" /></details>}
          </div>
        </section>
        <section className="incident-panel" aria-labelledby="incident-executions">
          <div className="incident-section-heading"><h2 id="incident-executions">Execution history</h2><p>History can include earlier incidents with the same failure signature. Execution state and incident outcome are separate; a completed run is not proof of resolution.</p></div>
          <div className="incident-panel-body">
            {data.detail.runs.length ? <ul className="incident-runs" aria-label="Incident executions">{data.detail.runs.map((run, index) => <li key={run.id}>
              <div className="incident-run-heading"><button type="button" className="text-button" onClick={() => onOpenRun(run.id)} aria-label={`Open run ${run.id}`}><strong>{run.agent_name || 'Controller run'}</strong><ArrowUpRight size={14} aria-hidden="true" /></button><time dateTime={run.started_at}>{formatDate(run.started_at)}</time></div>
              <code className="incident-run-id">Run {run.id}</code>
              <dl className="incident-run-facts"><Field label="Execution"><StatusBadge status={run.state} /></Field><Field label="Outcome">{run.outcome ? <StatusBadge status={run.outcome} /> : 'No terminal outcome'}</Field><Field label="Duration">{duration(run.duration_ms)}</Field><Field label="Recorded writes">{run.write_actions}</Field></dl>
              <RunExplanation text={run.summary} initiallyOpen={index === 0} />
            </li>)}</ul> : <p className="muted small">No executions are linked to this incident.</p>}
            <details className="raw-details"><summary>Controller event timeline ({data.detail.timeline.length})</summary><Timeline events={data.detail.timeline} /></details>
          </div>
        </section>
        <section className="incident-panel" aria-labelledby="incident-notes">
          <div className="incident-section-heading"><h2 id="incident-notes"><FileText size={18} aria-hidden="true" />Notes</h2><p>Notes are append-only and retained with the incident. They do not change controller evidence.</p></div>
          <div className="incident-panel-body">
            <ActivityEntries entries={data.activity.filter((entry) => entry.kind === 'note')} label="Incident notes" empty="No human notes have been recorded." />
            {errors.note && <ErrorNotice error={errors.note} retry={refresh} />}
            {data.capabilities.note ? <form className="incident-compose" onSubmit={saveNote}>
              <label htmlFor="incident-note-draft">Add a note</label><textarea id="incident-note-draft" value={note} onChange={(event) => setNote(event.target.value)} maxLength={incidentTextLimit} required rows={3} aria-describedby="incident-note-help" />
              <p className="small muted" id="incident-note-help">Record findings and context. Do not include credentials or sensitive personal data.</p>
              <button type="submit" className="button primary" disabled={!ready || !note.trim()}>{mutation === 'note' ? 'Saving note...' : 'Save note'}</button>
            </form> : <p className="incident-readonly">Read-only access: your current role cannot add incident notes.</p>}
            {feedback?.kind === 'note' && <p className="incident-feedback" role="status">{feedback.text}</p>}
          </div>
        </section>
        <section className="incident-panel" aria-labelledby="incident-discussion">
          <div className="incident-section-heading"><h2 id="incident-discussion"><MessageSquare size={18} aria-hidden="true" />Discuss with the read-only agent</h2><p>Questions and answers are saved with this incident. Asking does not dispatch triage, authorize remediation, or resolve the incident.</p></div>
          <div className="incident-panel-body">
            <ActivityEntries entries={data.activity.filter((entry) => entry.kind === 'question' || entry.kind === 'answer')} label="Incident discussion" empty="No discussion has been recorded." />
            {errors.discussion && <ErrorNotice error={errors.discussion} retry={refresh} />}
            {data.capabilities.ask ? <form className="incident-compose" onSubmit={ask}>
              <label htmlFor="incident-question-draft">Question about this incident</label><textarea id="incident-question-draft" value={question} onChange={(event) => setQuestion(event.target.value)} maxLength={incidentQuestionLimit} required rows={3} />
              <button type="submit" className="button primary" disabled={!ready || !question.trim()}>{mutation === 'discussion' ? 'Saving question...' : 'Ask read-only agent'}</button>
            </form> : <p className="incident-readonly">Read-only access: discussion history is available, but your current role cannot ask new questions.</p>}
            {feedback?.kind === 'discussion' && <p className="incident-feedback" role="status">{feedback.text}</p>}
          </div>
        </section>
        <section className="incident-panel incident-resolution" aria-labelledby="incident-resolution">
          <div className="incident-section-heading"><h2 id="incident-resolution">Human resolution</h2><p>Record an explicit human decision to close incident tracking. This does not verify a repair, approve an action, or authorize remediation. New controller evidence can reopen the incident.</p></div>
          <div className="incident-panel-body">
            <ActivityEntries entries={data.activity.filter((entry) => entry.kind === 'resolution')} label="Human resolution history" empty="No human resolution has been recorded." />
            {errors.resolution && <ErrorNotice error={errors.resolution} retry={refresh} />}
            {conflict && <div className="notice tone-warning"><p>The incident changed or the submission conflicted with another record. Refresh the incident, review its current evidence, and confirm again. Your draft has been retained.</p></div>}
            {data.capabilities.resolve && data.tracking.status === 'open' ? <form className="incident-compose" onSubmit={reviewResolution}>
              <label htmlFor="incident-resolution-reason">Resolution reason</label><textarea id="incident-resolution-reason" value={reason} onChange={(event) => { setReason(event.target.value); setReview(null); setConfirmed(false) }} maxLength={incidentTextLimit} required rows={3} />
              <button type="submit" className="button secondary" disabled={!ready || conflict || !reason.trim()}>Review human resolution</button>
              {review && <div className="incident-confirmation" role="group" aria-labelledby="incident-resolution-confirmation">
                <h3 id="incident-resolution-confirmation">Confirm resolved by user</h3><p className="preserve-lines">{review.reason}</p>
                {reviewChanged && <p className="notice tone-warning">Evidence or tracking changed after this review. Refresh and review the current incident before confirming.</p>}
                <label className="incident-confirm-checkbox"><input type="checkbox" checked={confirmed} onChange={(event) => setConfirmed(event.target.checked)} disabled={!ready || reviewChanged} /><span>I am closing human incident tracking only. I understand that this does not verify a repair or authorize remediation.</span></label>
                <div className="incident-confirm-actions"><button type="button" className="button primary" onClick={resolve} disabled={!ready || conflict || !confirmed || reviewChanged}>{mutation === 'resolution' ? 'Recording resolution...' : 'Confirm resolved by user'}</button><button type="button" className="button secondary" disabled={mutation !== null} onClick={() => { setReview(null); setConfirmed(false) }}>Cancel</button></div>
              </div>}
            </form> : <p className="incident-readonly">{data.tracking.status !== 'open' ? 'Incident tracking is closed. Its evidence, notes, and discussion remain readable.' : 'Read-only access: your current role cannot record a human resolution.'}</p>}
            {feedback?.kind === 'resolution' && <p className="incident-feedback" role="status">{feedback.text}</p>}
          </div>
        </section>
      </div>
    </>}
  </article>
}

function RunExplanation({ text, initiallyOpen }: { text: string; initiallyOpen: boolean }) {
  const [open, setOpen] = useState(initiallyOpen)
  return <details className="incident-run-explanation" open={open} onToggle={(event) => setOpen(event.currentTarget.open)}>
    <summary>Run explanation</summary><MarkdownText text={text} />
  </details>
}

function ActivityEntries({ entries, label, empty }: { entries: IncidentActivity[]; label: string; empty: string }) {
  if (!entries.length) return <p className="incident-inline-empty">{empty}</p>
  return <ol className="incident-activity" aria-label={label} aria-live="polite" aria-relevant="additions text">{[...entries].sort((left, right) => Date.parse(left.created_at) - Date.parse(right.created_at)).map((entry) => <li key={entry.id}>
    <div className="incident-activity-meta"><strong>{humanize(entry.kind)}</strong><span>{entry.user_name || entry.user_id || (entry.kind === 'answer' ? 'Read-only agent' : 'Identity not recorded')}</span><time dateTime={entry.created_at}>{formatDate(entry.created_at)}</time><Badge tone={entry.status === 'failed' ? 'danger' : entry.status === 'pending' ? 'warning' : 'neutral'}>{humanize(entry.status)}</Badge></div>
    {entry.kind === 'answer' && entry.mode !== null && <p className="incident-answer-mode">{entry.mode === 'model' ? 'Model-generated answer' : 'Records-based answer'}</p>}
    {entry.body && (entry.kind === 'answer' ? <MarkdownText text={entry.body} /> : <p className="preserve-lines">{entry.body}</p>)}
    {entry.status === 'pending' && <p className="small muted">An answer is pending. Refreshing reads the saved discussion; it does not submit another question.</p>}
    {entry.status === 'failed' && <p className="small muted">This discussion entry failed. No answer or action has been inferred, and no automatic retry has been sent.</p>}
  </li>)}</ol>
}
