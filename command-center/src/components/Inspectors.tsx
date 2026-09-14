import { useCallback, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'
import { Activity, ArrowUpRight, FileSearch, LockKeyhole, RefreshCw, ShieldCheck } from 'lucide-react'
import type { ApiClient } from '../api/client'
import type { Snapshot, WorkSelection } from '../api/types'
import { duration, effectiveStatus, formatDate, humanize } from '../domain'
import { useResource } from '../hooks/useResource'
import { AskPanel } from './AskPanel'
import { DecisionPanel } from './DecisionPanel'
import { CommandReconciliation } from './CommandReconciliation'
import { Badge, EmptyState, ErrorNotice, Field, LoadingState, StatusBadge, StructuredData } from './shared'
import { Timeline } from './Timeline'
import { MarkdownText } from './MarkdownText'

type Tab = 'evidence' | 'proposal' | 'activity'
const tabs: { id: Tab; label: string }[] = [
  { id: 'evidence', label: 'Evidence' }, { id: 'proposal', label: 'Proposal' }, { id: 'activity', label: 'Activity' },
]

export function WorkInspector({ api, selection, capabilities, snapshotFresh, onChanged, onOpenRun, onOpenIncident, admin = false }: {
  api: ApiClient
  selection: WorkSelection | null
  capabilities: Snapshot['capabilities']
  snapshotFresh: boolean
  onChanged: () => void
  onOpenRun: (id: string) => void
  onOpenIncident?: (id: string) => void
  admin?: boolean
}) {
  if (!selection) return <aside className="inspector"><div className="inspector-label"><FileSearch size={15} aria-hidden="true" /> Inspector</div><EmptyState title="Select a request">Evidence, policy, and recorded activity appear here. Selecting an item does not run any action.</EmptyState></aside>
  return <SelectedWorkInspector key={`${selection.kind}:${selection.source_id}`} {...{ api, selection, capabilities, snapshotFresh, onChanged, onOpenRun, onOpenIncident, admin }} />
}

function SelectedWorkInspector({ api, selection, capabilities, snapshotFresh, onChanged, onOpenRun, onOpenIncident, admin }: {
  api: ApiClient
  selection: WorkSelection
  capabilities: Snapshot['capabilities']
  snapshotFresh: boolean
  onChanged: () => void
  onOpenRun: (id: string) => void
  onOpenIncident?: (id: string) => void
  admin: boolean
}) {
  const [tab, setTab] = useState<Tab>(selection.kind === 'approval' ? 'proposal' : 'evidence')
  const tabRefs = useRef<(HTMLButtonElement | null)[]>([])
  const load = useCallback((signal: AbortSignal) => api.detail(selection, signal), [api, selection])
  const detail = useResource(`detail:${selection.kind}:${selection.source_id}`, load, 5000)
  const data = detail.data
  const fresh = snapshotFresh && !detail.error && Boolean(data)
  const incidentId = data?.item.incident_id || data?.runs.find((run) => run.incident_id)?.incident_id

  function changeTab(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    let next = index
    if (event.key === 'ArrowRight') next = (index + 1) % tabs.length
    else if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length
    else if (event.key === 'Home') next = 0
    else if (event.key === 'End') next = tabs.length - 1
    else return
    event.preventDefault()
    setTab(tabs[next]!.id)
    tabRefs.current[next]?.focus()
  }

  return <aside className="inspector" aria-label="Selected request inspector" id="request-inspector">
    <div className="inspector-label"><FileSearch size={15} aria-hidden="true" /> Request inspector <button type="button" className="icon-button" onClick={detail.refresh} aria-label="Refresh selected request" title="Refresh selected request"><RefreshCw size={14} className={detail.loading ? 'spin' : ''} /></button></div>
    {detail.error && <div className="inspector-error"><ErrorNotice error={detail.error} retry={detail.refresh} /></div>}
    {!data ? detail.loading ? <LoadingState label="Loading request evidence" /> : <EmptyState title="Request unavailable">The selected record could not be loaded. It may have moved or become inaccessible.</EmptyState> : <>
      <header className="inspector-heading">
        <div className="split-line"><span className="eyebrow">{humanize(data.item.kind)}</span><StatusBadge status={effectiveStatus(data.item)} /></div>
        <h2>{data.item.title}</h2>
        <p>{data.item.summary}</p>
        <div className="locked-target"><LockKeyhole size={14} aria-hidden="true" /><span>{data.item.target}</span></div>
        <div className="inspector-meta"><span>{humanize(data.item.workload)}</span><span>{data.item.agent}</span><Badge tone={data.item.severity === 'high' ? 'danger' : data.item.severity === 'medium' ? 'warning' : 'neutral'}>{humanize(data.item.severity)} severity</Badge></div>
        {incidentId && onOpenIncident && <button type="button" className="button secondary" onClick={() => onOpenIncident(incidentId)}>See full incident details <ArrowUpRight size={15} aria-hidden="true" /></button>}
      </header>
      <div className="tabs" role="tablist" aria-label="Request detail">
        {tabs.map((entry, index) => <button key={entry.id} type="button" role="tab" id={`tab-${entry.id}`} aria-controls={`panel-${entry.id}`} aria-selected={tab === entry.id} tabIndex={tab === entry.id ? 0 : -1} ref={(node) => { tabRefs.current[index] = node }} onClick={() => setTab(entry.id)} onKeyDown={(event) => changeTab(event, index)}>{entry.label}{entry.id === 'activity' && data.timeline.length > 0 && <span className="tab-count">{data.timeline.length}</span>}</button>)}
      </div>
      <div className="tab-panel" role="tabpanel" id="panel-evidence" aria-labelledby="tab-evidence" tabIndex={0} hidden={tab !== 'evidence'}>
          <section className="detail-section">
            <h3><ShieldCheck size={16} aria-hidden="true" /> Recorded evidence</h3>
            <p className="small muted">Controller records are the source of truth. Model claims do not override deterministic findings.</p>
            {data.evidence.length ? <dl className="evidence-list">{data.evidence.map((entry, index) => <Field key={`${entry.label}:${index}`} label={entry.label}><span className="preserve-lines">{entry.value}</span></Field>)}</dl> : <p className="muted small">No evidence has been recorded for this request.</p>}
          </section>
          <section className="detail-section">
            <h3>Record details</h3>
            <dl className="metadata">
              <Field label="Source ID"><code>{data.item.source_id}</code></Field>
              <Field label="Incident"><code>{data.item.incident_id || 'Not linked'}</code></Field>
              <Field label="Created">{formatDate(data.item.created_at)}</Field>
              <Field label="Updated">{formatDate(data.item.updated_at)}</Field>
            </dl>
            {data.incident && <details className="raw-details"><summary>Structured incident record</summary><StructuredData data={data.incident} label="Structured incident record" /></details>}
          </section>
          {data.notes && <section className="detail-section"><h3>Controller notes</h3><p className="preserve-lines">{data.notes}</p></section>}
      </div>
      <div className="tab-panel" role="tabpanel" id="panel-proposal" aria-labelledby="tab-proposal" tabIndex={0} hidden={tab !== 'proposal'}>
        {data.proposal
          ? <DecisionPanel key={data.proposal.request_id} proposal={{ ...data.proposal, can_decide: data.proposal.can_decide && data.item.can_decide }} api={api} allowed={capabilities.approve} fresh={fresh} onChanged={() => { detail.refresh(); onChanged() }} />
          : <EmptyState title="No proposed action">No approval request is attached to this item. Investigations and observations do not imply permission to act.</EmptyState>}
      </div>
      <div className="tab-panel" role="tabpanel" id="panel-activity" aria-labelledby="tab-activity" tabIndex={0} hidden={tab !== 'activity'}>
          <section className="detail-section"><h3><Activity size={16} aria-hidden="true" /> Recorded sequence</h3><Timeline events={data.timeline} /></section>
          {data.runs.length > 0 && <section className="detail-section"><h3>Linked runs</h3><ul className="linked-runs">{data.runs.map((run) => <li key={run.id}><button type="button" onClick={() => onOpenRun(run.id)}><span><strong>{run.agent_name}</strong><small>{formatDate(run.started_at)}</small></span><StatusBadge status={run.state} /><ArrowUpRight size={15} aria-hidden="true" /></button></li>)}</ul></section>}
      </div>
      <CommandReconciliation key={`reconciliation:${data.item.source_id}`} api={api} item={data.item} admin={admin} fresh={fresh} onChanged={() => { detail.refresh(); onChanged() }} />
      <AskPanel key={data.item.incident_id ?? data.item.source_id} api={api} incidentId={data.item.incident_id ?? data.item.source_id} contextKind={data.item.incident_id !== null ? 'incident' : data.item.kind} target={data.item.target} allowed={capabilities.ask} fresh={fresh} />
    </>}
  </aside>
}

export function RunInspector({ api, id, capabilities, snapshotFresh }: {
  api: ApiClient; id: string | null; capabilities: Snapshot['capabilities']; snapshotFresh: boolean
}) {
  const load = useCallback((signal: AbortSignal) => api.run(id ?? '', signal), [api, id])
  const detail = useResource(`run:${id}`, load, 5000, Boolean(id))
  if (!id) return <aside className="inspector"><div className="inspector-label"><Activity size={15} aria-hidden="true" /> Run inspector</div><EmptyState title="Select a run">Review the recorded step timeline, tool usage, and terminal outcome.</EmptyState></aside>
  const data = detail.data
  return <aside className="inspector" aria-label="Selected run inspector" id="run-inspector">
    <div className="inspector-label"><Activity size={15} aria-hidden="true" /> Run inspector <button type="button" className="icon-button" onClick={detail.refresh} aria-label="Refresh selected run"><RefreshCw size={14} className={detail.loading ? 'spin' : ''} /></button></div>
    {detail.error && <div className="inspector-error"><ErrorNotice error={detail.error} retry={detail.refresh} /></div>}
    {!data ? detail.loading ? <LoadingState label="Loading recorded run" /> : <EmptyState title="Run unavailable">The API could not return this run.</EmptyState> : <>
      <header className="inspector-heading"><div className="split-line"><span className="eyebrow">Execution record</span><StatusBadge status={data.run.state} /></div><h2>{data.run.target}</h2><MarkdownText text={data.run.summary} /><div className="inspector-meta"><span>{data.run.agent_name}</span><span>{humanize(data.run.workload)}</span></div></header>
      <section className="detail-section">
        <div className="run-metrics"><div><strong>{duration(data.run.duration_ms)}</strong><span>Duration</span></div><div><strong>{data.run.tool_calls}</strong><span>Tool calls</span></div><div><strong>{data.run.write_actions}</strong><span>Writes</span></div><div><strong>{data.run.tokens_used.toLocaleString()}</strong><span>Tokens</span></div></div>
        <dl className="metadata"><Field label="Outcome">{data.run.outcome ? <StatusBadge status={data.run.outcome} /> : 'No terminal outcome recorded'}</Field><Field label="Started">{formatDate(data.run.started_at)}</Field><Field label="Finished">{data.run.finished_at ? formatDate(data.run.finished_at) : 'Not finished'}</Field><Field label="Run ID"><code>{data.run.id}</code></Field><Field label="Request"><code>{data.run.request_id || 'Not linked'}</code></Field><Field label="Signature"><code>{data.run.signature}</code></Field></dl>
        <p className="policy-footnote">A completed run is not necessarily a resolved incident. The recorded outcome above is separate from execution state.</p>
      </section>
      <section className="detail-section"><h3>Step timeline</h3><Timeline events={data.events} /></section>
      {data.result && <section className="detail-section"><details className="raw-details"><summary>Structured result</summary><StructuredData data={data.result} label="Structured run result" /></details></section>}
      <AskPanel key={data.run.incident_id || data.run.id} api={api} incidentId={data.run.incident_id || null} target={data.run.target} allowed={capabilities.ask} fresh={snapshotFresh && !detail.error} />
    </>}
  </aside>
}
