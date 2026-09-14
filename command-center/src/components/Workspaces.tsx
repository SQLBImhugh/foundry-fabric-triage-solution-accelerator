import { useCallback, useState } from 'react'
import { ArrowDownWideNarrow, ArrowRight, ArrowUpRight, BookOpen, ChevronLeft, ChevronRight, FileSearch, RefreshCw, Search, SlidersHorizontal } from 'lucide-react'
import type { ApiClient } from '../api/client'
import type { RunSummary, Snapshot, WorkItem, WorkSelection } from '../api/types'
import type { QueueFilter } from '../domain'
import { duration, effectiveStatus, filterWork, formatDate, humanize, queueFilterLabels, relativeTime, safeSourceUrl, workloadOptions } from '../domain'
import { useResource } from '../hooks/useResource'
import { Badge, EmptyState, ErrorNotice, Field, LoadingState, StatusBadge } from './shared'
import { RunInspector } from './Inspectors'
import { MarkdownText } from './MarkdownText'

const filters: { value: QueueFilter; label: string }[] = [
  { value: 'all', label: queueFilterLabels.all },
  { value: 'approval', label: queueFilterLabels.approval },
  { value: 'investigation', label: queueFilterLabels.investigation },
  { value: 'running', label: queueFilterLabels.running },
  { value: 'verifying', label: queueFilterLabels.verifying },
  { value: 'resolved', label: queueFilterLabels.resolved },
]

export function WorkQueue({ items, selection, onSelect, filter, setFilter, query, setQuery, workload, setWorkload, incidentsOnly, truncated, recentRuns, onOpenRun }: {
  items: WorkItem[]
  selection: WorkSelection | null
  onSelect: (item: WorkSelection) => void
  filter: QueueFilter
  setFilter: (filter: QueueFilter) => void
  query: string
  setQuery: (value: string) => void
  workload: string
  setWorkload: (value: string) => void
  incidentsOnly: boolean
  truncated: boolean
  recentRuns: RunSummary[]
  onOpenRun: (id: string) => void
}) {
  const [order, setOrder] = useState('priority')
  const visible = filterWork(items, query, filter, workload, incidentsOnly).sort((a, b) => {
    if (order === 'priority') {
      const priority = (item: WorkItem) => (item.can_decide ? 6 : 0) + ({ high: 3, medium: 2, low: 1 }[item.severity])
      const difference = priority(b) - priority(a)
      if (difference) return difference
    }
    return (Date.parse(b.updated_at) - Date.parse(a.updated_at)) * (order === 'oldest' ? -1 : 1)
  })
  const workloads = workloadOptions(items)
  const selectionVisible = !selection || visible.some((item) => item.kind === selection.kind && item.source_id === selection.source_id)
  const anyFilter = Boolean(query || filter !== 'all' || workload !== 'all')
  function clearFilters() { setQuery(''); setFilter('all'); setWorkload('all') }

  return <section className="queue-panel" aria-label={incidentsOnly ? 'Incident register' : 'Work queue'}>
    <div className="panel-heading">
      <div><div className="heading-line"><h2>{incidentsOnly ? 'Incident register' : 'Work queue'}</h2><span className="count-label">{visible.length}</span></div><p>{incidentsOnly ? 'Recorded incidents and their current outcomes.' : 'Review the evidence. Decide on one request at a time.'}</p></div>
      <SlidersHorizontal size={18} className="muted" aria-hidden="true" />
    </div>
    <div className="queue-controls">
      <div className="search-field"><Search size={16} aria-hidden="true" /><input type="search" aria-label="Search work queue" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search target, incident, or agent..." /></div>
      <div className="filter-row">
        <label><span className="sr-only">Filter work status</span><select value={filter} onChange={(event) => setFilter(event.target.value as QueueFilter)}>{filters.filter((entry) => !incidentsOnly || entry.value !== 'approval').map((entry) => <option key={entry.value} value={entry.value}>{entry.label}</option>)}</select></label>
        <label><span className="sr-only">Filter workload</span><select value={workload} onChange={(event) => setWorkload(event.target.value)}><option value="all">All workloads</option>{workloads.map((entry) => <option key={entry} value={entry}>{humanize(entry)}</option>)}</select></label>
        <label className="sort-control"><ArrowDownWideNarrow size={15} aria-hidden="true" /><span className="sr-only">Sort work queue</span><select value={order} onChange={(event) => setOrder(event.target.value)}><option value="priority">Priority first</option><option value="latest">Latest first</option><option value="oldest">Oldest first</option></select></label>
      </div>
      <p className="form-hint">Notebook activity failures are shown under their pipeline. Standalone notebook jobs are not monitored.</p>
    </div>
    {truncated && <div className="list-notice" role="status">This snapshot is bounded. Counts may include records outside the displayed queue.</div>}
    {!selectionVisible && <div className="list-notice">The inspected request is outside this filter. Your selection is retained.</div>}
    <div className="queue-column-headings" aria-hidden="true"><span>Request / target</span><span>Status / agent</span><span>Updated</span></div>
    {visible.length ? <ul className="work-list">
      {visible.map((item) => {
        const selected = selection?.kind === item.kind && selection.source_id === item.source_id
        return <li key={item.id}><button type="button" className={`work-row ${selected ? 'selected' : ''}`} aria-pressed={selected} onClick={() => onSelect({ kind: item.kind, source_id: item.source_id })}>
          <span className="work-primary"><span className="work-kind"><span className={`severity-marker severity-${item.severity}`} aria-hidden="true" />{humanize(item.kind)}<span className="sr-only">, {item.severity} severity</span></span><strong>{item.title}</strong><span className="work-target">{item.target}</span><span className="work-id">{item.source_id}</span></span>
          <span className="work-secondary"><StatusBadge status={effectiveStatus(item)} /><span className="agent-label">{item.agent}</span><span className="workload-label">{humanize(item.workload)}</span></span>
          <span className="work-updated"><time dateTime={item.updated_at} title={formatDate(item.updated_at)}>{relativeTime(item.updated_at)}</time><ArrowRight size={16} aria-hidden="true" /></span>
        </button></li>
      })}
    </ul> : <EmptyState title={anyFilter ? 'No matching work' : 'No work in this view'} action={anyFilter && <button type="button" className="button secondary" onClick={clearFilters}>Clear filters</button>}>{anyFilter ? 'Change the search or filters. Other queue records have not been removed.' : 'The server has not returned any items for this view.'}</EmptyState>}
    <div className="queue-footer"><span>{visible.length} of {incidentsOnly ? items.filter((item) => item.kind === 'incident').length : items.length} loaded records</span>{anyFilter ? <button type="button" className="text-button" onClick={clearFilters}>Clear filters</button> : <span>Selection opens the inspector <ArrowRight size={12} aria-hidden="true" /></span>}</div>
    {!incidentsOnly && recentRuns.length > 0 && <section className="recent-activity"><div className="split-line"><h3>Recent execution</h3><span className="eyebrow">Recorded runs</span></div><ul>{recentRuns.slice(0, 3).map((run) => <li key={run.id}><button type="button" onClick={() => onOpenRun(run.id)}><span><strong>{run.target}</strong><small>{run.agent_name} / {relativeTime(run.started_at)}</small></span><StatusBadge status={run.state} /><ArrowUpRight size={14} aria-hidden="true" /></button></li>)}</ul></section>}
  </section>
}

export function RunWorkspace({ api, selectedId, onSelect, capabilities, snapshotFresh }: {
  api: ApiClient; selectedId: string | null; onSelect: (id: string) => void
  capabilities: Snapshot['capabilities']; snapshotFresh: boolean
}) {
  const [offset, setOffset] = useState(0)
  const [query, setQuery] = useState('')
  const load = useCallback((signal: AbortSignal) => api.runs(offset, signal), [api, offset])
  const runs = useResource(`runs:${offset}`, load, 5000)
  const visible = runs.data?.items.filter((run) => [run.id, run.target, run.summary, run.agent_name, run.state, run.outcome].some((value) => value.toLowerCase().includes(query.trim().toLowerCase()))) ?? []
  const total = runs.data?.total ?? 0
  return <div className="workspace-grid">
    <section className="queue-panel" aria-label="Run history">
      <div className="panel-heading"><div><div className="heading-line"><h2>Run history</h2>{runs.data && <span className="count-label">{total}</span>}</div><p>Execution state and incident outcome are tracked separately.</p></div><button type="button" className="icon-button" aria-label="Refresh run history" onClick={runs.refresh}><RefreshCw size={17} className={runs.loading ? 'spin' : ''} /></button></div>
      <div className="queue-controls"><div className="search-field"><Search size={16} aria-hidden="true" /><input type="search" aria-label="Search current run page" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search this page of runs..." /></div></div>
      {runs.error && <div className="inspector-error"><ErrorNotice error={runs.error} retry={runs.refresh} /></div>}
      {!runs.data && runs.loading ? <LoadingState label="Loading run history" /> : visible.length ? <ul className="run-list">
        {visible.map((run) => <li key={run.id}><button type="button" className={`run-row ${selectedId === run.id ? 'selected' : ''}`} aria-pressed={selectedId === run.id} onClick={() => onSelect(run.id)}>
          <div className="split-line"><span className="eyebrow">{run.agent_name}</span><time dateTime={run.started_at}>{formatDate(run.started_at)}</time></div>
          <strong>{run.target}</strong><MarkdownText text={run.summary} preview />
          <div className="run-row-footer"><StatusBadge status={run.state} />{run.outcome && <span className="outcome-label">Outcome: {humanize(run.outcome)}</span>}<span className="run-numbers">{duration(run.duration_ms)} / {run.tool_calls} calls <ArrowUpRight size={13} aria-hidden="true" /></span></div>
        </button></li>)}
      </ul> : <EmptyState title={query ? 'No matching runs on this page' : 'No recorded runs'}>{query ? 'Try another search or page. Search applies to the currently loaded page.' : 'Runs will appear here after the controller records them.'}</EmptyState>}
      <div className="pagination"><span>{total ? `${offset + 1}-${Math.min(offset + 50, total)} of ${total}` : '0 runs'}{query ? ` / ${visible.length} matching` : ''}</span><div><button type="button" className="icon-button" aria-label="Previous run page" disabled={offset === 0 || runs.loading} onClick={() => setOffset(Math.max(0, offset - 50))}><ChevronLeft size={17} /></button><button type="button" className="icon-button" aria-label="Next run page" disabled={offset + 50 >= total || runs.loading} onClick={() => setOffset(offset + 50)}><ChevronRight size={17} /></button></div></div>
    </section>
    <RunInspector api={api} id={selectedId} capabilities={capabilities} snapshotFresh={snapshotFresh} />
  </div>
}

export function KnowledgeWorkspace({ api }: { api: ApiClient }) {
  const load = useCallback((signal: AbortSignal) => api.knowledge(signal), [api])
  const knowledge = useResource('knowledge', load)
  const [query, setQuery] = useState('')
  const [workload, setWorkload] = useState('all')
  const [selected, setSelected] = useState<string | null>(null)
  const items = knowledge.data?.items ?? []
  const visible = items.filter((item) => (workload === 'all' || workload === item.workload) && [item.name, item.summary, item.guidance].some((text) => text.toLowerCase().includes(query.toLowerCase().trim())))
  const item = items.find((entry) => entry.name === selected) ?? (!selected ? visible[0] : undefined)
  const source = item ? safeSourceUrl(item.source) : null
  return <div className="workspace-grid">
    <section className="queue-panel">
      <div className="panel-heading"><div><div className="heading-line"><h2>Knowledge library</h2>{knowledge.data && <span className="count-label">{items.length}</span>}</div><p>Controller playbooks grounded in public documentation.</p></div><button type="button" className="icon-button" aria-label="Refresh knowledge" onClick={knowledge.refresh}><RefreshCw size={17} className={knowledge.loading ? 'spin' : ''} /></button></div>
      <div className="queue-controls"><div className="search-field"><Search size={16} aria-hidden="true" /><input type="search" aria-label="Search knowledge" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search playbooks and guidance..." /></div><label><span className="sr-only">Filter knowledge workload</span><select value={workload} onChange={(event) => setWorkload(event.target.value)}><option value="all">All workloads</option>{[...new Set(items.map((entry) => entry.workload))].sort().map((entry) => <option key={entry} value={entry}>{humanize(entry)}</option>)}</select></label></div>
      {knowledge.error && <div className="inspector-error"><ErrorNotice error={knowledge.error} retry={knowledge.refresh} /></div>}
      {!knowledge.data && knowledge.loading ? <LoadingState label="Loading playbooks" /> : visible.length ? <ul className="knowledge-list">{visible.map((entry) => <li key={entry.name}><button type="button" className={`knowledge-row ${item?.name === entry.name ? 'selected' : ''}`} aria-pressed={item?.name === entry.name} onClick={() => setSelected(entry.name)}><span className="knowledge-icon"><BookOpen size={19} aria-hidden="true" /></span><span><span className="eyebrow">{humanize(entry.workload)}</span><strong>{entry.name}</strong><span className="knowledge-summary">{entry.summary}</span></span><ChevronRight size={16} aria-hidden="true" /></button></li>)}</ul> : <EmptyState title="No matching playbooks">No playbooks were returned for this search. There is no local fallback knowledge.</EmptyState>}
      <div className="queue-footer"><span>{visible.length} of {items.length} playbooks</span><span>Read-only reference</span></div>
    </section>
    <aside className="inspector">
      <div className="inspector-label"><BookOpen size={15} aria-hidden="true" /> Playbook reference</div>
      {item ? <><header className="inspector-heading"><span className="eyebrow">{humanize(item.workload)}</span><h2>{item.name}</h2><p>{item.summary}</p><Badge tone={item.retry_useful ? 'info' : 'warning'}>{item.retry_useful ? 'Retry may help' : 'Retry not recommended'}</Badge></header><section className="detail-section"><h3><FileSearch size={16} aria-hidden="true" /> Guidance</h3><p className="preserve-lines">{item.guidance}</p></section>{item.watch_out && <section className="detail-section"><h3>Watch out</h3><div className="notice tone-warning"><p className="preserve-lines">{item.watch_out}</p></div></section>}<section className="detail-section"><h3>Source</h3>{source ? <a className="source-link" href={source} target="_blank" rel="noopener noreferrer">Read public documentation <ArrowUpRight size={15} aria-hidden="true" /></a> : <p className="muted small">No valid HTTPS documentation link was supplied.</p>}<dl className="metadata"><Field label="Reference"><span className="break-anywhere">{item.source}</span></Field></dl><p className="policy-footnote">Playbook guidance does not grant permission to execute an action.</p></section></> : <EmptyState title="Select a playbook">Review its guidance, retry recommendation, and source.</EmptyState>}
    </aside>
  </div>
}
