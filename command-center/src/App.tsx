import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Activity, ArrowRight, BookOpen, ChevronDown, CircleAlert, FileSearch, FlaskConical, Layers3, LayoutDashboard, ListChecks, ListFilter, Moon, Plus, RefreshCw, ShieldCheck, Sun, UserRound } from 'lucide-react'
import { ApiError, asApiError } from './api/errors'
import type { ApiClient } from './api/client'
import type { AuthSession } from './api/auth'
import { AccessApiClient } from './api/access'
import { IncidentApiClient } from './api/incidents'
import type { AppConfig, Snapshot, WorkSelection } from './api/types'
import type { QueueFilter } from './domain'
import { formatDate, humanize, queueFilterLabels } from './domain'
import { useResource } from './hooks/useResource'
import { Dialog } from './components/Dialog'
import { NewInvestigation } from './components/NewInvestigation'
import { WorkInspector } from './components/Inspectors'
import { KnowledgeWorkspace, RunWorkspace, WorkQueue } from './components/Workspaces'
import { Badge, ErrorNotice, LoadingState } from './components/shared'
import { ValidationWorkspace } from './components/ValidationWorkspace'
import { UserMenu } from './components/UserMenu'
import { AccessCenter } from './components/AccessCenter'
import { IncidentWorkspace } from './components/IncidentWorkspace'
import { canValidateScenarios } from './validation'
import { approvalFromSearch } from './approvalLinks'
import { incidentUrl } from './incidentLinks'
import { workspaceFromSearch, workspaceUrl } from './workspaceNavigation'
import type { WorkspaceSection } from './workspaceNavigation'

type Section = WorkspaceSection
const navigation = [
  { id: 'command' as const, title: 'Command center', icon: LayoutDashboard },
  { id: 'incidents' as const, title: 'Incidents', icon: FileSearch },
  { id: 'runs' as const, title: 'Run history', icon: Activity },
  { id: 'knowledge' as const, title: 'Knowledge', icon: BookOpen },
  { id: 'validation' as const, title: 'Scenario validation', icon: ListChecks },
  { id: 'access' as const, title: 'Access & permissions', icon: ShieldCheck },
]
const metrics: { key: keyof Snapshot['counts']; label: string; filter: QueueFilter; tone: string }[] = [
  { key: 'pending_approvals', label: queueFilterLabels.approval, filter: 'approval', tone: 'warning' },
  { key: 'needs_investigation', label: queueFilterLabels.investigation, filter: 'investigation', tone: 'danger' },
  { key: 'running', label: queueFilterLabels.running, filter: 'running', tone: 'info' },
  { key: 'verification_pending', label: queueFilterLabels.verifying, filter: 'verifying', tone: 'warning' },
  { key: 'resolved', label: 'Resolved', filter: 'resolved', tone: 'success' },
]
const noCapabilities: Snapshot['capabilities'] = { approve: false, ask: false, request_triage: false }

export function App({ api, config, auth }: { api: ApiClient; config: AppConfig; auth: AuthSession }) {
  const refreshToken = auth.refreshAccess
  const accessRefresh = useRef<Promise<void> | null>(null)
  const accessApi = useMemo(() => new AccessApiClient(auth.getToken), [auth.getToken])
  const incidentApi = useMemo(() => new IncidentApiClient(auth.getToken), [auth.getToken])
  const [initialWorkspace] = useState(() => workspaceFromSearch(window.location.search))
  const [section, setSection] = useState<Section>(initialWorkspace.section)
  const [selectedIncident, setSelectedIncident] = useState<string | null>(initialWorkspace.incidentId)
  const [theme, setTheme] = useState<'dark' | 'light'>('dark')
  const [contextOpen, setContextOpen] = useState(false)
  const [newOpen, setNewOpen] = useState(false)
  const [selection, setSelection] = useState<WorkSelection | null>(null)
  const [initialApproval] = useState(() => approvalFromSearch(window.location.search))
  const [selectedRun, setSelectedRun] = useState<string | null>(null)
  const [validationVisited, setValidationVisited] = useState(false)
  const [filter, setFilter] = useState<QueueFilter>('all')
  const [query, setQuery] = useState('')
  const [workload, setWorkload] = useState('all')
  const [authError, setAuthError] = useState<ApiError | null>(null)
  const [accessError, setAccessError] = useState<ApiError | null>(null)
  const [accessRefreshing, setAccessRefreshing] = useState(false)
  const [permissionRevision, setPermissionRevision] = useState(0)
  const load = useCallback(async (signal: AbortSignal) => {
    const data = await api.snapshot(signal)
    if (data.mode !== config.mode) throw new ApiError(200, 'mode_mismatch', 'The API mode changed. Reload the app before taking any action.')
    return { value: data, permissionRevision }
  }, [api, config.mode, permissionRevision])
  const snapshot = useResource('snapshot', load, 5000)
  const data = snapshot.data?.value
  // A refreshed token must not re-enable actions using a pre-refresh snapshot.
  const fresh = Boolean(data && !snapshot.error && !accessError && !accessRefreshing
    && snapshot.data?.permissionRevision === permissionRevision)
  const capabilities = data?.capabilities ?? noCapabilities
  const validationAdmin = canValidateScenarios(data?.actor.roles ?? [])
  const workspaceHeading = section === 'access' || (Boolean(data) && section === 'incidents')
  const attentionFilter = section === 'command' && !query.trim() && workload === 'all' ? filter : null
  const unavailable = !data ? 'Waiting for server configuration and permissions.'
    : !fresh ? 'New investigations are unavailable while records cannot be refreshed.'
      : !capabilities.request_triage ? 'Your current role cannot request new investigations.'
        : !data.targets.length ? 'No server-configured targets are available for new investigations.' : ''
  const refreshAccess = useCallback(() => {
    // Leaving and reopening this page must not start overlapping MSAL renewals.
    if (accessRefresh.current) return accessRefresh.current
    setAccessError(null)
    setAccessRefreshing(true)
    const renewal = (async () => {
      try {
        if (!refreshToken && config.mode !== 'demo') {
          throw new ApiError(0, 'access_refresh_unavailable', 'This session cannot request a fresh API token. Reload or sign in again.')
        }
        await refreshToken?.()
        setPermissionRevision((value) => value + 1)
      } catch (caught) {
        const error = asApiError(caught)
        setAccessError(error)
        throw error
      } finally {
        setAccessRefreshing(false)
      }
    })()
    const pending = renewal.finally(() => { accessRefresh.current = null })
    accessRefresh.current = pending
    return pending
  }, [refreshToken, config.mode])

  useEffect(() => {
    document.documentElement.dataset.theme = theme
  }, [theme])
  useEffect(() => {
    document.title = `${navigation.find((entry) => entry.id === section)?.title} | ${config.app_name}`
  }, [section, config.app_name])
  useEffect(() => {
    const restore = () => {
      const location = workspaceFromSearch(window.location.search)
      setSection(location.section)
      setSelectedIncident(location.incidentId)
      const approval = approvalFromSearch(window.location.search)
      if (approval) setSelection({ kind: 'approval', source_id: approval })
    }
    window.addEventListener('popstate', restore)
    return () => window.removeEventListener('popstate', restore)
  }, [])
  useEffect(() => {
    if (section === 'validation' && validationAdmin) setValidationVisited(true)
  }, [section, validationAdmin])
  useEffect(() => {
    if (selection || !data) return
    if (initialApproval) {
      setSelection({ kind: 'approval', source_id: initialApproval })
      return
    }
    const first = data.work_items[0]
    if (first) setSelection({ kind: first.kind, source_id: first.source_id })
  }, [selection, data, initialApproval])

  function select(item: WorkSelection) {
    setSelection(item)
    if (window.matchMedia('(max-width: 900px)').matches) {
      requestAnimationFrame(() => document.getElementById('request-inspector')?.scrollIntoView({ behavior: 'smooth', block: 'start' }))
    }
  }
  function selectAttention(next: QueueFilter) {
    setSection('command')
    window.history.pushState({}, '', workspaceUrl('command'))
    setFilter(attentionFilter === next ? 'all' : next)
    setQuery('')
    setWorkload('all')
  }
  function openRun(id: string) {
    setSelectedRun(id)
    setSection('runs')
    window.history.pushState({}, '', workspaceUrl('runs'))
    if (window.matchMedia('(max-width: 900px)').matches) {
      requestAnimationFrame(() => document.getElementById('run-inspector')?.scrollIntoView({ behavior: 'smooth', block: 'start' }))
    }
  }
  function openIncident(id: string) {
    setSelectedIncident(id)
    setSection('incidents')
    window.history.pushState({}, '', incidentUrl(id))
  }
  function navigate(next: Section) {
    if (next === 'validation') {
      if (!validationAdmin) return
      setValidationVisited(true)
    }
    setSection(next)
    setSelectedIncident(null)
    window.history.pushState({}, '', workspaceUrl(next))
  }
  async function authenticate() {
    setAuthError(null)
    try { await auth.signIn() } catch (caught) { setAuthError(asApiError(caught)) }
  }

  return <div className="app-shell">
    <a className="skip-link" href="#main-content">Skip to main content</a>
    <aside className="sidebar">
      <div className="brand-mark"><img src="/triage-logo.png" alt="BI triage" width="44" height="52" /></div>
      <nav className="primary-nav" aria-label="Primary navigation">
        {navigation.filter((entry) => entry.id !== 'validation' || validationAdmin).map(({ id, title, icon: Icon }) => <button type="button" key={id} aria-current={section === id ? 'page' : undefined} className={section === id ? 'active' : ''} onClick={() => navigate(id)}><Icon size={21} strokeWidth={1.65} aria-hidden="true" /><span>{title}</span></button>)}
      </nav>
      <div className="sidebar-bottom"><ShieldCheck size={19} aria-hidden="true" /><span>Policy<br />controlled</span></div>
    </aside>
    <div className="main-shell">
      <header className="topbar">
        <div className="product-context"><span className="product-name">{config.app_name}</span><span className="context-separator">/</span><button type="button" className="context-button" onClick={() => setContextOpen(true)}><Layers3 size={15} aria-hidden="true" /><span>System overview</span><ChevronDown size={13} aria-hidden="true" /></button></div>
        <div className="topbar-controls">
          <Badge tone={config.mode === 'demo' ? 'warning' : 'info'}>{config.mode === 'demo' ? 'Demo' : 'Live'}</Badge>
          <button type="button" className="theme-toggle" aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`} aria-pressed={theme === 'light'} onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}>{theme === 'dark' ? <Sun size={16} aria-hidden="true" /> : <Moon size={16} aria-hidden="true" />}<span>{theme === 'dark' ? 'Light' : 'Dark'}</span></button>
          <UserMenu auth={auth} user={data?.actor ?? null} />
        </div>
      </header>
      {config.mode === 'demo' && <div className="demo-banner" role="note"><FlaskConical size={15} aria-hidden="true" /><strong>Synthetic demo</strong><span>No Azure effects. Decisions and investigations affect synthetic records only.</span></div>}
      <main id="main-content" className="main-content">
        {!workspaceHeading && <>
        <div className="page-heading"><div><span className="eyebrow">BI triage / Operations</span><h1>{navigation.find((entry) => entry.id === section)?.title}</h1><p>{section === 'command' ? 'Investigations, decisions, and verified outcomes.' : section === 'incidents' ? 'Follow the evidence and current state of each incident.' : section === 'runs' ? 'Inspect what the controller actually did.' : section === 'validation' ? 'Validate deployed code against synthetic executable scenarios.' : 'Operational guidance with traceable sources.'}</p></div><div className="page-actions"><button type="button" className="button secondary refresh-button" onClick={snapshot.refresh} disabled={snapshot.loading} aria-label="Refresh command center"><RefreshCw size={16} className={snapshot.loading ? 'spin' : ''} /><span>Refresh</span></button><button type="button" className="button primary" onClick={() => setNewOpen(true)} disabled={Boolean(unavailable)} aria-describedby={unavailable ? 'new-unavailable' : undefined}><Plus size={17} aria-hidden="true" />New investigation</button></div></div>
        </>}
        {unavailable && !workspaceHeading && <p className="availability-note" id="new-unavailable"><CircleAlert size={14} aria-hidden="true" />{unavailable}</p>}
        {authError && <ErrorNotice error={authError} />}
        {accessError && section !== 'access' && <ErrorNotice error={accessError}>
          <p>Previously loaded records are read-only until permissions can be refreshed.</p>
          <button type="button" className="button secondary" onClick={() => navigate('access')}>Open access &amp; permissions</button>
        </ErrorNotice>}
        {accessError?.status === 401 && auth.enabled && <button type="button" className="button secondary" onClick={() => void authenticate()}>Sign in again</button>}
        {snapshot.error && <ErrorNotice error={snapshot.error} retry={snapshot.refresh}>
          {data && <p className="small">Showing the last received records from {formatDate(data.as_of)}. Decisions and new investigations are locked.</p>}
          {auth.enabled && accessError?.status !== 401 && (snapshot.error.status === 401 || snapshot.error.code === 'sign_in_required') && <button type="button" className="button secondary" onClick={() => void authenticate()}>Sign in again</button>}
        </ErrorNotice>}
        {section === 'access' && <AccessCenter api={accessApi} fresh={fresh} mode={config.mode}
          refreshAccess={refreshToken || config.mode === 'demo' ? refreshAccess : undefined} refreshError={accessError} />}
        {!data && section !== 'access' ? <div className="initial-loading">{snapshot.loading ? <LoadingState label="Connecting to the triage API" /> : <div className="empty-state"><FileSearch size={30} aria-hidden="true" /><h2>No records loaded</h2><p>The command center requires a successful API connection. It does not substitute sample data.</p><button type="button" className="button secondary" onClick={snapshot.refresh}>Retry connection</button></div>}</div> : data && <>
          {section === 'command' && <>
            <div className="attention-strip" aria-label="Attention summary">
              {metrics.map((metric) => <button type="button" key={metric.key} className={`attention-cell tone-${metric.tone} ${attentionFilter === metric.filter ? 'active' : ''}`} aria-pressed={attentionFilter === metric.filter} onClick={() => selectAttention(metric.filter)}><strong>{data.counts[metric.key].toString().padStart(2, '0')}</strong><span>{metric.label}</span><ArrowRight size={14} aria-hidden="true" /></button>)}
            </div>
            <div className="workspace-grid">
              <WorkQueue items={data.work_items} selection={selection} onSelect={select} filter={filter} setFilter={setFilter} query={query} setQuery={setQuery} workload={workload} setWorkload={setWorkload} incidentsOnly={false} truncated={data.truncated} recentRuns={data.recent_runs} onOpenRun={openRun} />
              <WorkInspector api={api} selection={selection} capabilities={capabilities} snapshotFresh={fresh} admin={validationAdmin} onChanged={snapshot.refresh} onOpenRun={openRun} onOpenIncident={openIncident} />
            </div>
          </>}
          {section === 'incidents' && <IncidentWorkspace api={incidentApi} selectedId={selectedIncident} onSelect={openIncident} onBack={() => navigate('incidents')} onOpenRun={openRun} fresh={fresh} onChanged={snapshot.refresh} />}
          {section === 'runs' && <RunWorkspace api={api} selectedId={selectedRun} onSelect={openRun} capabilities={capabilities} snapshotFresh={fresh} />}
          {section === 'knowledge' && <KnowledgeWorkspace api={api} />}
          {section === 'validation' && !validationAdmin && <p role="alert">Administrator permission is required for scenario validation.</p>}
          {validationVisited && <div hidden={section !== 'validation'}><ValidationWorkspace api={api} admin={validationAdmin} fresh={fresh} active={section === 'validation'} onOpenRun={openRun} onChanged={snapshot.refresh} /></div>}
          <footer className="system-footer"><button type="button" className="text-button" onClick={() => setContextOpen(true)}><span className={`health-square ${!fresh ? 'tone-danger' : data.health.some((entry) => entry.status === 'error') ? 'tone-danger' : data.health.some((entry) => entry.status === 'warning') ? 'tone-warning' : 'tone-success'}`} aria-hidden="true" />{!fresh ? 'Connection needs attention' : data.health.length ? `${data.health.length} health checks` : 'No health checks reported'}<ChevronDown size={13} aria-hidden="true" /></button><span>Snapshot <time dateTime={data.as_of}>{formatDate(data.as_of)}</time><span className="footer-divider">/</span>Refreshes every 5s</span></footer>
        </>}
      </main>
    </div>
    <Dialog open={contextOpen} onClose={() => setContextOpen(false)} title="System overview" eyebrow="Monitoring and service status">
      {!data ? <p className="muted">Configuration details will appear after the API responds.</p> : <>
        <section className="context-section"><h3><UserRound size={16} aria-hidden="true" /> User</h3><strong>{data.actor.display_name}</strong><code className="block-code">{data.actor.id}</code><p className="small muted">{data.actor.roles.length ? data.actor.roles.map(humanize).join(' / ') : 'No roles assigned'}</p></section>
        <section className="context-section"><h3><ListFilter size={16} aria-hidden="true" /> Monitored resources</h3><p className="small muted">Power BI datasets and Fabric pipelines available to this controller.</p>{data.targets.length ? <ul className="context-list">{data.targets.map((target) => <li key={target.id}><strong>{target.name}</strong><Badge>{humanize(target.kind)}</Badge><p>{target.description}</p><code>{target.id}</code></li>)}</ul> : <p className="small muted">No resources have been configured for monitoring.</p>}</section>
        <section className="context-section"><h3><Layers3 size={16} aria-hidden="true" /> Agent team</h3>{data.agents.length ? <ul className="context-list">{data.agents.map((agent) => <li key={`${agent.name}:${agent.role}`}><strong>{agent.name}</strong><span className="eyebrow">{agent.role}</span><p>{agent.description}</p></li>)}</ul> : <p className="small muted">No agents were reported.</p>}</section>
        <section className="context-section"><h3><Activity size={16} aria-hidden="true" /> Health</h3><ul className="context-list">{data.health.map((check) => <li key={check.name}><div className="split-line"><strong>{check.name}</strong><Badge tone={check.status === 'ok' ? 'success' : check.status === 'warning' ? 'warning' : 'danger'}>{humanize(check.status)}</Badge></div><p>{check.detail}</p></li>)}</ul>{!data.health.length && <p className="small muted">No health checks were reported.</p>}</section>
      </>}
    </Dialog>
    {data && <NewInvestigation api={api} targets={data.targets} allowed={capabilities.request_triage} fresh={fresh} open={newOpen} onClose={() => setNewOpen(false)} onChanged={snapshot.refresh} onOpenCommand={(id) => { navigate('command'); select({ kind: 'command', source_id: id }) }} />}
  </div>
}
