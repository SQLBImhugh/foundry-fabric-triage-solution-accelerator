import { useCallback, useEffect, useRef, useState } from 'react'
import { ArrowUpRight, CheckCheck, FlaskConical, ListChecks, LoaderCircle, Play, RefreshCw, ShieldCheck, Square } from 'lucide-react'
import type { ApiClient } from '../api/client'
import type { ScenarioValidationResult, ValidationProvider, ValidationScenario } from '../api/types'
import { duration, humanize } from '../domain'
import { useResource } from '../hooks/useResource'
import { runValidationBatch, VALIDATION_CONCURRENCY } from '../validation'
import type { ValidationCaseState } from '../validation'
import { Badge, EmptyState, ErrorNotice, Field, LoadingState, StatusBadge } from './shared'

function ValidationStatus({ state }: { state?: ValidationCaseState }) {
  if (state?.state === 'finished') return <Badge tone={state.result.passed ? 'success' : 'danger'}>{state.result.passed ? 'Passed' : 'Failed'}</Badge>
  if (state?.state === 'running') return <Badge tone="info">Running validation</Badge>
  if (state?.state === 'error') return <Badge tone="danger">Result not confirmed</Badge>
  return <Badge>{state?.state === 'queued' ? 'Queued' : state?.state === 'skipped' ? 'Not run' : 'Not run in this view'}</Badge>
}

type Selection = { kind: 'scenario'; name: string } | { kind: 'record'; result: ScenarioValidationResult }

export function ValidationWorkspace({ api, admin, fresh, active, onOpenRun, onChanged }: {
  api: ApiClient
  admin: boolean
  fresh: boolean
  active: boolean
  onOpenRun: (id: string) => void
  onChanged: () => void
}) {
  if (!admin) return <EmptyState title="Administrator access required">Scenario validation is available only to actors with the server-assigned admin role.</EmptyState>
  return <AdminValidationWorkspace {...{ api, fresh, active, onOpenRun, onChanged }} />
}

function AdminValidationWorkspace({ api, fresh, active, onOpenRun, onChanged }: {
  api: ApiClient
  fresh: boolean
  active: boolean
  onOpenRun: (id: string) => void
  onChanged: () => void
}) {
  const loadCatalog = useCallback((signal: AbortSignal) => api.validationScenarios(signal), [api])
  const loadResults = useCallback((signal: AbortSignal) => api.validationResults(signal), [api])
  const [busy, setBusy] = useState(false)
  const catalog = useResource('validation-scenarios', loadCatalog, 0, active || busy)
  const history = useResource('validation-results', loadResults, 5000, active || busy)
  const [chosenProvider, setChosenProvider] = useState<ValidationProvider | ''>('')
  const [batchProvider, setBatchProvider] = useState<ValidationProvider | null>(null)
  const [states, setStates] = useState<Record<string, ValidationCaseState>>({})
  const [selection, setSelection] = useState<Selection | null>(null)
  const [completion, setCompletion] = useState<'completed' | 'stopped' | 'unconfirmed' | null>(null)
  const [stopRequested, setStopRequested] = useState(false)
  const locked = useRef(false)
  const stop = useRef(false)
  const mounted = useRef(true)
  const currentPermission = useRef(fresh)
  useEffect(() => { currentPermission.current = fresh }, [fresh])
  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false; stop.current = true }
  }, [])

  const scenarios = catalog.data?.items ?? []
  const providers = catalog.data?.providers ?? []
  const provider = chosenProvider && providers.includes(chosenProvider) ? chosenProvider
    : providers.includes('mock') ? 'mock' : providers[0]
  const forbidden = history.error?.status === 401 || history.error?.status === 403
  const blocked = !fresh ? 'Current permissions could not be refreshed. Validation is locked until the connection recovers.'
    : catalog.error ? 'The scenario catalog could not be loaded. Refresh it before running validation.'
      : forbidden ? 'The server did not authorize validation. Sign in with an administrator account.'
        : !catalog.data ? 'Loading the server scenario catalog.'
          : !provider ? 'No validation providers are enabled on the server.'
            : !scenarios.length ? 'The server has not advertised any executable scenarios.' : ''

  const selectedName = selection?.kind === 'scenario' ? selection.name
    : selection?.kind === 'record' ? selection.result.scenario : scenarios[0]?.name
  const selectedScenario = scenarios.find((scenario) => scenario.name === selectedName)
  const selectedState = selectedName ? states[selectedName] : undefined
  const selectedResult = selection?.kind === 'record' ? selection.result
    : selectedState?.state === 'finished' ? selectedState.result : null
  const resultState: ValidationCaseState | undefined = selectedResult ? { state: 'finished', result: selectedResult } : selectedState
  const cases = Object.values(states)
  const passed = cases.filter((entry) => entry.state === 'finished' && entry.result.passed).length
  const failed = cases.filter((entry) => entry.state === 'finished' && !entry.result.passed).length
  const errors = cases.filter((entry) => entry.state === 'error').length
  const skipped = cases.filter((entry) => entry.state === 'skipped').length
  const finished = passed + failed + errors

  async function start(chosen: ValidationScenario[], all: boolean) {
    if (!provider || blocked || locked.current || !chosen.length) return
    locked.current = true
    stop.current = false
    setBusy(true)
    setStopRequested(false)
    setCompletion(null)
    setBatchProvider(provider)
    if (all) setStates({})
    setSelection({ kind: 'scenario', name: chosen[0]!.name })
    try {
      const outcome = await runValidationBatch({
        scenarios: chosen,
        provider,
        run: (name, modelProvider) => api.validateScenario(name, modelProvider),
        shouldStop: () => stop.current || !currentPermission.current || !mounted.current,
        onUpdate: (name, state) => {
          if (!mounted.current) return
          setStates((previous) => ({ ...previous, [name]: state }))
          if (state.state === 'finished' || state.state === 'error') history.refresh()
        },
      })
      if (mounted.current) {
        setCompletion(outcome)
        history.refresh()
        onChanged()
      }
    } finally {
      locked.current = false
      if (mounted.current) setBusy(false)
    }
  }

  function requestStop() {
    stop.current = true
    setStopRequested(true)
  }

  function changeProvider(value: ValidationProvider) {
    setChosenProvider(value)
    setStates({})
    setCompletion(null)
    setBatchProvider(null)
    setSelection((previous) => previous?.kind === 'record' ? null : previous)
  }

  return <div className="validation-workspace">
    <div className="notice tone-info validation-notice" role="note"><FlaskConical size={20} aria-hidden="true" /><div><strong>Deployed-code validation / synthetic tools and isolated state</strong><p>These cases exercise the deployed controller, not production remediation. Foundry uses model calls; its tools and scenario state remain synthetic.</p></div><Badge>Admin only</Badge></div>
    <div className="workspace-grid">
      <div className="validation-main">
        <section className="queue-panel">
          <div className="panel-heading"><div><div className="heading-line"><h2>Executable scenarios</h2>{catalog.data && <span className="count-label">{scenarios.length}</span>}</div><p>Run every advertised case or inspect an individual result.</p></div><button type="button" className="icon-button" aria-label="Refresh validation catalog" disabled={busy || catalog.loading} onClick={() => { catalog.refresh(); history.refresh() }}><RefreshCw size={17} className={catalog.loading ? 'spin' : ''} /></button></div>
          <div className="validation-controls">
            <label><span className="form-label">Validation provider</span><select aria-label="Validation provider" value={provider ?? ''} onChange={(event) => changeProvider(event.target.value as ValidationProvider)} disabled={busy || !providers.length}>{!providers.length && <option value="">Unavailable</option>}{providers.map((entry) => <option key={entry} value={entry}>{entry === 'mock' ? 'Mock / deterministic provider' : 'Foundry / model provider'}</option>)}</select></label>
            <button type="button" className="button primary" onClick={() => void start(scenarios, true)} disabled={Boolean(blocked || busy)} aria-describedby="validation-execution-help"><Play size={15} aria-hidden="true" />Run all{scenarios.length ? ` (${scenarios.length})` : ''}</button>
            {busy && <button type="button" className="button secondary" onClick={requestStop} disabled={stopRequested}><Square size={13} aria-hidden="true" />{stopRequested ? 'Stopping...' : 'Stop queue'}</button>}
          </div>
          <p id="validation-execution-help" className="validation-help">Up to {VALIDATION_CONCURRENCY} scenarios at a time. No automatic retries. Stopping prevents new requests; in-flight cases finish on the server.</p>
          {blocked && <p className="validation-help tone-warning" role="status">{blocked}</p>}
          {catalog.error && <div className="inspector-error"><ErrorNotice error={catalog.error} retry={catalog.refresh} /></div>}
          {cases.length > 0 && <div className="validation-progress" role="status" aria-live="polite">
            {busy && <LoaderCircle size={14} className="spin" aria-hidden="true" />}
            <strong>{finished} of {cases.length} results confirmed or errored</strong><span>{passed} passed / {failed} failed / {errors} unconfirmed / {skipped} not run</span><Badge>{batchProvider}</Badge>
          </div>}
          {completion === 'unconfirmed' && <div className="list-notice">The batch stopped after an unconfirmed or unauthorized request. Refresh recorded results before starting another batch.</div>}
          {completion === 'stopped' && <div className="list-notice">The queue stopped. Cases marked "Not run" have not been validated.</div>}
          {completion === 'completed' && <div className="list-notice">{failed || errors ? 'Batch finished with failures or unconfirmed results. Review each recorded result.' : 'Batch requests finished. Review the per-case assertions and recorded runs below.'}</div>}
          {!catalog.data && catalog.loading ? <LoadingState label="Loading executable scenarios" /> : scenarios.length ? <ul className="validation-cases" aria-label="Executable scenarios">
            {scenarios.map((scenario) => <li key={scenario.name} className={selection?.kind !== 'record' && selectedName === scenario.name ? 'selected' : ''}>
              <button type="button" className="validation-select" aria-pressed={selection?.kind !== 'record' && selectedName === scenario.name} onClick={() => setSelection({ kind: 'scenario', name: scenario.name })}><strong>{scenario.title}</strong><span>{scenario.description}</span><code>{scenario.name}</code></button>
              <div className="validation-case-actions"><ValidationStatus state={states[scenario.name]} /><button type="button" className="button secondary" disabled={Boolean(blocked || busy)} aria-label={`Run scenario ${scenario.name}`} onClick={() => void start([scenario], false)}><Play size={12} aria-hidden="true" />Run</button></div>
            </li>)}
          </ul> : !catalog.loading && <EmptyState title="No scenarios loaded">Validation requires the server's scenario catalog. No local cases are substituted.</EmptyState>}
        </section>
        <section className="queue-panel validation-history">
          <div className="panel-heading"><div><div className="heading-line"><h2>Recorded validation results</h2>{history.data && <span className="count-label">{history.data.items.length}</span>}</div><p>Server-recorded results, including previous validation attempts.</p></div><button type="button" className="icon-button" aria-label="Refresh validation results" onClick={history.refresh} disabled={history.loading}><RefreshCw size={16} className={history.loading ? 'spin' : ''} /></button></div>
          {history.error && <div className="inspector-error"><ErrorNotice error={history.error} retry={history.refresh} /></div>}
          {!history.data && history.loading ? <LoadingState label="Loading recorded validation results" /> : history.data?.items.length ? <ul className="validation-records" aria-label="Recorded validation results">{history.data.items.map((result, index) => <li key={`${result.run_id}:${index}`}><button type="button" onClick={() => setSelection({ kind: 'record', result })} aria-label={`Inspect ${result.scenario} ${result.provider} ${result.passed ? 'passed' : 'failed'} result`}><span><strong>{result.scenario}</strong><small>{humanize(result.provider)} / {duration(result.duration_ms)}</small></span><Badge tone={result.passed ? 'success' : 'danger'}>{result.passed ? 'Passed' : 'Failed'}</Badge><ArrowUpRight size={14} aria-hidden="true" /></button></li>)}</ul> : !history.loading && <EmptyState title="No recorded validation results">Results appear after a scenario request is recorded. An empty history does not mean scenarios passed.</EmptyState>}
        </section>
      </div>
      <aside className="inspector" aria-label="Scenario validation inspector">
        <div className="inspector-label"><ListChecks size={15} aria-hidden="true" /> Validation result</div>
        {selectedName ? <>
          <header className="inspector-heading"><div className="split-line"><span className="eyebrow">Synthetic scenario</span><ValidationStatus state={resultState} /></div><h2>{selectedScenario?.title ?? selectedName}</h2>{selectedScenario && <p>{selectedScenario.description}</p>}<code className="block-code">{selectedName}</code></header>
          <section className="detail-section"><h3><ShieldCheck size={16} aria-hidden="true" /> Validation boundary</h3><p className="small muted">This checks deployed code with synthetic tools and scenario state. A passing assertion is not a production incident resolution.</p></section>
          {resultState?.state === 'error' && <section className="detail-section"><ErrorNotice error={resultState.error} /><p className="small muted">No pass/fail outcome was confirmed. Check recorded results before manually running this case again.</p></section>}
          {selectedResult ? <>
            <section className="detail-section"><h3><CheckCheck size={16} aria-hidden="true" /> Recorded assertions</h3>{selectedResult.failures.length ? <ul className="validation-failures">{selectedResult.failures.map((failure, index) => <li key={index}>{failure}</li>)}</ul> : <p className="small muted">{selectedResult.passed ? 'The server reported that this case met its expectations.' : 'The server reported a failed case without assertion details.'}</p>}<dl className="metadata"><Field label="Provider">{humanize(selectedResult.provider)}</Field><Field label="Outcome"><StatusBadge status={selectedResult.outcome} /></Field><Field label="Duration">{duration(selectedResult.duration_ms)}</Field><Field label="Run ID"><code>{selectedResult.run_id || 'Not recorded'}</code></Field></dl></section>
            <section className="detail-section"><button type="button" className="button primary full-width" disabled={!selectedResult.run_id} onClick={() => onOpenRun(selectedResult.run_id)}>Open recorded run <ArrowUpRight size={16} aria-hidden="true" /></button><p className="form-hint">Opens the step timeline and structured result. Other queued validation cases continue while this application remains open.</p></section>
          </> : resultState?.state !== 'error' && <section className="detail-section"><p className="small muted">{resultState?.state === 'running' ? 'Awaiting the server result. Starting a request does not prove that its assertions passed.' : resultState?.state === 'queued' ? 'Waiting for a bounded validation slot.' : 'No pass/fail result has been recorded for this case in the current view.'}</p></section>}
        </> : <EmptyState title="Select a scenario">Inspect its assertions and open its recorded execution when a result is available.</EmptyState>}
      </aside>
    </div>
  </div>
}
