import { useRef, useState } from 'react'
import { ArrowRight, CornerDownRight, LockKeyhole, Send } from 'lucide-react'
import type { ApiClient } from '../api/client'
import type { ApiError } from '../api/errors'
import { asApiError } from '../api/errors'
import type { Target } from '../api/types'
import { CommandIntent, humanize } from '../domain'
import { Dialog } from './Dialog'
import { Badge, ErrorNotice } from './shared'

export function NewInvestigation({ api, targets, allowed, fresh, open, onClose, onChanged, onOpenCommand }: {
  api: ApiClient
  targets: Target[]
  allowed: boolean
  fresh: boolean
  open: boolean
  onClose: () => void
  onChanged: () => void
  onOpenCommand: (id: string) => void
}) {
  const [targetId, setTargetId] = useState('')
  const [subject, setSubject] = useState('')
  const [body, setBody] = useState('')
  const [error, setError] = useState<ApiError | null>(null)
  const [busy, setBusy] = useState(false)
  const [commandId, setCommandId] = useState<string | null>(null)
  const intent = useRef(new CommandIntent())
  const target = targets.find((entry) => entry.id === targetId)
  const blocked = !allowed ? 'Your current role cannot request investigations.'
    : !targets.length ? 'No server-configured targets are available.'
      : !fresh ? 'Reconnect to current records before submitting an investigation.' : ''

  async function submit() {
    if (!target || blocked || !subject.trim() || !body.trim() || busy || commandId) return
    const input = { kind: target.kind, target_id: target.id, subject: subject.trim(), body: body.trim() }
    const key = intent.current.keyFor(input)
    setBusy(true)
    setError(null)
    try {
      const result = await api.command(input, key)
      setCommandId(result.command_id)
      onChanged()
    } catch (caught) {
      setError(asApiError(caught))
      onChanged()
    } finally {
      setBusy(false)
    }
  }

  function reset() {
    setCommandId(null)
    setError(null)
    setSubject('')
    setBody('')
    setTargetId('')
    intent.current = new CommandIntent()
  }

  return <Dialog open={open} title="New investigation" eyebrow="Explicit controller command" onClose={onClose} busy={busy}>
    {commandId ? <div className="command-receipt" role="status">
      <Badge tone="info">Queued</Badge>
      <h3>Investigation queued</h3>
      <p>The server accepted this command. It has not confirmed execution or resolution. Follow its recorded status in the queue.</p>
      <dl className="metadata"><div className="field"><dt>Command</dt><dd><code>{commandId}</code></dd></div><div className="field"><dt>Target</dt><dd>{target?.name ?? targetId}</dd></div></dl>
      <div className="form-actions"><button type="button" className="button secondary" onClick={reset}>Start another</button><button type="button" className="button primary" onClick={() => { onOpenCommand(commandId); onClose() }}>View request <ArrowRight size={16} aria-hidden="true" /></button></div>
    </div> : <form onSubmit={(event) => { event.preventDefault(); void submit() }}>
      <p className="muted">Submit a bounded investigation to the controller. Targets and available commands are configured on the server.</p>
      {blocked && <div className="notice tone-warning" role="status"><LockKeyhole size={17} aria-hidden="true" /><p>{blocked}</p></div>}
      <label className="form-label" htmlFor="investigation-target">Configured target</label>
      <select id="investigation-target" value={targetId} onChange={(event) => setTargetId(event.target.value)} required disabled={Boolean(busy || blocked)}>
        <option value="">Select a configured target</option>
        {targets.map((entry) => <option key={entry.id} value={entry.id}>{entry.name}</option>)}
      </select>
      {target && <div className="target-description"><CornerDownRight size={16} aria-hidden="true" /><div><strong>{humanize(target.kind)}</strong><p>{target.description}</p><code>{target.id}</code></div></div>}
      <label className="form-label" htmlFor="investigation-subject">Subject</label>
      <input id="investigation-subject" value={subject} onChange={(event) => setSubject(event.target.value)} required maxLength={300} placeholder="Describe the observed failure" disabled={busy} />
      <label className="form-label" htmlFor="investigation-body">Observed evidence & investigation request</label>
      <textarea id="investigation-body" value={body} onChange={(event) => setBody(event.target.value)} required rows={6} maxLength={4000} placeholder="Include the symptoms, relevant times, and what you want investigated. Do not include credentials or personal data." disabled={busy} />
      <p className="form-hint">This submits an investigation, not approval for remediation. Any proposed write still requires controller policy checks.</p>
      {error && <ErrorNotice error={error}><p className="small">An unchanged retry reuses this request's idempotency key. No automatic retry is performed.</p></ErrorNotice>}
      <div className="form-actions"><button type="button" className="button secondary" onClick={onClose} disabled={busy}>Cancel</button><button type="submit" className="button primary" disabled={Boolean(blocked || busy || !target || !subject.trim() || !body.trim())}><Send size={15} aria-hidden="true" />{busy ? 'Submitting...' : 'Submit investigation'}</button></div>
    </form>}
  </Dialog>
}
