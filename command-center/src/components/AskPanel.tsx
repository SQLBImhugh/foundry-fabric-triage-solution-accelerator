import { useState } from 'react'
import { ArrowUpRight, BookOpen, MessageSquare, Send } from 'lucide-react'
import type { ApiClient } from '../api/client'
import type { ApiError } from '../api/errors'
import { asApiError } from '../api/errors'
import type { AskAnswer, WorkKind } from '../api/types'
import { Dialog } from './Dialog'
import { Badge, ErrorNotice } from './shared'
import { MarkdownText } from './MarkdownText'

export function AskPanel({ api, incidentId, target, allowed, fresh, contextKind = 'incident' }: {
  api: ApiClient
  incidentId: string | null
  target: string
  allowed: boolean
  fresh: boolean
  contextKind?: WorkKind
}) {
  const [open, setOpen] = useState(false)
  const [question, setQuestion] = useState('')
  const [answer, setAnswer] = useState<AskAnswer | null>(null)
  const [error, setError] = useState<ApiError | null>(null)
  const [busy, setBusy] = useState(false)
  const title = contextKind === 'approval' ? 'Ask about this approval' : contextKind === 'command' ? 'Ask about this request' : 'Ask about this incident'
  const unavailable = !allowed ? 'Your current role cannot ask questions.'
    : !incidentId ? 'Available when this request has a recorded context.'
      : !fresh ? 'Reconnect to current records before asking a question.' : ''

  async function ask() {
    if (!incidentId || unavailable || !question.trim() || busy) return
    setBusy(true)
    setError(null)
    setAnswer(null)
    try {
      setAnswer(await api.ask(incidentId, question.trim()))
    } catch (caught) {
      setError(asApiError(caught))
    } finally {
      setBusy(false)
    }
  }

  return <>
    <div className="ask-entry">
      <button type="button" className="button secondary full-width" onClick={() => setOpen(true)} disabled={Boolean(unavailable)} aria-describedby={unavailable ? 'ask-unavailable' : undefined}>
        <MessageSquare size={16} aria-hidden="true" /> {title} <ArrowUpRight size={15} aria-hidden="true" />
      </button>
      {unavailable && <p className="form-hint" id="ask-unavailable">{unavailable}</p>}
    </div>
    <Dialog open={open} onClose={() => setOpen(false)} title={title} eyebrow="Read-only context" busy={busy} drawer>
      <div className="ask-context"><BookOpen size={18} aria-hidden="true" /><div><strong>{target}</strong><code>{incidentId}</code></div></div>
      <p className="muted small">Questions use the selected record's context. Asking does not dispatch triage, authorize remediation, or execute an action.</p>
      <form onSubmit={(event) => { event.preventDefault(); void ask() }}>
        <label className="form-label" htmlFor="incident-question">Question</label>
        <textarea id="incident-question" value={question} onChange={(event) => setQuestion(event.target.value)} rows={4} maxLength={2000} placeholder="What evidence supports the proposed action?" required disabled={busy} />
        <div className="form-actions"><button className="button primary" type="submit" disabled={Boolean(unavailable || busy || !question.trim())}><Send size={15} aria-hidden="true" />{busy ? 'Reading context...' : 'Ask question'}</button></div>
      </form>
      {error && <ErrorNotice error={error} />}
      {answer && <section className="answer-panel" aria-live="polite">
        <div className="split-line"><h3>Answer</h3><Badge tone="info">{answer.mode === 'records' ? 'Records-based answer' : 'Model-generated answer'}</Badge></div>
        <MarkdownText text={answer.answer} />
        {answer.references.length > 0 && <><h4 className="eyebrow">Referenced records</h4><ul className="reference-list">
          {answer.references.map((reference) => <li key={`${reference.kind}:${reference.id}`}><BookOpen size={14} aria-hidden="true" /><div><strong>{reference.label}</strong><code>{reference.kind} / {reference.id}</code></div></li>)}
        </ul></>}
        <p className="form-hint">Question ID: <code>{answer.question_id}</code></p>
      </section>}
    </Dialog>
  </>
}
