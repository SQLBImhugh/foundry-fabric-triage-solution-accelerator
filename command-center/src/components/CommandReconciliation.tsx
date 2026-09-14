import { useId, useRef, useState } from 'react'
import { Check, ShieldCheck, TriangleAlert } from 'lucide-react'
import type { ApiClient } from '../api/client'
import type { ApiError } from '../api/errors'
import { asApiError } from '../api/errors'
import type { WorkItem } from '../api/types'
import { needsCommandReconciliation } from '../domain'
import { ErrorNotice, Field } from './shared'

export function CommandReconciliation({ api, item, admin, fresh, onChanged }: {
  api: ApiClient
  item: WorkItem
  admin: boolean
  fresh: boolean
  onChanged: () => void
}) {
  const id = useId()
  const [reason, setReason] = useState('')
  const [confirmation, setConfirmation] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [recorded, setRecorded] = useState(false)
  const [error, setError] = useState<ApiError | null>(null)
  const locked = useRef(false)
  const reviewVersion = JSON.stringify([item.source_id, item.target, item.status, item.updated_at])
  const confirmed = confirmation === reviewVersion
  const eligible = needsCommandReconciliation(item)
  const blocked = !fresh || !eligible || !admin

  async function reconcile() {
    if (locked.current || blocked || recorded || !confirmed || !reason.trim()) return
    locked.current = true
    setBusy(true)
    setError(null)
    try {
      await api.reconcileCommand(item.source_id, reason.trim())
      setRecorded(true)
      onChanged()
    } catch (caught) {
      setError(asApiError(caught))
      setConfirmation(null)
      onChanged()
    } finally {
      locked.current = false
      setBusy(false)
    }
  }

  if (!admin || item.kind !== 'command' || (!eligible && !recorded)) return null
  return <section className="detail-section reconciliation-section" aria-label="Human command reconciliation">
    <h3><ShieldCheck size={16} aria-hidden="true" /> Human reconciliation</h3>
    {recorded ? <div className="notice tone-info" role="status"><Check size={17} aria-hidden="true" /><div><strong>Reconciliation recorded</strong><p>The server recorded this command's human review. No execution or retry was requested. Other running or interrupted commands may still block the target. Future actions still require normal controller policy checks.</p></div></div> : <>
      <div className="notice tone-warning" id={`${id}-boundary`}><TriangleAlert size={17} aria-hidden="true" /><div><strong>Do not blindly rerun an uncertain command</strong><p>Check external job history, recorded runs, and target state first. Reconciliation clears this command's uncertainty; it does not execute, retry, or verify a remediation. Other target blockers may remain.</p></div></div>
      <dl className="metadata"><Field label="Command"><code>{item.source_id}</code></Field><Field label="Target">{item.target}</Field></dl>
      {!fresh && <p className="form-hint" role="status">Current records could not be refreshed. Reconciliation is locked until the connection recovers.</p>}
      {confirmation !== null && !confirmed && <p className="form-hint" role="status">The command changed during review. Review the current records and confirm again.</p>}
      <label className="form-label" htmlFor={`${id}-reason`}>Reconciliation reason <span className="muted">(required)</span></label>
      <textarea id={`${id}-reason`} value={reason} onChange={(event) => setReason(event.target.value)} rows={4} maxLength={1000} disabled={busy || blocked} placeholder="Record the job IDs and evidence you checked, what actually happened, and why clearing the uncertainty block is safe." aria-describedby={`${id}-boundary`} />
      <label className="reconciliation-confirm">
        <input type="checkbox" checked={confirmed} disabled={busy || blocked} onChange={(event) => setConfirmation(event.target.checked ? reviewVersion : null)} />
        <span>I reviewed the command, external job history, and target state. I confirm it is safe to clear this target's uncertainty block. This does not rerun the command.</span>
      </label>
      <button type="button" className="button secondary full-width" onClick={() => void reconcile()} disabled={busy || blocked || !confirmed || !reason.trim()}><ShieldCheck size={15} aria-hidden="true" />{busy ? 'Recording reconciliation...' : 'Record reconciliation'}</button>
      <p className="form-hint">Administrator-only human review. Your reason is recorded by the server. No automatic retry is performed.</p>
      {error && <ErrorNotice error={error} />}
    </>}
  </section>
}
