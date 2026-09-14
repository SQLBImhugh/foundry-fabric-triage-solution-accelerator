import { useEffect, useState } from 'react'
import { Check, CircleAlert, Clock3, LockKeyhole, ShieldCheck, X } from 'lucide-react'
import type { ApiClient } from '../api/client'
import type { ApiError } from '../api/errors'
import { asApiError } from '../api/errors'
import type { Decision, Proposal } from '../api/types'
import { approvalBlockReason, formatDate, timeUntil } from '../domain'
import { ErrorNotice, Field, StatusBadge, StructuredData } from './shared'

export function DecisionPanel({ proposal, api, allowed, fresh, onChanged }: {
  proposal: Proposal
  api: ApiClient
  allowed: boolean
  fresh: boolean
  onChanged: () => void
}) {
  const [now, setNow] = useState(Date.now)
  const [reason, setReason] = useState('')
  const [reviewedFingerprint, setReviewedFingerprint] = useState(proposal.fingerprint)
  const [submitting, setSubmitting] = useState<Decision | null>(null)
  const [recorded, setRecorded] = useState<Proposal | null>(null)
  const [error, setError] = useState<ApiError | null>(null)
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])

  const displayed = recorded ?? proposal
  const changed = proposal.fingerprint !== reviewedFingerprint
  const blocked = !fresh
    ? 'Current records could not be refreshed. Decisions are locked until the connection recovers.'
    : changed
      ? 'The proposal changed during review. Review its current target and parameters before deciding.'
      : approvalBlockReason(displayed, allowed, now)
  const disabled = Boolean(blocked || submitting || recorded || !reason.trim())

  async function decide(decision: Decision) {
    if (submitting || recorded || !fresh || changed || !reason.trim() || approvalBlockReason(proposal, allowed)) return
    setSubmitting(decision)
    setError(null)
    try {
      const result = await api.decide({
        request_id: proposal.request_id,
        decision,
        fingerprint: proposal.fingerprint,
        reason: reason.trim(),
      })
      setRecorded(result.request)
      onChanged()
    } catch (caught) {
      setError(asApiError(caught))
      onChanged()
    } finally {
      setSubmitting(null)
    }
  }

  return <div className="proposal-content">
    <section className="detail-section">
      <div className="split-line"><h3>Proposed action</h3><StatusBadge status={displayed.status} /></div>
      <code className="action-name">{displayed.action}</code>
      <p className="preserve-lines">{displayed.justification}</p>
      {displayed.impact && <div className="impact-note"><span className="eyebrow">Expected impact</span><p>{displayed.impact}</p></div>}
    </section>
    <section className="detail-section">
      <h3><LockKeyhole size={15} aria-hidden="true" /> Immutable parameters</h3>
      <p className="small muted">A decision authorizes only this fingerprint and these parameters.</p>
      <StructuredData data={displayed.arguments} label="Immutable proposal parameters" />
      <dl className="metadata">
        <Field label="Fingerprint"><code className="fingerprint">{displayed.fingerprint || 'Missing'}</code></Field>
        <Field label="Requested"><time dateTime={displayed.requested_at}>{formatDate(displayed.requested_at)}</time></Field>
        <Field label="Expires"><time dateTime={displayed.expires_at}>{formatDate(displayed.expires_at)}</time></Field>
      </dl>
    </section>
    <section className="detail-section decision-section">
      <h3><ShieldCheck size={17} aria-hidden="true" /> Policy & decision</h3>
      {recorded && <div className="notice tone-info" role="status"><Check size={18} aria-hidden="true" /><div>
        <strong>Decision recorded</strong><p>{displayed.decision === 'approve'
          ? 'This is not an execution result. The controller must validate policy, perform the action, and verify its outcome.'
          : 'The proposal was refused. This receipt does not authorize an action or consume the remediation budget.'}</p>
      </div></div>}
      {displayed.decision
        ? <dl className="metadata">
          <Field label="Decision">{displayed.decision}</Field>
          <Field label="Recorded by">{displayed.responder || 'Not recorded'}</Field>
          <Field label="Reason"><span className="preserve-lines">{displayed.reason || 'No reason recorded'}</span></Field>
        </dl>
        : <>
          <div className={`policy-line ${blocked ? 'muted' : 'tone-warning'}`}><Clock3 size={15} aria-hidden="true" />{timeUntil(displayed.expires_at, now)}</div>
          {blocked && <div className="notice tone-warning"><CircleAlert size={17} aria-hidden="true" /><p>{blocked}</p></div>}
          {changed && !recorded && <button type="button" className="button secondary full-width" onClick={() => setReviewedFingerprint(proposal.fingerprint)}>I reviewed the updated proposal</button>}
          <label className="form-label" htmlFor={`decision-reason-${proposal.request_id}`}>Decision reason <span className="muted">(required)</span></label>
          <textarea
            id={`decision-reason-${proposal.request_id}`}
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            rows={3}
            maxLength={1000}
            disabled={Boolean(submitting || blocked)}
            placeholder="Record why this specific action should be approved or denied."
            aria-describedby={`decision-help-${proposal.request_id}`}
          />
          <p className="form-hint" id={`decision-help-${proposal.request_id}`}>Recorded with your server-verified identity. One request, one decision. No bulk approvals.</p>
          <div className="decision-actions">
            <button type="button" className="button primary" disabled={disabled} onClick={() => void decide('approve')}><Check size={16} aria-hidden="true" />{submitting === 'approve' ? 'Recording...' : 'Approve action'}</button>
            <button type="button" className="button danger-button" disabled={disabled} onClick={() => void decide('deny')}><X size={16} aria-hidden="true" />{submitting === 'deny' ? 'Recording...' : 'Deny'}</button>
          </div>
        </>}
      {error && <ErrorNotice error={error} />}
      <p className="policy-footnote">Approval does not bypass the controller allowlist, remediation budget, or verification requirements.</p>
    </section>
  </div>
}
