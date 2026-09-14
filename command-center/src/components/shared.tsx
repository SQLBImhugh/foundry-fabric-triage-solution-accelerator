import { AlertCircle, Inbox, LoaderCircle, RefreshCw } from 'lucide-react'
import type { ReactNode } from 'react'
import type { ApiError } from '../api/errors'
import { statusInfo } from '../domain'
import type { Tone } from '../domain'

export function Badge({ children, tone = 'neutral' }: { children: ReactNode; tone?: Tone }) {
  return <span className={`badge tone-${tone}`}><span className="status-dot" aria-hidden="true" />{children}</span>
}

export function StatusBadge({ status }: { status: string }) {
  const { label, tone } = statusInfo(status)
  return <Badge tone={tone}>{label}</Badge>
}

export function ErrorNotice({ error, retry, children }: { error: ApiError; retry?: () => void; children?: ReactNode }) {
  return (
    <div className="notice tone-danger" role="alert">
      <AlertCircle size={17} aria-hidden="true" />
      <div className="notice-body">
        <strong>{error.status === 409 ? 'Request changed or no longer available' : 'Request not completed'}</strong>
        <p>{error.message}</p>
        <span className="error-code">{error.code}{error.status ? ` / HTTP ${error.status}` : ''}</span>
        {children}
      </div>
      {retry && <button type="button" className="icon-button" onClick={retry} aria-label="Retry loading records" title="Retry loading records"><RefreshCw size={16} /></button>}
    </div>
  )
}

export function EmptyState({ title, children, action }: { title: string; children: ReactNode; action?: ReactNode }) {
  return <div className="empty-state"><Inbox size={28} aria-hidden="true" /><h3>{title}</h3><p>{children}</p>{action}</div>
}

export function LoadingState({ label = 'Loading records' }: { label?: string }) {
  return <div className="loading-state" role="status"><LoaderCircle size={20} className="spin" aria-hidden="true" /><span>{label}</span></div>
}

export function Field({ label, children }: { label: string; children: ReactNode }) {
  return <div className="field"><dt>{label}</dt><dd>{children}</dd></div>
}

export function StructuredData({ data, label = 'Structured details' }: { data: Record<string, unknown>; label?: string }) {
  const entries = Object.entries(data)
  if (!entries.length) return <p className="muted small">No parameters recorded.</p>
  return <dl className="structured-data" aria-label={label}>
    {entries.map(([key, value]) => <Field key={key} label={key}>
      {typeof value === 'object' && value !== null
        ? <pre tabIndex={0} aria-label={key}>{JSON.stringify(value, null, 2)}</pre>
        : <code>{value === null ? 'null' : String(value)}</code>}
    </Field>)}
  </dl>
}
