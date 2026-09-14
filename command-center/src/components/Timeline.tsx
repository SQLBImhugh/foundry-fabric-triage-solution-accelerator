import { Clock3 } from 'lucide-react'
import type { TimelineEvent } from '../api/types'
import { formatDate, humanize } from '../domain'
import { StatusBadge } from './shared'

export function Timeline({ events }: { events: TimelineEvent[] }) {
  if (!events.length) return <div className="inline-empty"><Clock3 size={19} aria-hidden="true" /><p>No activity has been recorded yet.</p></div>
  return <ol className="timeline" aria-label="Recorded activity">
    {[...events].sort((a, b) => a.sequence - b.sequence).map((event) =>
      <li key={event.id}>
        <span className="timeline-marker" aria-hidden="true">{event.sequence}</span>
        <div className="timeline-content">
          <div className="split-line"><span className="eyebrow">{humanize(event.kind)}</span><time dateTime={event.timestamp}>{formatDate(event.timestamp)}</time></div>
          <h4>{event.label}</h4>
          {event.status && <StatusBadge status={event.status} />}
          {event.tool_name && <code className="tool-name">{event.tool_name}</code>}
          {event.detail && <details className="event-details"><summary>Recorded details</summary><p className="preserve-lines">{event.detail}</p></details>}
        </div>
      </li>,
    )}
  </ol>
}
