import type { AppConfig, Proposal, RunSummary, Snapshot, WorkDetail, WorkItem } from '../api/types'

export const config: AppConfig = {
  mode: 'demo',
  app_name: 'BI triage',
  auth: { enabled: false, tenant_id: '', client_id: '', scope: '' },
}

export const proposal: Proposal = {
  request_id: 'approval-1',
  action: 'retry_refresh',
  justification: 'A deterministic scan recorded a transient failure.',
  impact: 'One refresh request for the configured semantic model.',
  fingerprint: 'reviewed-fingerprint-1',
  status: 'pending',
  requested_at: '2026-01-01T12:00:00Z',
  expires_at: '2099-01-01T12:30:00Z',
  decision: null,
  responder: null,
  reason: null,
  can_decide: true,
  arguments: { workspace_id: 'workspace-1', dataset_id: 'model-1' },
}

export const workItem: WorkItem = {
  id: 'approval:approval-1',
  source_id: 'approval-1',
  kind: 'approval',
  title: 'Review refresh retry',
  target: 'Operations semantic model',
  workload: 'powerbi',
  agent: 'Triage agent',
  status: 'pending_approval',
  severity: 'medium',
  summary: 'Refresh failed. An action is awaiting an operator decision.',
  created_at: '2026-01-01T12:00:00Z',
  updated_at: '2026-01-01T12:01:00Z',
  expires_at: proposal.expires_at,
  incident_id: 'incident-1',
  can_decide: true,
}

export const run: RunSummary = {
  id: 'run-1',
  request_id: 'request-1',
  incident_id: 'incident-1',
  signature: 'signature-1',
  target: 'Operations semantic model',
  workload: 'powerbi',
  agent_name: 'Triage agent',
  state: 'completed',
  outcome: 'needs_investigation',
  summary: 'The run completed without resolving the incident.',
  started_at: '2026-01-01T12:00:00Z',
  finished_at: '2026-01-01T12:01:00Z',
  duration_ms: 60_000,
  tool_calls: 4,
  tokens_used: 1200,
  write_actions: 0,
}

export const detail: WorkDetail = {
  item: workItem,
  proposal,
  incident: { incident_id: 'incident-1', status: 'open' },
  evidence: [{ label: 'Deterministic scan', value: 'Transient gateway failure.' }],
  runs: [],
  timeline: [],
  notes: 'No remediation has been executed.',
}

export const snapshot: Snapshot = {
  schema_version: 1,
  mode: 'demo',
  as_of: '2026-01-01T12:00:00Z',
  actor: { id: 'operator-1', display_name: 'Demo operator', roles: ['operator'] },
  counts: { pending_approvals: 1, needs_investigation: 2, running: 0, verification_pending: 0, resolved: 0 },
  work_items: [workItem],
  recent_runs: [],
  agents: [{ name: 'Triage agent', role: 'Reasoning', description: 'Interprets recorded evidence.' }],
  health: [{ name: 'State store', status: 'ok', detail: 'Synthetic records are available.' }],
  targets: [{ id: 'target-1', name: 'Configured model', kind: 'powerbi_triage', description: 'A server-configured triage target.' }],
  capabilities: { approve: true, ask: true, request_triage: true },
  truncated: false,
}
