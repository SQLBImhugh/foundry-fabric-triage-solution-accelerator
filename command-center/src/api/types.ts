export type Mode = 'demo' | 'live'
export type WorkKind = 'approval' | 'incident' | 'command'
export type TargetKind = 'powerbi_triage' | 'pipeline_sweep'
export type Decision = 'approve' | 'deny'

export interface AppConfig {
  mode: Mode
  auth: { enabled: boolean; tenant_id: string; client_id: string; scope: string; authorization_source?: 'entra_app_roles' | 'synthetic_demo' }
  app_name: string
}

export interface WorkItem {
  id: string
  source_id: string
  kind: WorkKind
  title: string
  target: string
  workload: string
  agent: string
  status: string
  severity: 'low' | 'medium' | 'high'
  summary: string
  created_at: string
  updated_at: string
  expires_at: string | null
  incident_id: string | null
  can_decide: boolean
}

export interface Proposal {
  request_id: string
  action: string
  justification: string
  impact: string
  fingerprint: string
  status: string
  requested_at: string
  expires_at: string
  decision: string | null
  responder: string | null
  reason: string | null
  can_decide: boolean
  arguments: Record<string, unknown>
}

export interface RunSummary {
  id: string
  request_id: string
  incident_id: string
  signature: string
  target: string
  workload: string
  agent_name: string
  state: string
  outcome: string
  summary: string
  started_at: string
  finished_at: string | null
  duration_ms: number
  tool_calls: number
  tokens_used: number
  write_actions: number
}

export interface TimelineEvent {
  id: string
  sequence: number
  timestamp: string
  kind: string
  label: string
  status: string
  detail: string
  tool_name: string
}

export interface Target {
  id: string
  name: string
  kind: TargetKind
  description: string
}

export interface Snapshot {
  schema_version: 1
  mode: Mode
  as_of: string
  actor: { id: string; display_name: string; roles: string[] }
  counts: {
    pending_approvals: number
    needs_investigation: number
    running: number
    verification_pending: number
    resolved: number
  }
  work_items: WorkItem[]
  recent_runs: RunSummary[]
  agents: { name: string; role: string; description: string }[]
  health: { name: string; status: 'ok' | 'warning' | 'error'; detail: string }[]
  targets: Target[]
  capabilities: { approve: boolean; ask: boolean; request_triage: boolean }
  truncated: boolean
}

export interface WorkDetail {
  item: WorkItem
  proposal: Proposal | null
  incident: Record<string, unknown> | null
  evidence: { label: string; value: string }[]
  runs: RunSummary[]
  timeline: TimelineEvent[]
  notes: string
}

export interface RunDetail {
  run: RunSummary
  events: TimelineEvent[]
  result: Record<string, unknown> | null
}

export interface Playbook {
  name: string
  workload: string
  summary: string
  retry_useful: boolean
  guidance: string
  source: string
  watch_out: string
}

export interface AskAnswer {
  answer: string
  mode: 'records' | 'model'
  references: { id: string; label: string; kind: string }[]
  question_id: string
}

export interface CommandInput {
  kind: TargetKind
  target_id: string
  subject: string
  body: string
}

export interface WorkSelection {
  kind: WorkKind
  source_id: string
}

export type ValidationProvider = 'mock' | 'foundry'

export interface ValidationScenario {
  name: string
  title: string
  description: string
}

export interface ScenarioCatalog {
  items: ValidationScenario[]
  providers: ValidationProvider[]
}

export interface ScenarioValidationResult {
  scenario: string
  provider: ValidationProvider
  passed: boolean
  failures: string[]
  run_id: string
  outcome: string
  duration_ms: number
}
