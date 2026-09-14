import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { ApiClient } from '../api/client'
import { ApiError } from '../api/errors'
import type { WorkItem } from '../api/types'
import { detail, workItem } from '../test/fixtures'
import { CommandReconciliation } from './CommandReconciliation'
import { WorkInspector } from './Inspectors'

const item: WorkItem = {
  ...workItem, id: 'command:command-1', source_id: 'command-1', kind: 'command',
  status: 'interrupted', incident_id: null, can_decide: false,
}

function setup(overrides: Partial<Parameters<typeof CommandReconciliation>[0]> = {}) {
  const api = new ApiClient(async () => null)
  const props = { api, item, admin: true, fresh: true, onChanged: vi.fn(), ...overrides }
  return { api, props, ...render(<CommandReconciliation {...props} />) }
}

describe('human command reconciliation', () => {
  it('requires both a reason and explicit confirmation, and never submits an execution', async () => {
    const user = userEvent.setup()
    const { api, props } = setup()
    const reconcile = vi.spyOn(api, 'reconcileCommand').mockResolvedValue({ status: 'reconciled', command_id: item.source_id })
    const command = vi.spyOn(api, 'command')
    const decide = vi.spyOn(api, 'decide')
    const button = screen.getByRole<HTMLButtonElement>('button', { name: 'Record reconciliation' })
    expect(screen.getByLabelText<HTMLTextAreaElement>(/Reconciliation reason/).maxLength).toBe(1000)
    expect(button.disabled).toBe(true)
    await user.type(screen.getByLabelText(/Reconciliation reason/), ' Job history checked; no job was submitted. ')
    expect(button.disabled).toBe(true)
    await user.click(screen.getByRole('checkbox', { name: /I reviewed the command, external job history, and target state/ }))
    expect(button.disabled).toBe(false)
    await user.click(button)
    expect(reconcile).toHaveBeenCalledWith('command-1', 'Job history checked; no job was submitted.')
    expect(await screen.findByText('Reconciliation recorded')).toBeTruthy()
    expect(screen.getByText(/No execution or retry was requested/)).toBeTruthy()
    expect(screen.getByText(/Other running or interrupted commands may still block the target/)).toBeTruthy()
    expect(command).not.toHaveBeenCalled()
    expect(decide).not.toHaveBeenCalled()
    expect(props.onChanged).toHaveBeenCalledOnce()
    expect(screen.queryByRole('button', { name: 'Record reconciliation' })).toBeNull()
  })
  it('does not expose a control to non-admins or ordinary queued commands', () => {
    const { props, rerender } = setup({ admin: false })
    expect(screen.queryByRole('button', { name: 'Record reconciliation' })).toBeNull()
    rerender(<CommandReconciliation {...props} admin item={{ ...item, status: 'queued' }} />)
    expect(screen.queryByRole('button', { name: 'Record reconciliation' })).toBeNull()
    rerender(<CommandReconciliation {...props} admin item={{ ...item, kind: 'incident' }} />)
    expect(screen.queryByRole('button', { name: 'Record reconciliation' })).toBeNull()
  })
  it('locks reconciliation when current records are unavailable', () => {
    setup({ fresh: false })
    expect(screen.getByText(/Reconciliation is locked/)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Record reconciliation' }).disabled).toBe(true)
    expect(screen.getByRole<HTMLInputElement>('checkbox').disabled).toBe(true)
  })
  it('preserves the reason but requires new confirmation when command evidence changes', async () => {
    const user = userEvent.setup()
    const { props, rerender } = setup()
    await user.type(screen.getByLabelText(/Reconciliation reason/), 'Reviewed target records.')
    await user.click(screen.getByRole('checkbox'))
    rerender(<CommandReconciliation {...props} item={{ ...item, updated_at: '2026-01-01T12:05:00Z' }} />)
    expect(screen.getByLabelText<HTMLTextAreaElement>(/Reconciliation reason/).value).toBe('Reviewed target records.')
    expect(screen.getByRole<HTMLInputElement>('checkbox').checked).toBe(false)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Record reconciliation' }).disabled).toBe(true)
    expect(screen.getByText(/The command changed during review/)).toBeTruthy()
  })
  it('shows a stale conflict, refreshes records, and requires renewed confirmation instead of retrying', async () => {
    const user = userEvent.setup()
    const { api, props } = setup({ item: { ...item, status: 'uncertain' } })
    const reconcile = vi.spyOn(api, 'reconcileCommand').mockRejectedValue(new ApiError(409, 'command_changed', 'This command is no longer awaiting reconciliation.'))
    await user.type(screen.getByLabelText(/Reconciliation reason/), 'External evidence reviewed.')
    await user.click(screen.getByRole('checkbox'))
    await user.click(screen.getByRole('button', { name: 'Record reconciliation' }))
    expect(await screen.findByRole('alert')).toBeTruthy()
    expect(reconcile).toHaveBeenCalledOnce()
    expect(props.onChanged).toHaveBeenCalledOnce()
    expect(screen.queryByText('Reconciliation recorded')).toBeNull()
    expect(screen.getByRole<HTMLInputElement>('checkbox').checked).toBe(false)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Record reconciliation' }).disabled).toBe(true)
  })
  it('is wired into command detail and refreshes that same record after reconciliation', async () => {
    const user = userEvent.setup()
    const api = new ApiClient(async () => null)
    const details = vi.spyOn(api, 'detail').mockResolvedValue({ ...detail, item, proposal: null, incident: null })
    vi.spyOn(api, 'reconcileCommand').mockResolvedValue({ status: 'reconciled', command_id: item.source_id })
    render(<WorkInspector api={api} selection={{ kind: 'command', source_id: item.source_id }} capabilities={{ ask: false, approve: false, request_triage: true }} snapshotFresh admin onChanged={vi.fn()} onOpenRun={vi.fn()} />)
    await user.type(await screen.findByLabelText(/Reconciliation reason/), 'Target and external job history checked.')
    await user.click(screen.getByRole('checkbox'))
    await user.click(screen.getByRole('button', { name: 'Record reconciliation' }))
    expect(await screen.findByText('Reconciliation recorded')).toBeTruthy()
    await waitFor(() => expect(details).toHaveBeenCalledTimes(2))
    expect(details.mock.calls[1]![0]).toEqual({ kind: 'command', source_id: item.source_id })
  })
})
