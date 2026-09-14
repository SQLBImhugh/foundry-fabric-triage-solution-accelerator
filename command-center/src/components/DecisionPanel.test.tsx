import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { ApiClient } from '../api/client'
import { ApiError } from '../api/errors'
import { detail, proposal } from '../test/fixtures'
import { DecisionPanel } from './DecisionPanel'
import { WorkInspector } from './Inspectors'

function setup(overrides: Partial<Parameters<typeof DecisionPanel>[0]> = {}) {
  const api = new ApiClient(async () => null)
  const changed = vi.fn()
  const props = { api, proposal, allowed: true, fresh: true, onChanged: changed, ...overrides }
  return { api, changed, props, ...render(<DecisionPanel {...props} />) }
}

describe('individual decisions', () => {
  it('requires a reason and sends exactly the reviewed fingerprint', async () => {
    const user = userEvent.setup()
    const { api, changed } = setup()
    const decide = vi.spyOn(api, 'decide').mockResolvedValue({ status: 'decision_recorded', request: { ...proposal, status: 'decision_recorded', decision: 'approve', responder: 'operator-1', reason: 'Target reviewed.', can_decide: false } })
    expect(screen.getByLabelText<HTMLTextAreaElement>(/Decision reason/).maxLength).toBe(1000)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Approve action' }).disabled).toBe(true)
    await user.type(screen.getByLabelText(/Decision reason/), 'Target reviewed.')
    await user.click(screen.getByRole('button', { name: 'Approve action' }))
    expect(decide).toHaveBeenCalledWith({ request_id: 'approval-1', decision: 'approve', fingerprint: 'reviewed-fingerprint-1', reason: 'Target reviewed.' })
    expect(changed).toHaveBeenCalledOnce()
    expect(await screen.findByText(/This is not an execution result/)).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Approve action' })).toBeNull()
  })
  it('does not imply that a denied action will execute', async () => {
    const user = userEvent.setup()
    const { api } = setup()
    vi.spyOn(api, 'decide').mockResolvedValue({ status: 'decision_recorded', request: { ...proposal, status: 'rejected', decision: 'decline', responder: 'operator-1', reason: 'Not approved.', can_decide: false } })
    await user.type(screen.getByLabelText(/Decision reason/), 'Not approved.')
    await user.click(screen.getByRole('button', { name: 'Deny' }))
    expect(await screen.findByText(/The proposal was refused/)).toBeTruthy()
    expect(screen.queryByText(/must validate policy, perform the action/)).toBeNull()
  })
  it('shows a conflict and refreshes without a success receipt', async () => {
    const user = userEvent.setup()
    const { api, changed } = setup()
    vi.spyOn(api, 'decide').mockRejectedValue(new ApiError(409, 'already_answered', 'Another operator already answered.'))
    await user.type(screen.getByLabelText(/Decision reason/), 'Do not rerun.')
    await user.click(screen.getByRole('button', { name: 'Deny' }))
    expect(await screen.findByRole('alert')).toBeTruthy()
    expect(screen.getByText('Another operator already answered.')).toBeTruthy()
    expect(screen.queryByText('This is not an execution result.')).toBeNull()
    expect(changed).toHaveBeenCalledOnce()
  })
  it('preserves typed text across polling but requires review when the fingerprint changes', async () => {
    const user = userEvent.setup()
    const { props, rerender } = setup()
    await user.type(screen.getByLabelText(/Decision reason/), 'Reviewed current target.')
    rerender(<DecisionPanel {...props} proposal={{ ...proposal }} />)
    expect(screen.getByLabelText<HTMLTextAreaElement>(/Decision reason/).value).toBe('Reviewed current target.')
    rerender(<DecisionPanel {...props} proposal={{ ...proposal, fingerprint: 'new-fingerprint', arguments: { dataset_id: 'new-target' } }} />)
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Approve action' }).disabled).toBe(true)
    expect(screen.getByText('new-target')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'I reviewed the updated proposal' }))
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Approve action' }).disabled).toBe(false)
  })
  it('locks expired or stale requests rather than treating silence as consent', () => {
    const { props, rerender } = setup({ proposal: { ...proposal, expires_at: '2020-01-01T00:00:00Z' } })
    expect(screen.getByText(/This request has expired/)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Deny' }).disabled).toBe(true)
    rerender(<DecisionPanel {...props} proposal={proposal} fresh={false} />)
    expect(screen.getByText(/Decisions are locked/)).toBeTruthy()
  })
  it('does not lose decision text when switching evidence tabs', async () => {
    const user = userEvent.setup()
    const api = new ApiClient(async () => null)
    vi.spyOn(api, 'detail').mockResolvedValue(detail)
    render(<WorkInspector api={api} selection={{ kind: 'approval', source_id: 'approval-1' }} capabilities={{ approve: true, ask: true, request_triage: true }} snapshotFresh onChanged={vi.fn()} onOpenRun={vi.fn()} />)
    await user.type(await screen.findByLabelText(/Decision reason/), 'Evidence reviewed.')
    await user.click(screen.getByRole('tab', { name: 'Evidence' }))
    expect(screen.getByText('Transient gateway failure.')).toBeTruthy()
    await user.click(screen.getByRole('tab', { name: 'Proposal' }))
    await waitFor(() => expect(screen.getByLabelText<HTMLTextAreaElement>(/Decision reason/).value).toBe('Evidence reviewed.'))
    await user.keyboard('{ArrowRight}')
    expect(screen.getByRole('tab', { name: 'Activity' }).getAttribute('aria-selected')).toBe('true')
  })
})
