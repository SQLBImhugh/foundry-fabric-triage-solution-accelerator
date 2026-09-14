import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { ApiClient } from '../api/client'
import { ApiError } from '../api/errors'
import { snapshot } from '../test/fixtures'
import { NewInvestigation } from './NewInvestigation'

describe('investigation commands', () => {
  it('reuses the original key on a manual retry, including after closing the dialog', async () => {
    const user = userEvent.setup()
    const api = new ApiClient(async () => null)
    const command = vi.spyOn(api, 'command')
      .mockRejectedValueOnce(new ApiError(0, 'network_error', 'Submission was not confirmed.'))
      .mockResolvedValueOnce({ command_id: 'command-1', status: 'queued' })
    const props = { api, targets: snapshot.targets, allowed: true, fresh: true, open: true, onClose: vi.fn(), onChanged: vi.fn(), onOpenCommand: vi.fn() }
    const { rerender } = render(<NewInvestigation {...props} />)
    expect(screen.getByLabelText<HTMLInputElement>('Subject').maxLength).toBe(300)
    expect(screen.getByLabelText<HTMLTextAreaElement>('Observed evidence & investigation request').maxLength).toBe(4000)
    await user.selectOptions(screen.getByLabelText('Configured target'), 'target-1')
    await user.type(screen.getByLabelText('Subject'), 'Investigate the refresh failure')
    await user.type(screen.getByLabelText('Observed evidence & investigation request'), 'The scheduled refresh failed. Inspect the recorded evidence.')
    await user.click(screen.getByRole('button', { name: 'Submit investigation' }))
    expect(await screen.findByText('Submission was not confirmed.')).toBeTruthy()
    expect(command).toHaveBeenCalledOnce()
    rerender(<NewInvestigation {...props} open={false} />)
    rerender(<NewInvestigation {...props} open />)
    await user.click(screen.getByRole('button', { name: 'Submit investigation' }))
    expect(await screen.findByText('Investigation queued')).toBeTruthy()
    expect(command).toHaveBeenCalledTimes(2)
    expect(command.mock.calls[0]![1]).toBe(command.mock.calls[1]![1])
    expect(command.mock.calls[0]![0].kind).toBe('powerbi_triage')
    expect(screen.getByText(/It has not confirmed execution or resolution/)).toBeTruthy()
  })
  it('disables submission with an explicit reason when targets are missing', () => {
    render(<NewInvestigation api={new ApiClient(async () => null)} targets={[]} allowed fresh open onClose={vi.fn()} onChanged={vi.fn()} onOpenCommand={vi.fn()} />)
    expect(screen.getByText('No server-configured targets are available.')).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Submit investigation' }).disabled).toBe(true)
  })
  it('does not send arbitrary target IDs or a command without permission', async () => {
    const api = new ApiClient(async () => null)
    const command = vi.spyOn(api, 'command')
    render(<NewInvestigation api={api} targets={snapshot.targets} allowed={false} fresh open onClose={vi.fn()} onChanged={vi.fn()} onOpenCommand={vi.fn()} />)
    expect(screen.getByText('Your current role cannot request investigations.')).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Submit investigation' }).disabled).toBe(true)
    expect(command).not.toHaveBeenCalled()
  })
})
