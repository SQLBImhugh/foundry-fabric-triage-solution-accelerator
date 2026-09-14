import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { ApiClient } from '../api/client'
import { detail } from '../test/fixtures'
import { WorkInspector } from './Inspectors'

describe('approval-scoped Ask', () => {
  it.each([
    ['records', 'Records-based answer'],
    ['model', 'Model-generated answer'],
  ] as const)('uses an approval ID without an incident and labels %s answers', async (mode, label) => {
    const user = userEvent.setup()
    const api = new ApiClient(async () => null)
    vi.spyOn(api, 'detail').mockResolvedValue({ ...detail, item: { ...detail.item, incident_id: null }, incident: null })
    const ask = vi.spyOn(api, 'ask').mockResolvedValue({
      answer: 'This approval is pending. No remediation has been executed.',
      mode,
      references: [{ id: 'approval-1', label: 'Pending approval', kind: 'approval' }],
      question_id: 'question-1',
    })
    const command = vi.spyOn(api, 'command')
    const decide = vi.spyOn(api, 'decide')
    render(<WorkInspector api={api} selection={{ kind: 'approval', source_id: 'approval-1' }} capabilities={{ ask: true, approve: true, request_triage: true }} snapshotFresh onChanged={vi.fn()} onOpenRun={vi.fn()} />)
    await user.click(await screen.findByRole('button', { name: 'Ask about this approval' }))
    expect(screen.getByText(/Asking does not dispatch triage/)).toBeTruthy()
    await user.type(screen.getByLabelText('Question'), 'Why is approval needed?')
    await user.click(screen.getByRole('button', { name: 'Ask question' }))
    expect(ask).toHaveBeenCalledWith('approval-1', 'Why is approval needed?')
    expect(await screen.findByText(label)).toBeTruthy()
    expect(command).not.toHaveBeenCalled()
    expect(decide).not.toHaveBeenCalled()
  })
})
