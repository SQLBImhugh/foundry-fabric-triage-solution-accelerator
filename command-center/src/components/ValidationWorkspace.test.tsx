import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { ApiClient } from '../api/client'
import { ApiError } from '../api/errors'
import type { ScenarioCatalog, ScenarioValidationResult, ValidationProvider } from '../api/types'
import { ValidationWorkspace } from './ValidationWorkspace'

const catalog: ScenarioCatalog = {
  items: [
    { name: 'transient', title: 'Transient failure', description: 'Validate a bounded retry.' },
    { name: 'denied', title: 'Approval denied', description: 'Validate that denial does not authorize a write.' },
    { name: 'verification', title: 'Verify result', description: 'Validate recorded evidence.' },
  ],
  providers: ['mock', 'foundry'],
}

function result(scenario: string, provider: ValidationProvider = 'mock', passed = true): ScenarioValidationResult {
  return { scenario, provider, passed, failures: passed ? [] : ['Unexpected remediation call.'], run_id: `run-${scenario}`, outcome: 'complete', duration_ms: 55 }
}

function setup(admin = true) {
  const api = new ApiClient(async () => null)
  const scenarios = vi.spyOn(api, 'validationScenarios').mockResolvedValue(catalog)
  const results = vi.spyOn(api, 'validationResults').mockResolvedValue({ items: [] })
  const run = vi.spyOn(api, 'validateScenario').mockImplementation(async (name, provider) => result(name, provider, name !== 'denied'))
  const props = { api, admin, fresh: true, active: true, onOpenRun: vi.fn(), onChanged: vi.fn() }
  return { ...render(<ValidationWorkspace {...props} />), api, scenarios, results, run, props }
}

describe('scenario validation page', () => {
  it('does not fetch or expose controls to a non-administrator', () => {
    const { scenarios, results, run } = setup(false)
    expect(screen.getByText('Administrator access required')).toBeTruthy()
    expect(screen.queryByRole('button', { name: /Run all/ })).toBeNull()
    expect(scenarios).not.toHaveBeenCalled()
    expect(results).not.toHaveBeenCalled()
    expect(run).not.toHaveBeenCalled()
  })
  it('runs all cases, shows assertion failures, and opens the exact recorded run', async () => {
    const user = userEvent.setup()
    const { run, props } = setup()
    expect(await screen.findByText('Deployed-code validation / synthetic tools and isolated state')).toBeTruthy()
    await user.click(await screen.findByRole('button', { name: 'Run all (3)' }))
    await waitFor(() => expect(run).toHaveBeenCalledTimes(3))
    expect(await screen.findByText(/2 passed \/ 1 failed \/ 0 unconfirmed \/ 0 not run/)).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Run all (3)' }).disabled).toBe(false)
    await user.click(screen.getByRole('button', { name: /Approval denied Validate that denial/ }))
    expect(screen.getByText('Unexpected remediation call.')).toBeTruthy()
    await user.click(screen.getByRole('button', { name: 'Open recorded run' }))
    expect(props.onOpenRun).toHaveBeenCalledWith('run-denied')
  })
  it('retains a bounded batch when the user navigates to the run inspector', async () => {
    const user = userEvent.setup()
    const { run, props, rerender } = setup()
    const pending: (() => void)[] = []
    run.mockImplementation((name, provider) => new Promise((resolve) => pending.push(() => resolve(result(name, provider)))))
    await user.click(await screen.findByRole('button', { name: 'Run all (3)' }))
    expect(run).toHaveBeenCalledTimes(2)
    rerender(<ValidationWorkspace {...props} active={false} />)
    await act(async () => pending[0]!())
    await waitFor(() => expect(run).toHaveBeenCalledTimes(3))
    await act(async () => { pending[1]!(); pending[2]!() })
    await waitFor(() => expect(props.onChanged).toHaveBeenCalledOnce())
  })
  it('stops scheduling if administrator access is removed', async () => {
    const user = userEvent.setup()
    const { run, props, rerender } = setup()
    const pending: (() => void)[] = []
    run.mockImplementation((name, provider) => new Promise((resolve) => pending.push(() => resolve(result(name, provider)))))
    await user.click(await screen.findByRole('button', { name: 'Run all (3)' }))
    expect(run).toHaveBeenCalledTimes(2)
    rerender(<ValidationWorkspace {...props} admin={false} />)
    await act(async () => pending.forEach((resolve) => resolve()))
    expect(run).toHaveBeenCalledTimes(2)
    expect(screen.getByText('Administrator access required')).toBeTruthy()
  })
  it('does not display mock results as Foundry validation when switching providers', async () => {
    const user = userEvent.setup()
    const { run } = setup()
    await user.click(await screen.findByRole('button', { name: 'Run scenario transient' }))
    expect(await screen.findByText('The server reported that this case met its expectations.')).toBeTruthy()
    await user.selectOptions(screen.getByLabelText('Validation provider'), 'foundry')
    expect(screen.queryByRole('button', { name: 'Open recorded run' })).toBeNull()
    await user.click(screen.getByRole('button', { name: 'Run scenario transient' }))
    await waitFor(() => expect(run).toHaveBeenLastCalledWith('transient', 'foundry'))
  })
  it('reports a missing endpoint instead of inventing validation cases', async () => {
    const { scenarios, props, rerender } = setup()
    scenarios.mockRejectedValue(new ApiError(404, 'not_available', 'Validation endpoint not deployed.'))
    rerender(<ValidationWorkspace {...props} active={false} />)
    rerender(<ValidationWorkspace {...props} active />)
    expect(await screen.findByText('Validation endpoint not deployed.')).toBeTruthy()
    expect(screen.getByRole<HTMLButtonElement>('button', { name: /Run all/ }).disabled).toBe(true)
  })
})
