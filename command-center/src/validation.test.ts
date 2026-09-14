import { describe, expect, it, vi } from 'vitest'
import { ApiError } from './api/errors'
import type { ScenarioValidationResult, ValidationScenario } from './api/types'
import { canValidateScenarios, runValidationBatch, VALIDATION_CONCURRENCY } from './validation'
import type { ValidationCaseState } from './validation'

const scenarios: ValidationScenario[] = Array.from({ length: 6 }, (_, index) => ({
  name: `scenario-${index + 1}`, title: `Case ${index + 1}`, description: 'Synthetic validation case.',
}))

function result(scenario: string, passed = true): ScenarioValidationResult {
  return { scenario, provider: 'foundry', passed, failures: passed ? [] : ['Expected refusal was not recorded.'], run_id: `run-${scenario}`, outcome: 'completed', duration_ms: 50 }
}

describe('administrator scenario validation', () => {
  it('requires the explicit server role, not operator or a similar-looking role', () => {
    expect(canValidateScenarios(['admin'])).toBe(true)
    expect(canValidateScenarios(['reader', 'operator'])).toBe(false)
    expect(canValidateScenarios(['not-admin'])).toBe(false)
    expect(canValidateScenarios([])).toBe(false)
  })
  it('runs every advertised case once, bounded to two concurrent requests', async () => {
    let active = 0
    let maxActive = 0
    const run = vi.fn(async (name: string) => {
      active += 1
      maxActive = Math.max(maxActive, active)
      await Promise.resolve()
      active -= 1
      return result(name, name !== 'scenario-2')
    })
    const updates = new Map<string, ValidationCaseState>()
    const outcome = await runValidationBatch({ scenarios, provider: 'foundry', run, shouldStop: () => false, onUpdate: (name, state) => updates.set(name, state) })
    expect(outcome).toBe('completed')
    expect(maxActive).toBe(VALIDATION_CONCURRENCY)
    expect(run).toHaveBeenCalledTimes(6)
    expect(new Set(run.mock.calls.map(([name]) => name)).size).toBe(6)
    expect(updates.get('scenario-2')).toEqual({ state: 'finished', result: result('scenario-2', false) })
    expect([...updates.values()].every((entry) => entry.state === 'finished')).toBe(true)
  })
  it('does not retry a lost response or schedule more cases after it', async () => {
    const run = vi.fn(async (name: string) => {
      if (name === 'scenario-1') throw new ApiError(0, 'network_error', 'Result not confirmed.')
      return result(name)
    })
    const updates = new Map<string, ValidationCaseState>()
    const outcome = await runValidationBatch({ scenarios, provider: 'foundry', run, shouldStop: () => false, onUpdate: (name, state) => updates.set(name, state) })
    expect(outcome).toBe('unconfirmed')
    expect(run).toHaveBeenCalledTimes(2)
    expect(updates.get('scenario-1')?.state).toBe('error')
    expect(updates.get('scenario-3')?.state).toBe('skipped')
  })
  it('stops queued work but awaits already accepted requests', async () => {
    const pending: (() => void)[] = []
    let stop = false
    const run = vi.fn((name: string) => new Promise<ScenarioValidationResult>((resolve) => pending.push(() => resolve(result(name)))))
    const updates = new Map<string, ValidationCaseState>()
    const batch = runValidationBatch({ scenarios, provider: 'foundry', run, shouldStop: () => stop, onUpdate: (name, state) => updates.set(name, state) })
    expect(run).toHaveBeenCalledTimes(2)
    stop = true
    pending.forEach((resolve) => resolve())
    expect(await batch).toBe('stopped')
    expect(run).toHaveBeenCalledTimes(2)
    expect(updates.get('scenario-1')?.state).toBe('finished')
    expect(updates.get('scenario-3')?.state).toBe('skipped')
  })
  it('stops additional requests after an authorization failure', async () => {
    const run = vi.fn(async () => { throw new ApiError(403, 'forbidden', 'Admin access required.') })
    const outcome = await runValidationBatch({ scenarios, provider: 'mock', run, shouldStop: () => false, onUpdate: vi.fn() })
    expect(outcome).toBe('unconfirmed')
    expect(run).toHaveBeenCalledTimes(2)
  })
})
