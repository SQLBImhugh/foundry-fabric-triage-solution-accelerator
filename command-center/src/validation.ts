import { asApiError } from './api/errors'
import type { ApiError } from './api/errors'
import type { ScenarioValidationResult, ValidationProvider, ValidationScenario } from './api/types'

export const VALIDATION_CONCURRENCY = 2

export type ValidationCaseState =
  | { state: 'queued' | 'running' | 'skipped' }
  | { state: 'finished'; result: ScenarioValidationResult }
  | { state: 'error'; error: ApiError }

export function canValidateScenarios(roles: readonly string[]): boolean {
  return roles.includes('admin')
}

export async function runValidationBatch({ scenarios, provider, run, onUpdate, shouldStop }: {
  scenarios: readonly ValidationScenario[]
  provider: ValidationProvider
  run: (name: string, provider: ValidationProvider) => Promise<ScenarioValidationResult>
  onUpdate: (name: string, state: ValidationCaseState) => void
  shouldStop: () => boolean
}): Promise<'completed' | 'stopped' | 'unconfirmed'> {
  let next = 0
  let unconfirmed = false
  let stopped = false
  for (const scenario of scenarios) onUpdate(scenario.name, { state: 'queued' })

  async function worker() {
    while (next < scenarios.length) {
      if (unconfirmed || shouldStop()) {
        stopped = true
        return
      }
      const scenario = scenarios[next++]!
      onUpdate(scenario.name, { state: 'running' })
      try {
        const result = await run(scenario.name, provider)
        onUpdate(scenario.name, { state: 'finished', result })
      } catch (caught) {
        const error = asApiError(caught)
        onUpdate(scenario.name, { state: 'error', error })
        // A lost response can leave validation running remotely. Do not add more work.
        if (error.status === 0 || error.status === 401 || error.status === 403 || error.code === 'unconfirmed_validation') unconfirmed = true
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(VALIDATION_CONCURRENCY, scenarios.length) }, () => worker()))
  for (const scenario of scenarios.slice(next)) onUpdate(scenario.name, { state: 'skipped' })
  return unconfirmed ? 'unconfirmed' : stopped ? 'stopped' : 'completed'
}
