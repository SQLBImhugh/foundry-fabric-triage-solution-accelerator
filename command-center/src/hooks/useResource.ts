import { useCallback, useEffect, useReducer, useState } from 'react'
import { asApiError, isAborted } from '../api/errors'
import type { ApiError } from '../api/errors'

interface ResourceState<T> {
  key: string
  data: T | null
  error: ApiError | null
  loading: boolean
  receivedAt: number | null
}

export function useResource<T>(
  key: string,
  load: (signal: AbortSignal) => Promise<T>,
  pollMs = 0,
  enabled = true,
) {
  const [revision, update] = useReducer((value: number) => value + 1, 0)
  const [state, setState] = useState<ResourceState<T>>({
    key, data: null, error: null, loading: enabled, receivedAt: null,
  })
  const refresh = useCallback(() => update(), [])

  useEffect(() => {
    if (!enabled) return
    let alive = true
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined

    const read = async () => {
      setState((previous) => previous.key === key
        ? { ...previous, loading: true }
        : { key, data: null, error: null, loading: true, receivedAt: null })
      try {
        const data = await load(controller.signal)
        if (alive) setState({ key, data, error: null, loading: false, receivedAt: Date.now() })
      } catch (error) {
        if (alive && !isAborted(error)) {
          setState((previous) => ({
            key, data: previous.key === key ? previous.data : null,
            error: asApiError(error), loading: false,
            receivedAt: previous.key === key ? previous.receivedAt : null,
          }))
        }
      } finally {
        // Sequential polling avoids overlapping requests and stale responses.
        if (alive && pollMs > 0) timer = setTimeout(() => void read(), pollMs)
      }
    }
    void read()
    return () => {
      alive = false
      controller.abort()
      clearTimeout(timer)
    }
  }, [key, load, pollMs, enabled, revision])

  return {
    data: state.key === key ? state.data : null,
    error: state.key === key ? state.error : null,
    loading: state.key !== key || state.loading,
    receivedAt: state.key === key ? state.receivedAt : null,
    refresh,
  }
}
