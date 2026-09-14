import { act, renderHook, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { ApiError } from '../api/errors'
import { useResource } from './useResource'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => { resolve = done })
  return { promise, resolve }
}

describe('race-safe record refresh', () => {
  it('does not let an old selection replace the current selection', async () => {
    const first = deferred<string>()
    const second = deferred<string>()
    const firstLoad = vi.fn(() => first.promise)
    const secondLoad = vi.fn(() => second.promise)
    const { result, rerender, unmount } = renderHook(({ id, load }) => useResource(id, load), { initialProps: { id: 'first', load: firstLoad } })
    rerender({ id: 'second', load: secondLoad })
    await act(async () => second.resolve('current evidence'))
    expect(result.current.data).toBe('current evidence')
    await act(async () => first.resolve('old evidence'))
    expect(result.current.data).toBe('current evidence')
    unmount()
  })
  it('retains the last record with an explicit error, then recovers on refresh', async () => {
    const load = vi.fn<(signal: AbortSignal) => Promise<string>>()
      .mockResolvedValueOnce('last received')
      .mockRejectedValueOnce(new ApiError(503, 'backend_unavailable', 'State store unavailable.'))
      .mockResolvedValueOnce('recovered')
    const { result } = renderHook(() => useResource('record', load))
    await waitFor(() => expect(result.current.data).toBe('last received'))
    act(() => result.current.refresh())
    await waitFor(() => expect(result.current.error?.code).toBe('backend_unavailable'))
    expect(result.current.data).toBe('last received')
    act(() => result.current.refresh())
    await waitFor(() => expect(result.current.data).toBe('recovered'))
    expect(result.current.error).toBeNull()
  })
  it('aborts a pending read and clears the poll when unmounted', async () => {
    vi.useFakeTimers()
    const signals: AbortSignal[] = []
    const load = vi.fn((signal: AbortSignal) => { signals.push(signal); return Promise.resolve('record') })
    const { unmount } = renderHook(() => useResource('record', load, 5000))
    await act(async () => undefined)
    expect(load).toHaveBeenCalledOnce()
    unmount()
    await act(async () => vi.advanceTimersByTimeAsync(10_000))
    expect(load).toHaveBeenCalledOnce()
    expect(signals[0]?.aborted).toBe(true)
  })
})
