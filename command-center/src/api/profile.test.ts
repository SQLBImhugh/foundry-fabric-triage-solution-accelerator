import { describe, expect, it, vi } from 'vitest'
import { loadProfilePhoto, PROFILE_PHOTO_URL } from './profile'

describe('Graph profile photo client', () => {
  it('requests only the fixed own-user endpoint with the separate Graph token', async () => {
    vi.mocked(fetch).mockResolvedValue(new Response(new Uint8Array([1, 2, 3]), { headers: { 'Content-Type': 'image/jpeg' } }))
    const getToken = vi.fn().mockResolvedValue('graph-only-token')
    const controller = new AbortController()
    const photo = await loadProfilePhoto(getToken, controller.signal, { interactive: false })
    expect(photo?.type).toBe('image/jpeg')
    expect(photo?.size).toBe(3)
    expect(getToken).toHaveBeenCalledExactlyOnceWith({ interactive: false })
    expect(fetch).toHaveBeenCalledExactlyOnceWith(PROFILE_PHOTO_URL, {
      headers: { Accept: 'image/*', Authorization: 'Bearer graph-only-token' },
      signal: controller.signal,
      credentials: 'omit',
      cache: 'no-store',
      redirect: 'error',
    })
  })
  it('treats only 404 as an absent photo', async () => {
    vi.mocked(fetch).mockResolvedValue(new Response(null, { status: 404 }))
    await expect(loadProfilePhoto(async () => 'graph-token', new AbortController().signal)).resolves.toBeNull()
  })
  it.each([
    [401, 'profile_sign_in_required'],
    [403, 'profile_access_denied'],
    [429, 'profile_request_failed'],
    [500, 'profile_request_failed'],
  ])('reports HTTP %i rather than pretending no photo exists', async (status, code) => {
    vi.mocked(fetch).mockResolvedValue(new Response('Untrusted Graph diagnostics', { status }))
    await expect(loadProfilePhoto(async () => 'graph-token', new AbortController().signal)).rejects.toMatchObject({ status, code })
  })
  it.each(['text/html', 'application/json', 'image/svg+xml', 'application/octet-stream'])('refuses %s content', async (contentType) => {
    vi.mocked(fetch).mockResolvedValue(new Response('not a profile photo', { headers: { 'Content-Type': contentType } }))
    await expect(loadProfilePhoto(async () => 'graph-token', new AbortController().signal)).rejects.toMatchObject({ code: 'invalid_profile_photo' })
  })
  it('refuses an empty image', async () => {
    vi.mocked(fetch).mockResolvedValue(new Response(null, { headers: { 'Content-Type': 'image/jpeg' } }))
    await expect(loadProfilePhoto(async () => 'graph-token', new AbortController().signal)).rejects.toMatchObject({ code: 'invalid_profile_photo' })
  })
  it('reports network failures without returning fetch diagnostics', async () => {
    vi.mocked(fetch).mockRejectedValue(new Error('Untrusted transport diagnostics'))
    await expect(loadProfilePhoto(async () => 'graph-token', new AbortController().signal)).rejects.toMatchObject({
      code: 'profile_network_error',
      message: 'Cannot reach Microsoft Graph for your profile photo. Check the connection and try again.',
    })
  })
  it('reports an interrupted image body download', async () => {
    const response = new Response(new Uint8Array([1]), { headers: { 'Content-Type': 'image/jpeg' } })
    vi.spyOn(response, 'blob').mockRejectedValue(new Error('Download stopped'))
    vi.mocked(fetch).mockResolvedValue(response)
    await expect(loadProfilePhoto(async () => 'graph-token', new AbortController().signal)).rejects.toMatchObject({ code: 'profile_network_error' })
  })
  it('does not acquire a token or fetch after cancellation', async () => {
    const controller = new AbortController()
    controller.abort()
    const getToken = vi.fn().mockResolvedValue('graph-token')
    await expect(loadProfilePhoto(getToken, controller.signal)).rejects.toMatchObject({ name: 'AbortError' })
    expect(getToken).not.toHaveBeenCalled()
    expect(fetch).not.toHaveBeenCalled()
  })
  it('checks cancellation again after token acquisition', async () => {
    const controller = new AbortController()
    const getToken = vi.fn(async () => {
      controller.abort()
      return 'graph-token'
    })
    await expect(loadProfilePhoto(getToken, controller.signal)).rejects.toMatchObject({ name: 'AbortError' })
    expect(fetch).not.toHaveBeenCalled()
  })
  it('preserves fetch cancellation rather than presenting it as a network failure', async () => {
    const error = new DOMException('Canceled', 'AbortError')
    vi.mocked(fetch).mockRejectedValue(error)
    await expect(loadProfilePhoto(async () => 'graph-token', new AbortController().signal)).rejects.toBe(error)
  })
  it('requires a nonempty token and propagates authorization failures', async () => {
    await expect(loadProfilePhoto(async () => '', new AbortController().signal)).rejects.toMatchObject({ code: 'profile_sign_in_required' })
    const failure = new Error('Authorization stopped')
    await expect(loadProfilePhoto(async () => { throw failure }, new AbortController().signal)).rejects.toBe(failure)
    expect(fetch).not.toHaveBeenCalled()
  })
  it('forwards explicit consent only when requested by the caller', async () => {
    vi.mocked(fetch).mockResolvedValue(new Response(null, { status: 404 }))
    const getToken = vi.fn().mockResolvedValue('graph-token')
    await loadProfilePhoto(getToken, new AbortController().signal, { interactive: true })
    expect(getToken).toHaveBeenCalledExactlyOnceWith({ interactive: true })
  })
})
