import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { AuthSession } from '../api/auth'
import { ApiError } from '../api/errors'
import { UserMenu } from './UserMenu'

const actor = { id: 'user-1', display_name: 'Casey Example' }
const createObjectURL = vi.fn()
const revokeObjectURL = vi.fn()

function session(overrides: Partial<AuthSession> = {}): AuthSession {
  return {
    enabled: true,
    signedIn: true,
    currentUser: { ...actor, username: 'casey.example@example.test' },
    getToken: vi.fn().mockResolvedValue('api-only-token'),
    getProfileToken: vi.fn().mockResolvedValue('graph-only-token'),
    signIn: vi.fn().mockResolvedValue(undefined),
    signOut: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  }
}

function photoResponse(): Response {
  return new Response(new Uint8Array([1, 2, 3]), { headers: { 'Content-Type': 'image/jpeg' } })
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((complete, fail) => { resolve = complete; reject = fail })
  return { promise, resolve, reject }
}

beforeEach(() => {
  createObjectURL.mockReset().mockReturnValue('blob:test-profile-1')
  revokeObjectURL.mockReset()
  vi.stubGlobal('URL', class extends URL {
    static createObjectURL = createObjectURL
    static revokeObjectURL = revokeObjectURL
  })
  vi.mocked(fetch).mockImplementation(async () => photoResponse())
})

describe('user account menu', () => {
  it('shows the real photo and signed-in identity, using only the Graph token helper', async () => {
    const user = userEvent.setup()
    const auth = session()
    render(<UserMenu auth={auth} user={actor} />)
    const image = await screen.findByRole('img', { name: 'Casey Example profile photo' })
    expect(image.getAttribute('src')).toBe('blob:test-profile-1')
    expect(auth.getToken).not.toHaveBeenCalled()
    expect(auth.getProfileToken).toHaveBeenCalledExactlyOnceWith({ interactive: false })
    const button = screen.getByRole('button', { name: 'User menu for Casey Example' })
    expect(button.getAttribute('aria-expanded')).toBe('false')
    expect(button.getAttribute('aria-haspopup')).toBe('menu')
    expect(screen.queryByRole('menu')).toBeNull()
    await user.click(button)
    expect(button.getAttribute('aria-expanded')).toBe('true')
    expect(button.getAttribute('aria-controls')).toBe(screen.getByRole('menu').id)
    expect(screen.getByText('User')).toBeTruthy()
    expect(screen.getByText('casey.example@example.test')).toBeTruthy()
    expect(screen.getByText('Signed in')).toBeTruthy()
    expect(document.activeElement).toBe(screen.getByRole('menuitem', { name: 'Sign out' }))
  })
  it('opens with the keyboard and restores focus on Escape', async () => {
    const user = userEvent.setup()
    render(<UserMenu auth={session({ getProfileToken: undefined })} user={actor} />)
    const button = screen.getByRole('button', { name: /User menu/ })
    button.focus()
    await user.keyboard('{ArrowDown}')
    expect(document.activeElement).toBe(screen.getByRole('menuitem', { name: 'Sign out' }))
    await user.keyboard('{Escape}')
    expect(screen.queryByRole('menu')).toBeNull()
    expect(document.activeElement).toBe(button)
    await user.keyboard('{Enter}')
    expect(screen.getByRole('menu')).toBeTruthy()
    await user.keyboard('{Escape}')
    await user.keyboard(' ')
    expect(screen.getByRole('menu')).toBeTruthy()
  })
  it('supports arrows, Home, End, and typeahead through menu actions', async () => {
    const user = userEvent.setup()
    vi.mocked(fetch).mockResolvedValue(new Response(null, { status: 404 }))
    render(<UserMenu auth={session()} user={actor} />)
    await waitFor(() => expect(fetch).toHaveBeenCalledOnce())
    const button = screen.getByRole('button', { name: /User menu/ })
    button.focus()
    await user.keyboard('{ArrowUp}')
    const signOut = screen.getByRole('menuitem', { name: 'Sign out' })
    const retry = screen.getByRole('menuitem', { name: 'Retry profile photo' })
    expect(document.activeElement).toBe(signOut)
    await user.keyboard('{ArrowDown}')
    expect(document.activeElement).toBe(retry)
    await user.keyboard('{ArrowUp}')
    expect(document.activeElement).toBe(signOut)
    await user.keyboard('{Home}')
    expect(document.activeElement).toBe(retry)
    await user.keyboard('{End}')
    expect(document.activeElement).toBe(signOut)
    await user.keyboard('r')
    expect(document.activeElement).toBe(retry)
    await user.keyboard('s')
    expect(document.activeElement).toBe(signOut)
  })
  it('closes on outside click without taking focus from the clicked control', async () => {
    const user = userEvent.setup()
    render(<><UserMenu auth={session({ getProfileToken: undefined })} user={actor} /><button type="button">Outside</button></>)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    const outside = screen.getByRole('button', { name: 'Outside' })
    await user.click(outside)
    expect(screen.queryByRole('menu')).toBeNull()
    expect(document.activeElement).toBe(outside)
  })
  it('closes on Tab without trapping focus and returns to the trigger on Shift+Tab', async () => {
    const user = userEvent.setup()
    render(<><button type="button">Before</button><UserMenu auth={session({ getProfileToken: undefined })} user={actor} /><button type="button">After</button></>)
    const button = screen.getByRole('button', { name: /User menu/ })
    await user.click(button)
    await user.tab()
    expect(screen.queryByRole('menu')).toBeNull()
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'After' }))
    await user.click(button)
    await user.tab({ shift: true })
    expect(screen.queryByRole('menu')).toBeNull()
    expect(document.activeElement).toBe(button)
  })
  it('does not close when clicking the identity and closes when focus leaves the menu', async () => {
    const user = userEvent.setup()
    render(<><UserMenu auth={session({ getProfileToken: undefined })} user={actor} /><button type="button">Outside</button></>)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    await user.click(screen.getByText('casey.example@example.test'))
    expect(screen.getByRole('menu')).toBeTruthy()
    act(() => screen.getByRole('button', { name: 'Outside' }).focus())
    expect(screen.queryByRole('menu')).toBeNull()
  })
  it('prevents duplicate sign-out submissions and displays asynchronous errors', async () => {
    const user = userEvent.setup()
    const pending = deferred<void>()
    const auth = session({ getProfileToken: undefined, signOut: vi.fn().mockReturnValue(pending.promise) })
    render(<UserMenu auth={auth} user={actor} />)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    await user.click(screen.getByRole('menuitem', { name: 'Sign out' }))
    const busy = screen.getByRole('menuitem', { name: 'Signing out...' })
    expect(busy.getAttribute('aria-disabled')).toBe('true')
    await user.click(busy)
    expect(auth.signOut).toHaveBeenCalledOnce()
    await act(async () => pending.reject(new ApiError(0, 'logout_failed', 'Sign out was interrupted. Your session may still be active.')))
    expect(screen.getByText('Sign out not completed')).toBeTruthy()
    expect(screen.getByText('logout_failed')).toBeTruthy()
    expect(screen.getByRole('menuitem', { name: 'Sign out' }).getAttribute('aria-disabled')).toBe('false')
    await user.keyboard('{Escape}')
    expect(screen.getByRole('alert').textContent).toBe('Sign out failed')
    expect(auth.signIn).not.toHaveBeenCalled()
    vi.mocked(auth.signOut).mockResolvedValueOnce(undefined)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    await user.click(screen.getByRole('menuitem', { name: 'Sign out' }))
    expect(auth.signOut).toHaveBeenCalledTimes(2)
    expect(screen.queryByText('Sign out failed')).toBeNull()
  })
  it('keeps a sign-out failure visible if the user closed the menu while waiting', async () => {
    const user = userEvent.setup()
    const pending = deferred<void>()
    render(<UserMenu auth={session({ getProfileToken: undefined, signOut: () => pending.promise })} user={actor} />)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    await user.click(screen.getByRole('menuitem', { name: 'Sign out' }))
    await user.keyboard('{Escape}')
    await act(async () => pending.reject(new Error('The sign-out redirect was blocked.')))
    expect(screen.queryByRole('menu')).toBeNull()
    expect(screen.getByRole('alert').textContent).toBe('Sign out failed')
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    expect(screen.getByText('Sign out not completed')).toBeTruthy()
  })
})

describe('profile fallback and consent', () => {
  it('uses real initials for a missing photo without calling it a connection failure', async () => {
    const user = userEvent.setup()
    vi.mocked(fetch).mockResolvedValue(new Response(null, { status: 404 }))
    render(<UserMenu auth={session()} user={actor} />)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    expect(await screen.findByText('No profile photo is set.')).toBeTruthy()
    expect(screen.getByText('CE')).toBeTruthy()
    expect(screen.queryByRole('img')).toBeNull()
    expect(screen.queryByRole('alert')).toBeNull()
    expect(createObjectURL).not.toHaveBeenCalled()
  })
  it.each([
    ['casey.example@example.test', 'CE'],
    ['Casey', 'C'],
    [' Casey   Morgan Example ', 'CE'],
  ])('derives initials from %s rather than substituting an unrelated avatar', (display_name, expected) => {
    render(<UserMenu auth={session({ enabled: false, currentUser: undefined })} user={{ ...actor, display_name }} />)
    expect(screen.getByText(expected)).toBeTruthy()
    expect(fetch).not.toHaveBeenCalled()
  })
  it('supports offline sessions and old auth fixtures without Graph access or fake authentication', async () => {
    const user = userEvent.setup()
    const auth = session({ enabled: false, currentUser: undefined })
    const { rerender } = render(<UserMenu auth={auth} user={actor} />)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    expect(screen.getByText('Local session')).toBeTruthy()
    const signOut = screen.getByRole('menuitem', { name: 'Sign out' })
    expect(signOut.getAttribute('aria-disabled')).toBe('true')
    await user.click(signOut)
    expect(auth.signOut).not.toHaveBeenCalled()
    expect(auth.getProfileToken).not.toHaveBeenCalled()
    expect(fetch).not.toHaveBeenCalled()
    rerender(<UserMenu auth={session({ currentUser: undefined, getProfileToken: undefined })} user={actor} />)
    expect(screen.getByText('Signed in')).toBeTruthy()
    expect(screen.getByText('user-1')).toBeTruthy()
    expect(screen.getByRole('menuitem', { name: 'Sign out' }).getAttribute('aria-disabled')).toBe('false')
  })
  it('does not fabricate initials or fetch photos before a user is known', async () => {
    const user = userEvent.setup()
    const auth = session({ signedIn: false, currentUser: null })
    render(<UserMenu auth={auth} user={null} />)
    await user.click(screen.getByRole('button', { name: 'User menu' }))
    expect(screen.getByText('Not signed in')).toBeTruthy()
    expect(screen.queryByText('CE')).toBeNull()
    expect(screen.queryByText('U')).toBeNull()
    expect(auth.getProfileToken).not.toHaveBeenCalled()
    expect(fetch).not.toHaveBeenCalled()
  })
  it('hides a stale identity when the session is no longer signed in', async () => {
    const user = userEvent.setup()
    render(<UserMenu auth={session({ signedIn: false })} user={actor} />)
    await user.click(screen.getByRole('button', { name: 'User menu' }))
    expect(screen.queryByText('Casey Example')).toBeNull()
    expect(screen.queryByText('casey.example@example.test')).toBeNull()
    expect(screen.getByText('Not signed in')).toBeTruthy()
    expect(fetch).not.toHaveBeenCalled()
  })
  it('shows consent requirements nonblockingly and requests interaction only on an explicit click', async () => {
    const user = userEvent.setup()
    const getProfileToken = vi.fn().mockRejectedValueOnce(new ApiError(401, 'profile_interaction_required', 'Your profile photo needs sign-in or consent.'))
      .mockResolvedValue('graph-only-token')
    const auth = session({ getProfileToken })
    render(<UserMenu auth={auth} user={actor} />)
    const trigger = screen.getByRole('button', { name: /User menu/ })
    await waitFor(() => expect(trigger.getAttribute('aria-describedby')).toBeTruthy())
    expect(fetch).not.toHaveBeenCalled()
    expect(auth.signIn).not.toHaveBeenCalled()
    expect(screen.queryByRole('alert')).toBeNull()
    await user.click(trigger)
    expect(screen.getByRole('status').textContent).toBe('Your profile photo needs sign-in or consent.')
    await user.click(screen.getByRole('menuitem', { name: 'Connect profile photo' }))
    expect(await screen.findByRole('img')).toBeTruthy()
    expect(getProfileToken.mock.calls).toEqual([[{ interactive: false }], [{ interactive: true }]])
    expect(document.activeElement).toBe(screen.getByRole('menuitem', { name: 'Sign out' }))
  })
  it('surfaces a network failure and retries silently only on request', async () => {
    const user = userEvent.setup()
    vi.mocked(fetch).mockRejectedValueOnce(new Error('Offline'))
    const auth = session()
    render(<UserMenu auth={auth} user={actor} />)
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    expect(await screen.findByRole('menuitem', { name: 'Retry profile photo' })).toBeTruthy()
    expect(screen.getByRole('status').textContent).toContain('Cannot reach Microsoft Graph')
    await user.click(screen.getByRole('menuitem', { name: 'Retry profile photo' }))
    expect(await screen.findByRole('img')).toBeTruthy()
    expect(auth.getProfileToken).toHaveBeenNthCalledWith(2, { interactive: false })
    expect(auth.signIn).not.toHaveBeenCalled()
  })
  it('uses initials and revokes the blob if the browser cannot decode a photo', async () => {
    const user = userEvent.setup()
    render(<UserMenu auth={session()} user={actor} />)
    const image = await screen.findByRole('img')
    fireEvent.error(image)
    expect(screen.queryByRole('img')).toBeNull()
    expect(screen.getByText('CE')).toBeTruthy()
    expect(revokeObjectURL).toHaveBeenCalledExactlyOnceWith('blob:test-profile-1')
    await user.click(screen.getByRole('button', { name: /User menu/ }))
    expect(screen.getByRole('status').textContent).toContain('could not be displayed')
    expect(screen.getByRole('menuitem', { name: 'Retry profile photo' })).toBeTruthy()
  })
})

describe('profile request lifetime', () => {
  it('does not refetch on repeated snapshots for the same identity', async () => {
    const auth = session()
    const { rerender } = render(<UserMenu auth={auth} user={actor} />)
    await screen.findByRole('img')
    rerender(<UserMenu auth={auth} user={{ ...actor }} />)
    rerender(<UserMenu auth={auth} user={{ ...actor }} />)
    expect(auth.getProfileToken).toHaveBeenCalledOnce()
    expect(fetch).toHaveBeenCalledOnce()
  })
  it('revokes the old image on account replacement and the new image on unmount', async () => {
    createObjectURL.mockReturnValueOnce('blob:first').mockReturnValueOnce('blob:second')
    const { rerender, unmount } = render(<UserMenu auth={session()} user={actor} />)
    expect((await screen.findByRole('img')).getAttribute('src')).toBe('blob:first')
    const nextActor = { id: 'user-2', display_name: 'Jordan Sample' }
    rerender(<UserMenu auth={session({ currentUser: { ...nextActor, username: 'jordan@example.test' } })} user={nextActor} />)
    expect(screen.queryByRole('img', { name: 'Casey Example profile photo' })).toBeNull()
    await waitFor(() => expect(screen.getByRole('img', { name: 'Jordan Sample profile photo' }).getAttribute('src')).toBe('blob:second'))
    expect(revokeObjectURL).toHaveBeenCalledExactlyOnceWith('blob:first')
    unmount()
    expect(revokeObjectURL.mock.calls).toEqual([['blob:first'], ['blob:second']])
  })
  it('aborts obsolete fetches and prevents their results from replacing the current identity', async () => {
    const first = deferred<Response>()
    vi.mocked(fetch).mockReturnValueOnce(first.promise)
    const firstAuth = session()
    const { rerender } = render(<UserMenu auth={firstAuth} user={actor} />)
    await waitFor(() => expect(fetch).toHaveBeenCalledOnce())
    const firstSignal = vi.mocked(fetch).mock.calls[0]![1]!.signal!
    const nextActor = { id: 'user-2', display_name: 'Jordan Sample' }
    rerender(<UserMenu auth={session({ currentUser: { ...nextActor, username: 'jordan@example.test' } })} user={nextActor} />)
    await screen.findByRole('img', { name: 'Jordan Sample profile photo' })
    expect(firstSignal.aborted).toBe(true)
    await act(async () => first.resolve(photoResponse()))
    expect(createObjectURL).toHaveBeenCalledOnce()
    expect(screen.queryByRole('img', { name: 'Casey Example profile photo' })).toBeNull()
  })
  it('never starts Graph fetch after an unmounted token request completes', async () => {
    const token = deferred<string>()
    const { unmount } = render(<UserMenu auth={session({ getProfileToken: () => token.promise })} user={actor} />)
    unmount()
    await act(async () => token.resolve('obsolete-token'))
    expect(fetch).not.toHaveBeenCalled()
    expect(createObjectURL).not.toHaveBeenCalled()
  })
  it('aborts an in-flight download on unmount without creating an unused blob URL', async () => {
    const request = deferred<Response>()
    vi.mocked(fetch).mockReturnValueOnce(request.promise)
    const { unmount } = render(<UserMenu auth={session()} user={actor} />)
    await waitFor(() => expect(fetch).toHaveBeenCalledOnce())
    const signal = vi.mocked(fetch).mock.calls[0]![1]!.signal!
    unmount()
    expect(signal.aborted).toBe(true)
    await act(async () => request.resolve(photoResponse()))
    expect(createObjectURL).not.toHaveBeenCalled()
  })
})
