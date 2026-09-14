import { useCallback, useEffect, useId, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'
import { ChevronDown, CircleAlert, LogOut, RefreshCw, UserRound } from 'lucide-react'
import type { AuthSession } from '../api/auth'
import { ApiError, asApiError } from '../api/errors'
import { loadProfilePhoto } from '../api/profile'
import type { Snapshot } from '../api/types'
import './UserMenu.css'

export interface UserMenuProps {
  auth: AuthSession
  user: Pick<Snapshot['actor'], 'id' | 'display_name'> | null
}

interface PhotoState {
  owner: string
  status: 'unavailable' | 'loading' | 'ready' | 'missing' | 'error'
  url: string | null
  error: ApiError | null
}

const interactivePhotoErrors = new Set([
  'profile_interaction_required', 'profile_sign_in_required', 'profile_access_denied', 'profile_auth_error',
])

function initialsFor(name: string): string {
  const words = name.split('@')[0]?.split(/[\s._-]+/).filter(Boolean) ?? []
  const first = words[0]
  const last = words.length > 1 ? words[words.length - 1] : undefined
  return `${first ? Array.from(first)[0] ?? '' : ''}${last ? Array.from(last)[0] ?? '' : ''}`.toLocaleUpperCase()
}

function menuItems(menu: HTMLDivElement | null): HTMLButtonElement[] {
  return Array.from(menu?.querySelectorAll<HTMLButtonElement>('[role="menuitem"]') ?? [])
}

export function UserMenu({ auth, user }: UserMenuProps) {
  const id = useId()
  const root = useRef<HTMLDivElement>(null)
  const trigger = useRef<HTMLButtonElement>(null)
  const menu = useRef<HTMLDivElement>(null)
  const openingFocus = useRef<'first' | 'last' | null>(null)
  const photoRequest = useRef<AbortController | null>(null)
  const photoUrl = useRef<string | null>(null)
  const mounted = useRef(true)
  const signOutPending = useRef(false)
  const [open, setOpen] = useState(false)
  const [signingOut, setSigningOut] = useState(false)
  const [signOutError, setSignOutError] = useState<ApiError | null>(null)
  const name = auth.enabled && !auth.signedIn ? ''
    : auth.currentUser?.display_name.trim() || user?.display_name.trim() || auth.currentUser?.username.trim() || ''
  const displayName = name || 'User'
  const initials = initialsFor(name)
  const identity = auth.enabled && !auth.signedIn ? ''
    : auth.currentUser?.username || user?.id || auth.currentUser?.id || ''
  const owner = auth.currentUser?.id ?? user?.id ?? ''
  const getProfileToken = auth.enabled && auth.signedIn ? auth.getProfileToken : undefined
  const [photo, setPhoto] = useState<PhotoState>({ owner, status: 'unavailable', url: null, error: null })
  const currentPhoto = photo.owner === owner && getProfileToken ? photo : null
  const photoError = currentPhoto?.error
  const photoNeedsReload = photoError?.code === 'profile_account_changed'
  const connectPhoto = Boolean(photoError && interactivePhotoErrors.has(photoError.code))
  const photoLoading = Boolean(getProfileToken && (!currentPhoto || currentPhoto.status === 'loading'))
  const showPhotoAction = Boolean(getProfileToken && currentPhoto?.status !== 'ready')
  const canSignOut = auth.enabled && auth.signedIn
  const photoMessage = photoError?.message
    || (photoLoading ? 'Loading profile photo...'
      : currentPhoto?.status === 'missing' ? 'No profile photo is set.'
        : !getProfileToken ? 'Profile photos are not requested in this session.' : '')

  const releasePhoto = useCallback(() => {
    if (photoUrl.current) URL.revokeObjectURL(photoUrl.current)
    photoUrl.current = null
  }, [])

  const refreshPhoto = useCallback(async (interactive = false) => {
    photoRequest.current?.abort()
    releasePhoto()
    const controller = new AbortController()
    photoRequest.current = controller
    setPhoto({ owner, status: getProfileToken ? 'loading' : 'unavailable', url: null, error: null })
    if (!getProfileToken) return
    try {
      const image = await loadProfilePhoto(getProfileToken, controller.signal, { interactive })
      if (controller.signal.aborted) return
      const url = image ? URL.createObjectURL(image) : null
      photoUrl.current = url
      setPhoto({ owner, status: image ? 'ready' : 'missing', url, error: null })
    } catch (error) {
      if (!controller.signal.aborted) setPhoto({ owner, status: 'error', url: null, error: asApiError(error) })
    }
  }, [getProfileToken, owner, releasePhoto])

  useEffect(() => {
    void refreshPhoto()
    return () => {
      photoRequest.current?.abort()
      releasePhoto()
    }
  }, [refreshPhoto, releasePhoto])

  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])

  useEffect(() => {
    if (!open) return
    const closeOutside = (event: Event) => {
      if (event.target instanceof Node && !root.current?.contains(event.target)) setOpen(false)
    }
    document.addEventListener('pointerdown', closeOutside)
    document.addEventListener('focusin', closeOutside)
    return () => {
      document.removeEventListener('pointerdown', closeOutside)
      document.removeEventListener('focusin', closeOutside)
    }
  }, [open])

  useEffect(() => {
    if (!open) return
    const items = menuItems(menu.current)
    if (openingFocus.current || !root.current?.contains(document.activeElement)) {
      const item = openingFocus.current === 'last' ? items[items.length - 1] : items[0]
      item?.focus()
      openingFocus.current = null
    }
  }, [open, showPhotoAction])

  function openMenu(edge: 'first' | 'last' = 'first') {
    openingFocus.current = edge
    setOpen(true)
    if (open) {
      const items = menuItems(menu.current)
      const item = edge === 'first' ? items[0] : items[items.length - 1]
      item?.focus()
      openingFocus.current = null
    }
  }

  function closeMenu() {
    setOpen(false)
    trigger.current?.focus()
  }

  function onTriggerKeyDown(event: KeyboardEvent<HTMLButtonElement>) {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault()
      openMenu(event.key === 'ArrowDown' ? 'first' : 'last')
    } else if (event.key === 'Escape' && open) {
      event.preventDefault()
      closeMenu()
    }
  }

  function onMenuKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    if (event.key === 'Escape') {
      event.preventDefault()
      event.stopPropagation()
      closeMenu()
      return
    }
    if (event.key === 'Tab') {
      // Start normal tab navigation at the trigger before removing the focused menu item.
      if (event.shiftKey) event.preventDefault()
      closeMenu()
      return
    }
    const items = menuItems(menu.current)
    const index = items.findIndex((item) => item === document.activeElement)
    let next: HTMLButtonElement | undefined
    if (event.key === 'ArrowDown') next = items[(index + 1) % items.length]
    else if (event.key === 'ArrowUp') next = items[(index - 1 + items.length) % items.length]
    else if (event.key === 'Home') next = items[0]
    else if (event.key === 'End') next = items[items.length - 1]
    else if (event.key.length === 1 && event.key.trim() && !event.ctrlKey && !event.altKey && !event.metaKey) {
      next = [...items.slice(index + 1), ...items.slice(0, index + 1)]
        .find((item) => item.textContent?.trim().toLocaleLowerCase().startsWith(event.key.toLocaleLowerCase()))
    }
    if (next) {
      event.preventDefault()
      next.focus()
    }
  }

  async function signOut() {
    if (!canSignOut || signOutPending.current) return
    signOutPending.current = true
    setSigningOut(true)
    setSignOutError(null)
    try {
      await auth.signOut()
    } catch (error) {
      if (mounted.current) setSignOutError(asApiError(error))
    } finally {
      signOutPending.current = false
      if (mounted.current) setSigningOut(false)
    }
  }

  function photoFailed() {
    if (!currentPhoto?.url || photoUrl.current !== currentPhoto.url) return
    releasePhoto()
    setPhoto({ owner, status: 'error', url: null, error: new ApiError(0, 'invalid_profile_photo', 'Your profile photo could not be displayed. Try loading it again.') })
  }

  return <div className="user-menu" ref={root}>
    <button type="button" className="user-menu-trigger" ref={trigger} id={`${id}-trigger`}
      aria-label={`User menu${name ? ` for ${name}` : ''}`} aria-haspopup="menu" aria-expanded={open}
      aria-controls={open ? `${id}-menu` : undefined} aria-describedby={photoError || signOutError ? `${id}-status` : undefined}
      onClick={() => open ? closeMenu() : openMenu()} onKeyDown={onTriggerKeyDown}>
      <span className="user-menu-avatar">
        {currentPhoto?.url ? <img src={currentPhoto.url} alt={`${displayName} profile photo`} onError={photoFailed} />
          : initials ? <span aria-hidden="true">{initials}</span> : <UserRound size={20} aria-hidden="true" />}
      </span>
      <span className="user-menu-trigger-label"><span className="user-menu-name">{displayName}</span>
        {signOutError && <span className="user-menu-signout-summary" role="alert">Sign out failed</span>}
      </span>
      {photoError && <CircleAlert size={13} className="user-menu-photo-warning" aria-hidden="true" />}
      <ChevronDown size={13} aria-hidden="true" />
    </button>
    <span id={`${id}-status`} className="sr-only">{signOutError?.message || photoError?.message}</span>
    {open && <div className="user-menu-dropdown">
      <div className="user-menu-identity" id={`${id}-identity`}>
        <span className="user-menu-heading">User</span>
        <strong>{displayName}</strong>
        {identity && identity !== name && <span className="user-menu-account">{identity}</span>}
        <span className="user-menu-session">{!auth.enabled ? 'Local session' : auth.signedIn ? 'Signed in' : 'Not signed in'}</span>
      </div>
      {photoMessage && <p className={`user-menu-photo-status${photoError ? ' user-menu-photo-warning' : ''}`} role="status">{photoMessage}</p>}
      {signOutError && <div className="user-menu-signout-error" role="alert"><strong>Sign out not completed</strong><p>{signOutError.message}</p><span>{signOutError.code}</span></div>}
      <div role="menu" id={`${id}-menu`} aria-labelledby={`${id}-trigger`} aria-describedby={`${id}-identity`} ref={menu} onKeyDown={onMenuKeyDown}>
        {showPhotoAction && <button type="button" className="user-menu-item" role="menuitem" tabIndex={-1}
          aria-disabled={photoLoading || photoNeedsReload} onClick={() => { if (!photoLoading && !photoNeedsReload) void refreshPhoto(connectPhoto) }}>
          <RefreshCw size={15} className={photoLoading ? 'spin' : undefined} aria-hidden="true" />
          {photoLoading ? 'Loading profile photo' : photoNeedsReload ? 'Reload required for photo' : connectPhoto ? 'Connect profile photo' : 'Retry profile photo'}
        </button>}
        <button type="button" className="user-menu-item" role="menuitem" tabIndex={-1} aria-disabled={!canSignOut || signingOut} onClick={() => void signOut()}>
          <LogOut size={16} aria-hidden="true" />{signingOut ? 'Signing out...' : 'Sign out'}
        </button>
      </div>
      {!canSignOut && <p className="user-menu-session-note">Sign out is unavailable without a signed-in account.</p>}
    </div>}
  </div>
}
