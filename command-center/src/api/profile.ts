import { ApiError, isAborted } from './errors'

// User.Read permits the signed-in user's photo without directory-wide photo access.
// https://learn.microsoft.com/en-us/graph/api/profilephoto-get?view=graph-rest-1.0
export const PROFILE_SCOPE = 'https://graph.microsoft.com/User.Read'
export const PROFILE_PHOTO_URL = 'https://graph.microsoft.com/v1.0/me/photo/$value'

export interface ProfileTokenOptions {
  interactive?: boolean
}

export type ProfileTokenProvider = (options?: ProfileTokenOptions) => Promise<string>

export async function loadProfilePhoto(
  getProfileToken: ProfileTokenProvider,
  signal: AbortSignal,
  options: ProfileTokenOptions = {},
): Promise<Blob | null> {
  signal.throwIfAborted()
  const token = await getProfileToken(options)
  signal.throwIfAborted()
  if (!token) throw new ApiError(401, 'profile_sign_in_required', 'Sign in to load your profile photo.')

  let response: Response
  try {
    response = await fetch(PROFILE_PHOTO_URL, {
      headers: { Accept: 'image/*', Authorization: `Bearer ${token}` },
      signal,
      credentials: 'omit',
      cache: 'no-store',
      redirect: 'error',
    })
  } catch (error) {
    // DOMException is not an Error in every host, including the offline DOM.
    if (isAborted(error) || (error instanceof DOMException && error.name === 'AbortError')) throw error
    throw new ApiError(0, 'profile_network_error', 'Cannot reach Microsoft Graph for your profile photo. Check the connection and try again.')
  }
  signal.throwIfAborted()
  if (response.status === 404) return null
  if (response.status === 401) {
    throw new ApiError(401, 'profile_sign_in_required', 'Microsoft Graph could not authenticate your profile photo request. Select Connect profile photo to sign in again.')
  }
  if (response.status === 403) {
    throw new ApiError(403, 'profile_access_denied', 'Microsoft Graph denied access to your profile photo. Your organization may need to allow delegated User.Read.')
  }
  if (!response.ok) {
    throw new ApiError(response.status, 'profile_request_failed', `Microsoft Graph could not load your profile photo (HTTP ${response.status}). Try again later.`)
  }
  const contentType = response.headers.get('Content-Type')?.split(';')[0]?.trim().toLowerCase()
  if (!contentType || !['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/avif', 'image/bmp'].includes(contentType)) {
    throw new ApiError(response.status, 'invalid_profile_photo', 'Microsoft Graph did not return a supported profile photo image.')
  }
  let photo: Blob
  try {
    photo = await response.blob()
  } catch (error) {
    if (isAborted(error) || (error instanceof DOMException && error.name === 'AbortError')) throw error
    throw new ApiError(0, 'profile_network_error', 'The profile photo download was interrupted. Check the connection and try again.')
  }
  signal.throwIfAborted()
  if (!photo.size) throw new ApiError(response.status, 'invalid_profile_photo', 'Microsoft Graph returned an empty profile photo.')
  return photo
}
