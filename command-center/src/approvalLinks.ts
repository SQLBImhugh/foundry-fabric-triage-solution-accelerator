const authStateKey = 'command_center_approval'

function requestId(value: string | null): string | null {
  return value?.trim() ? value : null
}

export function approvalFromSearch(search: string): string | null {
  return requestId(new URLSearchParams(search).get('approval'))
}

export function approvalSignInState(search: string): string | undefined {
  const approval = approvalFromSearch(search)
  return approval ? new URLSearchParams({ [authStateKey]: approval }).toString() : undefined
}

export function approvalFromSignInState(state: string | undefined): string | null {
  return requestId(new URLSearchParams(state).get(authStateKey))
}
