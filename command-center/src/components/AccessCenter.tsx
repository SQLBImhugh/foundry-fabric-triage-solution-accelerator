import { useCallback, useId, useReducer, useRef } from 'react'
import { ExternalLink, RefreshCw, ShieldCheck } from 'lucide-react'
import { ENTRA_MANAGEMENT_URL } from '../api/access'
import type { AccessApiClient } from '../api/access'
import { ApiError } from '../api/errors'
import type { Mode } from '../api/types'
import { useResource } from '../hooks/useResource'
import { Badge, ErrorNotice, LoadingState } from './shared'
import './AccessCenter.css'

export function AccessCenter({
  api, fresh, mode = 'live', refreshAccess, refreshError = null,
}: {
  api: AccessApiClient
  fresh: boolean
  mode?: Mode
  refreshAccess?: () => Promise<void>
  refreshError?: ApiError | null
}) {
  const id = useId()
  const [revision, requestRefresh] = useReducer((value: number) => value + 1, 0)
  const refreshing = useRef(false)
  const canRefresh = mode === 'demo' || Boolean(refreshAccess)
  const load = useCallback(async (signal: AbortSignal) => {
    try {
      if (revision > 0) {
        if (mode === 'live' && !refreshAccess) {
          throw new ApiError(0, 'access_refresh_unavailable', 'This session cannot request a fresh API token. Reload or sign in again.')
        }
        await refreshAccess?.()
        if (signal.aborted) throw new DOMException('Access refresh was canceled.', 'AbortError')
      }
      const result = await api.current(signal)
      if (result.source !== (mode === 'demo' ? 'synthetic_demo' : 'entra_app_roles')) {
        throw new ApiError(200, 'access_source_mismatch',
          'The API access source does not match the application mode. Reload the app; no current permissions have been inferred.')
      }
      return result
    } finally {
      if (!signal.aborted) refreshing.current = false
    }
  }, [api, mode, refreshAccess, revision])
  const access = useResource(`effective-access:${mode}`, load, 0, !refreshError || revision > 0)
  const error = refreshError ?? access.error
  const data = error || access.loading ? null : access.data
  const expired = Boolean(data?.token_expires_at && Date.parse(data.token_expires_at) <= Date.now())
  const stale = !fresh || expired
  const missingMetadata = mode === 'demo' ? 'Not available in synthetic demo' : 'Not reported by the API'

  function refreshPermissions() {
    if (!canRefresh || access.loading || refreshing.current) return
    refreshing.current = true
    requestRefresh()
  }

  function tokenTime(value: string | null) {
    return value ? <time dateTime={value} title={value}>{new Date(value).toLocaleString()}</time> : missingMetadata
  }

  return <div className="access-center">
    <header className="page-heading">
      <div><span className="eyebrow">Application access</span><h1>Access &amp; permissions</h1>
        <p>Your effective app roles and where IT manages access.</p></div>
      <div className="page-actions">
        <button type="button" className="button secondary" onClick={refreshPermissions}
          disabled={access.loading || !canRefresh} aria-describedby={`${id}-refresh-help`}>
          <RefreshCw size={15} aria-hidden="true" className={access.loading ? 'spin' : ''} />Refresh permissions
        </button>
      </div>
    </header>
    <div className={`notice ${mode === 'demo' ? 'tone-warning' : 'tone-info'}`}>
      <ShieldCheck size={18} aria-hidden="true" /><div>
        <strong>{mode === 'demo' ? 'Synthetic demo permissions' : 'Access is managed in Microsoft Entra ID'}</strong>
        <p>{mode === 'demo'
          ? 'These are synthetic identities and app roles, not evidence of Entra access. No directory access is checked or changed.'
          : 'This page displays app roles reported by the API. It cannot grant or revoke access, change groups, or edit directory accounts.'}</p>
      </div>
    </div>
    {!canRefresh && <div className="notice tone-warning" role="status">
      This session cannot request a fresh API token. Reload or sign in again to refresh permissions.
    </div>}
    {error && <ErrorNotice error={error}>
      <p>Effective permissions are unavailable. No permissions have been inferred from earlier responses.</p>
    </ErrorNotice>}
    {access.loading && <LoadingState label={revision ? 'Refreshing permissions' : 'Loading effective access'} />}
    {data && <>
      {stale && <div className="notice tone-warning" role="status">
        {expired ? 'The reported API token has expired. Refresh permissions.' : 'The command-center snapshot is not current.'}
        {' '}Access details below are stale, read-only records, not evidence of current permissions.
      </div>}
      <div className="access-panels">
        <section className="access-panel" aria-labelledby={`${id}-user`}>
          <div className="panel-heading"><h2 id={`${id}-user`}>Current user</h2>
            <Badge tone={mode === 'demo' ? 'warning' : 'info'}>{mode === 'demo' ? 'Synthetic demo' : 'Entra app roles'}</Badge></div>
          <div className="access-panel-body">
            <p className="access-source">{mode === 'demo'
              ? 'Identity and roles from the synthetic demo API.'
              : 'Identity and roles from the validated API access token, not a directory membership lookup.'}</p>
            <dl className="access-facts">
              <div><dt>Display name</dt><dd>{data.current_user.display_name}</dd></div>
              <div><dt>User ID</dt><dd><code>{data.current_user.id}</code></dd></div>
              <div><dt>Tenant ID</dt><dd>{data.tenant_id ? <code>{data.tenant_id}</code> : missingMetadata}</dd></div>
              <div><dt>Application ID</dt><dd>{data.application_id ? <code>{data.application_id}</code> : missingMetadata}</dd></div>
            </dl>
          </div>
        </section>
        <section className="access-panel" aria-labelledby={`${id}-token`}>
          <div className="panel-heading"><h2 id={`${id}-token`}>Token freshness</h2></div>
          <div className="access-panel-body">
            <dl className="access-facts">
              <div><dt>Token issued</dt><dd>{tokenTime(data.token_issued_at)}</dd></div>
              <div><dt>Token expires</dt><dd>{tokenTime(data.token_expires_at)}</dd></div>
              <div><dt>Details received</dt><dd>{access.receivedAt !== null
                && <time dateTime={new Date(access.receivedAt).toISOString()}>{new Date(access.receivedAt).toLocaleString()}</time>}</dd></div>
            </dl>
            <p className="access-source">{mode === 'demo'
              ? 'No Entra token is issued or renewed for this demo.'
              : 'Token dates describe the API token used for this response, not a new directory check. Missing dates are not inferred.'}
              {' '}Times are shown in your browser&apos;s local time zone.</p>
          </div>
        </section>
      </div>
      <section className="access-panel" aria-labelledby={`${id}-roles`}>
        <div className="panel-heading"><div><h2 id={`${id}-roles`}>Effective app roles</h2>
          <p>{stale ? 'Roles in the last received API response.' : 'Roles reported as effective by the API for this session.'}</p></div></div>
        <ul className="access-role-list" aria-label="Reported app roles">
          {data.role_catalog.filter((role) => data.current_user.roles.includes(role.id)).map((role) =>
            <li key={role.id}><Badge>{role.label}</Badge></li>)}
        </ul>
        <div className="access-table-scroll" role="region" aria-label="Application role permissions" tabIndex={0}>
          <table className="access-table">
            <caption className="sr-only">App role definitions and effective roles reported by the API, not group membership</caption>
            <thead><tr><th scope="col">App role</th><th scope="col">What it permits</th><th scope="col">In this response</th></tr></thead>
            <tbody>{data.role_catalog.map((role) => <tr key={role.id}>
              <th scope="row">{role.label}</th><td>{role.description}</td>
              <td>{data.current_user.roles.includes(role.id) ? 'Reported as effective' : 'Not reported'}</td>
            </tr>)}</tbody>
          </table>
        </div>
        <p className="access-role-help">Higher app roles include Reader access. Operator and Approver are separate;
          neither includes the other. Admin includes all app capabilities, not directory administration.</p>
      </section>
    </>}
    <section className="access-panel access-management" aria-labelledby={`${id}-management`}>
      <div className="panel-heading"><h2 id={`${id}-management`}>Manage access in Entra</h2></div>
      <div className="access-panel-body">
        <p>{mode === 'demo' ? 'For a live deployment, IT' : 'IT'} and group owners manage four Entra security groups
          mapped to the Reader, Operator, Approver and Admin roles. Access changes happen in Entra, not in this app.</p>
        <ol>
          <li>Ask IT or your group owner to review the appropriate security group membership.</li>
          <li>In the Entra admin center, navigate to <strong>Entra ID &gt; Enterprise applications &gt; this app &gt; Users and groups</strong>.
            Use the application ID reported above to identify this app, or ask IT if it is not available.</li>
          <li>IT manages the groups&apos; app role assignments. Group owners manage membership in those groups.</li>
        </ol>
        <a className="button secondary" href={ENTRA_MANAGEMENT_URL} target="_blank" rel="noopener noreferrer">
          Open Microsoft Entra admin center<ExternalLink size={14} aria-hidden="true" /><span className="sr-only"> (opens in a new tab)</span>
        </a>
        <p>This page does not read a user roster or group memberships and cannot tell you which group supplied a role.</p>
        <p><strong>App-only permissions.</strong> These roles do not grant Azure, Fabric or Entra directory permissions.</p>
      </div>
    </section>
    <section className="access-refresh-help" aria-labelledby={`${id}-renewal`}>
      <h2 id={`${id}-renewal`}>After an access change</h2>
      <p id={`${id}-refresh-help`}>{mode === 'demo'
        ? 'Refresh permissions reloads synthetic access details and the command-center snapshot. No Entra token is requested.'
        : 'Refresh permissions requests a fresh token for this API, then reloads effective access and the command-center snapshot. It does not open sign-in or consent popups automatically.'}</p>
      <p>Role changes can take effect on token renewal. Refreshing your session does not revoke other users&apos; already-issued tokens.
        The backend checks app roles on every request.</p>
    </section>
  </div>
}
