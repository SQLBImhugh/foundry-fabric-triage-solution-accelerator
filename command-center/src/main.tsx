import { Component, StrictMode, useEffect, useState } from 'react'
import type { ErrorInfo, ReactNode } from 'react'
import { createRoot } from 'react-dom/client'
import { ArrowRight, ShieldCheck } from 'lucide-react'
import { App } from './App'
import { initializeAuth } from './api/auth'
import type { AuthSession } from './api/auth'
import { ApiClient, loadConfig } from './api/client'
import type { AppConfig } from './api/types'
import { asApiError } from './api/errors'
import type { ApiError } from './api/errors'
import { ErrorNotice, LoadingState } from './components/shared'
import './styles.css'

interface Runtime { config: AppConfig; auth: AuthSession; api: ApiClient }

// StrictMode remounts effects in development. Redirect processing must run once.
let initialization: Promise<Runtime> | undefined
function initialize(): Promise<Runtime> {
  initialization ??= (async () => {
    const config = await loadConfig()
    const auth = await initializeAuth(config)
    return { config, auth, api: new ApiClient(auth.getToken) }
  })().catch((error: unknown) => { initialization = undefined; throw error })
  return initialization
}

function Bootstrap() {
  const [runtime, setRuntime] = useState<Runtime | null>(null)
  const [error, setError] = useState<ApiError | null>(null)
  const [attempt, setAttempt] = useState(0)
  const [signingIn, setSigningIn] = useState(false)
  useEffect(() => {
    let active = true
    setError(null)
    initialize().then((result) => { if (active) setRuntime(result) }).catch((caught: unknown) => { if (active) setError(asApiError(caught)) })
    return () => { active = false }
  }, [attempt])
  async function signIn() {
    if (!runtime) return
    setSigningIn(true)
    setError(null)
    try { await runtime.auth.signIn() } catch (caught) { setError(asApiError(caught)); setSigningIn(false) }
  }
  if (runtime?.auth.signedIn) return <App {...runtime} />
  return <div className="boot-screen"><div className="boot-panel"><div className="brand-mark"><img src="/triage-logo.png" alt="BI triage" width="44" height="52" /></div><span className="eyebrow">BI triage / Operations</span><h1>{runtime?.config.app_name ?? 'Command center'}</h1><p>Review evidence, decide on proposed actions, and follow recorded outcomes.</p>
    {error && <ErrorNotice error={error} retry={() => setAttempt((value) => value + 1)} />}
    {runtime ? <><div className="notice tone-info"><ShieldCheck size={19} aria-hidden="true" /><p>Sign in with your work account. Permissions and available targets are determined by the server.</p></div><button type="button" className="button primary full-width" onClick={() => void signIn()} disabled={signingIn}>{signingIn ? 'Redirecting to sign-in...' : 'Sign in with Microsoft'}<ArrowRight size={17} aria-hidden="true" /></button></> : !error && <LoadingState label="Loading API configuration" />}
  </div></div>
}

class AppErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false }
  static getDerivedStateFromError() { return { failed: true } }
  componentDidCatch(error: Error, info: ErrorInfo) { console.error('Command center rendering failed', error, info.componentStack) }
  render() {
    if (this.state.failed) return <div className="boot-screen"><div className="boot-panel" role="alert"><h1>Records could not be displayed</h1><p>The application encountered an unexpected response or rendering error. No action has been inferred from it.</p><button type="button" className="button primary" onClick={() => window.location.reload()}>Reload command center</button></div></div>
    return this.props.children
  }
}

const root = document.getElementById('root')
if (!root) throw new Error('The application root is missing.')
createRoot(root).render(<StrictMode><AppErrorBoundary><Bootstrap /></AppErrorBoundary></StrictMode>)
