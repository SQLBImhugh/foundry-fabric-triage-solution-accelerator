import { createRoot } from 'react-dom/client';

import App from '@/App';

import './global.css';

// Monitoring cockpit entry.
//
// No webfont imports on purpose. The theme is system faces only — Segoe UI and
// Cascadia Mono — so the kit's bundled Inter / Space Grotesk / JetBrains Mono
// would be ~350 kB of downloads for typefaces the palette explicitly rules out.
//
// Queries run through the Fabric embed proxy (src/lib/fabric-client.ts), which
// Fabric authenticates, so no AuthProvider is needed inside the portal.
createRoot(document.getElementById('root')!).render(<App />);
