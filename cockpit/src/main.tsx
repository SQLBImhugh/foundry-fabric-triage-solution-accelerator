import { createRoot } from 'react-dom/client';

import App from '@/App';

import './global.css';

// Monitoring cockpit entry.
//
// No webfonts on purpose. The theme is system faces only — Segoe UI and
// Cascadia Mono — so the kit's bundled Inter / Space Grotesk / JetBrains Mono
// would be ~350 kB of downloads for typefaces the palette explicitly rules out.
//
// This comment was wrong for two commits: it said "no webfont imports", which
// was true of this file, while index.html still carried the scaffold's
// <link> to fonts.googleapis.com. A module-scoped claim cannot describe what
// the HTML shell does. The link is gone; the "Inter"/"Caveat" names left in
// the kit's CSS now fall through to ui-sans-serif and Segoe Print. If you add
// a font, check both places — and note a Fabric App under Private Link has no
// route to fonts.googleapis.com, so a webfont there fails closed to fallback.
//
// Queries run through the Fabric embed proxy (src/lib/fabric-client.ts), which
// Fabric authenticates, so no AuthProvider is needed inside the portal.
createRoot(document.getElementById('root')!).render(<App />);
