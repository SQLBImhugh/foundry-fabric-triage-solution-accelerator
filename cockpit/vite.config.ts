import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react-swc';
import { resolve } from 'path';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': resolve(import.meta.dirname, 'src'),
    },
  },
  build: {
    target: 'es2022',
    rollupOptions: {
      output: {
        // Split the two large, rarely-changing dependencies out of the app
        // chunk. The dashboard redeploys whenever a query or panel changes,
        // and without this every one of those deploys invalidates ~800 kB of
        // React and charting code that did not change. Splitting them lets the
        // browser keep both across deploys and re-fetch only the app.
        manualChunks: {
          react: ['react', 'react-dom'],
          graphein: ['graphein'],
        },
      },
    },
  },
  esbuild: {
    target: 'es2022',
  },
  optimizeDeps: {
    esbuildOptions: {
      target: 'es2022',
    },
  },
});
