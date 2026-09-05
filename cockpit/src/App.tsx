import { FilterStateProvider, SelectionStoreProvider } from '@/components/dashboard';
import { SketchContext } from '@/hooks/sketch.context';
import { ThemeContext } from '@/hooks/theme.context';
import { useAppSketch } from '@/hooks/use-sketch';
import { useCanonicalDarkTheme } from '@/hooks/use-canonical-dark-theme';
import { CockpitPage } from '@/pages/CockpitPage';

// Monitoring cockpit for the BI triage accelerator.
//
// Read-only by construction: every tile is a DAX query against the Direct Lake
// model over the controller's state, and nothing here can write. The controller
// is the only thing that changes state; this reports what it recorded.
//
// No ThemeToggle in the masthead. The palette is dark-only and canonical (see
// the override block at the end of src/global.css), so a toggle would switch
// between two identical themes.
function App() {
  const theme = useCanonicalDarkTheme();
  const sketch = useAppSketch();

  return (
    <ThemeContext.Provider value={theme}>
      <SketchContext.Provider value={sketch}>
        <SelectionStoreProvider>
          <FilterStateProvider>
            <CockpitPage />
          </FilterStateProvider>
        </SelectionStoreProvider>
      </SketchContext.Provider>
    </ThemeContext.Provider>
  );
}

export default App;

