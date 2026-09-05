import { useEffect } from "react";

/**
 * Pin the app to the canonical dark palette.
 *
 * The kit's `useAppTheme` follows the host: it reads `data-appearance` on
 * `<html>`, watches `prefers-color-scheme`, and toggles the `dark` class. The
 * Fabric portal sets `data-appearance`, so in a light-themed portal the class
 * is removed — and that matters beyond CSS, because `readGrapheinTheme` picks
 * its base from exactly that class:
 *
 *     base: root.classList.contains("dark") ? "dark" : "light"
 *
 * Without the class, every chart renders on a white canvas inside otherwise
 * dark cards. The CSS variables alone cannot fix it; graphein resolves its own
 * palette from the class.
 *
 * This accelerator's palette is dark-only and has no light variant, so the
 * honest thing is to assert the class and keep asserting it. The observer
 * re-adds it if the portal or a kit hook removes it later.
 */
export function useCanonicalDarkTheme(): { isDark: true; toggleTheme: () => void } {
    useEffect(() => {
        const root = document.documentElement;
        const pin = () => {
            if (!root.classList.contains("dark")) root.classList.add("dark");
        };
        pin();

        // The portal can flip `data-appearance` after first paint, and the kit
        // reacts to it. Re-assert rather than race.
        const observer = new MutationObserver(pin);
        observer.observe(root, {
            attributes: true,
            attributeFilter: ["class", "data-appearance"],
        });
        return () => observer.disconnect();
    }, []);

    return { isDark: true, toggleTheme: () => {} };
}
