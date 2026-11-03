"""Render the README architecture diagram from SVG to PNG.

GitHub renders inline SVG in markdown inconsistently -- it strips some
attributes and refuses others outright -- so the README references a PNG. The
SVG is the source of truth; re-run this after editing it.

Needs Playwright, which is not a dependency of the accelerator itself:

    python -m pip install playwright && python -m playwright install chromium
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README_IMAGES = ROOT / "docs" / "images" / "readme"
DIAGRAMS: tuple[tuple[Path, int, int], ...] = (
    (README_IMAGES / "solution-architecture.svg", 1240, 750),
    (README_IMAGES / "azure-architecture.svg", 1360, 890),
)


def _find_edge() -> str | None:
    for candidate in (
        shutil.which("msedge"),
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ):
        if candidate and Path(candidate).exists():
            return str(candidate)
    return None


def _render_with_edge(edge_bin: str) -> None:
    with tempfile.TemporaryDirectory(prefix="bi-triage-svg-") as profile_dir:
        for svg, width, height in DIAGRAMS:
            png = svg.with_suffix(".png")
            subprocess.run(
                [
                    edge_bin,
                    "--headless=new",
                    f"--user-data-dir={profile_dir}",
                    "--disable-gpu",
                    "--hide-scrollbars",
                    "--force-device-scale-factor=2",
                    f"--window-size={width},{height}",
                    f"--screenshot={png}",
                    svg.as_uri(),
                ],
                check=True,
            )
            print(f"wrote {png.relative_to(ROOT)} ({png.stat().st_size:,} bytes)")


def main() -> None:
    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError:
        edge_bin = _find_edge()
        if edge_bin is not None:
            _render_with_edge(edge_bin)
            return
        sys.exit(
            "This script needs Playwright or Microsoft Edge.\n"
            "  python -m pip install playwright\n"
            "  python -m playwright install chromium"
        )

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch()
        except Exception:
            browser = p.chromium.launch(channel="msedge")
        for svg, width, height in DIAGRAMS:
            png = svg.with_suffix(".png")
            page = browser.new_page(
                viewport={"width": width, "height": height},
                device_scale_factor=2,
            )
            page.goto(svg.as_uri())
            page.wait_for_timeout(400)
            page.screenshot(
                path=str(png),
                clip={"x": 0, "y": 0, "width": width, "height": height},
            )
            page.close()
            print(f"wrote {png.relative_to(ROOT)} ({png.stat().st_size:,} bytes)")
        browser.close()


if __name__ == "__main__":
    main()
