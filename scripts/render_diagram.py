"""Render the README architecture diagram from SVG to PNG.

GitHub renders inline SVG in markdown inconsistently -- it strips some
attributes and refuses others outright -- so the README references a PNG. The
SVG is the source of truth; re-run this after editing it.

Needs Playwright, which is not a dependency of the accelerator itself:

    python -m pip install playwright && python -m playwright install chromium
"""
from __future__ import annotations

import sys
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:  # pragma: no cover - a tooling path, not a runtime one
    sys.exit(
        "This script needs Playwright, which the accelerator does not depend on.\n"
        "  python -m pip install playwright\n"
        "  python -m playwright install chromium"
    )

ROOT = Path(__file__).resolve().parents[1]
SVG = ROOT / "docs" / "images" / "readme" / "solution-architecture.svg"
PNG = SVG.with_suffix(".png")


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1240, "height": 750},
                                device_scale_factor=2)
        page.goto(SVG.as_uri())
        page.wait_for_timeout(400)
        page.screenshot(path=str(PNG), clip={"x": 0, "y": 0, "width": 1240, "height": 750})
        browser.close()
    print(f"wrote {PNG.relative_to(ROOT)} ({PNG.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
