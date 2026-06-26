"""Build a self-contained side-by-side comparison of the original notebook charts
vs the MozillaOnline-scenario charts.

"Old" = the desktop/mobile handoff charts rendered by the final cell of
``tmp_downloads/kpi_forecast_desktop_looker_replica_executed.ipynb`` (cell id
``88490d45``), extracted from the notebook's saved outputs.
"New" = ``desktop_mozillaonline.png`` / ``mobile_with_april.png`` from this folder.

The old PNGs are also written to disk (``old_*_notebook.png``) so the comparison
no longer depends on the transient notebook once built. The HTML embeds every image
as base64, so it is fully portable on its own.

Run:
    source .venv/bin/activate
    python3 data-official/2026-06/update_scenarios/build_comparison_html.py
"""

from __future__ import annotations

import base64
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
NOTEBOOK = os.path.join(
    REPO_ROOT, "tmp_downloads", "kpi_forecast_desktop_looker_replica_executed.ipynb"
)
OLD_CELL_ID = "88490d45"  # final cell: plot_canonical_handoff -> [desktop, mobile]

OUT_HTML = os.path.join(HERE, "comparison.html")
OLD_DESKTOP_PNG = os.path.join(HERE, "old_desktop_notebook.png")
OLD_MOBILE_PNG = os.path.join(HERE, "old_mobile_notebook.png")
NEW_DESKTOP_PNG = os.path.join(HERE, "desktop_mozillaonline.png")
NEW_MOBILE_PNG = os.path.join(HERE, "mobile_with_april.png")


def extract_old_pngs() -> tuple[bytes, bytes]:
    """Pull the two PNG outputs (desktop, mobile) from the notebook's final cell."""
    nb = json.load(open(NOTEBOOK))
    cell = next(c for c in nb["cells"] if c.get("id") == OLD_CELL_ID)
    pngs = [
        base64.b64decode(o["data"]["image/png"])
        for o in cell.get("outputs", [])
        if "data" in o and "image/png" in o["data"]
    ]
    if len(pngs) != 2:
        raise RuntimeError(f"expected 2 PNG outputs in cell {OLD_CELL_ID}, found {len(pngs)}")
    return pngs[0], pngs[1]


def b64_img(path: str) -> str:
    with open(path, "rb") as fh:
        return base64.b64encode(fh.read()).decode("ascii")


def img_tag(b64: str) -> str:
    return f'<img src="data:image/png;base64,{b64}" alt="" />'


ROWS = [
    {
        "title": "Desktop DAU",
        "old": OLD_DESKTOP_PNG,
        "new": NEW_DESKTOP_PNG,
        "change": "Added <b>June Forecast + MozillaOnline</b> (solid pink/purple-red; "
                  "June +Iran is solid purple, ex-Iran solid blue): the June +Iran forecast with a +500k daily-DAU "
                  "step on 2026-06-02 (a 28-day ramp in MA space). Dec-15 28dMA 47.8M &rarr; 48.3M.",
    },
    {
        "title": "Mobile DAU",
        "old": OLD_MOBILE_PNG,
        "new": NEW_MOBILE_PNG,
        "change": "Added the prior <b>April Forecast</b> line (purple dashed), "
                  "clipped to start 2026-04-01. June +Iran is solid purple, ex-Iran solid blue.",
    },
]


def build_html() -> str:
    sections = []
    for row in ROWS:
        sections.append(f"""
    <section class="cmp">
      <h2>{row['title']}</h2>
      <p class="change">{row['change']}</p>
      <div class="pair">
        <figure>
          <figcaption>Original (notebook)</figcaption>
          {img_tag(b64_img(row['old']))}
        </figure>
        <figure>
          <figcaption>New (MozillaOnline scenario)</figcaption>
          {img_tag(b64_img(row['new']))}
        </figure>
      </div>
    </section>""")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>June 2026 Forecast — MozillaOnline scenario comparison</title>
<style>
  :root {{ color-scheme: light; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    margin: 0; padding: 2rem; background: #f6f7f9; color: #1a1a1a;
  }}
  header {{ max-width: 1400px; margin: 0 auto 1.5rem; }}
  h1 {{ font-size: 1.6rem; margin: 0 0 .4rem; }}
  header p {{ margin: .2rem 0; color: #555; font-size: .95rem; }}
  .cmp {{
    max-width: 1400px; margin: 0 auto 2rem; background: #fff; border: 1px solid #e2e5ea;
    border-radius: 10px; padding: 1.25rem 1.5rem 1.5rem; box-shadow: 0 1px 3px rgba(0,0,0,.05);
  }}
  .cmp h2 {{ font-size: 1.2rem; margin: 0 0 .3rem; }}
  .change {{ margin: 0 0 1rem; font-size: .92rem; color: #333; line-height: 1.45; }}
  .pair {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.25rem; }}
  figure {{ margin: 0; }}
  figcaption {{
    font-size: .8rem; font-weight: 600; text-transform: uppercase; letter-spacing: .04em;
    color: #6b7280; margin-bottom: .4rem;
  }}
  figure:last-child figcaption {{ color: #6d28d9; }}
  img {{ width: 100%; height: auto; display: block; border: 1px solid #eceef1; border-radius: 6px; }}
  @media (max-width: 900px) {{ .pair {{ grid-template-columns: 1fr; }} }}
  footer {{ max-width: 1400px; margin: 1rem auto 0; color: #888; font-size: .8rem; }}
</style>
</head>
<body>
<header>
  <h1>June 2026 Forecast — MozillaOnline scenario comparison</h1>
  <p>Left: the original handoff charts (notebook). Right: the same charts with the
     scenario overlays added. Both built from the June canonical ALL-level 28dMA curves.</p>
</header>
{''.join(sections)}
<footer>
  Self-contained (images embedded). Source: <code>data-official/2026-06/update_scenarios/</code>.
  Regenerate with <code>build_comparison_html.py</code>.
</footer>
</body>
</html>
"""


def main() -> None:
    old_desktop, old_mobile = extract_old_pngs()
    with open(OLD_DESKTOP_PNG, "wb") as fh:
        fh.write(old_desktop)
    with open(OLD_MOBILE_PNG, "wb") as fh:
        fh.write(old_mobile)

    with open(OUT_HTML, "w") as fh:
        fh.write(build_html())
    print(f"Wrote {OLD_DESKTOP_PNG}")
    print(f"Wrote {OLD_MOBILE_PNG}")
    print(f"Wrote {OUT_HTML}")


if __name__ == "__main__":
    main()
