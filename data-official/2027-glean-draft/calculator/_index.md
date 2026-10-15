# calculator/ — the 2027 MAU what-if toy (stakeholder sensitivity page)

**What's here:** `index.html`, one self-contained page (no external libraries or fonts) built by
`scripts/build_mau_sensitivity_calculator.py` from `handoff/glean2027_mau_desktop_and_mobile.csv` and the Legacy desktop MAU
pull in `raw_legacy_osxcountry_2026-10-09/`. Deployable as-is with `quick deploy` (site names must start `brwells-`).

**What it is:** a deliberate cartoon. Real 2026 MAU (weekly points) to the last actual, then `last actual × (1 + yearly rate)`
per platform, with no seasonality and no model. Sliders start at the steady rate that lands on the pre-draft builds' 2027-12-15 values
(desktop −4.0%/yr, mobile +14.5%/yr, rounded to the 0.5 slider step). Above each slider is a histogram of the year-over-year change
actually seen over the past two years, read every two weeks (53 readings): **desktop from Legacy telemetry** (Brendan 2026-10-15,
called out on the page; real Glean desktop only starts 2025-05-22, and the Glean-equivalent history agrees within 0.3 pt), mobile
from real Glean mobile. Desktop + mobile are summed: MAU counts profiles, and desktop and mobile profiles are distinct.

**What isn't here:** any real forecast curve. Never add the pre-draft curves to this page.

**Where new code goes:** page drawing in `scripts/mau_calculator_template.html`; numbers in the builder; tests in
`tests/test_build_mau_sensitivity_calculator.py`. Rebuilding refreshes the freshness banner.
