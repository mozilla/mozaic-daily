# calculator/ — the 2027 MAU what-if toy (stakeholder sensitivity page)

**What's here:** `index.html`, one self-contained page (no external libraries or fonts) built by
`scripts/build_mau_sensitivity_calculator.py` from `handoff/glean2027_mau_desktop_and_mobile.csv` and the Legacy desktop MAU
pull in `raw_legacy_osxcountry_2026-10-09/`. Deployable as-is with `quick deploy` (site names must start `brwells-`).

**What it is:** a deliberate cartoon. Real 2026 MAU (weekly points) to the last actual, then per platform
`last actual × S(day)/S(last day) × (1 + yearly rate)^years`, where `S` is a simplified typical year (median ratio of each calendar day
to its centred 365-day mean, fitted with four yearly waves; desktop from Legacy since 2020, mobile from Glean since 2021). Because `S`
repeats yearly, the slider is exactly the Dec-15-to-Dec-15 change. Sliders start at the rate that lands the cartoon on the pre-draft
builds' 2027-12-15 values (desktop −4.0%/yr, mobile +12.5%/yr at the 0.5 step). The slider panel is sticky (stays at the top while the
charts scroll; static below 800 px wide). Above each slider is a histogram of the real year-over-year change, read every two weeks
over the past two years. Desktop has a radio button for the source (Brendan 2026-10-15): **Legacy** (default; 53 readings, median
−4.5%), **Simulated history** (the Glean-equivalent build-5 input; median −4.3%), **Actual Glean** (11 readings since 2026-05-22 only,
median −6.5%). Mobile: Glean (median +7.0%). Desktop + mobile are summed: MAU counts profiles, and desktop and mobile profiles are distinct.

The ~2% desktop decline sometimes quoted is build 5's *forecast* for 2027 (−1.9% Dec→Dec) or the last four months of readings
(−2.5% to 0%); the two-year historical median is about −4.5% in every source and window checked (1–5 years, MAU and DAU).

**What isn't here:** any real forecast curve. Never add the pre-draft curves to this page.

**Where new code goes:** page drawing in `scripts/mau_calculator_template.html`; numbers in the builder, the typical-year maths in
`scripts/mau_toy_seasonality.py` (its JavaScript twin is in the template; `tests/test_mau_toy_seasonality.py` runs both under node and
fails if they drift); tests in `tests/test_build_mau_sensitivity_calculator.py`. Rebuilding refreshes the freshness banner.
