# `mozillaonline/` — the `o` curve as a data correction for the Glean-history source

Copy of `data-official/2026-10/mozillaonline/` (curve parquet + csv + model meta, byte-identical) with one spec change (file named `mozillaonline.glean_history.json` so the production resolver's `data-official/*/mozillaonline/mozillaonline.json` glob can never pick it up):
`allocation.flag_column` is `windows` (the OS × country layout has no `modern_windows`), `applies_to_data_source` is
`glean_history_desktop`, gate `2026-10-10`. Applied by `scripts/run_glean_history_forecast.py --overlay-spec mozillaonline/mozillaonline.glean_history.json`.

Why it is in a "no adjustments" build: PROCEDURE.html §9. The June 2026 CN step is an input artefact the raw model cannot fit;
subtracting the measured migration from CN's training rows and adding it back is a correction of the input, decided by Brendan
2026-10-12 (option 1). The curve is applied unscaled (Legacy units; see the spec notes).
