# mobile_organic_mau_2026-10-10/ — the chosen ORGANIC mobile MAU build (Brendan, 2026-10-15: "pick that one")

The build is round-3 probe `cps0.0065_cpr0.87`: it ran in `../probes_mobile_mau/round3_ioscap/cps0.0065_cpr0.87/cps0.0065_thresh055_recent13_cpr0.87_ncp25_clip0.6_sps0.1/`,
where its fitted pickle (724 MB) and `run.log` stay. This directory holds a copy of the forecast parquet + sidecar + `parameters.json`
(byte-identical) and the exports made from it: `csv/` (curves, Dec-15 summary), `plots/` (+ `plots/canonical/`).

What it is: the production mobile model with the October 2026 lock except changepoint prior scale **0.0065** and changepoint range **0.87**,
on real Glean mobile MAU from 2020-12-31, Fenix training rows scaled to their measured **organic** share (no add-back; marketing forecasts paid),
the Iran rolling-28 MAU fill, and the **provisional** India + Indonesia iOS-rate cap (`../organic_mau/ios_rate_cap/IOS_RATE_CAP.md`).
Why this one: PROCEDURE.html §16 and `../probes_mobile_mau/round3.html` (seam −1.5%, Dec→Dec +7.3%; the frontier's best compromise).
Rollback of the cap = rerun without `--ios-rate-cap` into a new directory; this one is never edited.
