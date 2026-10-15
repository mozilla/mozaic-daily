# mobile_total_mau_2026-10-10/ — TOTAL mobile MAU = organic (our model) + paid (marketing's plan); THE mobile deliverable (2026-10-15)

Brendan, 2026-10-15: "the handoff for the mobile forecast needs to include both paid and organic" — the organic-only package was not a
deliverable. `scripts/compose_mobile_total_mau.py` (rules in `mozaic_daily.mobile_total_mau`) composes, with no model run:

- organic: `../mobile_organic_mau_2026-10-10/` (prior 0.0065 / range 0.87, organic split, Iran fill, provisional IN + ID iOS-rate cap)
- paid: `../paid_mau_2027/paid_mau_2027.opt_dec15_mau.pull2026-10-15.parquet` (bridge from 2026-10-10 + the 2027 plan), a level on Fenix and world
- history rows = the raw actuals exactly (asserted); the IN + ID cap excess ("inauthentic organic", 440,081) is its own column `organic_inauthentic_`, held flat in the forecast (Brendan 2026-10-15: keep it flat, document it — `../organic_mau/ios_rate_cap/IOS_RATE_CAP.md`; `--cap-policy drop` for the other reading); total = authentic organic + inauthentic organic + paid
- per country: forecast paid by `../paid_mau_2027/paid_country_shares.2026-10-09.csv`; bands: organic (Prophet, from the pickle) and paid (the plan's)
  each as delivered, total = half-widths in quadrature (independence assumed)

| file | what |
|---|---|
| `csv/mobile2027_total_curves.csv` | date, data_type, organic_/organic_inauthentic_/paid_/total_ per population, three world 90% bands |
| `csv/mobile2027_total_canonical_curves.csv` | the total in the canonical `mau_<population>` layout (feeds `plot_glean_history_canonical.py`) |
| `csv/mobile2027_total_by_country.csv`, `csv/mobile2027_total_dec15_summary.csv` | per-country totals; the Dec-15 table with bands |
| `mobile2027_total.meta.json` | inputs with sha1s, rules, numbers |
| `plots/mobile2027_total_components.png`, `plots/mobile2027_total_2x5.png`, `plots/canonical/` | components with bands; per app; canonical format |

Numbers: world total 2026-12-15 58,188,189; **2027-12-15 66,327,324** [90% 61.55M, 71.58M] = authentic organic 51,545,439 + inauthentic organic 440,081 (flat) + paid 14,341,804; seam step −1.4%; Dec→Dec +14.0%, mostly the marketing plan (organic +7.3%). Handoff: `../handoff/glean2027_mobile_mau_handoff/`.
A new paid scenario or a new organic build is a rerun of the composer into a new directory; this one is never edited.
