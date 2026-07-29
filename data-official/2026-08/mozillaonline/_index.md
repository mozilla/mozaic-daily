# `data-official/2026-08/mozillaonline/` — MozillaOnline → Firefox migration tailwind (`o`)

MozillaOnline (the China distribution partner) is migrating users onto mainline Firefox desktop;
migrating users flip `app_name` and newly count as `Firefox Desktop`. Modelled as a bidirectional
overlay on `legacy_desktop` DAU, `modern_windows` segment — same machinery as `l`, but allocated by
**fixed geo shares** (CN ~92.8%, IR excluded, renormalized over training-present countries) rather than
trailing DAU share, because MozillaOnline has a fixed geographic footprint.

modern_windows-only **by measurement**: the pre-transition source population is 92% on recent Firefox
within modern_windows, while older-Windows (winX) users are 99% pinned on Firefox too old to receive
the migrating build, so they do not migrate.

| file | role |
|---|---|
| `mozillaonline.json` | the spec — gated on `applies_to_forecast_start: 2026-07-28` |
| `mozillaonline_migration_model.official.2026-06-29.parquet` | the curve (`migration_dau_daily`) |
| `mozillaonline_migration_model.official.2026-06-29.meta.json` | model provenance |

## Carried forward from July — STALE

Byte-identical copies of `../../2026-07/mozillaonline/` (Brad Ochocki Szasz's official model); only
`applies_to_forecast_start` moved. Curve shape: rise → peak ~724K/day around 2026-07-27 → churn-decline
to ~607K by Dec-15, held flat at the 2026-12-31 28d-MA (~550K) through the 2027-12-31 horizon (no cliff).

**Known conservative bias.** July recorded that the ramp was deliberately slower than actuals already
showed, so the forecast was expected to sit *below* realized data by design. Five more weeks of training
rows now subtract that under-stated curve, leaving real migration signal in the trend. Direction is
known; magnitude is not measured.

**Re-measure and swap before this cycle ships.** Uses the generic `desktop_overlay` spec type, so a
swap is a drop-in `data_file` change. Note the distinct `sentinel_attr`
(`mozillaonline_subtracted`) is what lets `o` stack with `l` on the same desktop training frame.

**Where new files go:** refreshed curve builds for this cycle and their provenance metas; diagnostic
plots under `plots/`. July's measurement writeups and handoffs stay in `../../2026-07/mozillaonline/` —
do not duplicate them here.
