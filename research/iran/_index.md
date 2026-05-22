# `research/iran/` — Iran internet-shutdown workaround

Iran went offline ~2026-02-28. Because IR is a top-DAU market, its missing/zero telemetry corrupted world-level forecasts. This cluster documents the spec, the synthetic-DAU methodology, and the partial-recovery model used to keep producing realistic numbers during the outage.

## Files

| File | What it does |
|---|---|
| `IRAN_SPEC.md` | Canonical spec for the workaround: how Iran is excluded from queries and added back via summation |
| `iran_partial_recovery_methodology.md` | Pinned logistic + log-OLS seasonality used to project a recovery curve |
| `iran_cap_reasoning.md` | Rationale for the 150k peak-cap on the partial-recovery curve |
| `iran_partial_recovery_mobile.TODO.md` | Open items for porting the desktop recovery model to mobile |
| `iran_partial_recovery_model.ipynb` | The model: leadership-approved 2026-05-15 |
| `compare_no_iran.ipynb` | Mobile comparison (45-country sum vs. world DAU) for the no-Iran subset |
| `compare_no_iran_desktop.ipynb` | Desktop version of the same comparison |
| `data/` | Iran-comparison parquets (gitignored): no-Iran actuals at country/desktop/mobile level, plus mobile actuals with synthetic Iran |

## Producers

- Synthetic Iran historical + forecast data: `scripts/generate_iran_synthetic.py` → `data-official/iran_synthetic/iran_synthetic.parquet`
- Add synthetic Iran to a no-Iran forecast: `scripts/add_iran_to_forecast.py`
