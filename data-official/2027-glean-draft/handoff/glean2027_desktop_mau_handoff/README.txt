Firefox Desktop MAU forecast through 2027 — PRE-DRAFT (generated 2026-10-15 09:51 PDT)

Open index.html. It explains the numbers, shows the charts, and describes the two CSVs.
Using an AI agent? Point it at HANDOFF.md first.
  glean2027_desktop_mau_forecast.csv     daily series 2025-05-22..2027-12-31: date, data_type, world, windows, mac, linux (raw daily value of the rolling 28-day MAU)
  glean2027_desktop_mau_by_country.csv  the same statistic per country (all OS) from 2024-01-01

  plots/                           the charts (PNG)

Headline: world MAU on 2027-12-15 = 161,546,764 (raw daily value of the rolling 28-day MAU). History ends 2026-10-09; rows after that are forecast.
Pre-draft: trained on 17 months of real Glean telemetry, re-tuned for a short history; Linux automated traffic measured and held flat (4.2M MAU).
Source: mozaic-daily branch glean-2027-forecast @ b026cd3, build comparison_glean_real_mau_additive_b_2026-10-14. Owner: Brendan Wells.
