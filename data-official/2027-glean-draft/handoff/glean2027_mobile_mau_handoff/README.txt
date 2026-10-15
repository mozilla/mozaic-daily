Firefox Mobile MAU forecast through 2027 (organic + paid) — PRE-DRAFT (generated 2026-10-15 11:59 PDT)

Open index.html. TOTAL mobile MAU = authentic organic (our model) + inauthentic organic (India/Indonesia excess, held flat) + paid (the marketing DS team's 2027 plan,
scenario opt_dec15_mau), with 90% bands for organic, paid and the total.
Using an AI agent? Point it at HANDOFF.md first.
  glean2027_mobile_mau_forecast.csv     daily 2020-12-31..2027-12-31: date, data_type, total_/organic_/organic_inauthentic_/paid_ for world + per app, world bands (raw daily value of the rolling 28-day MAU)
  glean2027_mobile_mau_by_country.csv  total per country (all apps) from 2024-01-01
  plots/                           the charts (PNG)
Beside this package: glean2027_mau_desktop_and_mobile.csv — desktop and mobile world MAU side by side (do not sum them).

Headline: TOTAL mobile MAU (all apps) on 2027-12-15 = 66,327,324 [90% 61,554,555, 71,576,697] = organic 51,545,439 + inauthentic 440,081 + paid 14,341,804.
History ends 2026-10-09; rows after that are forecast. Seam step (7-day means) -1.4%.
Pre-draft: not backtested; organic band = Prophet predictive (uncalibrated); paid band = the plan's own; India + Indonesia organic cap provisional.
Source: mozaic-daily branch glean-2027-forecast @ 8fcdc44, build mobile_total_mau_2026-10-10. Owner: Brendan Wells.
