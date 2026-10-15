Firefox Mobile ORGANIC MAU forecast through 2027 — PRE-DRAFT (generated 2026-10-15 11:11 PDT)

Open index.html. It explains the numbers, shows the charts, and describes the two CSVs:
  glean2027_mobile_organic_mau_forecast.csv     daily series 2020-12-31..2027-12-31: date, data_type, world, fenix_android, firefox_ios, focus_android, focus_ios (raw daily value of the rolling 28-day MAU)
  glean2027_mobile_organic_mau_by_country.csv  the same statistic per country (all apps) from 2024-01-01
  plots/                              the charts (PNG)
Beside this package: glean2027_mau_desktop_and_mobile_organic.csv — desktop and organic mobile world MAU side by side (do not sum them).

Headline: organic mobile MAU (all apps) on 2027-12-15 = 51,545,439 (raw daily value of the rolling 28-day MAU). History ends 2026-10-09; rows after that are forecast.
Pre-draft: production mobile model (October 2026 settings; changepoint prior scale 0.0065, range 0.87)
on real Glean MAU from 2020-12-31; Firefox Android scaled to its measured ORGANIC share, nothing paid added back (marketing forecasts paid);
Iran 2026 shutdown bridged in training; India + Indonesia organic capped at their own iOS growth from 2026-04-01 (provisional); not yet backtested.
Seam step (7-day means, forecast vs last week of history): -1.5%.
Source: mozaic-daily branch glean-2027-forecast @ 73dc6ba, build mobile_organic_mau_2026-10-10. Owner: Brendan Wells.
