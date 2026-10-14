# `linux_bot/` — measuring the Linux bot-like (LLM / automation) component from real Glean alone

Why: the real-Glean MAU comparison extrapolates Linux's 2025–26 surge as trend (COMPARISONS.html §4–5). Brendan (2026-10-13): the
surge is not a trend marker; separate short from long components; the measurement must NOT use the Glean-equivalent series (circular).

Source: `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` (client level; the table the aggregates are built from, same
`is_dau` / `is_mau`), Linux, Firefox Desktop, **10% sample (`sample_id` 0–9) scaled ×10**. SQL in `scripts/sql/glean_real_linux_*.sql`,
run through `bq_query.py --machine --max-gb 5000` on 2026-10-14.

| file | what |
|---|---|
| `linux_bot_segment_shares.csv` | 1st/15th of each month 2025-06 → 2026-10-09, per country: Linux MAU / DAU and the **behavioural bot-like** subset (first seen inside the 28-day window AND ≤ 2 active days), plus the two halves separately |
| `linux_bot_top_isps.csv` | top-15 ISPs by Linux MAU in DE / US / ROW on 2025-06-15 and 2026-10-09 with the behavioural share per ISP |

Findings (2026-10-14): the behavioural definition alone has a ~37% baseline (ordinary churn and id regeneration) and rises to 53% world /
71% DE / 72% US in the surges; per ISP it is near-binary — hosting / cloud providers (Hetzner, Aruba Networks, Azure, Google Cloud, Oracle,
Tencent, Ace Data Centers, Lumen, Strato, DataCrunch, …) run 0.95–1.00, residential ISPs ~0.3. Hosting-ISP Linux MAU on 2026-10-09 sums to
roughly 4M, matching the independent export term (real − Glean-equivalent, +4.2M). Plot: `../plots/linux_bot_segment_shares.png`.
Next: the daily curve (every day, per country) from clients in ISPs whose behavioural share is ≥ 0.8, as the overlay's measured input.
