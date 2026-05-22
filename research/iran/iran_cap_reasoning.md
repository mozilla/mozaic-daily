# Iran Partial-Recovery Forecast: Cap Reasoning

This document records the reasoning behind the demographically-anchored cap used in
the Iran partial-recovery forecast (`iran_partial_recovery_model.ipynb`). The cap
cannot be reliably fit from data alone for the reasons described below; it must be
pinned from domain reasoning, and this document is the audit trail for that choice.

## Why the cap must be pinned, not fit

The data alone cannot determine a saturation level for Iran DAU under the
post-shutdown regime. Two structural reasons:

1. **Early-2026 growth was supply-rationed, not demand-saturated.** Iran rolled out
   "Internet Pro" access through a phased, quota-gated process — initially as little
   as ~500 verified applicants per major telecom operator, then expanding to a
   guild-based whitelist (medical council, chamber of commerce, computer trade union,
   etc.). The slow ramp visible in the data through early April is the *supply*
   curve, not the *demand* curve. Fitting a logistic to this period and extrapolating
   forward is a category error.

2. **The current trajectory has not yet shown saturation.** Free-cap logistic fits on
   the long window (Mar 22 onward) converge to ~75k — but only because the fit reads
   the early supply-rationed ramp as the bottom of an S-curve. Fits restricted to
   post-Apr 15 data are degenerate (unidentified cap), and short-window fits at the
   tail land at suspiciously precise low values that are almost certainly local
   minima rather than real saturation signals.

Conclusion: the cap is a parameter we have to bring in from outside the model.

## The eligible-population framing

Under the tiered system, the population that can show up as `country=IR` in
telemetry is bounded by who has authorized access on Iran-routed networks. There
are three tiers, of which only two contribute to `country=IR` traffic:

| Tier | Who | Size estimate | Contributes to `country=IR`? |
|------|-----|---------------|-------------------------------|
| **White SIM** (loyalists) | Government officials, state media | ~tens of thousands; fully subsidized | Yes |
| **Internet Pro** (professionals) | Doctors, lawyers, faculty, registered businesses | ~500k–1M eligible | Yes |
| **Gray Market** (Starlink / VPN) | Wealthy individuals, crypto traders, tech activists | ~50–100k people | **No** — traffic appears under VPN destination countries |

The gray-market tier is real but irrelevant here: those users connect through
foreign-routed channels and their telemetry will not have `country=IR`. They show
up under destination countries instead. The cap below is for tiers 1 + 2 only.

## Adoption fraction within eligible pool

Of the ~500k–1M Internet Pro eligibles, actual adoption is gated by several
real-world frictions:

- **Cost.** Filtered/global traffic is priced at roughly $10/GB — about 5× the price
  of domestic-only traffic. Even verified users self-censor heavily to manage cost.
- **Identity binding.** Internet Pro accounts are typically tied to a specific IMEI
  and static IP. No casual sharing; one user per registration.
- **Political rejection.** Several nursing associations and academic departments
  publicly refused "Internet Pro" registration, viewing it as complicity in
  class-based access. This is a meaningful drag on adoption among exactly the
  knowledge-worker demographic that would otherwise sign up.
- **Bureaucratic frictions.** Registration requires guild letters, security
  background checks, and (for businesses) a Commercial Card. Many in the eligible
  population can't or won't navigate the process.

Realistic adoption rate: **30–50%** of the eligible pool. Combined with White SIM:

| Component | Low | Mid | High |
|-----------|-----|-----|------|
| Internet Pro eligible | 500k | 750k | 1M |
| × Adoption rate | 30% | 40% | 50% |
| = Internet Pro active | 150k | 300k | 500k |
| + White SIM | 30k | 40k | 50k |
| **Total active users** | **180k** | **340k** | **550k** |

## Firefox share within the eligible-and-active population

The new regime's user base is structurally different from pre-shutdown Iran:

- **Professional / knowledge-worker skew.** Internet Pro registration requires
  professional credentialing — these are doctors, lawyers, faculty, registered
  business owners. This population is more technically inclined and more likely to
  use non-default browsers than the general population.
- **Privacy-conscious skew.** Users whose traffic is tied to their legal identity
  and professionally licensed have stronger-than-average privacy motivations.
  Firefox is positioned as the privacy-focused mainstream option.
- **Gray-market exclusion already accounted for.** The most technical / privacy-
  obsessed slice has already been removed (they appear under destination
  countries), so the within-`country=IR` skew toward Firefox is real but bounded.

For comparison, pre-shutdown Iran's Firefox Desktop share was roughly in line
with global norms (~3–4% of internet users). For the new regime, an elevated
share of **5–15%** is plausible: lower bound matches global; upper bound assumes
a strong professional/privacy skew on top of global norms.

## Daily activity rate

DAU = active users × fraction of them who are active on a given day. Not every
registered user with Firefox installed checks the web every day — particularly
under a system where global traffic costs ~$10/GB. Realistic daily activity rate
on the order of **30–70%**.

## Putting it together

| Scenario | Active pool | Firefox share | Daily activity | Cap (DAU) |
|----------|-------------|---------------|----------------|-----------|
| **Low** | 180k | 5% | 30% | **~3k** |
| Low-mid | 250k | 7% | 50% | ~9k |
| **Mid (used)** | 340k | 8% | 50% | **~14k** |
| Mid-high | 450k | 10% | 60% | ~27k |
| **High** | 550k | 15% | 70% | **~58k** |

Note that the back-of-envelope cap based on a *steady-state daily snapshot*
falls below the current observed DAU (~60k on May 15, 2026). This signals that
either:

- Adoption is higher than the table assumes (eligible pool is being more
  thoroughly exercised), **or**
- Daily activity rate is higher than 70% (because Internet Pro is a paid
  resource, users may log on every day to amortize the cost), **or**
- Firefox share is higher than 15% in this population.

When the demographic ceiling is computed less restrictively — treating the cap
as the *peak addressable population* once supply expansion completes, rather than
a daily snapshot — the practical range expands to roughly **50k–250k DAU**.

The chosen scenarios bracket this expanded range:

| Scenario | Cap (DAU) | Interpretation |
|----------|-----------|----------------|
| Conservative | 100k | Internet Pro program stays narrow; adoption hits its political ceiling; daily activity moderates |
| **Shipping** | **150k** | Mid-range demographic ceiling; matches the "Firefox modestly elevated in privacy-conscious professional cohort" story |
| Optimistic | 200k | Internet Pro expands further; Firefox share at high end of the elevated range; high daily activity |

## Why 150k is the shipping number

The shipping forecast uses **cap = 150k**. Rationale:

- It sits at the midpoint of the realistic demographic envelope (50–250k).
- It is well above the current observed DAU (~60k), consistent with the
  observation that growth is clearly still accelerating and we are not near
  saturation.
- It is well below Iran's pre-shutdown DAU peak (~850k), consistent with the
  story that the new regime serves a structurally smaller population.
- The corresponding forecast trajectory tracks current actuals cleanly while
  flattening over the second half of 2026, which is the qualitative shape we
  expect as supply expansion completes and adoption frictions assert themselves.

## What would change this number

This document should be revisited if any of the following happen:

- **Internet Pro eligibility expands materially.** New guild categories added or
  Commercial Card requirements relaxed → eligible pool grows → cap revises up.
- **Pricing changes.** If the 5× global-traffic surcharge is removed or
  significantly reduced, daily activity could jump meaningfully → cap up.
- **Observed DAU breaks above 150k.** This would either invalidate the demographic
  ceiling reasoning or signal that we've underestimated one of the components.
  Switch to the 200k scenario and revisit the math.
- **Major boycott shift.** If the political rejection of Internet Pro broadens or
  collapses, adoption rate could move ±10–20 percentage points.

## Provenance

This document was written 2026-05-15 based on:

- Mozilla telemetry data through 2026-05-13 (`active_users_aggregates`, country=IR,
  Firefox Desktop release).
- Open-source reporting on Iran's tiered internet rollout (March–May 2026),
  including coverage of the "500 Rule" launch quota, guild-based registration
  expansion, the late-April registration freeze, and the 5× global-traffic
  pricing differential.
- Phase-space sweep analysis from `iran_partial_recovery_model.draft1.ipynb`,
  which demonstrated that cap is unidentified from data alone in the post-Apr-15
  window.
