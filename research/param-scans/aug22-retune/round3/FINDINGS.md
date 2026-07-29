# Aug-trough search — Round 3 findings: trend knobs under multiplicative (2026-07-10)

Tested whether trend knobs (cps/cpr/ncp/recent) gain leverage under multiplicative's linear growth
(logistic cap off) — enough to add the +0.52M Aug still missing after Round 2, holding Dec near 48.585M.
Base = mult_center (Aug 44,539,723 / Dec +192,900). All probes regime=multiplicative.

## Result (post-headwind Global; Δ vs mult_center on Aug, Dec-15 Δ vs 48.585M)

| probe | Aug trough | ΔAug vs mult | gap vs 45.06M | Dec-15 Δ |
|---|---|---|---|---|
| mult_center | 44,539,723 | — | −520,277 | +192,900 |
| cps 0.05 / 0.12 / 0.16 / 0.20 / 0.30 | 44.53–44.54M | ±≤6K | ~−520K | ~+190K |
| **cpr 0.55** | 44,640,182 | **+100K** | −419,818 | +241,000 |
| cpr 0.60 | 44,613,462 | +74K | −446,538 | +221,230 |
| cpr 0.70 / 0.80 / 0.90 | 44.54 / 44.47 / 44.43M | −2K / −71K / −114K | | +205K / −112K / +186K |
| ncp 20 / 35 | 44,548,114 / 44,576,778 | +8K / +37K | −483,222 (ncp35) | +162K / +161K |
| recent 8 / 20 | 44,544,327 / 44,515,857 | +5K / −24K | | +202K / +175K |

## Conclusions

1. **Trend knobs are weak under multiplicative too.** cps/recent are inert (±≤40K); ncp weak (+37K max);
   **cpr is the only real lever (~±100–114K)**, cpr↓ raising Aug. Best single probe cpr0.55 = 44.64M
   (still −0.42M short) and it *worsens* Dec to +241K.
2. **Parametric ceiling reached.** Across all three rounds the levers are: sps (symmetric amplitude —
   kills Dec), regime=multiplicative (shape — the big +1.29M lift but caps ~44.54M), and trend knobs
   (~±100–150K even stacked). **No exposed parametric combination reaches Aug 45.06M while holding Dec
   within 10k.** Aggressive cpr combos under mult would add maybe +250–300K (→~44.8M) but push Dec
   further over — still short and out of Dec-band.
3. Multiplicative's Dec sits **+160–240K high** across these probes — over the 10k tolerance, and the
   Aug-raising knob (cpr↓) makes it worse. So multiplicative-for-lock does not satisfy both bands.

## Recommendation for the endgame

The 3-round search has *demonstrated* params can't impose the mean_shape summer without breaking Dec —
which is the evidence that justifies a **targeted seasonal-shape overlay**:
- Stay on the production **auto** regime + locked params. Add a **summer-trough overlay** (new adjustment
  code) that lifts the Aug 28d-MA to 45.06M and **tapers to ~0 by Nov/Dec so the winter peak is held**.
- Bidirectional like `l`/`o`/`m` (subtract from training pre-mozaic, add back to forecast) so Prophet
  doesn't extrapolate it. Uses the mean_shape curve Brendan already has (this IS that correction).
- Hits the target exactly, holds Dec by construction, keeps the defensible per-tile auto regime, and
  makes the regime-generalizability question moot.

Alternatives: (a) accept a parametric compromise on the frontier (best ~44.6M / Dec +240K — both out of
band); (b) expose yearly Fourier order (another package handoff) and gamble that higher-order reshaping
helps — less controllable than an overlay.
