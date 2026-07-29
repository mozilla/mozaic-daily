# Aug-trough search — Round 2 findings: seasonality_regime as a SHAPE lever (2026-07-10)

Motivation: Round 1 showed sps (amplitude) can't lift the summer trough without dropping the Dec peak
symmetrically, and Dec-15 must hold within 10k (±≤100K headwind). Tested whether `seasonality_regime`
decouples trough from peak. Anchor 2026-07-06, desktop legacy-DAU, headwind display-layer.

## Result table (post-headwind Global; Dec-15 Δ vs locked 48,585,483)

| probe | regime | Aug trough | gap vs 45.06M | Dec-15 | Dec-15 Δ |
|---|---|---|---|---|---|
| auto sps0.00825 (center) | auto | 43,246,576 | −1,813,424 | 48,585,483 | +0 |
| auto sps0.003 | auto | 45,287,305 | +227,305 | 45,668,335 | −2,917,148 |
| **mult sps0.00825** | **multiplicative** | **44,539,723** | **−520,277** | **48,778,383** | **+192,900** |
| mult sps0.05 | multiplicative | 44,531,645 | −528,355 | 48,771,640 | +186,157 |
| mult sps0.5 | multiplicative | 44,536,639 | −523,361 | 48,785,646 | +200,163 |
| mult sps2.0 | multiplicative | 44,537,647 | −522,353 | 48,777,538 | +192,055 |
| mult sps10 | multiplicative | 44,534,409 | −525,591 | 48,787,181 | +201,698 |
| add sps0.00825 | additive | 43,243,611 | −1,816,389 | 48,586,588 | +1,105 |
| add sps0.003 | additive | 45,275,127 | +215,127 | 45,667,152 | −2,918,331 |

## Conclusions

1. **Multiplicative decouples trough from peak — the shape lever we needed.** vs the auto center it lifts
   the summer trough **+1.29M** (43.25M→44.54M) while moving the Dec peak only **+0.19M** (48.59M→48.78M):
   a ~6.7:1 Aug:Dec asymmetry, exactly because multiplicative scales the swing with trend level (higher
   into Dec). This is qualitatively different from the amplitude (sps) 1:1.4 symmetric trade.
2. **sps is inert under multiplicative.** All sps ∈ {0.00825…10} give Aug ~44.53–44.54M / Dec ~48.78M.
   So under multiplicative the operating point is fixed; sps stops being a knob.
3. **additive ≈ auto here** (additive sps0.00825 = 43.24M/48.59M ≈ center; additive+low-sps = auto+low-sps).
   The auto per-tile mix behaves like additive at these settings.
4. **Pure multiplicative lands: Aug 44.54M (−0.52M short of bullseye), Dec +193K (over the 10k tol).**
   Not a finished answer, but 71% of the needed +1.81M Aug lift with the peak nearly held.

## Open items / Round 3 proposal
- Multiplicative gets most of the way but leaves **Aug −0.52M short** and **Dec +193K high**. Test whether
  the trend knobs (cps/cpr/ncp/recent) have MORE leverage under multiplicative's **linear growth** (the
  logistic cap is off), which could add the remaining Aug lift and/or trim Dec — a decoupled 2-knob solve.
- **Caveat for any final lock:** forced global multiplicative overrides the data-driven per-tile regime
  for the ~40% of tiles auto makes additive (generalizability concern Brendan flagged). If multiplicative
  is the path, weigh that; a cleaner alternative is a dedicated seasonal-shape lever (yearly Fourier
  order / summer overlay) that reshapes without a global regime override.
- The Dec +193K overshoot can't be cheaply headwind-trimmed (needs +193K headwind add, >100K unreasonable,
  and it drops Aug ~106K). Prefer to close Dec via a trend/shape knob.
