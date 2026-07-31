"""Builder for the Win10 anchor HTML report.

Holds the report prose and embeds the three figures from ``plots/`` as base64,
so the output is a single self-contained file that can be forwarded without the
repo. Edit the prose here, not in the generated HTML.

Run from the repository root:

    python3 research/headwinds/build_report.py
"""
import base64
from pathlib import Path

PLOTS = Path("research/headwinds/plots")
OUT = Path("research/headwinds/win10_anchor_report.html")


def fig(name: str) -> str:
    return base64.b64encode((PLOTS / name).read_bytes()).decode("ascii")


CSS = """
:root {
  --ink: #1c1c1a; --ink-soft: #4a4a45; --ink-faint: #6e6e66;
  --bg: #fcfcfa; --card: #ffffff; --rule: #e0ded6;
  --accent: #7a2e2e; --accent-soft: #f6efe9;
  --good: #1f5c46; --warn: #8a5a12;
  --measure: 35em;
}
@media (prefers-color-scheme: dark) {
  :root {
    --ink: #e8e6e0; --ink-soft: #b8b5ac; --ink-faint: #8f8c84;
    --bg: #17171a; --card: #1f1f23; --rule: #33333a;
    --accent: #e08d8d; --accent-soft: #2a2020;
    --good: #7fc4a8; --warn: #d9a94e;
  }
}
* { box-sizing: border-box; }
html { -webkit-text-size-adjust: 100%; }
body {
  margin: 0; padding: 0 1.25rem 6rem;
  background: var(--bg); color: var(--ink);
  font: 400 19px/1.65 Charter, Georgia, "Iowan Old Style", "Times New Roman", serif;
  font-feature-settings: "kern" 1, "liga" 1, "onum" 1;
}
.wrap { max-width: var(--measure); margin: 0 auto; }
.wide { max-width: 52em; margin: 0 auto; }

header.title { padding: 4.5rem 0 1rem; }
header.title .kicker {
  font: 600 12px/1.4 ui-sans-serif, system-ui, -apple-system, sans-serif;
  letter-spacing: .13em; text-transform: uppercase; color: var(--accent);
  margin: 0 0 1.1rem;
}
h1 {
  font: 600 2.45rem/1.13 Charter, Georgia, serif;
  letter-spacing: -.018em; margin: 0 0 .85rem; text-wrap: balance;
}
.standfirst {
  font-size: 1.16rem; line-height: 1.55; color: var(--ink-soft);
  margin: 0 0 1.6rem; text-wrap: pretty;
}
.byline {
  font: 400 14px/1.6 ui-sans-serif, system-ui, sans-serif;
  color: var(--ink-faint); border-top: 1px solid var(--rule);
  padding-top: .9rem; margin: 0;
}
.byline code { font-size: .87em; }

h2 {
  font: 600 1.5rem/1.25 Charter, Georgia, serif;
  letter-spacing: -.012em; margin: 3.4rem 0 .2rem; text-wrap: balance;
}
h2 + .sub {
  font: 400 15px/1.5 ui-sans-serif, system-ui, sans-serif;
  color: var(--ink-faint); margin: 0 0 1.3rem;
}
h3 {
  font: 600 1.08rem/1.35 ui-sans-serif, system-ui, sans-serif;
  margin: 2.3rem 0 .55rem; letter-spacing: -.005em;
}
p { margin: 0 0 1.15rem; text-wrap: pretty; }
strong { font-weight: 650; }
a { color: var(--accent); text-decoration-thickness: 1px; text-underline-offset: 2px; }
code {
  font: 400 .855em/1.5 ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  background: var(--accent-soft); padding: .1em .34em; border-radius: 3px;
  overflow-wrap: anywhere;
}

.verdict {
  background: var(--card); border: 1px solid var(--rule);
  border-left: 3px solid var(--accent);
  border-radius: 4px; padding: 1.4rem 1.6rem .3rem; margin: 2rem 0 2.4rem;
}
.verdict h2 { margin-top: 0; font-size: 1.2rem; }
.verdict p:last-child { margin-bottom: 1.1rem; }

.callout {
  background: var(--card); border: 1px solid var(--rule);
  border-radius: 4px; padding: 1.15rem 1.4rem .2rem; margin: 1.9rem 0;
}
.callout .lbl {
  font: 600 11px/1.4 ui-sans-serif, system-ui, sans-serif;
  letter-spacing: .12em; text-transform: uppercase;
  color: var(--warn); margin: 0 0 .6rem;
}
.callout p:last-child { margin-bottom: 1.1rem; }

figure { margin: 2.4rem 0; }
figure img {
  width: 100%; height: auto; display: block;
  background: #fff; border: 1px solid var(--rule); border-radius: 4px;
  padding: 6px;
}
figcaption {
  font: 400 14px/1.55 ui-sans-serif, system-ui, sans-serif;
  color: var(--ink-faint); margin-top: .7rem;
}
figcaption b { color: var(--ink-soft); font-weight: 600; }

table {
  width: 100%; border-collapse: collapse; margin: 1.7rem 0;
  font: 400 15px/1.5 ui-sans-serif, system-ui, sans-serif;
}
caption {
  caption-side: top; text-align: left; color: var(--ink-faint);
  font-size: 14px; padding-bottom: .6rem;
}
th, td { padding: .52rem .7rem; border-bottom: 1px solid var(--rule); text-align: left; }
th {
  font-weight: 600; font-size: 12.5px; letter-spacing: .05em;
  text-transform: uppercase; color: var(--ink-faint);
  border-bottom: 1.5px solid var(--rule);
}
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
tr.hl td { background: var(--accent-soft); font-weight: 600; }
.no { color: var(--accent); font-weight: 650; }
.yes { color: var(--good); font-weight: 650; }

ul, ol { margin: 0 0 1.2rem; padding-left: 1.35rem; }
li { margin-bottom: .62rem; }

hr.sep { border: 0; border-top: 1px solid var(--rule); margin: 3.4rem 0; }

footer {
  font: 400 14px/1.65 ui-sans-serif, system-ui, sans-serif;
  color: var(--ink-faint); border-top: 1px solid var(--rule);
  padding-top: 1.2rem; margin-top: 3.5rem;
}
footer h3 { font-size: .95rem; color: var(--ink-soft); margin: 0 0 .6rem; }
footer ul { padding-left: 1.2rem; }
footer li { margin-bottom: .35rem; }
"""

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Did we size the Windows 10 headwind right?</title>
<style>__CSS__</style>
</head>
<body>

<div class="wrap">
<header class="title">
  <p class="kicker">Forecast review &middot; August 2026 cycle</p>
  <h1>Did we size the Windows&nbsp;10 headwind right?</h1>
  <p class="standfirst">We have trimmed the desktop headwind anchor four times by judgement.
  This is the first time anyone has held it against telemetry. The data settles less than
  we might hope &mdash; and more than we might like.</p>
  <p class="byline">Read-only review of <code>data-official/2026-08/adjustments/headwind.json</code>.
  No specs changed, no forecasts re-run. Legacy telemetry only.</p>
</header>

<div class="verdict">
<h2>The short answer</h2>
<p>Two findings, and they point in different directions.</p>
<p><strong>The data cannot choose between the candidates.</strong> Across the stretch of the ramp
that has already elapsed, &minus;1,295,000 and &minus;1,270,000 differ by about 11,000 daily users.
My measurement carries an uncertainty near 1,500,000. Choosing one over the other is a judgement,
and we should call it that rather than dress it as evidence.</p>
<p><strong>Every candidate nevertheless asks for a loss that has not arrived.</strong> The ramp
starts on 1 April, so by 22 July it has run 112 of its 258 days &mdash; 43 percent. A
&minus;1,245,000 anchor therefore implies that roughly 540,000 daily users have already gone.
I ran ninety variations of the measurement. Not one of them found that loss. The gloomiest found
388,000; the middle of the pack found a small <em>gain</em>.</p>
<p>The problem, then, is not the last 25,000. The problem is the shape.</p>
</div>
</div>

<div class="wrap">

<h2>What the ramp claims</h2>
<p class="sub">Five lines of JSON, and a strong assumption hiding in them</p>

<p>The spec draws a straight line from zero on 1 April 2026 to its anchor on 15 December 2026, and
the applier subtracts that line from the desktop forecast. The anchor stands for people who abandon
Firefox rather than carry it from Windows 10 to Windows 11.</p>

<p>A straight line makes a strong claim: the loss accrues evenly, day after day. That claim is
testable, because 43 percent of the line already lies in the past. Whatever the anchor's eventual
size, 43 percent of it should be visible in telemetry now. Everything below tests exactly that.</p>

<table>
<caption>Each candidate anchor implies a loss already on the books by 22 July 2026.</caption>
<thead><tr><th>Anchor</th><th class="num">Implied by 22 July</th><th>Status</th></tr></thead>
<tbody>
<tr><td>&minus;1,295,000</td><td class="num">&minus;562,171</td><td>prior cycle value</td></tr>
<tr><td>&minus;1,270,000</td><td class="num">&minus;551,318</td><td>the proposed trim</td></tr>
<tr><td>&minus;1,245,000</td><td class="num">&minus;540,465</td><td>live on disk today</td></tr>
</tbody>
</table>

<p>That third row deserves a note. The repository moved while I was working: the anchor now committed
is &minus;1,245,000, already past the &minus;1,270,000 under discussion. I therefore tested all three.
The conclusion covers each of them, which is convenient, because it means nobody has to re-run this
when the number moves again.</p>

<h2>A falling Windows&nbsp;10 curve is not a headwind</h2>
<p class="sub">The trap the analysis has to step around</p>

<p>When someone upgrades from Windows 10 to Windows 11 they keep Firefox, and our DAU does not move.
Migration shuffles users between two buckets; it does not empty them. So the Windows 10 curve, which
is falling steeply, measures migration far more than it measures loss. The headwind is attrition
&mdash; the people who walk away instead of upgrading &mdash; and attrition shows up only in the two
buckets added together.</p>

<p>The numbers make the trap concrete. Between 1 April and 22 July, Windows 10 shed 1,814,214 daily
users. Read that as the headwind and you would size the anchor near 1.8 million per four months. But
Windows 11 &mdash; supposedly the destination &mdash; lost 957,418 over the same stretch, because
summer drags on everything, and the two together lost 2,771,631. Mac and Linux, which have no
exposure to Windows 10 at all, fell 7.75 percent against the combined cohort's 7.15 percent.</p>

<p>That last comparison contains the entire difficulty of this exercise.</p>

<div class="callout">
<p class="lbl">Why the forecast cannot answer this on its own</p>
<p>The model's <code>modern_windows</code> segment is defined as <code>os_version LIKE
'%windows 1%'</code>, so it holds Windows 10 and Windows 11 together. The forecast parquet cannot
separate them. Every split in this note therefore comes from BigQuery.</p>
</div>
</div>

<div class="wide">
<figure>
  <img alt="Windows 10 falls while Windows 11 rises; the combined cohort moves far less. Lower panel shows Windows 11's share of the combined cohort climbing steadily through 2025 and 2026."
       src="data:image/png;base64,__FIG_LEVELS__">
  <figcaption><b>Figure 1.</b> Windows 10 falls, Windows 11 rises, and the two together barely
  register the exchange. The lower panel tracks Windows 11's share of the pair, which climbs
  through the Windows 10 end-of-support date without any matching break in the combined level.
  Source: <code>research/headwinds/plots/cohort_levels.png</code></figcaption>
</figure>
</div>

<div class="wrap">
<h2>Finding a clean measurement</h2>
<p class="sub">Four obstacles stood between the raw extract and an answer</p>

<h3>1. Summer swamps the signal</h3>
<p>April to July is the worst window in the year for this question. Every cohort falls by a similar
percentage, including cohorts with no Windows 10 exposure whatsoever. The raw combined decline of
2,771,631 is almost entirely seasonal, which makes the naive before-and-after difference useless.
Any honest estimate has to be a difference against a counterfactual, not a difference against zero.</p>

<h3>2. Iran and China had to come out</h3>
<p>Two countries carried large one-off shocks inside precisely this window, and neither has anything
to do with Windows 10. Iran's telemetry returned after the shutdown, adding 632,573 daily users to
the Windows cohort. China's MozillaOnline migration added 755,060. Together they contribute
<strong>1,387,633</strong> &mdash; more than twice the loss I was trying to detect, and pointing the
opposite way. Left in, they would have buried the answer completely.</p>

<h3>3. Two tailwinds already sit in the actuals</h3>
<p>Launch-on-login and the MozillaOnline overlay both lift the observed figures, so both had to be
removed before any comparison &mdash; 222,723 daily users in total. Two details surfaced while I did
this. The live <code>l</code> spec pointed at a higher ceiling than the 165K I had been told; the
baseline forecast's sidecar SHA1 confirms it. And launch-on-login's measurement window closed
permanently on 23 June at 130,296, so its value in July is itself an extrapolation. I carried that as
uncertainty rather than pretending the curve was measured.</p>
<p><em>Note added 30 July:</em> the launch-on-login ceiling has since been raised to
<strong>200,000</strong> and the lower variants were deleted, so the 222,723 above reflects a curve that
is no longer in use and would now be somewhat larger. It does not affect the conclusion &mdash; the
anchor values under discussion differ by about 21,705 users against an uncertainty envelope near
1,488,000, so a change of this size sits far inside the noise floor established below. The figure is
left as originally measured rather than recomputed.</p>

<h3>4. A mistake of my own</h3>
<p>My first attempt compared a Wednesday-only average for 2025 against an all-week 28-day average for
2026. Wednesdays run about 13 percent above a full week, so the two were never comparable as levels.
I redid the estimate with 2026 measured in all-week terms and 2025 contributing only a ratio, which
is unit-free and survives the mismatch. The correction moved the answer, which is why it is worth
recording.</p>

<h2>What the data shows</h2>
<p class="sub">Three approaches, none of which finds the loss</p>

<h3>The difference against two counterfactuals</h3>
<p>I compared the Windows cohort against the same population one year earlier, before Windows 10
end-of-support, and against Mac and Linux over the same months of 2026. The two counterfactuals are
independent of each other and rest on different assumptions.</p>

<table>
<caption>Change from 1 April to 22 July 2026, after removing Iran, China, and both overlay tailwinds.</caption>
<thead><tr><th>Series</th><th class="num">Change</th><th class="num">Excess vs. it</th></tr></thead>
<tbody>
<tr class="hl"><td>Windows 10 + 11, underlying</td><td class="num">&minus;7.73%</td><td class="num">&mdash;</td></tr>
<tr><td>Same cohort, 2025 (pre-EOL)</td><td class="num">&minus;7.84%</td><td class="num">+43,378</td></tr>
<tr><td>Mac and Linux, 2026</td><td class="num">&minus;7.75%</td><td class="num">+7,219</td></tr>
</tbody>
</table>

<p>All three land near &minus;7.8 percent. The cohort exposed to Windows 10 is not falling faster
than either counterfactual, and the excess attrition &mdash; the quantity the anchor is supposed to
capture &mdash; rounds to nothing. Of the two controls, Mac and Linux is the cleaner: same year, same
calendar, no cross-year and no cross-method step.</p>

<h3>Ninety variations</h3>
<p>A small difference between two large seasonal movements is fragile, so I swept the specification
rather than trusting one version of it: both counterfactuals, window endpoints moved a fortnight each
way, and all three launch-on-login curves. Ninety variants in total.</p>
</div>

<div class="wide">
<figure>
  <img alt="Scatter of ninety specification variants, nearly all above zero, with the three candidate anchor lines lying well below the entire distribution."
       src="data:image/png;base64,__FIG_VERDICT__">
  <figcaption><b>Figure 2.</b> Each dot is one specification variant. The band is the full envelope;
  the darker band spans the tenth to ninetieth percentile. The three dashed lines are what the
  candidate anchors require, and they sit below every dot. They also lie within 21,705 of one
  another, which is the visual form of the first finding: the candidates are indistinguishable.
  Source: <code>research/headwinds/plots/anchor_verdict.png</code></figcaption>
</figure>
</div>

<div class="wrap">
<p>The envelope runs from &minus;388,058 to +1,100,235, with a median of +134,926. Every candidate
anchor needs something near &minus;540,000. Nothing in the sweep reaches it.</p>

<h3>A check that needs no control group</h3>
<p>Both estimates above depend on a counterfactual, so I wanted one that does not. Year-over-year
change uses the same population twelve months apart and needs no control cohort at all. If Windows 10
attrition were biting, this rate would deteriorate as end-of-support pressure accumulated.</p>

<p>It does not. The rate stood at &minus;5.77 percent when the ramp began and &minus;4.99 percent at
the end of the window &mdash; 0.78 points <em>better</em>. A real &minus;1,245,000 anchor requires it
to have worsened by 1.39 points.</p>
</div>

<div class="wide">
<figure>
  <img alt="Year-over-year change for the combined Windows cohort through 2026, ending the window slightly above where it started, with a red line marking the much lower level a real anchor would require."
       src="data:image/png;base64,__FIG_YOY__">
  <figcaption><b>Figure 3.</b> Year-over-year change for the combined cohort, net of both overlays.
  The green line marks the level when the ramp began; the red line marks where the rate would sit if
  the live anchor were real. Note the volatility &mdash; and the early-June dip that momentarily
  reaches the red line before recovering.
  Source: <code>research/headwinds/plots/yoy_trajectory.png</code></figcaption>
</figure>
</div>

<div class="wrap">
<p>That dip matters, and I would rather raise it than have someone find it later. On one date in
early June the rate did touch the level a real anchor implies. The series swings between
&minus;2.78 and &minus;7.15 percent over six months, a range wider than the 1.39-point effect under
test. So this check cannot rule the headwind out on any particular day. What it shows is the absence
of any <em>sustained</em> deterioration &mdash; and it shows that the answer's sign depends on where
you stop measuring.</p>

<h3>What an abandoned cohort actually looks like</h3>
<p>Windows 7, 8, and 8.1 give us a natural picture of Firefox decaying on a version the world has
left behind. That cohort shed 15.9 percent across our window, against the Windows 10 and 11 pair's
6.6 percent, and it shed a near-identical 15.3 percent in the same months of 2025. Two things follow.
An abandoned Windows cohort loses roughly twice what our test cohort loses, and our test cohort is
not drifting toward that fate.</p>

<p>This also explains why I excluded Windows 7/8/8.1 from the headline range. Used as a seasonal
counterfactual it implies an absurd result &mdash; that the combined cohort should have lost nearly
four million &mdash; because it is not a healthy population having a normal summer. It is a bound,
not a control.</p>

<hr class="sep">

<h2>What I cannot claim</h2>
<p class="sub">The limits are real, and they are the most important part of this note</p>

<ul>
<li><strong>Windows 10 attrition does not separate cleanly from ordinary desktop decline.</strong>
My claim is narrow and negative: the exposed cohort shows no <em>excess</em> decline against its own
prior-year season or against Mac and Linux. That is not a claim that attrition is zero.</li>

<li><strong>A flat year-over-year rate reads two ways.</strong> Either attrition is absent, or it is
steady and already embedded in the roughly 5 percent annual decline Prophet trains on. Under the
second reading the correct <em>additional</em> headwind is still near zero, because Prophet already
carries the embedded rate forward. The anchor earns its place only if attrition accelerates beyond
that. Four months of data show no acceleration &mdash; and rule out no future acceleration either.
That part is a genuine forecast, and no amount of telemetry will settle it.</li>

<li><strong>The interval is wide and it is endpoint-sensitive.</strong> This is why I offer a range
and no point estimate. A single number here would look authoritative and be fiction.</li>

<li><strong>The scope is not an exact match.</strong> I measured the Windows cohort excluding Iran
and China; the anchor applies to total desktop DAU. The mechanism is Windows-specific, so this is the
right place to look, but the mapping is approximate.</li>
</ul>

<h2>A second problem: the seam</h2>
<p class="sub">Raised for the record, not fixed</p>

<p>The applier adds the ramp only from the forecast start onward, and it adds the value the ramp has
already reached. So the ramp reads zero on 27 July and &minus;569,419 on 28 July. The composite takes
a 569,419 step down on the first day of the forecast.</p>

<p>This misfires under either reading of the evidence. If the April-to-July loss <em>were</em> sitting
in the training actuals, Prophet would have fitted it into the level at the seam, and subtracting the
ramped value again would double-count it. This analysis finds the loss is not in the actuals &mdash;
so the step instead asserts a 569,000 overnight drop with no empirical support at all. Either way it
is not something telemetry justifies.</p>

<p>Worth noting: the brief cited roughly &minus;592,000 for this step, which corresponds to the
superseded &minus;1,295,000 anchor.</p>

<h2>What follows</h2>

<p>The evidence does not tell us to trim the anchor by another 25,000, and it does not tell us to
prefer &minus;1,270,000 over &minus;1,295,000. On that question it is silent, and five cycles of
attenuation have been silent judgement dressed as inference.</p>

<p>What the evidence does tell us is that the ramp asserts a past that did not happen. If we want to
keep the December magnitude on forward-looking grounds &mdash; a bet that abandonment accelerates as
Windows 10 decays &mdash; that bet deserves to be argued on its own terms and recorded as a bet.
Moving the ramp's start toward the forecast seam would let us hold that view without also claiming
540,000 users have already left, and it would remove the day-one step as a side effect.</p>

<p>The narrower recommendation is about how we talk. Each attenuation has been recorded as though
data drove it. Two of these numbers differ by less than one percent of the uncertainty around them.
Calling that a judgement costs us nothing and keeps the forecast honest.</p>

<footer>
<h3>Provenance</h3>
<ul>
<li>Analysis and figures: <code>research/headwinds/win10_anchor_validation.ipynb</code> (12 cells,
executes end to end)</li>
<li>Written verdict: <code>research/headwinds/WIN10_ANCHOR_FINDINGS.md</code></li>
<li>Source: <code>moz-fx-data-shared-prod.firefox_desktop_derived.active_users_aggregates_v4</code>,
the desktop-only table behind <code>telemetry.active_users_aggregates</code>, whose mobile UNION
defeats partition pruning (1.39&nbsp;TB against 515&nbsp;GB). Legacy telemetry only; no Glean.</li>
<li>Scope matches production exactly: <code>app_name = 'Firefox Desktop'</code>, all countries.
The extract reproduces the independently supplied controls for 27 July 2026 exactly &mdash; Windows 10
15,663,337, Windows 11 26,300,886, desktop total 51,669,205 &mdash; asserted in the notebook.</li>
<li>Extracts and SQL: <code>research/headwinds/extracts/</code>, committed so the notebook reruns
without repeating a 515&nbsp;GB scan.</li>
<li>Window: 1 April to 22 July 2026, the last date with a complete 28-day window in the extract.</li>
</ul>
</footer>
</div>

</body>
</html>
"""

html = (HTML
        .replace("__CSS__", CSS)
        .replace("__FIG_LEVELS__", fig("cohort_levels.png"))
        .replace("__FIG_VERDICT__", fig("anchor_verdict.png"))
        .replace("__FIG_YOY__", fig("yoy_trajectory.png")))

OUT.write_text(html, encoding="utf-8")
kb = OUT.stat().st_size / 1024
print(f"wrote {OUT} ({kb:.0f} KB, self-contained)")
