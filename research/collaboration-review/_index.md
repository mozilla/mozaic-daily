# `research/collaboration-review/` — how the human/agent workflow itself is going

Retrospectives on the *process* of running forecast cycles with Claude Code, as distinct
from every other cluster here, which studies the forecast. Sources are the session
transcripts in `~/.claude/projects/-Users-brendanwells-work-mozaic-daily/`, read with
`tooling/transcript_review/extract_turns.py`.

## What's here

| File | Covers |
|---|---|
| `2026-07-30_self_review.html` | Review of the 29–30 July sessions (August cycle open through the s01 lock and the seam fix), plus the aggregated `/insights` report over 52 June–July sessions. Recommendations for prompting, `CLAUDE.md` (project and global), skills, tools, permissions, and memory hygiene. **§13 is the operative section**: mechanisms the human owns, ranked by whether they enforce or merely encourage. |

## Status of its recommendations

**None have been applied.** The review is a proposal document. The top three asks are a
`SessionStart` hook injecting `data-official/CYCLE_CONTRACT.md`, a `PreToolUse` frozen-path
guard, and rewriting or deleting the stale `/monthly-forecast-update` command. Track
adoption by editing the table above, not by rewriting the report — the report is dated
evidence of what the transcripts showed on 30 July 2026.

## The framing that matters

Sections 6–12 are resolutions by the agent, and §13 exists because resolutions are not a
mechanism. The evidence: the "past forecasts are frozen" rule was broken twice in one
session, and a transcript check found **zero** mentions of it anywhere in that session's
conversation before the first violation — it existed only as a clause inside `CLAUDE.md`'s
`seam_ma.py` subsection, about a different file. Rules that must hold every time need a
channel that does not depend on the agent's judgement in the moment or the human's memory to
type them. Prefer hooks, tests, and plan mode over instructions.

## What isn't here

No forecast numbers, no model diagnostics, no artifact production. Numbers appear only as
evidence about process (how many rebuilds, how many tool calls). For the substance behind
any number cited in a report here, follow the path it cites.

## Where new code goes

One dated HTML report per review, at the cadence reviews actually happen (so far: cycle
milestones). Transcript-reading tooling goes in `tooling/transcript_review/`, not here.
