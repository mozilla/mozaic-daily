# `tooling/transcript_review/` — read Claude Code session logs

Renders the `.jsonl` session transcripts Claude Code writes under
`~/.claude/projects/<slug>/` into compact text, so a session can be reviewed without
loading megabytes of tool output into a context window.

## What's here

| File | What it does |
|---|---|
| `extract_turns.py` | Emit human turns (`--mode users`, default) or human turns interleaved with assistant text and tool names (`--mode outline`) for one or more session files |

```bash
python3 tooling/transcript_review/extract_turns.py \
  ~/.claude/projects/-Users-brendanwells-work-mozaic-daily/<session>.jsonl \
  --mode users --out tmp/transcript_review
```

Harness-injected pseudo-user turns (system reminders, slash-command echoes, local command
stdout) are filtered out of `users` mode — they are not things the human typed, and keeping
them buries the actual instructions.

## What isn't here

No analysis, no scoring, no aggregation. This is a reader. Conclusions drawn from its
output belong in `research/collaboration-review/`.

## Where new code goes

Extend `extract_turns.py` for new *renderings* of a transcript. A new script belongs here
only if it reads session logs and writes something other than a rendering (a tally, an
index). Anything that reads forecast artifacts belongs elsewhere in `tooling/`.
