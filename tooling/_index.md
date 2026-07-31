# `tooling/` — reusable utilities outside the production pipeline

Standalone CLI utilities that aren't part of `mozaic_daily` itself but support analysis. Each subdir has its own `_index.md`.

## Subdirs

| Subdir | What it does |
|---|---|
| `prophet_decompose/` | Decompose a desktop Mozaic forecast pkl into Prophet components (trend / weekly / yearly / holidays) and write to a long parquet |
| `transcript_review/` | Render Claude Code session `.jsonl` logs into compact reviewable text (human turns, or turns plus tool names) |

## Where new code goes

- **Reusable utility** with its own CLI and inputs/outputs → `tooling/<name>/`, with a one-page `_index.md`
- **Throwaway scratch** → `tmp/` (gitignored)
- **Production pipeline logic** → `src/mozaic_daily/`
- **Operational scripts** (backfill runners, validators, exporters) → `scripts/`
