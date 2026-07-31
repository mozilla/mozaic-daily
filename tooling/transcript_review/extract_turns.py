#!/usr/bin/env python3
"""Extract human turns (and light assistant/tool context) from Claude Code transcripts.

Reads the .jsonl session logs Claude Code writes under
``~/.claude/projects/<slug>/`` and emits a compact, greppable text rendering so a
session can be reviewed without loading megabytes of tool output.

Usage:
    python3 tooling/transcript_review/extract_turns.py SESSION.jsonl [...] \
        [--mode users|outline] [--out DIR]

Modes:
    users    - human turns only (default); the highest-signal view for reviewing
               how a collaboration went.
    outline  - human turns interleaved with assistant text and tool-call names.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MAX_TEXT_CHARS = 4000
SYSTEM_NOISE_MARKERS = (
    "<system-reminder>",
    "Caveat: The messages below",
    "<command-name>",
    "<local-command-stdout>",
)


def _blocks_to_text(content) -> tuple[str, list[str]]:
    """Return (joined text, tool names) for a message ``content`` field."""
    if isinstance(content, str):
        return content, []
    texts: list[str] = []
    tools: list[str] = []
    for block in content or []:
        if not isinstance(block, dict):
            continue
        kind = block.get("type")
        if kind == "text":
            texts.append(block.get("text", ""))
        elif kind == "thinking":
            continue
        elif kind == "tool_use":
            tools.append(block.get("name", "?"))
        elif kind == "tool_result":
            continue
    return "\n".join(texts), tools


def _is_real_human_turn(text: str) -> bool:
    """True when the text looks typed by the user rather than injected by the harness."""
    stripped = text.strip()
    if not stripped:
        return False
    return not any(marker in stripped for marker in SYSTEM_NOISE_MARKERS)


def render(path: Path, mode: str) -> str:
    lines: list[str] = [f"##### SESSION {path.stem}"]
    turn = 0
    with path.open() as handle:
        for raw in handle:
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if record.get("type") not in {"user", "assistant"}:
                continue
            message = record.get("message") or {}
            text, tools = _blocks_to_text(message.get("content"))
            timestamp = (record.get("timestamp") or "")[:16]
            if record["type"] == "user":
                if not _is_real_human_turn(text):
                    continue
                turn += 1
                lines.append(f"\n[HUMAN {turn} @ {timestamp}]\n{text[:MAX_TEXT_CHARS]}")
            elif mode == "outline":
                if text.strip():
                    lines.append(f"[asst @ {timestamp}] {text[:MAX_TEXT_CHARS]}")
                if tools:
                    lines.append(f"[tools] {', '.join(tools)}")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sessions", nargs="+", type=Path)
    parser.add_argument("--mode", choices=["users", "outline"], default="users")
    parser.add_argument("--out", type=Path, help="write one .txt per session here")
    args = parser.parse_args(argv)

    for session in args.sessions:
        rendered = render(session, args.mode)
        if args.out:
            args.out.mkdir(parents=True, exist_ok=True)
            target = args.out / f"{session.stem}.{args.mode}.txt"
            target.write_text(rendered)
            print(f"{target}  ({len(rendered):,} chars)")
        else:
            print(rendered)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
