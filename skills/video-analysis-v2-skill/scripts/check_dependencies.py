#!/usr/bin/env python3
"""Check local dependencies for video-analysis-v2-skill."""

from __future__ import annotations

import shutil
import subprocess
import sys


TOOLS = ["yt-dlp", "ffmpeg", "ffprobe", "python3"]


def version(tool: str) -> str:
    try:
        result = subprocess.run([tool, "--version"], check=False, capture_output=True, text=True)
    except OSError as exc:
        return f"unavailable: {exc}"
    first = (result.stdout or result.stderr).splitlines()
    return first[0] if first else "version output unavailable"


def main() -> int:
    missing = [tool for tool in TOOLS if shutil.which(tool) is None]
    for tool in TOOLS:
        status = "OK" if tool not in missing else "MISSING"
        print(f"{status}: {tool} - {version(tool) if tool not in missing else 'not found'}")
    if missing:
        print("\nMissing tools: " + ", ".join(missing), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
