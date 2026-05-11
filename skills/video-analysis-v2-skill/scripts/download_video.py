#!/usr/bin/env python3
"""Download a short-video URL with yt-dlp."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--filename", default="video.%(ext)s")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_template = str(output_dir / args.filename)
    command = ["yt-dlp", "-f", "mp4/best", "-o", output_template, args.url]
    if shutil.which("yt-dlp") is None:
        command = [sys.executable, "-m", "yt_dlp", "-f", "mp4/best", "-o", output_template, args.url]
    subprocess.run(
        command,
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
