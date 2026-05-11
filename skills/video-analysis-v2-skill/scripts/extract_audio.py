#!/usr/bin/env python3
"""Extract audio from a video for ASR."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("video")
    parser.add_argument("--output", default="audio.wav")
    parser.add_argument("--sample-rate", default="16000")
    parser.add_argument("--channels", default="1")
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-i",
            args.video,
            "-vn",
            "-acodec",
            "pcm_s16le",
            "-ar",
            args.sample_rate,
            "-ac",
            args.channels,
            args.output,
        ],
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
