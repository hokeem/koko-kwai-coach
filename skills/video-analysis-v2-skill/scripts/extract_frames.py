#!/usr/bin/env python3
"""Extract still frames at requested timestamps."""

from __future__ import annotations

import argparse
import csv
import subprocess
from pathlib import Path


def parse_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def extract(video: str, timestamp: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-ss",
            timestamp,
            "-i",
            video,
            "-frames:v",
            "1",
            "-q:v",
            "2",
            str(output),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("video")
    parser.add_argument("timestamps_csv", help="CSV with columns timestamp,output")
    args = parser.parse_args()

    for row in parse_rows(Path(args.timestamps_csv)):
        extract(args.video, row["timestamp"], Path(row["output"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
