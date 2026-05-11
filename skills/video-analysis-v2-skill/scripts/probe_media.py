#!/usr/bin/env python3
"""Probe video metadata with ffprobe and emit compact JSON."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def run_ffprobe(video: Path) -> dict:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration:stream=index,codec_type,codec_name,width,height",
            "-of",
            "json",
            str(video),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def summarize(raw: dict) -> dict:
    streams = raw.get("streams", [])
    video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    return {
        "duration": float(raw.get("format", {}).get("duration", 0) or 0),
        "resolution": {
            "width": video_stream.get("width"),
            "height": video_stream.get("height"),
        },
        "video_codec": video_stream.get("codec_name"),
        "audio_stream_exists": bool(audio_streams),
        "audio_codec": audio_streams[0].get("codec_name") if audio_streams else None,
        "audio_stream_count": len(audio_streams),
        "raw": raw,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("video")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    summary = summarize(run_ffprobe(Path(args.video)))
    text = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
