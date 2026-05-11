#!/usr/bin/env python3
"""Extract representative segment frames for video-analysis-v3 HTML evidence."""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path


def to_seconds(ts: str) -> float:
    if not ts:
        return 0.0
    nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", ts)]
    if len(nums) >= 3:
        return nums[-3] * 3600 + nums[-2] * 60 + nums[-1]
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    if len(nums) == 1:
        return nums[0]
    return 0.0


def extract(video: Path, t: float, out: Path) -> bool:
    cmd = ["ffmpeg", "-y", "-ss", f"{max(t,0):.2f}", "-i", str(video), "-frames:v", "1", "-vf", "scale=360:-1", str(out)]
    p = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return p.returncode == 0 and out.exists()


def parse_time_from_audio_line(text: str) -> float | None:
    match = re.match(r"^\s*(\d{2}:\d{2}(?::\d{2})?)", str(text))
    if not match:
        return None
    return to_seconds(match.group(1))


def choose_frame_times(seg: dict, start: float, end: float) -> list[tuple[str, float]]:
    if end <= start:
        end = start
    duration = max(end - start, 0.0)
    audio_times = []
    for line in seg.get("audio_lines") or []:
        ts = parse_time_from_audio_line(line)
        if ts is not None:
            audio_times.append(ts)
    key_times = [to_seconds(x) for x in (seg.get("key_action_times") or []) if str(x).strip()]

    # v2 audio-sop spirit:
    # start, start+0.3, start+0.8, end-0.2
    # then prefer an audio/key-action anchor as the middle evidence frame.
    start_candidate = start + 0.3 if duration >= 0.4 else start
    fallback_mid = min(start + 0.8, end) if duration >= 0.8 else (start + end) / 2
    audio_anchor = None
    if audio_times:
        audio_anchor = audio_times[0]
    elif key_times:
        audio_anchor = key_times[0]
    mid_candidate = audio_anchor if audio_anchor is not None and start <= audio_anchor <= end else fallback_mid
    end_candidate = max(end - 0.2, start)

    slots = [
        ("start", start_candidate),
        ("mid", mid_candidate),
        ("end", end_candidate),
    ]

    deduped: list[tuple[str, float]] = []
    seen: set[float] = set()
    for label, ts in slots:
        rounded = round(max(ts, start), 2)
        if rounded in seen:
            continue
        seen.add(rounded)
        deduped.append((label, rounded))
    return deduped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("video")
    ap.add_argument("analysis_json")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    if not shutil.which("ffmpeg"):
        raise SystemExit("ffmpeg not found")
    video = Path(args.video)
    data = json.loads(Path(args.analysis_json).read_text())
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest = []
    segments = data.get("synthesized_segments") or data.get("timeline", [])
    for i, seg in enumerate(segments, 1):
        start = to_seconds(str(seg.get("start", "0")))
        end = to_seconds(str(seg.get("end", "")))
        if end <= start:
            end = start
        item = {
            "index": i,
            "strategy": "audio-led" if (seg.get("audio_lines") or seg.get("dialogue_text")) else "segment-led",
            "segment_start": start,
            "segment_end": end,
        }
        for label, t in choose_frame_times(seg, start, end):
            path = out / f"segment_{i:02d}_{label}.jpg"
            if extract(video, t, path):
                item[label] = str(path)
                item[f"{label}_time"] = t
        manifest.append(item)
    (out / "frames_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"frames": len(manifest), "manifest": str(out / "frames_manifest.json")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
