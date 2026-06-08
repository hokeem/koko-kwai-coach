#!/usr/bin/env python3
"""Validate required video-analysis output artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


REQUIRED_COLUMNS = [
    "source_url",
    "time",
    "visual_content",
    "action",
    "dialogue_or_audio",
]

OPTIONAL_AUDIO_MULTIVIEW_TOP_LEVEL = [
    "source_url",
    "whole_audio_hypothesis",
    "speakers",
    "utterances",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output_dir")
    args = parser.parse_args()

    root = Path(args.output_dir)
    html_file = root / "script_table.html"
    json_file = root / "script_table.json"
    errors: list[str] = []

    if not html_file.exists():
        errors.append("missing script_table.html")
    if not json_file.exists():
        errors.append("missing script_table.json")
    if not (root / "selected_frames").exists():
        errors.append("missing selected_frames/")
    if not (root / "selected_frames_end").exists():
        errors.append("missing selected_frames_end/")

    if json_file.exists():
        data = json.loads(json_file.read_text(encoding="utf-8"))
        for field in [
            "title",
            "route",
            "audio_information_score",
            "source_url",
            "whole_video_summary",
            "core_viral_points",
            "replaceable_parts",
            "rows",
        ]:
            if field not in data:
                errors.append(f"missing JSON field: {field}")
        for field in ["core_viral_points", "replaceable_parts"]:
            if field in data and not data.get(field):
                errors.append(f"empty JSON field: {field}")
        for index, row in enumerate(data.get("rows", []), start=1):
            for field in REQUIRED_COLUMNS:
                if field not in row:
                    errors.append(f"row {index} missing field: {field}")

    audio_multiview_file = root / "audio_multiview.json"
    if audio_multiview_file.exists():
        multiview = json.loads(audio_multiview_file.read_text(encoding="utf-8"))
        for field in OPTIONAL_AUDIO_MULTIVIEW_TOP_LEVEL:
            if field not in multiview:
                errors.append(f"audio_multiview missing field: {field}")
        for index, speaker in enumerate(multiview.get("speakers", []), start=1):
            for field in ["speaker_id", "display_label", "gender_guess", "source_type"]:
                if field not in speaker:
                    errors.append(f"audio_multiview speaker {index} missing field: {field}")
        for index, utterance in enumerate(multiview.get("utterances", []), start=1):
            for field in ["utterance_id", "start", "end", "speaker_id", "text"]:
                if field not in utterance:
                    errors.append(f"audio_multiview utterance {index} missing field: {field}")


    if html_file.exists():
        html_text = html_file.read_text(encoding="utf-8")
        for needle in [
            "视频总结归纳 + 脚本表",
            "视频整体内容总结",
            "核心爆点",
            "可替换部分",
            "脚本表",
            "关键对白/旁白",
        ]:
            if needle not in html_text:
                errors.append(f"HTML missing text: {needle}")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("OK: output artifacts match the video-analysis contract")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
