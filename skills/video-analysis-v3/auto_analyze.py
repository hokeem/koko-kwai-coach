#!/usr/bin/env python3
"""One-shot entrypoint for video-analysis-v3."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

FALLBACK_MODELS = [
    "gemini-3-flash-preview",
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the full video-analysis-v3 pipeline from a single command.")
    ap.add_argument("source_path", help="Public video URL or local video path.")
    ap.add_argument("--out", help="Output directory. Defaults to repo tmp/video_analysis_v3/<slug>_<timestamp>.")
    ap.add_argument("--model", default="gemini-3-flash-preview")
    ap.add_argument("--analysis-model", default="gemini-2.5-flash-lite")
    ap.add_argument("--frames", action="store_true", help="Extract representative frames when ffmpeg is available.")
    ap.add_argument("--skip-frames", action="store_true", help="Skip frame extraction even if ffmpeg exists.")
    ap.add_argument("--prompt-file")
    ap.add_argument("--api-key")
    ap.add_argument("--api-key-file")
    return ap.parse_args()


def repo_root(skill_root: Path) -> Path:
    return skill_root.parent.parent


def slugify(value: str) -> str:
    parsed = urlparse(value)
    text = parsed.netloc + parsed.path if parsed.scheme else value
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return slug or "video"


def default_output_dir(root: Path, source_path: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return root / "tmp" / "video_analysis_v3" / f"{slugify(source_path)[:80]}_{stamp}"


def run_step(name: str, cmd: list[str], env: dict[str, str] | None = None) -> str:
    proc = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"[{name}] failed: {detail}")
    return (proc.stdout or "").strip()


def main() -> int:
    try:
        args = parse_args()
        skill_root = Path(__file__).resolve().parent
        root = repo_root(skill_root)
        scripts_dir = skill_root / "scripts"
        out_dir = Path(args.out).expanduser().resolve() if args.out else default_output_dir(root, args.source_path)
        out_dir.mkdir(parents=True, exist_ok=True)

        if args.frames and args.skip_frames:
            raise RuntimeError("Choose either --frames or --skip-frames, not both.")
        if args.frames and not shutil.which("ffmpeg"):
            raise RuntimeError("ffmpeg is required for --frames.")

        python = sys.executable
        source_mp4 = out_dir / "source.mp4"
        metadata_json = out_dir / "source_metadata.json"
        observations_json = out_dir / "observations.json"
        observations_raw_json = out_dir / "observations_raw_gemini.json"
        analysis_raw_json = out_dir / "analysis_raw_gemini.json"
        analysis_result_json = out_dir / "analysis_result.json"
        script_table_json = out_dir / "script_table.json"
        script_table_html = out_dir / "script_table.html"
        selected_frames_dir = out_dir / "selected_frames"

        base_env = os.environ.copy()

        download_cmd = [
            python,
            str(scripts_dir / "download_video.py"),
            args.source_path,
            "--out",
            str(out_dir),
        ]
        run_step("download_video", download_cmd, env=base_env)

        model_candidates = [args.model] + [m for m in FALLBACK_MODELS if m != args.model]
        last_observe_error = ""
        active_model = args.model
        for model_name in model_candidates:
            active_model = model_name
            observe_cmd = [
                python,
                str(scripts_dir / "gemini_video_observe.py"),
                str(source_mp4),
                "--metadata",
                str(metadata_json),
                "--out",
                str(observations_json),
                "--raw-out",
                str(observations_raw_json),
                "--model",
                model_name,
            ]
            if args.prompt_file:
                observe_cmd.extend(["--prompt-file", args.prompt_file])
            if args.api_key_file:
                observe_cmd.extend(["--api-key-file", args.api_key_file])
            elif args.api_key:
                observe_cmd.extend(["--api-key", args.api_key])
            try:
                run_step("gemini_video_observe", observe_cmd, env=base_env)
                break
            except RuntimeError as exc:
                last_observe_error = str(exc)
                retryable_model_error = any(token in last_observe_error for token in ["HTTP 503", "UNAVAILABLE", "high demand"])
                if not retryable_model_error or model_name == model_candidates[-1]:
                    raise
        else:
            raise RuntimeError(last_observe_error or "gemini_video_observe failed")

        analysis_cmd = [
            python,
            str(scripts_dir / "analyze_evidence_bundle.py"),
            str(observations_json),
            "--metadata",
            str(metadata_json),
            "--out",
            str(analysis_result_json),
            "--raw-out",
            str(analysis_raw_json),
            "--model",
            args.analysis_model,
        ]
        if args.api_key_file:
            analysis_cmd.extend(["--api-key-file", args.api_key_file])
        elif args.api_key:
            analysis_cmd.extend(["--api-key", args.api_key])
        try:
            run_step("analyze_evidence_bundle", analysis_cmd, env=base_env)
            analysis_payload = json.loads(analysis_result_json.read_text(encoding="utf-8"))
            script_table_json.write_text(json.dumps(analysis_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            synthesize_cmd = [
                python,
                str(scripts_dir / "synthesize_observations.py"),
                str(observations_json),
                "--metadata",
                str(metadata_json),
                "--out",
                str(script_table_json),
            ]
            run_step("synthesize_observations", synthesize_cmd, env=base_env)

        frames_enabled = args.frames and not args.skip_frames
        if frames_enabled:
            frames_cmd = [
                python,
                str(scripts_dir / "extract_segment_frames.py"),
                str(source_mp4),
                str(script_table_json),
                "--out",
                str(selected_frames_dir),
            ]
            run_step("extract_segment_frames", frames_cmd, env=base_env)

        render_cmd = [
            python,
            str(scripts_dir / "render_script_table.py"),
            str(script_table_json),
            "--metadata",
            str(metadata_json),
            "--out",
            str(script_table_html),
        ]
        if frames_enabled:
            render_cmd.extend(["--frames", str(selected_frames_dir)])
        run_step("render_script_table", render_cmd, env=base_env)

        payload = {
            "out_dir": str(out_dir),
            "html": str(script_table_html),
            "json": str(script_table_json),
            "analysis_json": str(analysis_result_json) if analysis_result_json.exists() else "",
            "frames_enabled": frames_enabled,
            "model": active_model,
            "analysis_model": args.analysis_model,
            "source_metadata": str(metadata_json),
        }
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
