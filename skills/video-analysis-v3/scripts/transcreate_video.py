#!/usr/bin/env python3
"""Create a Portuguese-dubbed version of a public short video when source media is available."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DOWNLOAD_SCRIPT = SCRIPT_DIR / "download_video.py"
REPO_ROOT = SCRIPT_DIR.parents[2]


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file(REPO_ROOT / ".env.local")
load_env_file(REPO_ROOT / "video-analysis-v3-web" / ".env.local")


def run(cmd: list[str], *, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def friendly_error(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or f"command exited with {proc.returncode}").strip()


def resolve_ffmpeg() -> str:
    configured = os.environ.get("FFMPEG_BIN", "").strip()
    if configured and Path(configured).exists():
        return configured
    found = shutil.which("ffmpeg")
    if found:
        return found
    try:
        import imageio_ffmpeg  # type: ignore

        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return ""


def ffprobe(ffmpeg: str, video: Path) -> dict:
    ffprobe_bin = os.environ.get("FFPROBE_BIN", "").strip() or shutil.which("ffprobe") or ""
    if not ffprobe_bin and ffmpeg:
        candidate = Path(ffmpeg).with_name("ffprobe")
        if candidate.exists():
            ffprobe_bin = str(candidate)
    if not ffprobe_bin:
        return {}
    proc = run(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration,size:stream=codec_type,width,height,codec_name",
            "-of",
            "json",
            str(video),
        ],
        timeout=60,
    )
    if proc.returncode != 0:
        return {}
    try:
        data = json.loads(proc.stdout)
    except Exception:
        return {}
    meta: dict = {}
    fmt = data.get("format") or {}
    if fmt.get("duration"):
        meta["duration"] = float(fmt["duration"])
    if fmt.get("size"):
        meta["size"] = int(fmt["size"])
    for stream in data.get("streams") or []:
        if stream.get("codec_type") == "video":
            meta["width"] = stream.get("width")
            meta["height"] = stream.get("height")
            meta["video_codec"] = stream.get("codec_name")
        if stream.get("codec_type") == "audio":
            meta["has_audio"] = True
            meta["audio_codec"] = stream.get("codec_name")
    meta.setdefault("has_audio", False)
    return meta


def download_source(url: str, out_dir: Path) -> dict:
    if not DOWNLOAD_SCRIPT.exists():
        raise RuntimeError(f"Missing downloader: {DOWNLOAD_SCRIPT}")
    proc = run([sys.executable, str(DOWNLOAD_SCRIPT), url, "--out", str(out_dir)], timeout=240)
    if proc.returncode != 0:
        raise RuntimeError(f"Video download failed: {friendly_error(proc)}")
    meta_path = out_dir / "source_metadata.json"
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"source_url": url, "local_video": str(out_dir / "source.mp4")}


def fallback_portuguese_script(metadata: dict, visual_summary: str) -> str:
    title = str(metadata.get("title") or "").strip()
    base = title or visual_summary or "Vídeo curto traduzido para português."
    return textwrap.shorten(base, width=420, placeholder="...")


def build_script_with_gemini(video: Path, metadata: dict, *, key: str, model: str) -> dict:
    if not key:
        return {
            "subject_summary": str(metadata.get("title") or "Public short video").strip(),
            "original_audio_summary": "Gemini key missing; could not inspect the original audio.",
            "portuguese_voiceover": fallback_portuguese_script(metadata, ""),
            "model": "fallback",
        }
    try:
        from hybrid_v2_pipeline import run_video_json_prompt_with_fallback, unique_models
    except Exception as exc:
        raise RuntimeError(f"Gemini video helper unavailable: {exc}") from exc

    prompt = """You are preparing a pt-BR dub for a short video.
Return strict JSON with:
{
  "subject_summary": "one sentence identifying the main visible subject and scene",
  "original_audio_summary": "faithful summary/transcript of the original speech or audio cues",
  "portuguese_voiceover": "pt-BR voiceover that preserves the original meaning and timing as closely as possible"
}
Do not add unrelated commentary. If the video has no speech, write a concise pt-BR narration of the visible action."""
    result, _, model_used = run_video_json_prompt_with_fallback(
        video,
        key,
        unique_models(model, "gemini-2.5-flash-lite", "gemini-2.5-flash"),
        prompt,
        "portuguese video transcreation",
    )
    if not isinstance(result, dict):
        raise RuntimeError("Gemini returned a non-object response.")
    result["model"] = model_used
    return result


def synthesize_with_say(text: str, out_audio: Path, *, voice: str) -> None:
    say_bin = shutil.which("say")
    if not say_bin:
        raise RuntimeError("No TTS provider is configured. On macOS install/use 'say', or add a cloud TTS integration.")
    proc = run([say_bin, "-v", voice, "-o", str(out_audio), text], timeout=180)
    if proc.returncode != 0:
        raise RuntimeError(f"Portuguese TTS failed: {friendly_error(proc)}")


def mux_video(ffmpeg: str, source: Path, audio: Path, output: Path) -> None:
    if not ffmpeg:
        raise RuntimeError("ffmpeg is required to remove the original audio and mux the Portuguese track.")
    proc = run(
        [
            ffmpeg,
            "-y",
            "-i",
            str(source),
            "-i",
            str(audio),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-shortest",
            str(output),
        ],
        timeout=300,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Video mux failed: {friendly_error(proc)}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--out", required=True)
    parser.add_argument("--language", default="pt-BR")
    parser.add_argument("--voice", default=os.environ.get("KOKO_PT_VOICE", "Luciana"))
    parser.add_argument("--model", default=os.environ.get("VIDEO_TRANSLATION_MODEL", "gemini-2.5-flash-lite"))
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    result_path = out_dir / "translation_result.json"
    try:
        metadata = download_source(args.url, out_dir)
        source = Path(metadata.get("local_video") or out_dir / "source.mp4")
        if not source.exists():
            raise RuntimeError("Downloaded source video is missing.")
        ffmpeg = resolve_ffmpeg()
        media_meta = ffprobe(ffmpeg, source)
        analysis = build_script_with_gemini(
            source,
            {**metadata, **media_meta},
            key=os.environ.get("GOOGLE_API_KEY", "").strip(),
            model=args.model,
        )
        voiceover = str(analysis.get("portuguese_voiceover") or "").strip()
        if not voiceover:
            raise RuntimeError("No Portuguese voiceover text was produced.")
        audio_path = out_dir / "portuguese_voiceover.aiff"
        synthesize_with_say(voiceover, audio_path, voice=args.voice)
        output_path = out_dir / "translated_pt.mp4"
        mux_video(ffmpeg, source, audio_path, output_path)
        payload = {
            "ok": True,
            "source_url": args.url,
            "language": args.language,
            "subject_summary": analysis.get("subject_summary") or "",
            "original_audio_summary": analysis.get("original_audio_summary") or "",
            "portuguese_voiceover": voiceover,
            "local_video": str(source),
            "translated_video": str(output_path),
            "audio_path": str(audio_path),
            "metadata": metadata,
            "media": media_meta,
            "model": analysis.get("model") or "",
        }
        write_json(result_path, payload)
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    except Exception as exc:
        payload = {"ok": False, "source_url": args.url, "error": str(exc)}
        write_json(result_path, payload)
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
