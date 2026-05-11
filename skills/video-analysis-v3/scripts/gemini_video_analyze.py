#!/usr/bin/env python3
"""Analyze a video with Google Gemini native video understanding."""
from __future__ import annotations

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_PROMPT = """你是短视频拆解助手。请直接分析这个视频，输出严格 JSON，不要输出 markdown，不要加解释。
要求：
1. 只描述视频中可见/可听的信息，不要编造。
2. 如果听不懂语言或无法确定对白，写“未能确认”。
3. 按时间顺序拆成 4-12 个片段；每个片段必须有 start/end。
4. 重点识别：人物、动作、场景、道具、对白/字幕/音乐、镜头变化、情绪变化、笑点/反转。
5. 如果是剧情/搞笑视频，说明铺垫、误导、反转、结尾。
6. 如果是口播/知识视频，说明 hook、观点、论据、节奏、结论。
JSON schema:
{
  "summary": "一句话概括视频发生了什么",
  "video_type": "情侣恶搞/剧情反转/口播/产品种草/教程/探店/其他",
  "hook": "开头吸引点",
  "turning_point": "反转点或关键信息变化",
  "ending": "结尾发生了什么",
  "why_it_works": "这个视频为什么有效/好笑/有传播点",
  "replicable_template": "可复用的脚本结构",
  "confidence": "high/medium/low",
  "timeline": [
    {
      "start": "00:00",
      "end": "00:05",
      "segment_role": "开场/铺垫/发展/反转/结尾",
      "visual": "画面内容",
      "audio_or_dialogue": "对白/字幕/音乐/声音线索；无法确认则写未能确认",
      "action": "人物动作",
      "camera": "镜头/机位/景别/运动；无法确认则写未能确认",
      "props": "关键道具",
      "content_function": "这一段在内容结构里的作用",
      "emotion_or_joke": "情绪变化/笑点/冲突点"
    }
  ]
}
"""


def api_key() -> str:
    if os.environ.get("GOOGLE_API_KEY"):
        return os.environ["GOOGLE_API_KEY"]
    p = Path("/root/.openclaw/agents/main/agent/models.json")
    if p.exists():
        data = json.loads(p.read_text())
        key = data.get("providers", {}).get("google", {}).get("apiKey")
        if key:
            return key
    raise SystemExit("GOOGLE_API_KEY not found and OpenClaw google provider key unavailable")


def post_json(url: str, body: dict, timeout: int = 180) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def extract_text(resp: dict) -> str:
    parts = resp.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "".join(p.get("text", "") for p in parts)


def parse_json_text(text: str) -> dict:
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json"):
            t = t[4:].strip()
    try:
        return json.loads(t)
    except Exception:
        start, end = t.find("{"), t.rfind("}")
        if start >= 0 and end > start:
            return json.loads(t[start:end + 1])
        raise


def inline_analyze(video: Path, key: str, model: str, prompt: str, mime: str) -> tuple[dict, dict]:
    data = base64.b64encode(video.read_bytes()).decode()
    body = {"contents": [{"parts": [{"inline_data": {"mime_type": mime, "data": data}}, {"text": prompt}]}]}
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    resp = post_json(url, body)
    return parse_json_text(extract_text(resp)), resp


def upload_file(video: Path, key: str, mime: str) -> dict:
    # Gemini resumable upload protocol.
    meta = {"file": {"display_name": video.name}}
    start = urllib.request.Request(
        f"https://generativelanguage.googleapis.com/upload/v1beta/files?key={key}",
        data=json.dumps(meta).encode(),
        headers={
            "X-Goog-Upload-Protocol": "resumable",
            "X-Goog-Upload-Command": "start",
            "X-Goog-Upload-Header-Content-Length": str(video.stat().st_size),
            "X-Goog-Upload-Header-Content-Type": mime,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(start, timeout=60) as resp:
        upload_url = resp.headers["X-Goog-Upload-URL"]
    upload = urllib.request.Request(
        upload_url,
        data=video.read_bytes(),
        headers={
            "Content-Length": str(video.stat().st_size),
            "X-Goog-Upload-Offset": "0",
            "X-Goog-Upload-Command": "upload, finalize",
        },
        method="POST",
    )
    with urllib.request.urlopen(upload, timeout=300) as resp:
        return json.loads(resp.read().decode())


def get_file(file_name: str, key: str) -> dict:
    req = urllib.request.Request(f"https://generativelanguage.googleapis.com/v1beta/{file_name}?key={key}")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def files_api_analyze(video: Path, key: str, model: str, prompt: str, mime: str) -> tuple[dict, dict]:
    uploaded = upload_file(video, key, mime)
    file_obj = uploaded.get("file", uploaded)
    name = file_obj.get("name")
    # Wait for processing if state is exposed.
    for _ in range(30):
        current = get_file(name, key)
        file_obj = current.get("file", current)
        state = file_obj.get("state")
        if state in (None, "ACTIVE"):
            break
        if state == "FAILED":
            raise RuntimeError(f"Gemini file processing failed: {file_obj}")
        time.sleep(2)
    body = {"contents": [{"parts": [{"file_data": {"mime_type": mime, "file_uri": file_obj["uri"]}}, {"text": prompt}]}]}
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    resp = post_json(url, body)
    return parse_json_text(extract_text(resp)), {"upload": uploaded, "generate": resp}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("video")
    ap.add_argument("--metadata")
    ap.add_argument("--out", required=True)
    ap.add_argument("--raw-out", required=True)
    ap.add_argument("--model", default="gemini-2.5-flash-lite")
    ap.add_argument("--mime", default="video/mp4")
    ap.add_argument("--inline-max-mb", type=float, default=18.0)
    ap.add_argument("--prompt-file")
    args = ap.parse_args()

    video = Path(args.video)
    prompt = Path(args.prompt_file).read_text(encoding="utf-8") if args.prompt_file else DEFAULT_PROMPT
    key = api_key()
    route = "inline"
    try:
        if video.stat().st_size <= args.inline_max_mb * 1024 * 1024:
            result, raw = inline_analyze(video, key, args.model, prompt, args.mime)
        else:
            route = "files-api"
            result, raw = files_api_analyze(video, key, args.model, prompt, args.mime)
    except urllib.error.HTTPError as e:
        if route == "inline":
            route = "files-api"
            result, raw = files_api_analyze(video, key, args.model, prompt, args.mime)
        else:
            raise

    result.setdefault("timeline", [])
    result["analysis_route"] = route
    result["gemini_model"] = args.model
    if args.metadata and Path(args.metadata).exists():
        result["source_metadata"] = json.loads(Path(args.metadata).read_text())
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.raw_out).write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"out": args.out, "raw_out": args.raw_out, "route": route, "model": args.model}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())