#!/usr/bin/env python3
"""Use Gemini native video understanding for dense per-second evidence extraction."""
from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

OBSERVATION_PROMPT = """你不是剧情分析师，你是视频证据提取器。
你的职责不是总结剧情，而是把整段视频转成一个“逐秒多模态证据包”。

请不要解释剧情，不要判断人物动机，不要总结笑点，不要推断未直接可见的行为。
所有字段值必须使用简体中文输出，不要使用英文描述。

请按每 1 秒输出一条 timeline entry。每条只记录客观可见/可听信息，尽量原子化，便于后续其他 LLM 再做分析。

禁止使用这些解释性词语或同义表达：假装、偷偷、骗、发现、以为、反转、搞笑、目的、动机、因为、所以、被恶搞、假钞。
除非画面文字/音频明确说明，或外观非常清楚，否则不要写“假钞”。
如果物体身份不确定，请写“纸状物/纸币状物/无法确认”，不要强行命名。
如果动作发生在遮挡区域，请明确写“被遮挡，无法确认”。

输出严格 JSON，schema 必须如下：
{
  "mode": "evidence_bundle_v1",
  "observation_interval_sec": 1,
  "duration_estimate": "视频时长估计",
  "timeline": [
    {
      "time": "00:00",
      "visual": {
        "scene": "这一秒的客观场景",
        "camera": "镜头状态，如固定/推进/晃动；不确定写无法确认",
        "people": [
          {
            "id": "人物标识",
            "position": "画面位置",
            "appearance": "外观衣着",
            "action": "这一秒可见动作",
            "expression": "可见表情；不确定写无法确认"
          }
        ],
        "objects": [
          {
            "label": "道具名或不确定标签",
            "position": "位置",
            "state": "可见状态"
          }
        ],
        "visible_text": ["画面文字1", "画面文字2"],
        "notable_change": "与上一秒相比最明显的可见变化；没有则写无",
        "uncertainty": "视觉上无法确认的点"
      },
      "audio": {
        "speech": "这一秒能听到的对白/旁白；没有则写无",
        "speakers": [
          {
            "speaker": "说话人，如男子/女子/旁白/无法确认",
            "utterance": "该说话人在这一秒说的话"
          }
        ],
        "music": "背景音乐情况；没有写无",
        "sfx": "明显音效/环境声；没有写无",
        "uncertainty": "音频上无法确认的点"
      }
    }
  ]
}
"""

RETRYABLE_NETWORK_ERRORS = (
    urllib.error.URLError,
    TimeoutError,
    socket.timeout,
)


def api_key(cli_key: str | None = None, cli_key_file: str | None = None) -> tuple[str, str]:
    if cli_key:
        return cli_key.strip(), "cli"
    if cli_key_file:
        key_text = Path(cli_key_file).read_text(encoding="utf-8").strip()
        if key_text:
            return key_text, "api-key-file"
    if os.environ.get("GOOGLE_API_KEY"):
        return os.environ["GOOGLE_API_KEY"], "env"
    p = Path("/root/.openclaw/agents/main/agent/models.json")
    if p.exists():
        data = json.loads(p.read_text())
        key = data.get("providers", {}).get("google", {}).get("apiKey")
        if key:
            return key, "openclaw-config"
    raise SystemExit(
        "Gemini API key not found. Tried: --api-key, --api-key-file, GOOGLE_API_KEY, /root/.openclaw/agents/main/agent/models.json"
    )


def post_json(url: str, body: dict, timeout: int = 240) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"Gemini HTTP {exc.code}: {detail}") from exc


def retry_call(label: str, fn, attempts: int = 3, sleep_sec: int = 2):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            text = str(exc)
            retryable = isinstance(exc, RETRYABLE_NETWORK_ERRORS) or any(
                token in text for token in ["timed out", "EOF occurred", "Temporary failure", "Connection reset"]
            )
            if not retryable or attempt == attempts:
                break
            time.sleep(sleep_sec * attempt)
    raise RuntimeError(f"{label} failed after retries: {last_error}") from last_error


def extract_text(resp: dict) -> str:
    parts = resp.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "".join(p.get("text", "") for p in parts)


def parse_json_text(text: str) -> dict:
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        if t.lower().startswith("json"):
            t = t[4:].strip()
    decoder = json.JSONDecoder()
    try:
        obj, _ = decoder.raw_decode(t)
        return obj
    except Exception:
        pass
    start = t.find("{")
    if start >= 0:
        try:
            obj, _ = decoder.raw_decode(t[start:])
            return obj
        except Exception:
            pass
    depth = 0
    in_str = False
    esc_next = False
    begin = -1
    for i, ch in enumerate(t):
        if esc_next:
            esc_next = False
            continue
        if ch == "\\" and in_str:
            esc_next = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            if depth == 0:
                begin = i
            depth += 1
        elif ch == "}" and depth:
            depth -= 1
            if depth == 0 and begin >= 0:
                return json.loads(t[begin:i + 1])
    raise ValueError("No parseable JSON object found in Gemini response")


def _timeline_to_observations(timeline: list[dict]) -> list[dict]:
    observations: list[dict] = []
    for item in timeline:
        visual = item.get("visual") or {}
        audio = item.get("audio") or {}
        speakers = audio.get("speakers") or []
        speaker_lines = []
        for speaker in speakers:
            if isinstance(speaker, dict):
                who = str(speaker.get("speaker") or "说话人")
                utt = str(speaker.get("utterance") or "")
                speaker_lines.append(f"{who}：{utt}" if utt else who)
            else:
                speaker_lines.append(str(speaker))
        audio_parts = [
            str(audio.get("speech") or ""),
            "；".join(x for x in speaker_lines if x),
            str(audio.get("music") or ""),
            str(audio.get("sfx") or ""),
        ]
        uncertainty_parts = [
            str(visual.get("uncertainty") or ""),
            str(audio.get("uncertainty") or ""),
        ]
        visible_text = visual.get("visible_text") or []
        if isinstance(visible_text, list):
            visible_text_value = "；".join(str(x) for x in visible_text if str(x).strip()) or "无"
        else:
            visible_text_value = str(visible_text or "无")
        observations.append(
            {
                "time": item.get("time", ""),
                "visual_scene": visual.get("scene", ""),
                "people": [
                    {
                        "id": person.get("id", ""),
                        "position": person.get("position", ""),
                        "appearance": person.get("appearance", ""),
                        "visible_action": person.get("action", ""),
                        "expression": person.get("expression", ""),
                    }
                    for person in (visual.get("people") or [])
                    if isinstance(person, dict)
                ],
                "objects": [
                    {
                        "label": obj.get("label", ""),
                        "position": obj.get("position", ""),
                        "state": obj.get("state", ""),
                    }
                    for obj in (visual.get("objects") or [])
                    if isinstance(obj, dict)
                ],
                "visible_text": visible_text_value,
                "audio": "；".join(x for x in audio_parts if x and x != "无") or "无",
                "uncertainty": "；".join(x for x in uncertainty_parts if x and x != "无") or "无",
            }
        )
    return observations


def _observations_to_timeline(observations: list[dict]) -> list[dict]:
    timeline: list[dict] = []
    for obs in observations:
        timeline.append(
            {
                "time": obs.get("time", ""),
                "visual": {
                    "scene": obs.get("visual_scene", ""),
                    "camera": "无法确认",
                    "people": [
                        {
                            "id": person.get("id", ""),
                            "position": person.get("position", ""),
                            "appearance": person.get("appearance", ""),
                            "action": person.get("visible_action", ""),
                            "expression": person.get("expression", "无法确认"),
                        }
                        for person in (obs.get("people") or [])
                        if isinstance(person, dict)
                    ],
                    "objects": obs.get("objects") or [],
                    "visible_text": [obs.get("visible_text", "无")],
                    "notable_change": "无",
                    "uncertainty": obs.get("uncertainty", "无"),
                },
                "audio": {
                    "speech": obs.get("audio", "无"),
                    "speakers": [],
                    "music": "无法确认",
                    "sfx": "无法确认",
                    "uncertainty": obs.get("uncertainty", "无"),
                },
            }
        )
    return timeline


def normalize_evidence_bundle(result: dict) -> dict:
    timeline = result.get("timeline")
    observations = result.get("observations")
    if isinstance(timeline, list) and timeline:
        result["observations"] = _timeline_to_observations(timeline)
    elif isinstance(observations, list) and observations:
        result["timeline"] = _observations_to_timeline(observations)
    else:
        result["timeline"] = []
        result["observations"] = []
    result.setdefault("mode", "evidence_bundle_v1")
    return result


def inline_observe(video: Path, key: str, model: str, prompt: str, mime: str) -> tuple[dict, dict]:
    data = base64.b64encode(video.read_bytes()).decode()
    body = {"contents": [{"parts": [{"inline_data": {"mime_type": mime, "data": data}}, {"text": prompt}]}]}
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-goog-api-key": key},
    )
    def _send():
        try:
            with urllib.request.urlopen(req, timeout=240) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"Gemini inline HTTP {exc.code}: {detail}") from exc

    api_resp = retry_call("Gemini inline request", _send, attempts=2, sleep_sec=2)
    return parse_json_text(extract_text(api_resp)), api_resp


def upload_file(video: Path, key: str, mime: str) -> dict:
    meta = {"file": {"display_name": video.name}}
    start = urllib.request.Request(
        "https://generativelanguage.googleapis.com/upload/v1beta/files",
        data=json.dumps(meta).encode(),
        headers={
            "x-goog-api-key": key,
            "X-Goog-Upload-Protocol": "resumable",
            "X-Goog-Upload-Command": "start",
            "X-Goog-Upload-Header-Content-Length": str(video.stat().st_size),
            "X-Goog-Upload-Header-Content-Type": mime,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    def _start():
        try:
            with urllib.request.urlopen(start, timeout=60) as resp:
                return resp.headers["X-Goog-Upload-URL"]
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"Gemini upload start HTTP {exc.code}: {detail}") from exc

    upload_url = retry_call("Gemini upload start", _start, attempts=2, sleep_sec=2)
    upload = urllib.request.Request(
        upload_url,
        data=video.read_bytes(),
        headers={"Content-Length": str(video.stat().st_size), "X-Goog-Upload-Offset": "0", "X-Goog-Upload-Command": "upload, finalize"},
        method="POST",
    )
    def _upload():
        try:
            with urllib.request.urlopen(upload, timeout=300) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"Gemini upload finalize HTTP {exc.code}: {detail}") from exc

    return retry_call("Gemini upload finalize", _upload, attempts=2, sleep_sec=3)


def get_file(file_name: str, key: str) -> dict:
    req = urllib.request.Request(
        f"https://generativelanguage.googleapis.com/v1beta/{file_name}",
        headers={"x-goog-api-key": key},
    )
    def _get():
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"Gemini get file HTTP {exc.code}: {detail}") from exc

    return retry_call("Gemini get file", _get, attempts=2, sleep_sec=2)


def files_api_observe(video: Path, key: str, model: str, prompt: str, mime: str) -> tuple[dict, dict]:
    uploaded = upload_file(video, key, mime)
    file_obj = uploaded.get("file", uploaded)
    name = file_obj.get("name")
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
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-goog-api-key": key},
    )
    def _generate():
        try:
            with urllib.request.urlopen(req, timeout=240) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"Gemini files generate HTTP {exc.code}: {detail}") from exc

    generate_resp = retry_call("Gemini files generate", _generate, attempts=2, sleep_sec=2)
    return parse_json_text(extract_text(generate_resp)), {"upload": uploaded, "generate": generate_resp}


def main() -> int:
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("video")
        ap.add_argument("--metadata")
        ap.add_argument("--out", required=True)
        ap.add_argument("--raw-out", required=True)
        ap.add_argument("--model", default="gemini-3-flash-preview")
        ap.add_argument("--mime", default="video/mp4")
        ap.add_argument("--inline-max-mb", type=float, default=18.0)
        ap.add_argument("--prompt-file")
        ap.add_argument("--api-key")
        ap.add_argument("--api-key-file")
        args = ap.parse_args()

        video = Path(args.video)
        prompt = Path(args.prompt_file).read_text(encoding="utf-8") if args.prompt_file else OBSERVATION_PROMPT
        key, key_source = api_key(args.api_key, args.api_key_file)
        route = "inline-observation"
        if video.stat().st_size <= args.inline_max_mb * 1024 * 1024:
            result, raw = inline_observe(video, key, args.model, prompt, args.mime)
        else:
            route = "files-api-observation"
            result, raw = files_api_observe(video, key, args.model, prompt, args.mime)
    except RuntimeError as exc:
        if "User location is not supported for the API use" in str(exc):
            print(str(exc), file=sys.stderr)
            return 1
        if route == "inline-observation":
            route = "files-api-observation"
            result, raw = files_api_observe(video, key, args.model, prompt, args.mime)
        else:
            print(str(exc), file=sys.stderr)
            return 1
    try:
        result = normalize_evidence_bundle(result)
        result["analysis_route"] = route
        result["gemini_model"] = args.model
        result["api_key_source"] = key_source
        if args.metadata and Path(args.metadata).exists():
            result["source_metadata"] = json.loads(Path(args.metadata).read_text())
        Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        Path(args.raw_out).write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            json.dumps(
                {
                    "out": args.out,
                    "raw_out": args.raw_out,
                    "route": route,
                    "model": args.model,
                    "observations": len(result.get("observations", [])),
                    "api_key_source": key_source,
                },
                ensure_ascii=False,
            )
        )
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
