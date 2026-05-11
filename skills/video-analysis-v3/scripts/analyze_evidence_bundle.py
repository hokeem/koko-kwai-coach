#!/usr/bin/env python3
"""Analyze a Gemini evidence bundle into audited story/script JSON."""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ANALYSIS_PROMPT = """你是短视频证据分析器，也是最终中文脚本页的内容策划器。
你现在不会直接看视频，只会看到一个“逐秒证据包 JSON”。
请严格基于证据包做分析，不要发明视频里没有的事实。

工作目标：
1. 先根据 evidence timeline 还原视频里实际发生了什么。
2. 再把它整理成适合中文 HTML 脚本页展示的自然语言内容。
3. 不只描述表层动作，还要指出人物情绪、关系博弈、控制欲、面子、欲望、失控感、报复感、权力变化等“背后的原因”。
4. 提炼故事类型、包袱机制、关键复核窗口。
5. 输出可直接用于最终 HTML 渲染的结构化 JSON。

规则：
- 不要把遮挡处动作写成事实。
- 不要把不确定物体强行命名。
- 如果证据不足，用“无法确认/需复核/疑似”。
- 结论必须能回溯到 timeline 中的人物动作、道具变化、音频或文字。
- 所有字段值都使用简体中文。
- 最终内容风格要像“视频总结归纳 + 脚本表”，不是技术报告。
- `whole_video_summary` 必须写成完整自然语言段落。
- `synthesized_segments` 里的每一段，也必须是适合直接展示给用户的自然语言，而不是内部标记。
- 对“故事类视频”，不能只写“谁说了什么、做了什么”，还要点出其背后的情绪原因和关系机制。
- 例如：如果表面是“女子不乐意继续伺候男人”，你要进一步判断这背后是否体现了控制欲受损、地位被松动、对男人脱离管控后的快乐感到不爽、或对既有权力关系被挑战的不适。
- 这些“背后原因”必须写进 `whole_video_summary`、`humor_mechanism` 或 `core_points`，不要只停留在动作描述。
- `dialogue_text` 必须是对白/旁白的中文直译：只翻译，不改写，不润色，不替换说法，不补充解释，不故意变得更顺口。

输出严格 JSON，schema 必须如下：
{
  "mode": "final_script_package_v1",
  "report_route": "比如 evidence-bundle-llm",
  "audio_information_score": 1,
  "whole_video_summary": "整段视频的保守总结",
  "humor_mechanism": {
    "setup": "铺垫",
    "incongruity": "违和点",
    "reversal": "反转点",
    "punchline": "笑点落点",
    "underlying_reason": "这支视频真正成立的深层原因/情绪机制/权力关系"
  },
  "story_analysis": {
    "genre_guess": "类型判断",
    "confirmed_facts": ["事实1"],
    "mechanism_hypotheses": [
      {
        "type": "大类",
        "name": "机制名",
        "likelihood": "high/medium/low/candidate",
        "story_question": "要验证的核心问题",
        "story_chain": ["步骤1", "步骤2"],
        "evidence_for": ["支持证据1"],
        "evidence_against": ["反证或缺口"]
      }
    ],
    "verification_windows": [
      {"start": "00:10", "end": "00:14", "reason": "为什么要复核"}
    ],
    "safe_final_story": "仅基于证据的保守版最终故事",
    "core_points": [
      {"title": "核心爆点标题", "text": "核心爆点说明"}
    ],
    "replaceable_parts": [
      {"title": "可替换项", "text": "替换说明"}
    ],
    "must_not_claim_without_verification": ["不能直接下的结论"]
  },
  "logic_quality": "consistent/unresolved/suspicious",
  "synthesized_segments": [
    {
      "start": "00:00",
      "end": "00:04",
      "segment_role": "建立场景/动作发展/冲突升级/反转揭示/收尾",
      "objective_visual": "这一段用于 HTML 的自然语言画面描述",
      "integrated_summary": "这一段的综合说明",
      "action_text": "这一段的动作归纳，自然语言",
      "dialogue_text": "这一段关键对白/旁白的中文直译，可分句，可带人物名，但不能改写原意或换一种说法",
      "action_chain": ["动作1", "动作2"],
      "object_tracks": ["道具轨迹1", "道具轨迹2"],
      "raw_action_chain": ["逐秒动作摘要1"],
      "raw_object_tracks": ["逐秒道具摘要1"],
      "transition_reason": "为什么切到下一段",
      "key_action_times": ["00:01", "00:03"],
      "audio_lines": ["00:02 女子：..."],
      "uncertainty": "该段不确定点",
      "suspicion_notes": ["需要复核的怀疑点"],
      "allowed_claims": ["当前证据支持的说法"],
      "blocked_claims": ["当前证据不允许直接下的说法"],
      "logic_status": "consistent/unresolved/suspicious"
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


def retry_call(label: str, fn, attempts: int = 3, sleep_sec: int = 2):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            text = str(exc)
            retryable = isinstance(exc, RETRYABLE_NETWORK_ERRORS) or any(
                token in text for token in ["timed out", "EOF occurred", "Temporary failure", "Connection reset", "HTTP 503", "UNAVAILABLE"]
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
        obj, _ = decoder.raw_decode(t[start:])
        return obj
    raise ValueError("No parseable JSON object found in analysis response")


def analyze_bundle(bundle: dict, key: str, model: str) -> tuple[dict, dict]:
    body = {
        "contents": [
            {
                "parts": [
                    {"text": ANALYSIS_PROMPT},
                    {"text": json.dumps(bundle, ensure_ascii=False)},
                ]
            }
        ]
    }
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
            raise RuntimeError(f"Gemini analysis HTTP {exc.code}: {detail}") from exc

    api_resp = retry_call("Gemini analysis request", _send, attempts=2, sleep_sec=2)
    return parse_json_text(extract_text(api_resp)), api_resp


def main() -> int:
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("evidence_json")
        ap.add_argument("--metadata")
        ap.add_argument("--out", required=True)
        ap.add_argument("--raw-out")
        ap.add_argument("--model", default="gemini-2.5-flash-lite")
        ap.add_argument("--api-key")
        ap.add_argument("--api-key-file")
        args = ap.parse_args()

        bundle = json.loads(Path(args.evidence_json).read_text(encoding="utf-8"))
        key, key_source = api_key(args.api_key, args.api_key_file)
        result, raw = analyze_bundle(bundle, key, args.model)
        result["mode"] = result.get("mode", "final_script_package_v1")
        result.setdefault("report_route", "evidence-bundle-llm")
        result["analysis_route"] = bundle.get("analysis_route") or bundle.get("mode")
        result["gemini_model"] = bundle.get("gemini_model")
        result["analysis_model"] = args.model
        result["api_key_source"] = key_source
        result["timeline"] = bundle.get("timeline") or []
        result["observations"] = bundle.get("observations") or []
        if args.metadata and Path(args.metadata).exists():
            result["source_metadata"] = json.loads(Path(args.metadata).read_text())
        else:
            result["source_metadata"] = bundle.get("source_metadata", {})
        Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        if args.raw_out:
            Path(args.raw_out).write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            json.dumps(
                {
                    "out": args.out,
                    "model": args.model,
                    "segments": len(result.get("synthesized_segments") or []),
                    "logic_quality": result.get("logic_quality"),
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
