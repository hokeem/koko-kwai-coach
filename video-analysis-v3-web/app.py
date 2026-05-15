#!/usr/bin/env python3
"""Public-facing web UI for video-analysis-v3."""
from __future__ import annotations

import html
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
from collections import Counter, deque
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from uuid import uuid4


PORT = int(os.environ.get("PORT", 8310))
PIPELINE_TIMEOUT_SEC = int(os.environ.get("VIDEO_ANALYSIS_PIPELINE_TIMEOUT_SEC", "480"))
MAX_CONCURRENT_ANALYSES = max(1, int(os.environ.get("VIDEO_ANALYSIS_MAX_CONCURRENT_JOBS", "1")))
STAGE_TIMEOUTS_SEC = {
    "download": 150,
    "media_prep": 45,
    "gemini_analysis": 210,
    "v2_analysis": 150,
    "consistency_audit": 90,
    "targeted_recheck": 180,
    "arbitration": 60,
    "final_output": 180,
}
BASE = Path(__file__).resolve().parent
REPO_ROOT = BASE.parent
SKILL_ROOT = REPO_ROOT / "skills" / "video-analysis-v3"
V2_SKILL_ROOT = REPO_ROOT / "skills" / "video-analysis-v2-skill"
AUTO_ANALYZE = SKILL_ROOT / "scripts" / "hybrid_v2_pipeline.py"
DATA_ROOT = Path(os.environ.get("VIDEO_ANALYSIS_WEB_DATA_DIR", str(BASE / "data"))).expanduser()
JOBS_FILE = DATA_ROOT / "jobs.json"
RESULTS_ROOT = DATA_ROOT / "results"
LIBRARY_FILE = DATA_ROOT / "script_library.json"
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
ASSETS_ROOT = BASE / "assets"
HERO_WORDMARK = ASSETS_ROOT / "kwai-wordmark.svg"
MODEL_CANDIDATES = [
    os.environ.get("VIDEO_ANALYSIS_MODEL", "gemini-2.5-flash-lite"),
    "gemini-2.5-flash",
    "gemini-3-flash-preview",
]

job_lock = threading.Lock()
jobs: dict[str, dict[str, Any]] = {}
job_queue: deque[str] = deque()
queued_job_ids: set[str] = set()
queue_condition = threading.Condition()
analysis_slots = threading.BoundedSemaphore(MAX_CONCURRENT_ANALYSES)

if (SKILL_ROOT / "scripts").exists():
    sys.path.insert(0, str(SKILL_ROOT / "scripts"))

try:
    from docx import Document
except Exception:
    Document = None

try:
    from hybrid_v2_pipeline import (
        PRIMARY_FALLBACK_MODELS,
        SUPPLEMENT_FALLBACK_MODELS,
        enforce_chinese_dialogue_translation,
        run_text_json_prompt_with_fallback,
        run_video_json_prompt_with_fallback,
        unique_models,
    )
except Exception:
    PRIMARY_FALLBACK_MODELS = ["gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-3-flash-preview"]
    SUPPLEMENT_FALLBACK_MODELS = ["gemini-2.5-flash-lite", "gemini-2.5-flash", "gemini-3-flash-preview"]
    def enforce_chinese_dialogue_translation(script_json: dict, key: str, models: list[str]) -> dict:
        return script_json
    run_text_json_prompt_with_fallback = None
    run_video_json_prompt_with_fallback = None
    def unique_models(*names: str) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for name in names:
            value = str(name or "").strip()
            if value and value not in seen:
                seen.add(value)
                out.append(value)
        return out


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def html_escape(value: object) -> str:
    return html.escape(str(value or ""), quote=True)


def read_json_file(path: Path, *, default: Any, backup_on_error: bool = True) -> Any:
    if not path.exists():
        return default
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return default
    if not raw.strip():
        if backup_on_error:
            try:
                broken = path.with_suffix(path.suffix + f".empty-{int(time.time())}.bak")
                path.replace(broken)
            except Exception:
                pass
        return default
    try:
        return json.loads(raw)
    except Exception:
        if backup_on_error:
            try:
                broken = path.with_suffix(path.suffix + f".broken-{int(time.time())}.bak")
                path.replace(broken)
            except Exception:
                pass
        return default


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.replace(path)


def write_json_atomic(path: Path, payload: Any) -> None:
    write_text_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2))


def load_jobs() -> None:
    global jobs
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
    jobs = read_json_file(JOBS_FILE, default={})
    if not isinstance(jobs, dict):
        jobs = {}
    backfill_completed_jobs()
    sync_library_from_jobs()


def save_jobs() -> None:
    write_json_atomic(JOBS_FILE, jobs)


def enqueue_job(job_id: str) -> None:
    with queue_condition:
        if job_id in queued_job_ids:
            return
        queued_job_ids.add(job_id)
        job_queue.append(job_id)
        queue_condition.notify()


def restore_pending_jobs_to_queue() -> None:
    pending_job_ids: list[str] = []
    changed = False
    with job_lock:
        for job_id, job in jobs.items():
            items = job.get("items") or []
            if not items:
                continue
            pending = False
            completed_count = 0
            failed_count = 0
            for item in items:
                status = str(item.get("status") or "").strip()
                if status == "completed":
                    completed_count += 1
                    continue
                if status == "failed":
                    failed_count += 1
                    continue
                pending = True
                if status != "queued":
                    item["status"] = "queued"
                    item["stage"] = "queued"
                    item["stage_message"] = "Queued after service restart."
                    item["updated_at"] = now_iso()
                    changed = True
            if pending:
                job["status"] = "queued"
                job["stage"] = "queued"
                job["stage_message"] = "Queued after service restart."
                job["updated_at"] = now_iso()
                pending_job_ids.append(job_id)
                changed = True
            elif items and completed_count == len(items):
                if job.get("status") != "completed":
                    job["status"] = "completed"
                    job["stage"] = "completed"
                    job["stage_message"] = f"Completed {completed_count}/{len(items)} items. Failed {failed_count}."
                    changed = True
            elif items and failed_count == len(items):
                if job.get("status") != "failed":
                    job["status"] = "failed"
                    job["stage"] = "failed"
                    job["stage_message"] = "All batch items failed."
                    changed = True
        if changed:
            save_jobs()
    for job_id in pending_job_ids:
        enqueue_job(job_id)


def job_worker_loop() -> None:
    while True:
        with queue_condition:
            while not job_queue:
                queue_condition.wait()
            job_id = job_queue.popleft()
            queued_job_ids.discard(job_id)
        with analysis_slots:
            run_job_batch(job_id)


def start_job_workers() -> None:
    for index in range(MAX_CONCURRENT_ANALYSES):
        thread = threading.Thread(target=job_worker_loop, name=f"koko-job-worker-{index+1}", daemon=True)
        thread.start()


def read_json(path: Path) -> dict[str, Any]:
    data = read_json_file(path, default={})
    return data if isinstance(data, dict) else {}


def load_library_entries() -> list[dict[str, Any]]:
    data = read_json_file(LIBRARY_FILE, default=[])
    if not isinstance(data, list):
        return []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        content_type = str(entry.get("content_type") or "").strip()
        if content_type not in ALLOWED_CONTENT_TYPES:
            entry["content_type"] = DEFAULT_CONTENT_TYPE
    return data


def save_library_entries(entries: list[dict[str, Any]]) -> None:
    write_json_atomic(LIBRARY_FILE, entries)


def split_video_urls(raw: str) -> list[str]:
    urls: list[str] = []
    for part in re.split(r"[\n\r,]+", raw or ""):
        value = part.strip()
        if not value:
            continue
        if re.match(r"^https?://", value, re.IGNORECASE):
            urls.append(value)
    seen: set[str] = set()
    ordered: list[str] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        ordered.append(url)
    return ordered


CONTENT_TYPE_RULES: list[tuple[str, list[str]]] = [
    ("夫妻吵架", ["院子", "打扫", "装修", "凉棚", "干活", "兄弟", "朋友", "亲戚", "打电话", "花钱找人"]),
    ("夫妻出轨", ["抓奸", "奸夫", "脚印", "倒着走", "假脚印", "电工", "修水管", "换灯泡", "第三者"]),
    ("夫妻好色", ["美女", "帅哥", "照镜子", "车窗", "放下车窗", "偷看", "搭讪", "暴揍", "抓包"]),
    ("妻管严", ["喝酒", "训斥", "别管", "给你钱", "疯老婆", "爱骂我的", "送酒", "回到原样"]),
    ("夫妻欺骗", ["酣睡", "睡觉", "偷偷溜", "信用卡", "商场", "购物", "赶回床上", "包揽家务"]),
    ("夫妻算计", ["穿搭", "惊喜", "模仿", "嫉妒", "聚会", "不给你看", "太傻", "风格"]),
    ("夫妻整蛊", ["气球", "恐怖电视", "关键时候", "扎破", "吓得", "掉下沙发", "夫妻整蛊"]),
    ("夫妻黄段子", ["没穿内衣", "什么都没穿", "挑逗", "性暗示", "银行卡", "买内衣", "太穷", "取钱"]),
    ("撬墙角", ["撬墙角", "抢男友", "抢女友", "闺蜜", "兄弟老婆", "挖墙脚", "横刀夺爱"]),
    ("偷吃东西", ["偷吃", "偷喝", "冰箱", "零食", "吃独食", "藏吃的", "背着吃"]),
    ("赖账", ["赖账", "不给钱", "欠钱", "不认账", "逃单", "不给结账", "不还钱"]),
    ("骗子", ["骗子", "骗局", "被骗", "假装", "冒充", "诈骗", "忽悠", "圈套"]),
    ("偷奸耍滑", ["偷懒", "耍滑", "偷奸耍滑", "装病", "钻空子", "耍小聪明", "蒙混过关"]),
    ("整蛊", ["整蛊", "恶作剧", "捉弄", "恶搞", "吓唬", "陷阱", "搞怪"]),
]

ALLOWED_CONTENT_TYPES = {label for label, _ in CONTENT_TYPE_RULES}
DEFAULT_CONTENT_TYPE = "待分类"
LIBRARY_FILTER_LABELS = [
    "夫妻吵架",
    "夫妻出轨",
    "夫妻好色",
    "妻管严",
    "夫妻欺骗",
    "夫妻算计",
    "夫妻整蛊",
    "夫妻黄段子",
    "撬墙角",
    "偷吃东西",
    "赖账",
    "骗子",
    "偷奸耍滑",
    "整蛊",
    DEFAULT_CONTENT_TYPE,
]


def detect_content_type(script: dict[str, Any], bundle: dict[str, Any] | None = None) -> str:
    routing = (bundle or {}).get("routing") or script.get("type_router") or {}
    primary = routing.get("primary_type") or ""
    subtype = routing.get("subtype_guess") or ""
    for candidate in [subtype, primary]:
        if candidate:
            text = str(candidate)
            if text in ALLOWED_CONTENT_TYPES:
                return text
    text = " ".join(
        str(x or "")
        for x in [
            routing.get("reasoning_summary"),
            script.get("whole_video_summary"),
            script.get("title"),
            (script.get("mechanism") or {}).get("reason"),
            (script.get("type_router") or {}).get("reasoning_summary"),
        ]
    )
    for label, keywords in CONTENT_TYPE_RULES:
        hit_count = sum(1 for word in keywords if word in text)
        if hit_count >= 2:
            return label
    return DEFAULT_CONTENT_TYPE


def detect_content_type_for_output(
    output_dir: Path,
    script: dict[str, Any] | None = None,
    bundle: dict[str, Any] | None = None,
) -> str:
    script_json = script or read_json(output_dir / "script_table.json") or {}
    bundle_json = bundle or read_json(output_dir / "evidence_bundle.json") or {}
    type_router = read_json(output_dir / "type_router.json") or {}
    if type_router and not bundle_json.get("routing"):
        bundle_json = {**bundle_json, "routing": type_router}
    if type_router and not script_json.get("type_router"):
        script_json = {**script_json, "type_router": type_router}
    return detect_content_type(script_json, bundle_json)


def choose_script_rows(script: dict[str, Any]) -> list[dict[str, Any]]:
    rows = script.get("rows")
    if isinstance(rows, list) and rows:
        return [row for row in rows if isinstance(row, dict)]
    rows = script.get("synthesized_segments")
    if isinstance(rows, list) and rows:
        return [row for row in rows if isinstance(row, dict)]
    return []


def fill_text(value: Any, fallback: str = "无") -> str:
    text = str(value or "").strip()
    return text if text else fallback


HEAVY_REVIEW_FAILURE_LAYERS = {
    "story_spine",
    "primary_analysis",
    "entity_mapping",
}


def should_run_review_video_recheck(review_plan: dict[str, Any]) -> tuple[bool, str]:
    if not bool(review_plan.get("needs_video_recheck")):
        return False, "Review plan says the prior evidence is enough."
    layer = str(review_plan.get("likely_failure_layer") or "").strip()
    if layer in HEAVY_REVIEW_FAILURE_LAYERS:
        return True, f"Heavy review triggered for {layer}."
    return False, f"Skipping video recheck for lighter failure layer: {layer or 'unknown'}."


REVIEW_PLAN_PROMPT = """你是一个短视频分析复盘诊断器。

输入里会包含：
1. 旧的脚本结果
2. 旧的主分析、补证据、音频侧产物
3. 人类审阅者用自然语言写的反馈，说明这次分析哪里错了

你的任务不是直接重写脚本，而是先做“复盘诊断”：
- 判断这次错误更像是故事主轴理解错误、人物/关系映射错误、关键物体错误、因果链错误，还是仅仅需要局部复核
- 判断旧证据是否仍可用
- 判断是否需要重新让 Gemini 回看视频
- 如果需要回看，给出最值得复核的时间窗和关注点

要求：
- 把人类反馈当作“高优先级纠错假设”，但不是绝对真理
- 不要替旧答案辩护
- 优先寻找最省资源的纠正路径
- 如果你认为不需要重新看视频，也要解释原因

输出严格 JSON：
{
  "problem_summary": "一句话概括人类指出的问题",
  "likely_failure_layer": "final_refine/primary_analysis/supplement/type_router/entity_mapping/story_spine/unknown",
  "needs_video_recheck": true,
  "needs_structural_rewrite": true,
  "reasoning": "为什么这样判断",
  "focus_windows": [
    {
      "start": "00:20",
      "end": "00:55",
      "reason": "这里决定了故事主轴"
    }
  ],
  "focus_entities": [
    "需要重点确认的人物、关系、道具或动作"
  ],
  "correction_goal": "纠偏后最应该纠正成什么方向",
  "confidence": "low/medium/high"
}
"""


REVIEW_VIDEO_PROMPT = """你现在是在做一次“复盘回看”，不是从零分析整条视频。

背景：
- 之前这条视频已经被分析过，但人类审阅者指出核心理解可能错了
- 你需要带着这个反馈，重新确认关键证据

要求：
- 不要为旧答案辩护
- 把人类反馈当成一个需要验证的纠错假设
- 只关注：人物关系、关键道具、关键动作、关键对白、故事主轴
- 如果某个关键点仍无法证实，要明确说不确定
- 如果给出了 focus windows，请优先围绕这些窗口回看

输出严格 JSON：
{
  "feedback_hypothesis": "人类反馈想纠正的核心点",
  "verification_result": "confirmed/partially_confirmed/rejected/inconclusive",
  "corrected_story_spine": "基于回看后，更稳妥的一句话故事主轴",
  "evidence_findings": [
    {
      "time": "00:20-00:55",
      "finding": "这一段真正发生了什么",
      "supports_feedback": true,
      "confidence": "low/medium/high"
    }
  ],
  "entity_corrections": [
    {
      "wrong": "之前模型误写的东西",
      "correct": "更可能正确的理解",
      "evidence": "为什么"
    }
  ],
  "rewrite_notes": [
    "后续重写脚本时必须遵守的纠偏说明"
  ]
}
"""


REVIEW_REFINE_PROMPT = """你是一个“复盘重做”脚本整理器。

输入里会有：
1. 旧脚本结果
2. 人类反馈
3. review_plan（复盘诊断）
4. 可选的 review_video_recheck（带着纠错假设重新回看视频后的结果）

你的任务：
- 不要机械修几个字
- 要根据人类指出的核心问题，对脚本做结构性纠偏
- 如果 review_video_recheck 提供了更可靠的故事主轴或实体纠正，优先使用
- 如果 review_video_recheck 不充分，也要基于 review_plan 做保守改写
- 必须保持 v2 script_table.json 结构
- `dialogue_or_audio` 是最终 HTML 里“对话”部分的直接来源，必须继续保持中文 1:1 直译：只翻译，不改写，不润色，不概括，不补解释，不合并句子，不删减语气词
- 如果某个实体仍不确定，宁可保守，也不要继续沿用明显错误的旧判断

成品写法 SOP（必须遵守）：
- `title` 必须写成完整的故事钩子句，不要只写前半段设定。要尽量把“人物 + 冲突/伪装 + 最终落点/反转”放在同一句里，优先使用 `不料`、`却`、`反而`、`最终`、`结果` 等连接词把结局说满。
- `whole_video_summary` 要按“起因 -> 推进 -> 对质/证据 -> 最终落点”来写，重点落在具体剧情和最终结果，不要用抽象的“揭示了复杂关系/人性弱点/社会判断”来替代真正的结局描述。
- 背后原因可以写，但必须嵌在具体人物动机里，例如“他为了维护面子选择否认真相”，而不是另起一段空泛说教。
- 如果人物关系在复盘后已经足够稳定，`title` 和 `whole_video_summary` 优先用自然角色称呼（丈夫/妻子/邻居等），不要为了机械一致性继续保留 `男性A/女性A`。
- `core_viral_points` 要写“为什么成立”，不能只是再讲一遍剧情。优先写反差、打脸、掩饰、错位、面子、防守、关系翻转等机制。
- `mechanism.items[*].text` 继续保持具体，尤其 `背后原因` 必须落在这条视频里的真实人物心理和关系机制上。
- 如果旧版本里有空泛的抽象收束句，请在复盘版里收掉，改成更具体的剧情归纳和结局描述。

输出严格 JSON：
{
  "title": "视频总结归纳 + 脚本表",
  "route": "audio-sop 或 keyframe-sop",
  "audio_information_score": "0/10 到 10/10",
  "source_url": "原视频链接",
  "whole_video_summary": "纠偏后的完整总结",
  "core_viral_points": [
    {"label": "核心点标题", "text": "为什么成立"}
  ],
  "replaceable_parts": [
    {"label": "可替换项", "text": "替换说明"}
  ],
  "rows": [
    {
      "source_url": "原视频链接",
      "time": "00:00-00:15",
      "visual_content": "这一段整体看到了什么",
      "action": "这一段动作如何推进",
      "dialogue_or_audio": "中文 1:1 直译，不改写，不概括，不润色",
      "integrated_summary": "如果有必要可补充"
    }
  ],
  "mechanism": {
    "title": "包袱机制",
    "items": [
      {"label": "铺垫", "text": "..."},
      {"label": "违和点", "text": "..."},
      {"label": "反转点", "text": "..."},
      {"label": "笑点落点", "text": "..."},
      {"label": "背后原因", "text": "..."}
    ]
  }
}
"""


def write_script_docx(output_dir: Path, script: dict[str, Any], video_url: str) -> Path | None:
    if Document is None:
        return None
    path = output_dir / "script_export.docx"
    try:
        doc = Document()
        doc.add_heading(script.get("title") or "Video Script", 0)
        doc.add_paragraph(f"Source URL: {video_url}")
        doc.add_paragraph(f"Content Type: {detect_content_type(script)}")
        summary = script.get("whole_video_summary") or ""
        if summary:
            doc.add_heading("Whole Video Summary", level=1)
            doc.add_paragraph(summary)
        mechanism = script.get("mechanism") or {}
        if mechanism:
            doc.add_heading("Mechanism", level=1)
            for key in ["name", "reason", "backfire_point", "story_question"]:
                value = mechanism.get(key)
                if value:
                    doc.add_paragraph(f"{key}: {value}")
        rows = choose_script_rows(script)
        if rows:
            doc.add_heading("Script Rows", level=1)
            for idx, row in enumerate(rows, start=1):
                title = row.get("time") or row.get("start") or f"Row {idx}"
                doc.add_heading(f"{idx}. {title}", level=2)
                for key in ["visual_content", "action", "dialogue_or_audio", "integrated_summary", "logic_status"]:
                    value = row.get(key)
                    if value:
                        doc.add_paragraph(f"{key}: {value}")
        doc.save(path)
        return path if path.exists() else None
    except Exception:
        return None


def append_library_entry(entry: dict[str, Any]) -> None:
    with job_lock:
        entries = load_library_entries()
        entries = [existing for existing in entries if existing.get("entry_id") != entry.get("entry_id")]
        entries.insert(0, entry)
        save_library_entries(entries[:500])


def delete_library_entry(entry_id: str) -> bool:
    removed = False
    with job_lock:
        entries = load_library_entries()
        next_entries = [entry for entry in entries if entry.get("entry_id") != entry_id]
        if len(next_entries) != len(entries):
            save_library_entries(next_entries)
            removed = True

        job_ids_to_delete: list[str] = []
        for job_id, job in list(jobs.items()):
            items = job.get("items") or []
            if items:
                filtered_items = [item for item in items if item.get("id") != entry_id]
                if len(filtered_items) != len(items):
                    removed = True
                    if filtered_items:
                        job["items"] = filtered_items
                        current_index = int(job.get("current_item_index") or 0)
                        job["current_item_index"] = max(0, min(current_index, len(filtered_items) - 1))
                        job["updated_at"] = now_iso()
                    else:
                        job_ids_to_delete.append(job_id)
            elif job_id == entry_id:
                removed = True
                job_ids_to_delete.append(job_id)

        for job_id in job_ids_to_delete:
            jobs.pop(job_id, None)

        if removed:
            save_jobs()

    output_dir = RESULTS_ROOT / entry_id
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
        removed = True
    return removed


def should_try_next_model(error_text: str) -> bool:
    hay = (error_text or "").upper()
    return any(
        token in hay
        for token in [
            "HTTP 400",
            "FAILED_PRECONDITION",
            "HTTP 503",
            "UNAVAILABLE",
            "RESOURCE_EXHAUSTED",
            "TIMED OUT",
            "REMOTE END CLOSED CONNECTION WITHOUT RESPONSE",
            "EOF OCCURRED",
            "CONNECTION RESET",
            "BROKEN PIPE",
            "NO PARSEABLE JSON OBJECT FOUND",
            "JSON",
        ]
    )


def friendly_error(error_text: str) -> str:
    text = (error_text or "").strip()
    if not text:
        return "分析失败，未返回具体错误。"
    if "HTTP 503" in text or "UNAVAILABLE" in text or "HIGH DEMAND" in text.upper():
        return "Gemini 当前负载较高，已自动尝试回退模型，但这次仍未成功。请稍后重试。"
    if "FAILED_PRECONDITION" in text and "User location is not supported" in text:
        return "当前运行环境所在地区不支持这个 Gemini 接口。"
    if "timed out" in text.lower():
        return "请求超时，可能是目标视频或 Gemini 响应过慢。"
    if "remote end closed connection without response" in text.lower():
        return "Gemini 连接被远端中断了，这次已尝试自动换模型；如果仍失败，通常是接口瞬时不稳定。"
    return text


def extract_json_line(text: str) -> dict[str, Any] | None:
    for line in reversed([line.strip() for line in text.splitlines() if line.strip()]):
        if not line.startswith("{"):
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def collect_observation_rows(observations: list[dict[str, Any]], limit: int = 24) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for obs in observations[:limit]:
        people = []
        for person in obs.get("people") or []:
            if isinstance(person, dict):
                people.append(
                    " / ".join(
                        str(person.get(key, ""))
                        for key in ["id", "position", "visible_action"]
                        if person.get(key)
                    )
                )
            else:
                people.append(str(person))
        objects = []
        for obj in obs.get("objects") or []:
            if isinstance(obj, dict):
                objects.append(
                    " / ".join(
                        str(obj.get(key, ""))
                        for key in ["label", "position", "state"]
                        if obj.get(key)
                    )
                )
            else:
                objects.append(str(obj))
        rows.append(
            {
                "time": str(obs.get("time", "")),
                "scene": str(obs.get("visual_scene", "")),
                "people": "；".join(x for x in people if x),
                "objects": "；".join(x for x in objects if x),
                "audio": str(obs.get("audio", "")),
                "visible_text": str(obs.get("visible_text", "")),
                "uncertainty": str(obs.get("uncertainty", "")),
            }
        )
    return rows


def raw_text_preview(raw: dict[str, Any], limit: int = 2200) -> str:
    parts = raw.get("generate", {}).get("candidates", [{}])[0].get("content", {}).get("parts", [])
    text = "".join(part.get("text", "") for part in parts if isinstance(part, dict)).strip()
    if not text:
        return ""
    return text[:limit] + ("\n..." if len(text) > limit else "")


def summarize_artifacts(job_id: str, output_dir: Path) -> dict[str, str]:
    names = [
        "source.mp4",
        "source_metadata.json",
        "primary_v2_draft.json",
        "primary_analysis_raw_gemini.json",
        "type_router.json",
        "audio_multiview.json",
        "audio_multiview_raw_gemini.json",
        "supplement_evidence.json",
        "supplement_raw_gemini.json",
        "final_refine_raw_gemini.json",
        "case_memory_entry.json",
        "observations.json",
        "observations_raw_gemini.json",
        "analysis_raw_gemini.json",
        "analysis_result.json",
        "script_table.json",
        "script_table.html",
        "evidence_bundle.json",
        "product_report.html",
    ]
    out: dict[str, str] = {}
    for name in names:
        if (output_dir / name).exists():
            out[name] = f"/results/{job_id}/{name}"
    return out


def backfill_completed_jobs() -> None:
    changed = False
    for job_id, job in jobs.items():
        if job.get("status") != "completed":
            continue
        output_dir = RESULTS_ROOT / job_id
        artifacts = summarize_artifacts(job_id, output_dir)
        if not artifacts:
            continue
        if artifacts.get("product_report.html") and not job.get("report_url"):
            job["report_url"] = artifacts["product_report.html"]
            changed = True
        if artifacts.get("evidence_bundle.json") and not job.get("evidence_url"):
            job["evidence_url"] = artifacts["evidence_bundle.json"]
            changed = True
        if artifacts != (job.get("artifacts") or {}):
            job["artifacts"] = artifacts
            changed = True
    if changed:
        save_jobs()


def sync_library_from_jobs() -> None:
    existing_entries = {entry.get("entry_id"): entry for entry in load_library_entries()}
    for parent_job_id, job in jobs.items():
        items = job.get("items") or []
        if not items and job.get("status") == "completed":
            output_dir = RESULTS_ROOT / parent_job_id
            script_json = read_json(output_dir / "script_table.json") or read_json(output_dir / "analysis_result.json") or job.get("result_json") or {}
            if script_json:
                docx_path = write_script_docx(output_dir, script_json, job.get("video_url") or "")
                docx_url = f"/results/{parent_job_id}/{docx_path.name}" if docx_path and docx_path.exists() else ""
                entry = {
                    "entry_id": parent_job_id,
                    "parent_job_id": parent_job_id,
                    "created_at": job.get("completed_at") or job.get("updated_at") or now_iso(),
                    "video_url": job.get("video_url") or "",
                    "title": script_json.get("title") or "Video Script",
                    "content_type": detect_content_type_for_output(output_dir, script_json, read_json(output_dir / "evidence_bundle.json")),
                    "whole_video_summary": script_json.get("whole_video_summary") or "",
                    "html_url": job.get("html_url") or f"/results/{parent_job_id}/script_table.html",
                    "report_url": job.get("report_url") or f"/results/{parent_job_id}/product_report.html",
                    "evidence_url": job.get("evidence_url") or f"/results/{parent_job_id}/evidence_bundle.json",
                    "docx_url": docx_url,
                }
                append_library_entry(entry)
                existing_entries[parent_job_id] = entry
        for item in items:
            if item.get("status") != "completed":
                continue
            output_dir = RESULTS_ROOT / item["id"]
            script_json = read_json(output_dir / "script_table.json") or read_json(output_dir / "analysis_result.json") or item.get("result_json") or {}
            if not script_json:
                continue
            if not item.get("docx_url"):
                docx_path = write_script_docx(output_dir, script_json, item.get("video_url") or "")
                if docx_path and docx_path.exists():
                    item["docx_url"] = f"/results/{item['id']}/{docx_path.name}"
            item["content_type"] = item.get("content_type") or detect_content_type_for_output(output_dir, script_json, read_json(output_dir / "evidence_bundle.json"))
            item["title"] = item.get("title") or script_json.get("title") or "Video Script"
            persist_library_entry(parent_job_id, item)
            existing_entries[item.get("id")] = item
    save_jobs()


def build_evidence_bundle(job_id: str, output_dir: Path, result_json: dict[str, Any]) -> dict[str, Any]:
    observations = read_json(output_dir / "observations.json")
    raw = read_json(output_dir / "observations_raw_gemini.json")
    analysis = read_json(output_dir / "analysis_result.json")
    script = analysis or read_json(output_dir / "script_table.json")
    type_router = read_json(output_dir / "type_router.json") or script.get("type_router") or {}
    case_memory_entry = read_json(output_dir / "case_memory_entry.json")
    similar_cases = script.get("similar_cases_used") or []

    usage = raw.get("generate", {}).get("usageMetadata", {})
    upload = raw.get("upload", {}).get("file", {})
    source_meta = script.get("source_metadata") or observations.get("source_metadata") or {}
    observation_rows = collect_observation_rows(observations.get("observations") or [], limit=30)
    segments = (script.get("synthesized_segments") or [])[:12]
    windows = (script.get("story_analysis") or {}).get("verification_windows") or []
    hypotheses = (script.get("story_analysis") or {}).get("mechanism_hypotheses") or []
    raw_preview = raw_text_preview(raw)

    bundle = {
        "job_id": job_id,
        "generated_at": now_iso(),
        "source": {
            "video_url": source_meta.get("source_url") or source_meta.get("input") or "",
            "title": source_meta.get("title") or "",
            "route": source_meta.get("route") or observations.get("analysis_route") or "",
            "local_video": source_meta.get("local_video") or "",
            "downloaded_bytes": source_meta.get("downloaded_bytes") or source_meta.get("size") or 0,
            "mime_type": source_meta.get("mime_type") or "",
        },
        "extraction": {
            "analysis_route": observations.get("analysis_route") or "",
            "gemini_model": observations.get("gemini_model") or result_json.get("model") or "",
            "analysis_model": script.get("analysis_model") or result_json.get("analysis_model") or "",
            "api_key_source": observations.get("api_key_source") or "",
            "duration_estimate": observations.get("duration_estimate") or "",
            "observation_interval_sec": observations.get("observation_interval_sec") or "",
            "observation_count": len(observations.get("observations") or []),
            "upload_file": {
                "name": upload.get("name") or "",
                "mime_type": upload.get("mimeType") or "",
                "size_bytes": upload.get("sizeBytes") or "",
                "state": upload.get("state") or "",
            },
            "usage": {
                "prompt_tokens": usage.get("promptTokenCount"),
                "candidate_tokens": usage.get("candidatesTokenCount"),
                "total_tokens": usage.get("totalTokenCount"),
                "thoughts_tokens": usage.get("thoughtsTokenCount"),
            },
        },
        "evidence": {
            "timeline_preview": observation_rows,
            "raw_gemini_preview": raw_preview,
            "verification_windows": windows[:14],
            "mechanism_hypotheses": hypotheses[:8],
        },
        "reasoning": {
            "whole_video_summary": script.get("whole_video_summary") or "",
            "safe_final_story": (script.get("story_analysis") or {}).get("safe_final_story") or "",
            "logic_quality": script.get("logic_quality") or "",
            "analysis_route": script.get("analysis_route") or "",
            "segments_preview": segments,
        },
        "routing": {
            "routing_mode": type_router.get("routing_mode") or "",
            "primary_type": type_router.get("primary_type") or "",
            "subtype_guess": type_router.get("subtype_guess") or "",
            "confidence": type_router.get("confidence") or "",
            "reasoning_summary": type_router.get("reasoning_summary") or "",
            "matched_templates": type_router.get("matched_templates") or [],
            "review_questions": type_router.get("review_questions") or [],
        },
        "case_memory": {
            "similar_cases_used": similar_cases[:3],
            "current_entry": case_memory_entry,
        },
        "artifacts": summarize_artifacts(job_id, output_dir),
    }
    return bundle


def render_template_cards(items: list[dict[str, Any]]) -> str:
    if not items:
        return '<div class="empty">这次没有命中明确模板，系统退回通用故事框架。</div>'
    cards = []
    for item in items:
        cards.append(
            "<article class='chip-card'>"
            f"<h4>{html_escape(item.get('title') or item.get('id') or '模板')}</h4>"
            f"<p>{html_escape(item.get('description') or '')}</p>"
            "</article>"
        )
    return "".join(cards)


def render_similar_cases(items: list[dict[str, Any]]) -> str:
    if not items:
        return '<div class="empty">这次没有命中可复用的历史案例，系统按通用框架独立分析。</div>'
    cards = []
    for item in items:
        cards.append(
            "<article class='segment-card'>"
            f"<div class='segment-range'>相似度 {html_escape(item.get('score') or '')}</div>"
            f"<h4>{html_escape(item.get('subtype_guess') or item.get('primary_type') or '历史案例')}</h4>"
            f"<p>{html_escape(item.get('whole_video_summary') or item.get('safe_final_story') or '')}</p>"
            f"<div class='meta-line'>route: {html_escape(item.get('route') or '')}</div>"
            "</article>"
        )
    return "".join(cards)


def render_hypothesis_cards(items: list[dict[str, Any]]) -> str:
    if not items:
        return '<div class="empty">这次没有产出机制假设。</div>'
    cards = []
    for item in items:
        cards.append(
            "<article class='chip-card'>"
            f"<h4>{html_escape(item.get('name') or '候选机制')}</h4>"
            f"<p>{html_escape(item.get('story_question') or '')}</p>"
            f"<div class='meta-line'>可能性：{html_escape(item.get('likelihood') or '')}</div>"
            "</article>"
        )
    return "".join(cards)


def render_windows(items: list[dict[str, Any]]) -> str:
    if not items:
        return '<div class="empty">没有建议复核窗口。</div>'
    rows = []
    for item in items:
        rows.append(
            "<tr>"
            f"<td>{html_escape(item.get('start'))} - {html_escape(item.get('end'))}</td>"
            f"<td>{html_escape(item.get('reason'))}</td>"
            "</tr>"
        )
    return "".join(rows)


def render_timeline(rows: list[dict[str, str]]) -> str:
    if not rows:
        return '<div class="empty">这次没有生成逐秒 observation。</div>'
    out = []
    for row in rows:
        out.append(
            "<tr>"
            f"<td>{html_escape(row['time'])}</td>"
            f"<td>{html_escape(row['scene'])}</td>"
            f"<td>{html_escape(row['people'])}</td>"
            f"<td>{html_escape(row['objects'])}</td>"
            f"<td>{html_escape(row['audio'])}</td>"
            f"<td>{html_escape(row['uncertainty'])}</td>"
            "</tr>"
        )
    return "".join(out)


def render_segments(items: list[dict[str, Any]]) -> str:
    if not items:
        return '<div class="empty">还没有合成后的脚本分段。</div>'
    blocks = []
    for item in items:
        blocks.append(
            "<article class='segment-card'>"
            f"<div class='segment-range'>{html_escape(item.get('start'))} - {html_escape(item.get('end'))}</div>"
            f"<h4>{html_escape(item.get('segment_role') or '段落')}</h4>"
            f"<p>{html_escape(item.get('integrated_summary') or '')}</p>"
            f"<div class='segment-meta'>逻辑状态：{html_escape(item.get('logic_status') or '')}</div>"
            "</article>"
        )
    return "".join(blocks)


def render_artifact_links(artifacts: dict[str, str]) -> str:
    if not artifacts:
        return '<div class="empty">没有找到过程产物文件。</div>'
    links = []
    for name, url in artifacts.items():
        links.append(f"<a class='artifact-link' href='{html_escape(url)}' target='_blank' rel='noreferrer'>{html_escape(name)}</a>")
    return "".join(links)


def render_product_report(bundle: dict[str, Any]) -> str:
    source = bundle.get("source", {})
    extraction = bundle.get("extraction", {})
    evidence = bundle.get("evidence", {})
    reasoning = bundle.get("reasoning", {})
    routing = bundle.get("routing", {})
    case_memory = bundle.get("case_memory", {})
    artifacts = bundle.get("artifacts", {})

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>视频证据报告</title>
  <style>
    :root {{
      --bg: #f5f4ee;
      --paper: rgba(255,255,255,.96);
      --ink: #1e293b;
      --muted: #526071;
      --line: rgba(30,41,59,.12);
      --brand: #0f766e;
      --warm: #d97706;
      --soft: #e7ecef;
      --shadow: 0 30px 80px rgba(15,23,42,.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top right, rgba(15,118,110,.10), transparent 24%),
        linear-gradient(180deg, #fbfaf6 0%, var(--bg) 100%);
      color: var(--ink);
    }}
    .wrap {{ max-width: 1320px; margin: 0 auto; padding: 36px 24px 72px; }}
    .hero {{
      display: grid;
      grid-template-columns: 1.4fr .8fr;
      gap: 18px;
      margin-bottom: 18px;
    }}
    .card {{
      background: var(--paper);
      border: 1px solid rgba(255,255,255,.9);
      border-radius: 28px;
      padding: 24px;
      box-shadow: var(--shadow);
    }}
    h1 {{
      margin: 0 0 10px;
      font-family: "Iowan Old Style", Georgia, serif;
      font-size: clamp(2.1rem, 3.8vw, 3.6rem);
      line-height: 1.02;
      letter-spacing: -.04em;
    }}
    h2 {{ margin: 0 0 12px; font-size: 24px; }}
    h3 {{ margin: 0 0 10px; font-size: 18px; }}
    h4 {{ margin: 0 0 8px; font-size: 16px; }}
    p {{ margin: 0; line-height: 1.8; color: var(--muted); }}
    .eyebrow {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--brand);
      background: rgba(15,118,110,.08);
      border: 1px solid rgba(15,118,110,.16);
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .08em;
      text-transform: uppercase;
      margin-bottom: 18px;
    }}
    .lede {{ font-size: 16px; max-width: 56ch; }}
    .grid-3 {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
      margin-top: 18px;
    }}
    .metric {{
      border-radius: 20px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,.72);
      padding: 16px;
    }}
    .metric b {{ display: block; font-size: 28px; margin-bottom: 6px; color: var(--ink); }}
    .stack {{ display: grid; gap: 18px; margin-top: 18px; }}
    .two-up {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      margin-top: 18px;
    }}
    .chip-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
    }}
    .chip-card, .segment-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255,255,255,.8);
      padding: 16px;
    }}
    .meta-line {{
      margin-top: 8px;
      font-size: 13px;
      color: var(--muted);
    }}
    .segment-range {{
      display: inline-flex;
      margin-bottom: 10px;
      border-radius: 999px;
      padding: 5px 10px;
      background: rgba(217,119,6,.10);
      color: var(--warm);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .06em;
      text-transform: uppercase;
    }}
    .summary-box {{
      padding: 16px 18px;
      border-radius: 20px;
      background: #f9fafb;
      border: 1px solid var(--line);
      color: var(--ink);
      line-height: 1.85;
      white-space: pre-wrap;
    }}
    .artifact-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }}
    .artifact-link {{
      text-decoration: none;
      color: var(--ink);
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 10px 14px;
      font-size: 14px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      border-radius: 18px;
      overflow: hidden;
    }}
    th, td {{
      border: 1px solid var(--line);
      padding: 12px 10px;
      text-align: left;
      vertical-align: top;
      font-size: 14px;
      line-height: 1.7;
      word-break: break-word;
    }}
    th {{ background: #f8fafc; font-size: 13px; letter-spacing: .03em; text-transform: uppercase; color: var(--muted); }}
    pre {{
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      border-radius: 20px;
      background: #0f172a;
      color: #dbeafe;
      padding: 18px;
      font-size: 13px;
      line-height: 1.7;
      overflow: auto;
    }}
    .empty {{
      padding: 16px;
      border: 1px dashed var(--line);
      border-radius: 16px;
      color: var(--muted);
    }}
    a.inline-link {{ color: var(--brand); text-decoration: none; }}
    @media (max-width: 1040px) {{
      .hero, .grid-3, .two-up {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <article class="card">
        <div class="eyebrow">Evidence-First Video Analysis</div>
        <h1>这不是“让 Gemini 直接写结论”，而是先把视频拆成证据，再做推理。</h1>
        <p class="lede">当前这份报告按三层结构组织：第一层是 Gemini 的感知提取，第二层是结构化证据包，第三层才是脚本和机制判断。这样我们能看见模型到底看到了什么，又是如何被后续逻辑使用的。</p>
      </article>
      <article class="card">
        <h3>源视频</h3>
        <p><a class="inline-link" href="{html_escape(source.get('video_url'))}" target="_blank" rel="noreferrer">{html_escape(source.get('video_url'))}</a></p>
        <div class="stack">
          <div class="metric"><b>{html_escape(extraction.get('gemini_model') or 'unknown')}</b><span>Gemini 提取模型</span></div>
          <div class="metric"><b>{html_escape(extraction.get('analysis_model') or 'unknown')}</b><span>LLM 分析模型</span></div>
          <div class="metric"><b>{html_escape(extraction.get('observation_count') or 0)}</b><span>逐秒 observation 条数</span></div>
          <div class="metric"><b>{html_escape(extraction.get('duration_estimate') or 'n/a')}</b><span>Gemini 估计时长</span></div>
        </div>
      </article>
    </section>

    <section class="grid-3">
      <article class="card">
        <h3>感知层</h3>
        <p>Gemini 在这里主要负责把视频切成逐秒 observation，而不是直接下剧情结论。</p>
      </article>
      <article class="card">
        <h3>证据层</h3>
        <p>我们保留逐秒场景、人物动作、道具、音频、不确定点，以及建议复核窗口。</p>
      </article>
      <article class="card">
        <h3>推理层</h3>
        <p>最后才把这些证据压成机制假设、脚本分段和可分享的最终 HTML。</p>
      </article>
    </section>

    <section class="two-up">
      <article class="card">
        <h2>模板路由系统</h2>
        <div class="stack">
          <div class="metric"><b>{html_escape(routing.get('primary_type') or '通用故事')}</b><span>主类型</span></div>
          <div class="metric"><b>{html_escape(routing.get('subtype_guess') or '未细分')}</b><span>子类型判断</span></div>
          <div class="metric"><b>{html_escape(routing.get('routing_mode') or 'universal')}</b><span>路由模式</span></div>
          <div class="metric"><b>{html_escape(routing.get('confidence') or 'low')}</b><span>模板置信度</span></div>
        </div>
        <div class="summary-box" style="margin-top:14px;">{html_escape(routing.get('reasoning_summary') or '这次没有稳定命中模板，已退回通用故事框架。')}</div>
      </article>
      <article class="card">
        <h2>命中的模板</h2>
        <div class="chip-grid">{render_template_cards(routing.get('matched_templates') or [])}</div>
      </article>
    </section>

    <section class="two-up">
      <article class="card">
        <h2>第一层：Gemini 感知提取</h2>
        <div class="stack">
          <div class="metric"><b>{html_escape(extraction.get('analysis_route') or 'n/a')}</b><span>提取通道</span></div>
          <div class="metric"><b>{html_escape(extraction.get('upload_file', {}).get('state') or 'n/a')}</b><span>Gemini 文件状态</span></div>
          <div class="metric"><b>{html_escape(extraction.get('usage', {}).get('total_tokens') or 'n/a')}</b><span>本次总 token</span></div>
        </div>
      </article>
      <article class="card">
        <h2>原始产物</h2>
        <div class="artifact-row">{render_artifact_links(artifacts)}</div>
      </article>
    </section>

    <section class="card" style="margin-top:18px;">
      <h2>Gemini 原始返回摘录</h2>
      <p style="margin-bottom:14px;">这里展示的不是后处理结果，而是 Gemini 返回文本里的原始 JSON 片段。</p>
      <pre>{html_escape(evidence.get('raw_gemini_preview') or '这次没有可展示的 Gemini 原始文本。')}</pre>
    </section>

    <section class="card" style="margin-top:18px;">
      <h2>第二层：结构化证据包</h2>
      <p style="margin-bottom:14px;">这部分已经把 Gemini 的文本观察整理成可回溯的逐秒证据表。当前没有关键帧图片，是因为本机还没启用抽帧依赖，但时间、动作、道具、音频和不确定点已经能独立审阅。</p>
      <table>
        <thead>
          <tr>
            <th>时间</th>
            <th>场景</th>
            <th>人物动作</th>
            <th>道具</th>
            <th>音频</th>
            <th>不确定点</th>
          </tr>
        </thead>
        <tbody>{render_timeline(evidence.get('timeline_preview') or [])}</tbody>
      </table>
    </section>

    <section class="two-up">
      <article class="card">
        <h2>建议复核窗口</h2>
        <table>
          <thead><tr><th>时间窗</th><th>原因</th></tr></thead>
          <tbody>{render_windows(evidence.get('verification_windows') or [])}</tbody>
        </table>
      </article>
      <article class="card">
        <h2>候选机制假设</h2>
        <div class="chip-grid">{render_hypothesis_cards(evidence.get('mechanism_hypotheses') or [])}</div>
      </article>
    </section>

    <section class="two-up">
      <article class="card">
        <h2>案例沉淀系统</h2>
        <p style="margin-bottom:14px;">系统会把当前结果压成一个案例条目，并在整理终稿前检索相似案例，避免重复犯同一种误判。</p>
        <div class="chip-grid">{render_similar_cases(case_memory.get('similar_cases_used') or [])}</div>
      </article>
      <article class="card">
        <h2>当前沉淀条目</h2>
        <div class="summary-box">{html_escape((case_memory.get('current_entry') or {}).get('whole_video_summary') or '这次尚未写入案例摘要。')}</div>
      </article>
    </section>

    <section class="card" style="margin-top:18px;">
      <h2>第三层：推理与脚本输出</h2>
      <div class="stack">
        <div>
          <h3>整段总结</h3>
          <div class="summary-box">{html_escape(reasoning.get('whole_video_summary') or '暂无')}</div>
        </div>
        <div>
          <h3>保守版最终故事</h3>
          <div class="summary-box">{html_escape(reasoning.get('safe_final_story') or '暂无')}</div>
        </div>
      </div>
    </section>

    <section class="card" style="margin-top:18px;">
      <h2>合成后的脚本分段预览</h2>
      <div class="chip-grid">{render_segments(reasoning.get('segments_preview') or [])}</div>
      <div class="meta-line" style="margin-top:14px;">逻辑质量：{html_escape(reasoning.get('logic_quality') or 'n/a')} · 最终脚本 HTML：<a class="inline-link" href="{html_escape(artifacts.get('script_table.html', ''))}" target="_blank" rel="noreferrer">打开</a></div>
    </section>
  </div>
</body>
</html>"""


def write_product_outputs(job_id: str, output_dir: Path, result_json: dict[str, Any]) -> dict[str, str]:
    bundle = build_evidence_bundle(job_id, output_dir, result_json)
    bundle_path = output_dir / "evidence_bundle.json"
    report_path = output_dir / "product_report.html"
    bundle_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(render_product_report(bundle), encoding="utf-8")
    return {
        "report_url": f"/results/{job_id}/product_report.html",
        "evidence_url": f"/results/{job_id}/evidence_bundle.json",
    }


def public_item_view(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": item.get("id"),
        "index": item.get("index"),
        "video_url": item.get("video_url"),
        "status": item.get("status"),
        "stage": item.get("stage") or "",
        "stage_message": item.get("stage_message") or "",
        "html_url": item.get("html_url") or "",
        "report_url": item.get("report_url") or "",
        "evidence_url": item.get("evidence_url") or "",
        "docx_url": item.get("docx_url") or "",
        "error": item.get("error") or "",
        "artifacts": item.get("artifacts") or {},
        "result_json": item.get("result_json"),
        "content_type": item.get("content_type") or "",
        "title": item.get("title") or "",
        "review_status": item.get("review_status") or "",
        "review_stage": item.get("review_stage") or "",
        "review_message": item.get("review_message") or "",
        "review_feedback": item.get("review_feedback") or "",
        "reviewed": bool(item.get("reviewed")),
        "edited": bool(item.get("edited")),
    }


def public_job_view(job: dict[str, Any]) -> dict[str, Any]:
    items = [public_item_view(item) for item in job.get("items") or []]
    total_items = len(items)
    completed_items = sum(1 for item in items if item.get("status") == "completed")
    failed_items = sum(1 for item in items if item.get("status") == "failed")
    payload = {
        "id": job["id"],
        "mode": job.get("mode") or ("batch" if total_items > 1 else "single"),
        "status": job["status"],
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "video_url": job.get("video_url"),
        "video_urls": job.get("video_urls") or [],
        "error": job.get("error"),
        "html_url": job.get("html_url"),
        "report_url": job.get("report_url"),
        "evidence_url": job.get("evidence_url"),
        "docx_url": job.get("docx_url"),
        "result_json": job.get("result_json"),
        "artifacts": job.get("artifacts") or {},
        "stage": job.get("stage") or "",
        "stage_message": job.get("stage_message") or "",
        "items": items,
        "total_items": total_items,
        "completed_items": completed_items,
        "failed_items": failed_items,
    }
    if job.get("status") == "running":
        payload["message"] = job.get("stage_message") or "正在提取视频证据并生成脚本，请稍候。"
    elif job.get("status") == "queued":
        payload["message"] = "任务已进入队列。"
    elif job.get("status") == "completed":
        if total_items > 1:
            payload["message"] = f"已完成 {completed_items}/{total_items} 条脚本，失败 {failed_items} 条。"
        else:
            payload["message"] = "证据报告和最终脚本都已生成。"
    elif job.get("status") == "failed":
        payload["message"] = "分析失败，请检查错误信息。"
    return payload


def update_job(job_id: str, **changes: Any) -> None:
    with job_lock:
        job = jobs[job_id]
        job.update(changes)
        job["updated_at"] = now_iso()
        save_jobs()


def update_job_item(job_id: str, item_index: int, **changes: Any) -> None:
    with job_lock:
        job = jobs[job_id]
        item = job["items"][item_index]
        item.update(changes)
        item["updated_at"] = now_iso()
        job["updated_at"] = now_iso()
        job["current_item_index"] = item_index
        if "stage" in changes and "stage_message" in changes:
            total = len(job["items"])
            job["stage"] = changes.get("stage") or ""
            job["stage_message"] = f"Video {item_index + 1}/{total}: {changes.get('stage_message') or ''}"
        save_jobs()


def start_review_job(item_id: str, feedback: str) -> tuple[bool, str]:
    context = find_item_context(item_id)
    if not context:
        return False, "Script item not found."
    parent_job_id, item_index, item = context
    if item.get("status") != "completed" or not item.get("result_json"):
        return False, "Only completed scripts can be reviewed."
    feedback_text = str(feedback or "").strip()
    if not feedback_text:
        return False, "Please describe what the analysis got wrong."
    update_job_item(
        parent_job_id,
        item_index,
        review_status="running",
        review_stage="queued",
        review_message="Queued for review. Waiting for an available analysis slot.",
        review_feedback=feedback_text,
        reviewed=False,
    )
    threading.Thread(
        target=run_review_with_slot,
        args=(parent_job_id, item_index, item_id, feedback_text),
        daemon=True,
    ).start()
    return True, parent_job_id


def persist_library_entry(parent_job_id: str, item: dict[str, Any]) -> None:
    script = item.get("result_json") or {}
    output_dir = RESULTS_ROOT / item["id"]
    bundle = read_json(output_dir / "evidence_bundle.json")
    entry = {
        "entry_id": item["id"],
        "parent_job_id": parent_job_id,
        "created_at": item.get("completed_at") or now_iso(),
        "video_url": item.get("video_url"),
        "title": item.get("title") or script.get("title") or "Untitled Script",
        "content_type": item.get("content_type") or detect_content_type(script, bundle),
        "whole_video_summary": script.get("whole_video_summary") or "",
        "html_url": item.get("html_url") or "",
        "report_url": item.get("report_url") or "",
        "evidence_url": item.get("evidence_url") or "",
        "docx_url": item.get("docx_url") or "",
        "source": "edited" if item.get("edited") else "ai",
        "saved_at": item.get("saved_to_library_at") or now_iso(),
    }
    append_library_entry(entry)


def find_item_context(item_id: str) -> tuple[str, int, dict[str, Any]] | None:
    with job_lock:
        for job_id, job in jobs.items():
            for index, item in enumerate(job.get("items") or []):
                if item.get("id") == item_id:
                    return job_id, index, json.loads(json.dumps(item, ensure_ascii=False))
            if job_id == item_id and job.get("result_json"):
                pseudo_item = {
                    "id": job_id,
                    "index": 0,
                    "video_url": job.get("video_url") or "",
                    "status": job.get("status"),
                    "stage": job.get("stage") or "",
                    "stage_message": job.get("stage_message") or "",
                    "html_url": job.get("html_url") or "",
                    "report_url": job.get("report_url") or "",
                    "evidence_url": job.get("evidence_url") or "",
                    "docx_url": job.get("docx_url") or "",
                    "artifacts": job.get("artifacts") or {},
                    "error": job.get("error") or "",
                    "result_json": json.loads(json.dumps(job.get("result_json") or {}, ensure_ascii=False)),
                    "content_type": job.get("content_type") or "",
                    "title": job.get("title") or "",
                    "review_status": job.get("review_status") or "",
                    "review_message": job.get("review_message") or "",
                    "review_feedback": job.get("review_feedback") or "",
                    "reviewed": bool(job.get("reviewed")),
                    "edited": bool(job.get("edited")),
                    "original_result_json": json.loads(json.dumps(job.get("original_result_json") or {}, ensure_ascii=False)),
                    "updated_at": job.get("updated_at"),
                }
                return job_id, 0, pseudo_item
    return None


def apply_script_edits(script: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    edited = json.loads(json.dumps(script or {}, ensure_ascii=False))
    edited["title"] = fill_text(payload.get("title") or edited.get("title") or "Video Script", "Video Script")
    edited["whole_video_summary"] = fill_text(payload.get("whole_video_summary") or edited.get("whole_video_summary") or "", "无")
    mechanism = dict(edited.get("mechanism") or {})
    mechanism["reason"] = fill_text(payload.get("mechanism_reason") or mechanism.get("reason") or "", "无")
    edited["mechanism"] = mechanism
    incoming_points = payload.get("core_viral_points")
    if isinstance(incoming_points, list):
        edited["core_viral_points"] = [
            {
                "label": fill_text(point.get("label"), "要点"),
                "text": fill_text(point.get("text"), "无"),
            }
            for point in incoming_points
            if isinstance(point, dict)
        ]
    rows = choose_script_rows(edited)
    incoming_rows = payload.get("rows")
    if isinstance(incoming_rows, list):
        for idx, incoming in enumerate(incoming_rows):
            if idx >= len(rows) or not isinstance(incoming, dict):
                continue
            row = rows[idx]
            for key in ["time", "visual_content", "action", "dialogue_or_audio", "integrated_summary"]:
                if key in incoming:
                    row[key] = fill_text(incoming.get(key), "无")
    if isinstance(edited.get("rows"), list) and edited.get("rows"):
        edited["rows"] = rows
    elif isinstance(edited.get("synthesized_segments"), list) and edited.get("synthesized_segments"):
        edited["synthesized_segments"] = rows
    return edited


def regenerate_item_outputs(
    parent_job_id: str,
    item_index: int,
    item_id: str,
    video_url: str,
    script_json: dict[str, Any],
    persist_library: bool = False,
) -> dict[str, Any]:
    output_dir = RESULTS_ROOT / item_id
    output_dir.mkdir(parents=True, exist_ok=True)
    script_json = enforce_chinese_dialogue_translation(
        json.loads(json.dumps(script_json or {}, ensure_ascii=False)),
        GOOGLE_API_KEY,
        unique_models(*MODEL_CANDIDATES),
    )
    script_json_path = output_dir / "script_table.json"
    script_json_path.write_text(json.dumps(script_json, ensure_ascii=False, indent=2), encoding="utf-8")

    render_script = V2_SKILL_ROOT / "scripts" / "render_script_table.py"
    subprocess.run(
        [os.environ.get("PYTHON_BIN", "python3"), str(render_script), str(script_json_path), "--output", str(output_dir / "script_table.html")],
        check=True,
        capture_output=True,
        text=True,
    )
    docx_path = write_script_docx(output_dir, script_json, video_url)
    docx_url = f"/results/{item_id}/{docx_path.name}" if docx_path and docx_path.exists() else ""
    content_type = detect_content_type_for_output(output_dir, script_json, read_json(output_dir / "evidence_bundle.json"))

    update_payload = {
        "result_json": script_json,
        "original_result_json": jobs[parent_job_id]["items"][item_index].get("original_result_json") or script_json,
        "title": script_json.get("title") or "Video Script",
        "content_type": content_type,
        "docx_url": docx_url,
        "html_url": f"/results/{item_id}/script_table.html",
        "artifacts": summarize_artifacts(item_id, output_dir),
        "edited": True,
        "updated_at": now_iso(),
    }
    if persist_library:
        update_payload["saved_to_library_at"] = now_iso()
    update_job_item(parent_job_id, item_index, **update_payload)
    with job_lock:
        job = jobs.get(parent_job_id)
        if job and (job.get("id") == item_id or len(job.get("items") or []) == 1):
            job["result_json"] = script_json
            job["original_result_json"] = job.get("original_result_json") or script_json
            job["title"] = script_json.get("title") or "Video Script"
            job["content_type"] = content_type
            job["docx_url"] = docx_url
            job["html_url"] = f"/results/{item_id}/script_table.html"
            job["artifacts"] = summarize_artifacts(item_id, output_dir)
            job["updated_at"] = now_iso()
            save_jobs()
        item = jobs[parent_job_id]["items"][item_index]
    if persist_library:
        persist_library_entry(parent_job_id, item)
    return public_item_view(item)


def run_review_reanalysis(parent_job_id: str, item_index: int, item_id: str, feedback: str) -> None:
    if not GOOGLE_API_KEY:
        update_job_item(parent_job_id, item_index, review_status="failed", review_message="Missing GOOGLE_API_KEY for review.")
        return
    if run_text_json_prompt_with_fallback is None:
        update_job_item(parent_job_id, item_index, review_status="failed", review_message="Review pipeline helpers are unavailable.")
        return

    output_dir = RESULTS_ROOT / item_id
    source_video = output_dir / "source.mp4"
    script_json_path = output_dir / "script_table.json"
    primary_path = output_dir / "primary_v2_draft.json"
    supplement_path = output_dir / "supplement_evidence.json"
    audio_multiview_path = output_dir / "audio_multiview.json"
    type_router_path = output_dir / "type_router.json"
    review_request_path = output_dir / "review_request.json"
    review_plan_path = output_dir / "review_plan.json"
    review_plan_raw_path = output_dir / "review_plan_raw_gemini.json"
    review_video_path = output_dir / "review_video_recheck.json"
    review_video_raw_path = output_dir / "review_video_recheck_raw_gemini.json"
    review_refine_raw_path = output_dir / "review_refine_raw_gemini.json"
    original_backup_path = output_dir / "script_table.original.json"

    try:
        current_script = read_json(script_json_path)
        if not current_script:
            raise RuntimeError("No existing script result to review.")
        stored_original = jobs[parent_job_id]["items"][item_index].get("original_result_json") or {}
        original_script = stored_original or read_json(original_backup_path) or current_script
        if not original_backup_path.exists():
            original_backup_path.write_text(json.dumps(original_script, ensure_ascii=False, indent=2), encoding="utf-8")

        update_job_item(
            parent_job_id,
            item_index,
            review_status="running",
            review_stage="plan",
            review_message="Reviewing your feedback.",
            review_feedback=feedback,
            original_result_json=original_script,
        )

        review_request = {
            "feedback": feedback,
            "video_url": jobs[parent_job_id]["items"][item_index].get("video_url") or "",
            "original_script": original_script,
            "current_script": current_script,
            "primary_draft": read_json(primary_path),
            "supplement": read_json(supplement_path),
            "audio_multiview": read_json(audio_multiview_path),
            "type_router": read_json(type_router_path),
            "source_metadata": read_json(output_dir / "source_metadata.json"),
        }
        review_request_path.write_text(json.dumps(review_request, ensure_ascii=False, indent=2), encoding="utf-8")

        update_job_item(parent_job_id, item_index, review_stage="plan", review_message="Comparing your feedback against the prior analysis.")
        review_plan, review_plan_raw, _ = run_text_json_prompt_with_fallback(
            review_request,
            GOOGLE_API_KEY,
            unique_models(*MODEL_CANDIDATES),
            REVIEW_PLAN_PROMPT,
            "review plan",
        )
        review_plan_path.write_text(json.dumps(review_plan, ensure_ascii=False, indent=2), encoding="utf-8")
        review_plan_raw_path.write_text(json.dumps(review_plan_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        review_video = {}
        should_recheck, recheck_reason = should_run_review_video_recheck(review_plan)
        if should_recheck and source_video.exists():
            update_job_item(parent_job_id, item_index, review_stage="recheck", review_message="Rechecking the video with your correction in mind.")
            prompt = REVIEW_VIDEO_PROMPT + "\n\nHuman feedback:\n" + feedback + "\n\nReview plan:\n" + json.dumps(review_plan, ensure_ascii=False)
            review_video, review_video_raw, _ = run_video_json_prompt_with_fallback(
                source_video,
                GOOGLE_API_KEY,
                unique_models(MODEL_CANDIDATES[0], *SUPPLEMENT_FALLBACK_MODELS),
                prompt,
                "review video recheck",
            )
            review_video_path.write_text(json.dumps(review_video, ensure_ascii=False, indent=2), encoding="utf-8")
            review_video_raw_path.write_text(json.dumps(review_video_raw, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            review_video = {
                "skipped": True,
                "reason": recheck_reason if source_video.exists() else "Source video is unavailable for recheck.",
            }
            review_video_path.write_text(json.dumps(review_video, ensure_ascii=False, indent=2), encoding="utf-8")

        update_job_item(parent_job_id, item_index, review_stage="rebuild", review_message="Rebuilding the script from the reviewed evidence.")
        refine_payload = {
            "feedback": feedback,
            "review_plan": review_plan,
            "review_video_recheck": review_video,
            "original_script": original_script,
            "current_script": current_script,
            "primary_draft": read_json(primary_path),
            "supplement": read_json(supplement_path),
            "audio_multiview": read_json(audio_multiview_path),
            "type_router": read_json(type_router_path),
        }
        corrected_script, corrected_raw, _ = run_text_json_prompt_with_fallback(
            refine_payload,
            GOOGLE_API_KEY,
            unique_models(*MODEL_CANDIDATES),
            REVIEW_REFINE_PROMPT,
            "review refine",
        )
        review_refine_raw_path.write_text(json.dumps(corrected_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        merged_script = json.loads(json.dumps(original_script, ensure_ascii=False))
        for key in ["title", "route", "audio_information_score", "source_url", "whole_video_summary", "core_viral_points", "replaceable_parts", "rows", "mechanism"]:
            if corrected_script.get(key):
                merged_script[key] = corrected_script.get(key)
        merged_script = enforce_chinese_dialogue_translation(
            merged_script,
            GOOGLE_API_KEY,
            unique_models(*MODEL_CANDIDATES),
        )
        updated_item = regenerate_item_outputs(
            parent_job_id,
            item_index,
            item_id,
            jobs[parent_job_id]["items"][item_index].get("video_url") or "",
            merged_script,
            persist_library=False,
        )
        update_job_item(
            parent_job_id,
            item_index,
            review_status="completed",
            review_stage="completed",
            review_message="Reviewed version is ready.",
            reviewed=True,
            review_feedback=feedback,
        )
        return updated_item
    except Exception as exc:
        update_job_item(parent_job_id, item_index, review_status="failed", review_stage="failed", review_message=friendly_error(str(exc)), review_feedback=feedback)


def run_review_with_slot(parent_job_id: str, item_index: int, item_id: str, feedback: str) -> None:
    with analysis_slots:
        run_review_reanalysis(parent_job_id, item_index, item_id, feedback)


def execute_single_pipeline(parent_job_id: str, item_index: int, item: dict[str, Any]) -> None:
    output_dir = RESULTS_ROOT / item["id"]
    progress_path = output_dir / "progress.json"
    proc_env = os.environ.copy()
    last_error = "Unknown pipeline failure"
    tried: list[str] = []
    update_job_item(parent_job_id, item_index, status="running", started_at=now_iso(), command=[], stage="queued", stage_message="Queued for analysis.")
    for model_name in dict.fromkeys(MODEL_CANDIDATES):
        cmd = [
            os.environ.get("PYTHON_BIN", "python3"),
            str(AUTO_ANALYZE),
            item["video_url"],
            "--out",
            str(output_dir),
            "--model",
            model_name,
        ]
        tried.append(model_name)
        update_job_item(
            parent_job_id,
            item_index,
            command=cmd,
            model=model_name,
            frames_enabled=bool(shutil.which("ffmpeg")),
            stage="starting",
            stage_message=f"Preparing {model_name}.",
        )
        try:
            proc = subprocess.Popen(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=proc_env,
            )
            start_time = time.time()
            stdout_lines: list[str] = []
            progress_mtime = 0.0
            current_stage = "starting"
            stage_started_at = start_time
            while proc.poll() is None:
                if progress_path.exists():
                    stat = progress_path.stat()
                    if stat.st_mtime > progress_mtime:
                        progress_mtime = stat.st_mtime
                        progress = read_json(progress_path)
                        if progress:
                            next_stage = progress.get("stage") or "running"
                            if next_stage != current_stage:
                                current_stage = next_stage
                                stage_started_at = time.time()
                            update_job_item(
                                parent_job_id,
                                item_index,
                                stage=next_stage,
                                stage_message=progress.get("message") or "Running analysis.",
                            )
                stage_limit = STAGE_TIMEOUTS_SEC.get(current_stage)
                if stage_limit and time.time() - stage_started_at > stage_limit:
                    proc.kill()
                    stdout_text, stderr_text = proc.communicate()
                    if stdout_text:
                        stdout_lines.append(stdout_text)
                    last_error = f"Stage `{current_stage}` exceeded {stage_limit} seconds and was stopped."
                    break
                if time.time() - start_time > PIPELINE_TIMEOUT_SEC:
                    proc.kill()
                    stdout_text, stderr_text = proc.communicate()
                    if stdout_text:
                        stdout_lines.append(stdout_text)
                    last_error = f"Analysis exceeded {PIPELINE_TIMEOUT_SEC // 60} minutes and was stopped."
                    break
                time.sleep(1)
            else:
                stdout_text, stderr_text = proc.communicate()
                if stdout_text:
                    stdout_lines.append(stdout_text)
                stderr_text = stderr_text or ""
                proc_stdout = "".join(stdout_lines)
                result_json = extract_json_line(proc_stdout or "")
                if proc.returncode == 0:
                    html_path = output_dir / "script_table.html"
                    if not html_path.exists():
                        last_error = "Pipeline finished but script_table.html was not created."
                        continue
                    product = write_product_outputs(item["id"], output_dir, result_json or {})
                    script_json = read_json(output_dir / "script_table.json") or read_json(output_dir / "analysis_result.json") or result_json or {}
                    docx_path = write_script_docx(output_dir, script_json, item["video_url"])
                    docx_url = f"/results/{item['id']}/{docx_path.name}" if docx_path and docx_path.exists() else ""
                    content_type = detect_content_type_for_output(output_dir, script_json, read_json(output_dir / "evidence_bundle.json"))
                    update_job_item(
                        parent_job_id,
                        item_index,
                        status="completed",
                        error="",
                        completed_at=now_iso(),
                        html_url=f"/results/{item['id']}/script_table.html",
                        report_url=product["report_url"],
                        evidence_url=product["evidence_url"],
                        docx_url=docx_url,
                        artifacts=summarize_artifacts(item["id"], output_dir),
                        result_json=script_json,
                        original_result_json=script_json,
                        tried_models=tried,
                        stage="completed",
                        stage_message="Completed.",
                        content_type=content_type,
                        title=script_json.get("title") or "Video Script",
                    )
                    persist_library_entry(parent_job_id, jobs[parent_job_id]["items"][item_index])
                    return
                last_error = (stderr_text or proc_stdout or "").strip() or "Unknown pipeline failure"
                if not should_try_next_model(last_error):
                    break
                continue
            break
        except Exception as exc:
            last_error = str(exc)
            if not should_try_next_model(last_error):
                break
            continue
    update_job_item(
        parent_job_id,
        item_index,
        status="failed",
        error=friendly_error(last_error),
        completed_at=now_iso(),
        tried_models=tried,
        stage="failed",
        stage_message="Failed.",
    )


def run_job_batch(job_id: str) -> None:
    try:
        update_job(job_id, status="running", started_at=now_iso(), stage="queued", stage_message="Batch task started.")
        items = jobs[job_id]["items"]
        for idx, item in enumerate(items):
            execute_single_pipeline(job_id, idx, item)
        final_items = jobs[job_id]["items"]
        completed = sum(1 for item in final_items if item.get("status") == "completed")
        failed = sum(1 for item in final_items if item.get("status") == "failed")
        first_completed = next((item for item in final_items if item.get("status") == "completed"), None)
        update_job(
            job_id,
            status="completed" if completed else "failed",
            completed_at=now_iso(),
            html_url=first_completed.get("html_url") if first_completed else "",
            report_url=first_completed.get("report_url") if first_completed else "",
            evidence_url=first_completed.get("evidence_url") if first_completed else "",
            docx_url=first_completed.get("docx_url") if first_completed else "",
            result_json=first_completed.get("result_json") if first_completed else None,
            stage="completed" if completed else "failed",
            stage_message=f"Completed {completed}/{len(final_items)} items. Failed {failed}.",
            error="" if completed else "All batch items failed.",
        )
    except Exception as exc:
        update_job(job_id, status="failed", error=friendly_error(str(exc)), completed_at=now_iso(), stage="failed", stage_message="Batch failed.")


def create_job(video_urls: list[str]) -> dict[str, Any]:
    job_id = uuid4().hex
    items = []
    for index, video_url in enumerate(video_urls):
        item_id = uuid4().hex
        items.append(
            {
                "id": item_id,
                "index": index,
                "video_url": video_url,
                "status": "queued",
                "stage": "queued",
                "stage_message": "Queued.",
                "created_at": now_iso(),
                "updated_at": now_iso(),
                "html_url": "",
                "report_url": "",
                "evidence_url": "",
                "docx_url": "",
                "artifacts": {},
                "error": "",
                "result_json": None,
                "original_result_json": None,
                "content_type": "",
                "title": "",
                "review_status": "",
                "review_stage": "",
                "review_message": "",
                "review_feedback": "",
                "reviewed": False,
                "edited": False,
            }
        )
    job = {
        "id": job_id,
        "mode": "batch" if len(video_urls) > 1 else "single",
        "video_url": video_urls[0] if len(video_urls) == 1 else "",
        "video_urls": video_urls,
        "status": "queued",
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "html_url": "",
        "report_url": "",
        "evidence_url": "",
        "docx_url": "",
        "artifacts": {},
        "error": "",
        "result_json": None,
        "stage": "queued",
        "stage_message": "Queued.",
        "items": items,
        "current_item_index": 0,
    }
    with job_lock:
        jobs[job_id] = job
        save_jobs()
    enqueue_job(job_id)
    return public_job_view(job)


def page_html() -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Koko · Kwai Coach</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Readex+Pro:wght@300;400;500;600;700&family=Instrument+Serif:ital@1&display=swap');
    :root {{
      --bg: #FFF8F2;
      --card: rgba(255,255,255,.96);
      --ink: #FF8200;
      --muted: #FF8200;
      --line: rgba(255,130,0,.16);
      --brand: #FF8200;
      --brand-deep: #F97300;
      --brand-soft: #FFF4E8;
      --brand-soft-2: #FFF0DE;
      --ok: #157347;
      --err: #b42318;
      --wait: #935f14;
      --shadow: 0 28px 80px rgba(249,115,0,.12);
    }}
    * {{
      box-sizing: border-box;
      font-family: 'Readex Pro', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      background:
        radial-gradient(circle at 10% 8%, rgba(255,130,0,.24), transparent 28%),
        radial-gradient(circle at 82% 14%, rgba(249,115,0,.22), transparent 26%),
        radial-gradient(circle at 50% 48%, rgba(255,244,232,.96), transparent 30%),
        linear-gradient(180deg, #FFD6AE 0%, #FFF0DE 38%, #FFFFFF 100%);
    }}
    .shell {{ width: 100%; }}
    .site {{ width: 100%; }}
    .hero-shell {{
      position: relative;
      min-height: 100vh;
      padding: 18px;
    }}
    .hero-panel {{
      --mouse-x: 50%;
      --mouse-y: 50%;
      --tilt-x: 0px;
      --tilt-y: 0px;
      width: min(1320px, 100%);
      margin: 0 auto;
      position: relative;
      border-radius: 34px;
      overflow: hidden;
      border: 1px solid rgba(255,255,255,.85);
      box-shadow: var(--shadow);
      background:
        radial-gradient(circle at 16% 18%, rgba(255,130,0,.28), rgba(255,130,0,0) 24%),
        radial-gradient(circle at 82% 18%, rgba(249,115,0,.24), rgba(249,115,0,0) 22%),
        radial-gradient(circle at 72% 56%, rgba(255,244,232,.86), rgba(255,244,232,0) 28%),
        linear-gradient(180deg, #FFC792 0%, #FFF0DE 46%, #FFFFFF 100%);
      min-height: calc(100vh - 36px);
      display: flex;
      flex-direction: column;
    }}
    .hero-panel::before {{
      content: "";
      position: absolute;
      inset: 0;
      background:
        radial-gradient(circle at 18% 18%, rgba(255,255,255,.74), rgba(255,255,255,0) 16%),
        radial-gradient(circle at 82% 30%, rgba(255,248,235,.86), rgba(255,248,235,0) 14%),
        radial-gradient(circle at 72% 62%, rgba(255,244,232,.72), rgba(255,244,232,0) 18%);
      opacity: .92;
      pointer-events: none;
    }}
    .hero-panel::after {{
      content: "";
      position: absolute;
      inset: 0;
      background-image:
        radial-gradient(rgba(255,255,255,.12) 0.7px, transparent 0.7px),
        radial-gradient(rgba(255,130,0,.05) 0.7px, transparent 0.7px);
      background-size: 8px 8px, 12px 12px;
      background-position: 0 0, 3px 3px;
      mix-blend-mode: soft-light;
      opacity: .18;
      pointer-events: none;
    }}
    .brandbar {{
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 16px;
      padding: 16px 20px 0;
      position: relative;
      z-index: 3;
    }}
    .navpill {{
      display: inline-flex;
      align-items: center;
      gap: 12px;
      border: 1px solid rgba(0,0,0,.08);
      color: rgba(255,255,255,.86);
      background: rgba(0,0,0,.78);
      backdrop-filter: blur(16px);
      padding: 8px 10px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      box-shadow: 0 16px 32px rgba(0,0,0,.16);
    }}
    .navpill a {{
      color: rgba(255,255,255,.78);
      text-decoration: none;
      padding: 8px 16px;
      border-radius: 999px;
      font-size: 12px;
      transition: color .18s ease, background .18s ease;
      letter-spacing: .02em;
    }}
    .navpill a:hover {{
      color: #FFFFFF;
      background: rgba(255,255,255,.08);
    }}
    .hero-stage {{
      position: relative;
      flex: 1;
      min-height: 0;
      padding: 16px 18px 18px;
    }}
    .hero-stage::before {{
      content: "";
      position: absolute;
      inset: 0;
      pointer-events: none;
      background:
        radial-gradient(circle at var(--mouse-x) var(--mouse-y), rgba(255,255,255,.46), rgba(255,255,255,0) 10%),
        radial-gradient(circle at var(--mouse-x) var(--mouse-y), rgba(255,130,0,.34), rgba(255,130,0,0) 18%),
        radial-gradient(circle at var(--mouse-x) var(--mouse-y), rgba(249,115,0,.22), rgba(249,115,0,0) 28%);
      opacity: .92;
      transition: opacity .16s ease-out;
      mix-blend-mode: screen;
      filter: blur(2px);
    }}
    .hero-corner-logo {{
      position: absolute;
      top: 26px;
      right: 26px;
      z-index: 4;
      display: block;
      pointer-events: none;
    }}
    .hero-corner-logo img {{
      width: clamp(420px, 42vw, 720px);
      height: auto;
      object-fit: contain;
      display: block;
      filter: saturate(1.04);
    }}
.hero-copy {{
      position: relative;
      min-height: calc(100vh - 138px);
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(280px, .65fr);
      gap: 24px;
      align-items: end;
      border-radius: 28px;
      padding: 24px 22px 26px;
    }}
    .hero-left h1 {{
      margin: 0;
      font-size: clamp(7rem, 14vw, 13.4rem);
      line-height: .80;
      letter-spacing: -.06em;
      font-weight: 600;
      max-width: 7ch;
      color: #FF8200;
      text-shadow: 0 10px 30px rgba(255,130,0,.10);
    }}
    .hero-left h1 span {{
      display: inline-block;
    }}
    .hero-left h1 .koko-k {{
      font-size: 1.22em;
      line-height: .72;
      letter-spacing: -.08em;
    }}
    .hero-left h1 .koko-rest {{
      margin-left: -.06em;
    }}
    .lede {{
      margin: 0;
      color: #FF8200;
      line-height: 1.35;
      font-size: 18px;
      max-width: 18ch;
      padding-left: 6px;
    }}
.hero-side {{
      align-self: end;
      justify-self: end;
      display: flex;
      flex-direction: column;
      gap: 18px;
      padding-bottom: 6px;
    }}
    .hero-side p {{
      margin: 0;
      color: #FF8200;
      font-size: 14px;
      line-height: 1.6;
      max-width: 24ch;
      text-shadow: none;
    }}
    .hero-cta {{
      display: inline-flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      width: fit-content;
      padding: 8px 8px 8px 18px;
      border-radius: 999px;
      background: #FFF8EA;
      color: #1F1F1F;
      text-decoration: none;
      font-size: 14px;
      font-weight: 700;
      box-shadow: 0 14px 28px rgba(0,0,0,.16);
      transition: transform .18s ease, box-shadow .18s ease;
    }}
    .hero-cta:hover {{
      transform: translateY(-1px);
      box-shadow: 0 18px 32px rgba(0,0,0,.20);
    }}
    .hero-cta span {{
      width: 36px;
      height: 36px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      background: #1F1F1F;
      color: #FFFFFF;
      font-size: 16px;
    }}
    .hero-scroll {{
      position: absolute;
      left: 0;
      bottom: 0;
      z-index: 2;
      opacity: 0;
      pointer-events: none;
    }}
    .work-shell {{
      padding: 10px 24px 32px;
      background: transparent;
    }}
    .workspace {{
      width: min(1320px, 100%);
      margin: 0 auto;
      border-radius: 34px;
      overflow: hidden;
      border: 1px solid rgba(255,255,255,.85);
      box-shadow: var(--shadow);
      background:
        radial-gradient(circle at 12% 16%, rgba(255,130,0,.22), rgba(255,130,0,0) 22%),
        radial-gradient(circle at 86% 18%, rgba(249,115,0,.18), rgba(249,115,0,0) 22%),
        radial-gradient(circle at 70% 62%, rgba(255,244,232,.82), rgba(255,244,232,0) 24%),
        linear-gradient(180deg, #FFC792 0%, #FFF0DE 44%, #FFFFFF 100%);
      display: flex;
      flex-direction: column;
      gap: 0;
      align-items: stretch;
      min-width: 0;
    }}
    .workspace::before {{
      content: "";
      position: absolute;
      inset: 0;
      pointer-events: none;
    }}
    .composer {{
      border-radius: 0;
      border: 0;
      border-bottom: 1px solid rgba(255,130,0,.14);
      background: transparent;
      box-shadow: none;
      padding: 28px 28px 22px;
    }}
    textarea {{
      width: 100%;
      min-height: 160px;
      resize: vertical;
      border: 1px solid rgba(255,130,0,.16);
      background: rgba(255,255,255,.98);
      border-radius: 20px;
      padding: 18px 18px;
      font-size: 16px;
      outline: none;
      color: var(--ink);
      transition: border-color .2s ease, box-shadow .2s ease, transform .2s ease;
      line-height: 1.65;
    }}
    textarea:focus {{
      border-color: rgba(255,130,0,.45);
      box-shadow: 0 0 0 4px rgba(255,130,0,.10);
      transform: translateY(-1px);
    }}
    .composer-head {{
      display: block;
      margin-bottom: 14px;
    }}
    .composer-title {{
      font-size: 28px;
      line-height: 1.02;
      letter-spacing: -.04em;
      margin: 0 0 8px;
      font-weight: 600;
    }}
    .composer-copy {{
      margin: 0;
      color: var(--muted);
      line-height: 1.7;
      font-size: 14px;
      max-width: 42ch;
    }}
    .coach-badge {{
      flex: 0 0 auto;
      border-radius: 20px;
      padding: 10px 12px;
      background: var(--brand-soft);
      border: 1px solid rgba(255,130,0,.14);
      color: var(--brand-deep);
      font-size: 12px;
      line-height: 1.5;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: .06em;
    }}
    label {{
      display: block;
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 10px;
      color: #FF8200;
    }}
    input[type="url"] {{
      width: 100%;
      border: 1px solid rgba(255,130,0,.16);
      background: rgba(255,255,255,.98);
      border-radius: 20px;
      padding: 18px 18px;
      font-size: 16px;
      outline: none;
      color: var(--ink);
      transition: border-color .2s ease, box-shadow .2s ease, transform .2s ease;
    }}
    input[type="url"]:focus {{
      border-color: rgba(255,130,0,.45);
      box-shadow: 0 0 0 4px rgba(255,130,0,.10);
      transform: translateY(-1px);
    }}
    .actions {{ margin-top: 16px; }}
    button {{
      width: 100%;
      border: 0;
      cursor: pointer;
      border-radius: 18px;
      padding: 16px 20px;
      font-size: 15px;
      font-weight: 700;
      background: linear-gradient(135deg, var(--brand), var(--brand-deep));
      color: #fff;
      box-shadow: 0 16px 28px rgba(249,115,0,.22);
      transition: transform .18s ease, box-shadow .18s ease, filter .18s ease;
    }}
    button:hover {{
      transform: translateY(-1px);
      box-shadow: 0 18px 30px rgba(249,115,0,.26);
      filter: saturate(1.02);
    }}
    .server-note {{
      margin-top: 12px;
      font-size: 13px;
      color: var(--muted);
      line-height: 1.65;
    }}
    .mini-tags {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }}
    .mini-tag {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 7px 11px;
      font-size: 12px;
      font-weight: 600;
      color: var(--muted);
      background: rgba(255,255,255,.9);
      border: 1px solid rgba(31,31,31,.08);
    }}
    .status-box {{
      min-height: 260px;
      border-radius: 0 0 34px 34px;
      border: 0;
      background: transparent;
      padding: 24px 28px 28px;
      color: var(--muted);
      line-height: 1.7;
      box-shadow: none;
    }}
    .status-empty {{
      display: flex;
      flex-direction: column;
      justify-content: center;
      min-height: 210px;
      gap: 14px;
    }}
    .status-empty-title {{
      font-size: 26px;
      line-height: 1.04;
      letter-spacing: -.03em;
      color: var(--ink);
      font-weight: 600;
      max-width: 16ch;
    }}
    .status-empty-copy {{
      max-width: 48ch;
      font-size: 14px;
      color: var(--muted);
    }}
    .progress-wrap {{
      margin: 10px 0 18px;
    }}
    .progress-top {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
      color: #FF8200;
      font-size: 14px;
      font-weight: 700;
    }}
    .progress-rail {{
      width: 100%;
      height: 10px;
      border-radius: 999px;
      background: rgba(255,130,0,.18);
      overflow: hidden;
      border: 1px solid rgba(255,130,0,.14);
    }}
    .progress-fill {{
      height: 100%;
      width: 0%;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--brand), var(--brand-deep));
      transition: width .35s ease;
    }}
    .step-list {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 14px;
    }}
    .step-pill {{
      border-radius: 16px;
      padding: 10px 8px;
      text-align: center;
      font-size: 12px;
      font-weight: 700;
      color: #FF8200;
      background: rgba(255,255,255,.88);
      border: 1px solid rgba(255,130,0,.16);
    }}
    .step-pill.active {{
      color: #FF8200;
      border-color: rgba(255,130,0,.30);
      background: rgba(255,130,0,.14);
    }}
    .step-pill.done {{
      color: #FF8200;
      border-color: rgba(255,130,0,.24);
      background: rgba(255,244,232,.96);
    }}
    .status-box.ready {{
      color: var(--ink);
      border-style: solid;
    }}
    .batch-summary {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin: 4px 0 16px;
    }}
    .batch-chip {{
      border-radius: 999px;
      padding: 8px 12px;
      font-size: 12px;
      font-weight: 700;
      color: #FF8200;
      background: rgba(255,255,255,.84);
      border: 1px solid rgba(255,130,0,.16);
    }}
    .item-stack {{
      display: flex;
      flex-direction: column;
      gap: 14px;
      margin-top: 12px;
    }}
    .item-card {{
      border: 1px solid rgba(255,130,0,.16);
      border-radius: 18px;
      background: rgba(255,255,255,.82);
      overflow: hidden;
    }}
    .item-card summary {{
      list-style: none;
      cursor: pointer;
      padding: 14px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      font-weight: 700;
      color: #FF8200;
    }}
    .item-card summary::-webkit-details-marker {{ display: none; }}
    .item-meta {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      font-size: 12px;
      padding: 0 16px 14px;
      color: #FF8200;
    }}
    .item-body {{
      padding: 0 16px 16px;
    }}
    .link-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 12px;
    }}
    .action-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      text-decoration: none;
      border-radius: 999px;
      padding: 10px 14px;
      color: #FF8200;
      border: 1px solid rgba(255,130,0,.18);
      background: rgba(255,255,255,.9);
      font-weight: 700;
      font-size: 13px;
      cursor: pointer;
    }}
    .action-link.primary {{
      background: linear-gradient(135deg, var(--brand), var(--brand-deep));
      color: #fff;
      border-color: transparent;
    }}
    .toast {{
      position: fixed;
      right: 24px;
      bottom: 24px;
      z-index: 60;
      min-width: 240px;
      max-width: min(420px, calc(100vw - 32px));
      padding: 14px 16px;
      border-radius: 18px;
      color: #FF8200;
      background: rgba(255,255,255,.84);
      border: 1px solid rgba(255,130,0,.16);
      box-shadow: 0 18px 38px rgba(249,115,0,.16);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      opacity: 0;
      transform: translateY(10px);
      pointer-events: none;
      transition: opacity .22s ease, transform .22s ease;
    }}
    .toast.show {{
      opacity: 1;
      transform: translateY(0);
    }}
    .toast-title {{
      font-size: 14px;
      font-weight: 800;
      margin-bottom: 4px;
    }}
    .toast-copy {{
      font-size: 13px;
      line-height: 1.55;
    }}
    .editor-shell {{
      margin-bottom: 14px;
      border: 1px solid rgba(255,130,0,.16);
      border-radius: 16px;
      background: rgba(255,255,255,.72);
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }}
    .editor-disclosure {{
      margin-bottom: 14px;
      border: 1px solid rgba(255,130,0,.16);
      border-radius: 16px;
      background: rgba(255,255,255,.62);
      overflow: hidden;
    }}
    .editor-summary {{
      list-style: none;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      color: #FF8200;
      font-size: 13px;
      font-weight: 800;
      letter-spacing: .04em;
      text-transform: uppercase;
    }}
    .editor-summary::-webkit-details-marker {{
      display: none;
    }}
    .editor-summary::after {{
      content: "＋";
      font-size: 18px;
      line-height: 1;
    }}
    .editor-disclosure[open] .editor-summary::after {{
      content: "－";
    }}
    .editor-summary-copy {{
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0;
      text-transform: none;
      opacity: .82;
      margin-left: auto;
    }}
    .editor-grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }}
    .editor-field {{
      display: flex;
      flex-direction: column;
      gap: 8px;
    }}
    .editor-label {{
      font-size: 12px;
      font-weight: 800;
      letter-spacing: .04em;
      text-transform: uppercase;
      color: #FF8200;
      margin: 0;
    }}
    .editor-input, .editor-textarea {{
      width: 100%;
      border: 1px solid rgba(255,130,0,.14);
      border-radius: 14px;
      background: rgba(255,255,255,.92);
      color: #FF8200;
      padding: 12px 14px;
      font-size: 14px;
      line-height: 1.6;
      outline: none;
    }}
    .editor-textarea {{
      min-height: 96px;
      resize: vertical;
    }}
    .editor-row-card {{
      border: 1px solid rgba(255,130,0,.12);
      border-radius: 14px;
      background: rgba(255,244,232,.56);
      padding: 12px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }}
    .editor-row-title {{
      font-size: 13px;
      font-weight: 800;
      color: #FF8200;
    }}
    .review-shell {{
      margin-bottom: 14px;
      border: 1px solid rgba(255,130,0,.16);
      border-radius: 16px;
      background: rgba(255,248,235,.72);
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }}
    .review-progress {{
      display: flex;
      flex-direction: column;
      gap: 10px;
      border: 1px solid rgba(255,130,0,.12);
      border-radius: 14px;
      background: rgba(255,255,255,.76);
      padding: 12px;
    }}
    .review-progress-top {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      font-size: 12px;
      font-weight: 700;
      color: #FF8200;
    }}
    .review-progress-rail {{
      height: 8px;
      border-radius: 999px;
      background: rgba(255,130,0,.10);
      overflow: hidden;
    }}
    .review-progress-fill {{
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(135deg, var(--brand), var(--brand-deep));
      transition: width .24s ease;
    }}
    .review-progress-fill.failed {{
      background: linear-gradient(135deg, #D9572A, #B43A20);
    }}
    .review-steps {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
    }}
    .review-step {{
      border-radius: 999px;
      border: 1px solid rgba(255,130,0,.12);
      background: rgba(255,255,255,.72);
      padding: 8px 10px;
      font-size: 11px;
      font-weight: 700;
      text-align: center;
      color: rgba(255,130,0,.78);
    }}
    .review-step.done {{
      background: rgba(255,130,0,.12);
      color: #FF8200;
    }}
    .review-step.active {{
      background: linear-gradient(135deg, rgba(255,130,0,.18), rgba(249,115,0,.16));
      border-color: rgba(255,130,0,.22);
      color: #FF8200;
    }}
    .review-step.failed {{
      border-color: rgba(180,35,24,.22);
      background: rgba(180,35,24,.08);
      color: #B43A20;
    }}
    .review-note {{
      font-size: 13px;
      line-height: 1.6;
      color: #FF8200;
      opacity: .92;
    }}
    .item-sections {{
      display: flex;
      flex-direction: column;
      gap: 14px;
      margin-bottom: 14px;
    }}
    .item-actions-shell {{
      border: 1px solid rgba(255,130,0,.16);
      border-radius: 16px;
      background: rgba(255,255,255,.72);
      padding: 14px;
    }}
    .item-card iframe {{
      width: 100%;
      min-height: 420px;
      border: 1px solid rgba(255,130,0,.14);
      border-radius: 16px;
      background: #fff;
    }}
    .library-shell {{
      padding: 24px;
    }}
    .library-wrap {{
      width: min(1320px, 100%);
      margin: 0 auto;
      border-radius: 34px;
      overflow: hidden;
      border: 1px solid rgba(255,255,255,.85);
      box-shadow: var(--shadow);
      background:
        radial-gradient(circle at 12% 16%, rgba(255,130,0,.22), rgba(255,130,0,0) 22%),
        radial-gradient(circle at 86% 18%, rgba(249,115,0,.18), rgba(249,115,0,0) 22%),
        radial-gradient(circle at 70% 62%, rgba(255,244,232,.82), rgba(255,244,232,0) 24%),
        linear-gradient(180deg, #FFC792 0%, #FFF0DE 44%, #FFFFFF 100%);
      padding: 28px;
    }}
    .library-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      margin-top: 20px;
    }}
    .library-card {{
      border: 1px solid rgba(255,130,0,.16);
      border-radius: 22px;
      background: rgba(255,255,255,.82);
      padding: 18px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }}
    .library-card h3 {{
      margin: 0;
      font-size: 20px;
      line-height: 1.12;
      letter-spacing: -.03em;
    }}
    .library-card p {{
      margin: 0;
      line-height: 1.65;
      font-size: 14px;
      color: #FF8200;
    }}
    .library-topbar {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 20px;
      flex-wrap: wrap;
    }}
    .library-topbar h1 {{
      margin: 0;
      font-size: clamp(3rem, 7vw, 5.4rem);
      letter-spacing: -.08em;
      line-height: .88;
    }}
    .library-stats {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 12px;
    }}
    .status-box iframe {{
      width: 100%;
      min-height: 620px;
      border: 1px solid var(--line);
      border-radius: 22px;
      background: #fff;
      margin-top: 14px;
      box-shadow: inset 0 0 0 1px rgba(255,255,255,.9);
    }}
    .status {{
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .08em;
      font-weight: 700;
    }}
    .status-queued, .status-running {{ background: rgba(147,95,20,.12); color: var(--wait); }}
    .status-completed {{ background: rgba(21,115,71,.12); color: var(--ok); }}
    .status-failed {{ background: rgba(180,35,24,.12); color: var(--err); }}
    .result-link {{
      color: #FF8200;
      text-decoration: none;
      font-weight: 700;
      margin-right: 16px;
    }}
    code {{
      font-family: "SFMono-Regular", Menlo, monospace;
      background: rgba(255,130,0,.08);
      padding: 2px 6px;
      border-radius: 8px;
    }}
    .brand-icon {{
      width: 38px;
      height: 38px;
      border-radius: 12px;
      background: var(--brand-soft);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      flex: 0 0 auto;
    }}
    .brand-icon img {{
      width: 28px;
      height: 28px;
      object-fit: contain;
      object-position: left center;
      transform: scale(1.65);
    }}
    @media (max-width: 1080px) {{
      .hero-copy {{
        grid-template-columns: 1fr;
        min-height: auto;
        gap: 18px;
      }}
      .hero-side {{
        align-self: start;
        justify-self: start;
      }}
    }}
    @media (max-width: 720px) {{
      .hero-shell {{
        padding: 12px;
      }}
      .hero-panel {{
        min-height: calc(100vh - 24px);
      }}
      .hero-stage {{
        padding: 16px 12px 74px;
      }}
      .hero-copy {{
        padding: 18px 14px 88px;
        min-height: calc(100vh - 152px);
      }}
      .work-shell {{
        padding: 16px 14px 24px;
      }}
      .brandbar {{
        padding: 14px 14px 0;
        flex-direction: column;
        align-items: stretch;
      }}
      .navpill {{
        justify-content: center;
        flex-wrap: wrap;
      }}
      .composer {{
        padding: 18px;
      }}
      .composer-head {{
        flex-direction: column;
      }}
      .hero-left h1 {{ font-size: 5rem; }}
      .hero-left h1 .koko-k {{
        font-size: 1.16em;
      }}
      .lede {{
        font-size: 15px;
        max-width: 24ch;
      }}
      .step-list {{ grid-template-columns: 1fr 1fr; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <div class="site">
      <section class="hero-shell">
        <div class="hero-panel">
          <div class="brandbar">
            <div class="navpill">
              <a href="#workbench">Start</a>
              <a href="#workbench">Studio</a>
              <a href="#workbench">Preview</a>
              <a href="/library">Library</a>
            </div>
          </div>
          <div class="hero-stage">
            <div class="hero-corner-logo">
              <img src="/brand/kwai-wordmark.svg" alt="Kwai" />
            </div>
            <div class="hero-copy">
              <div class="hero-left">
                <h1><span class="koko-k">K</span><span class="koko-rest">oKo</span></h1>
                <p class="lede">Kwai Coach</p>
              </div>
              <div class="hero-side">
                <p>
                  Creator-side analysis for story clarity, stronger hooks, cleaner payoff, and better-performing short-form ideas.
                </p>
                <a class="hero-cta" href="#workbench">Start with a link <span>→</span></a>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="workbench" class="work-shell">
        <div class="workspace">
          <div class="composer">
            <div class="composer-head">
              <div></div>
            </div>
            <label for="video-url">Video links</label>
            <textarea id="video-url" placeholder="Paste one link per line&#10;https://www.kwai.com/@.../video/...&#10;https://www.kwai.com/@.../video/..."></textarea>
            <div class="actions">
              <button id="submit-btn">Generate scripts</button>
            </div>
          </div>
          <div id="status-box" class="status-box">
            <div class="status-empty">
              <div class="status-empty-title">Ready.</div>
            </div>
          </div>
        </div>
      </section>
    </div>
  </main>
  <div id="app-toast" class="toast" aria-hidden="true">
    <div class="toast-title" id="app-toast-title"></div>
    <div class="toast-copy" id="app-toast-copy"></div>
  </div>

  <script>
    const videoInput = document.getElementById("video-url");
    const submitBtn = document.getElementById("submit-btn");
    const statusBox = document.getElementById("status-box");
    const heroPanel = document.querySelector(".hero-panel");
    const appToast = document.getElementById("app-toast");
    const appToastTitle = document.getElementById("app-toast-title");
    const appToastCopy = document.getElementById("app-toast-copy");
    let activeJobId = "";
    let activeReviewItemId = "";
    let toastTimer = null;
    const reviewTracker = Object.create(null);
    const STAGE_ORDER = ["queued", "download", "media_prep", "gemini_analysis", "v2_analysis", "consistency_audit", "targeted_recheck", "arbitration", "final_output", "completed"];
    const STAGE_LABELS = {{
      queued: "Queued",
      download: "Download",
      media_prep: "Media prep",
      gemini_analysis: "Gemini analysis",
      v2_analysis: "V2 analysis",
      consistency_audit: "Consistency audit",
      targeted_recheck: "Targeted recheck",
      arbitration: "Arbitration",
      final_output: "Final output",
      completed: "Completed",
      failed: "Failed",
      starting: "Starting"
    }};
    const STAGE_COPY = {{
      queued: "Task created. Preparing analysis.",
      starting: "Preparing the workflow.",
      download: "Downloading the source video.",
      media_prep: "Reading the source video structure.",
      gemini_analysis: "Running the Gemini global analysis pass.",
      v2_analysis: "Running the v2 local analysis pass.",
      consistency_audit: "Comparing both tracks and checking story logic.",
      targeted_recheck: "Rechecking only the risky evidence windows.",
      arbitration: "Selecting the safest story spine.",
      final_output: "Building the final script, export, and HTML.",
      completed: "Analysis completed."
    }};
    const REVIEW_STAGE_ORDER = ["queued", "plan", "recheck", "rebuild", "completed"];
    const REVIEW_STAGE_LABELS = {{
      queued: "Queued",
      plan: "Review plan",
      recheck: "Video recheck",
      rebuild: "Rebuild",
      completed: "Ready",
      failed: "Failed"
    }};

    function setStatus(html, ready = false) {{
      statusBox.className = ready ? "status-box ready" : "status-box";
      statusBox.innerHTML = html;
    }}

    function showToast(title, copy) {{
      if (!appToast || !appToastTitle || !appToastCopy) return;
      appToastTitle.textContent = title || "";
      appToastCopy.textContent = copy || "";
      appToast.classList.add("show");
      appToast.setAttribute("aria-hidden", "false");
      if (toastTimer) clearTimeout(toastTimer);
      toastTimer = setTimeout(() => {{
        appToast.classList.remove("show");
        appToast.setAttribute("aria-hidden", "true");
      }}, 2600);
    }}

    if (heroPanel) {{
      heroPanel.addEventListener("mousemove", (event) => {{
        const rect = heroPanel.getBoundingClientRect();
        const x = (event.clientX - rect.left) / rect.width;
        const y = (event.clientY - rect.top) / rect.height;
        heroPanel.style.setProperty("--mouse-x", `${{(x * 100).toFixed(2)}}%`);
        heroPanel.style.setProperty("--mouse-y", `${{(y * 100).toFixed(2)}}%`);
        heroPanel.style.setProperty("--tilt-x", "0px");
        heroPanel.style.setProperty("--tilt-y", "0px");
      }});

      heroPanel.addEventListener("mouseleave", () => {{
        heroPanel.style.setProperty("--mouse-x", "50%");
        heroPanel.style.setProperty("--mouse-y", "50%");
        heroPanel.style.setProperty("--tilt-x", "0px");
        heroPanel.style.setProperty("--tilt-y", "0px");
      }});
    }}

    function escapeHtml(value) {{
      return String(value || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    }}

    function collectUrls() {{
      return String(videoInput.value || "")
        .split(/[\\n\\r,]+/)
        .map((value) => value.trim())
        .filter((value, index, arr) => value && /^https?:\\/\\//i.test(value) && arr.indexOf(value) === index);
    }}

    function normalizeRows(script) {{
      if (!script || typeof script !== "object") return [];
      if (Array.isArray(script.rows) && script.rows.length) return script.rows;
      if (Array.isArray(script.synthesized_segments) && script.synthesized_segments.length) return script.synthesized_segments;
      return [];
    }}

    function normalizedText(value, fallback = "无") {{
      const text = String(value || "").trim();
      return text || fallback;
    }}

    function buildEditorMarkup(item) {{
      if (item.status !== "completed" || !item.result_json) return "";
      const script = item.result_json || {{}};
      const rows = normalizeRows(script);
      const mechanismReason = (((script.mechanism || {{}}).reason) || "");
      const corePoints = Array.isArray(script.core_viral_points) && script.core_viral_points.length
        ? script.core_viral_points
        : [{{ label: "要点", text: "无" }}];
      const corePointBlocks = corePoints.map((point, idx) => `
        <div class="editor-row-card" data-core-point-index="${{idx}}">
          <div class="editor-row-title">Core point ${{idx + 1}}</div>
          <div class="editor-grid">
            <div class="editor-field">
              <div class="editor-label">Label</div>
              <input class="editor-input" data-core-point-field="label" value="${{escapeHtml(normalizedText(point.label, "要点"))}}">
            </div>
            <div class="editor-field">
              <div class="editor-label">Text</div>
              <textarea class="editor-textarea" data-core-point-field="text">${{escapeHtml(normalizedText(point.text))}}</textarea>
            </div>
          </div>
        </div>
      `).join("");
      const rowBlocks = rows.map((row, idx) => `
        <div class="editor-row-card" data-row-index="${{idx}}">
          <div class="editor-row-title">Row ${{idx + 1}}${{row.time ? ` · ${{escapeHtml(row.time)}}` : ""}}</div>
          <div class="editor-grid">
            <div class="editor-field">
              <div class="editor-label">Time</div>
              <input class="editor-input" data-row-field="time" value="${{escapeHtml(normalizedText(row.time))}}">
            </div>
            <div class="editor-field">
              <div class="editor-label">Visual</div>
              <textarea class="editor-textarea" data-row-field="visual_content">${{escapeHtml(normalizedText(row.visual_content))}}</textarea>
            </div>
            <div class="editor-field">
              <div class="editor-label">Action</div>
              <textarea class="editor-textarea" data-row-field="action">${{escapeHtml(normalizedText(row.action))}}</textarea>
            </div>
            <div class="editor-field">
              <div class="editor-label">Dialogue / audio</div>
              <textarea class="editor-textarea" data-row-field="dialogue_or_audio">${{escapeHtml(normalizedText(row.dialogue_or_audio))}}</textarea>
            </div>
            <div class="editor-field">
              <div class="editor-label">Integrated summary</div>
              <textarea class="editor-textarea" data-row-field="integrated_summary">${{escapeHtml(normalizedText(row.integrated_summary))}}</textarea>
            </div>
          </div>
        </div>
      `).join("");
      return `
        <details class="editor-disclosure">
          <summary class="editor-summary">
            <span>Direct edits</span>
            <span class="editor-summary-copy">Open to adjust title, rows, and core points</span>
          </summary>
          <div class="editor-shell" data-editor-item="${{item.id}}">
            <div class="editor-field">
              <div class="editor-label">Title</div>
              <input class="editor-input" data-edit-field="title" value="${{escapeHtml(normalizedText(script.title || item.title || "", "Video Script"))}}">
            </div>
            <div class="editor-field">
              <div class="editor-label">Whole video summary</div>
              <textarea class="editor-textarea" data-edit-field="whole_video_summary">${{escapeHtml(normalizedText(script.whole_video_summary))}}</textarea>
            </div>
            <div class="editor-field">
              <div class="editor-label">Mechanism reason</div>
              <textarea class="editor-textarea" data-edit-field="mechanism_reason">${{escapeHtml(normalizedText(mechanismReason))}}</textarea>
            </div>
            ${{corePointBlocks}}
            ${{rowBlocks}}
            <div class="link-row">
              <button class="action-link" type="button" data-save-edits="${{item.id}}">保存修改</button>
              <button class="action-link primary" type="button" data-save-library="${{item.id}}">保存到脚本库</button>
            </div>
          </div>
        </details>
      `;
    }}

    function buildReviewMarkup(item) {{
      if (item.status !== "completed" || !item.result_json) return "";
      const status = normalizedText(item.review_status || "", "");
      const stage = normalizedText(item.review_stage || "", "");
      const message = normalizedText(item.review_message || "", "");
      const feedback = normalizedText(item.review_feedback || "", "");
      const editedBadge = item.edited ? `<span class="batch-chip">Manual edits exist</span>` : "";
      const reviewedBadge = item.reviewed ? `<span class="batch-chip">Reviewed version active</span>` : "";
      const reviewState = status ? `<div class="review-note">${{escapeHtml(status)}}${{message ? ` · ${{escapeHtml(message)}}` : ""}}</div>` : "";
      const reviewProgress = buildReviewProgressMarkup(stage, status, message);
      return `
        <div class="review-shell" data-review-item="${{item.id}}">
          <div class="editor-label">Review and rebuild</div>
          <div class="review-note">Describe the core mistake in natural language. Koko will compare your feedback against the original AI analysis, revisit the prior evidence, re-check the video only when needed, and rebuild the script without rerunning the entire workflow.</div>
          ${{editedBadge}}
          ${{reviewedBadge}}
          ${{reviewProgress}}
          ${{reviewState}}
          <textarea class="editor-textarea" data-review-feedback placeholder="Example: The real core is that the husband bragged about his network, but nobody actually came to help him. The current story spine is wrong.">${{escapeHtml(feedback)}}</textarea>
          <div class="link-row">
            <button class="action-link" type="button" data-run-review="${{item.id}}">${{status === "running" ? "复盘中..." : "复盘重做"}}</button>
          </div>
        </div>
      `;
    }}

    function buildReviewProgressMarkup(stage, status, message) {{
      if (!status && !stage) return "";
      const normalizedStage = stage || (status === "completed" ? "completed" : status === "failed" ? "failed" : "queued");
      const stageIndex = REVIEW_STAGE_ORDER.indexOf(normalizedStage);
      const percent = normalizedStage === "completed"
        ? 100
        : normalizedStage === "failed"
          ? 100
          : stageIndex >= 0
            ? Math.max(8, Math.round(((stageIndex + 1) / REVIEW_STAGE_ORDER.length) * 100))
            : 12;
      const fillClass = normalizedStage === "failed" ? "review-progress-fill failed" : "review-progress-fill";
      const steps = REVIEW_STAGE_ORDER.slice(0, 4).map((key) => {{
        let cls = "review-step";
        const keyIndex = REVIEW_STAGE_ORDER.indexOf(key);
        if (normalizedStage === "completed" || (stageIndex > keyIndex && stageIndex >= 0)) cls += " done";
        else if (normalizedStage === key) cls += " active";
        else if (normalizedStage === "failed" && stageIndex === -1 && key === "rebuild") cls += " failed";
        return `<div class="${{cls}}">${{REVIEW_STAGE_LABELS[key]}}</div>`;
      }}).join("");
      const badge = normalizedStage === "failed" ? "Failed" : (REVIEW_STAGE_LABELS[normalizedStage] || "Review");
      return `
        <div class="review-progress">
          <div class="review-progress-top">
            <span>${{escapeHtml(message || "Preparing review.")}}</span>
            <span>${{badge}} · ${{percent}}%</span>
          </div>
          <div class="review-progress-rail"><div class="${{fillClass}}" style="width:${{percent}}%"></div></div>
          <div class="review-steps">${{steps}}</div>
        </div>
      `;
    }}

    function collectItemEdits(itemId) {{
      const root = document.querySelector(`[data-editor-item="${{itemId}}"]`);
      if (!root) return null;
      const core_viral_points = Array.from(root.querySelectorAll("[data-core-point-index]")).map((pointCard) => {{
        return {{
          label: pointCard.querySelector('[data-core-point-field="label"]')?.value || "",
          text: pointCard.querySelector('[data-core-point-field="text"]')?.value || "",
        }};
      }});
      const rows = Array.from(root.querySelectorAll("[data-row-index]")).map((rowCard) => {{
        return {{
          time: rowCard.querySelector('[data-row-field="time"]')?.value || "",
          visual_content: rowCard.querySelector('[data-row-field="visual_content"]')?.value || "",
          action: rowCard.querySelector('[data-row-field="action"]')?.value || "",
          dialogue_or_audio: rowCard.querySelector('[data-row-field="dialogue_or_audio"]')?.value || "",
          integrated_summary: rowCard.querySelector('[data-row-field="integrated_summary"]')?.value || "",
        }};
      }});
      return {{
        title: root.querySelector('[data-edit-field="title"]')?.value || "",
        whole_video_summary: root.querySelector('[data-edit-field="whole_video_summary"]')?.value || "",
        mechanism_reason: root.querySelector('[data-edit-field="mechanism_reason"]')?.value || "",
        core_viral_points,
        rows,
      }};
    }}

    function collectReviewFeedback(itemId) {{
      const root = document.querySelector(`[data-review-item="${{itemId}}"]`);
      if (!root) return "";
      return root.querySelector('[data-review-feedback]')?.value || "";
    }}

    async function persistItemEdits(itemId, mode, button) {{
      const payload = collectItemEdits(itemId);
      if (!payload) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = mode === "library" ? "保存中..." : "更新中...";
      try {{
        const endpoint = mode === "library" ? `/api/items/${{itemId}}/save-to-library` : `/api/items/${{itemId}}/save`;
        const response = await fetch(endpoint, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(payload),
        }});
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Save failed");
        if (activeJobId) {{
          pollJob(activeJobId);
        }} else {{
          window.location.reload();
        }}
      }} catch (error) {{
        alert(String(error.message || error));
        button.disabled = false;
        button.textContent = original;
      }}
    }}

    async function downloadScript(url, button) {{
      if (!url) return;
      const original = button ? button.textContent : "";
      if (button) {{
        button.disabled = true;
        button.textContent = "导出中...";
      }}
      try {{
        const link = document.createElement("a");
        link.href = url;
        link.download = url.split("/").pop() || "script_export.docx";
        link.rel = "noopener";
        link.target = "_self";
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => {{
          showToast("导出成功", "脚本文件已经开始下载到你的本地。");
        }}, 150);
      }} catch (error) {{
        alert("导出脚本失败，请重试。");
      }} finally {{
        if (button) {{
          button.disabled = false;
          button.textContent = original;
        }}
      }}
    }}

    async function runReview(itemId, button) {{
      const feedback = collectReviewFeedback(itemId).trim();
      if (!feedback) {{
        showToast("请先填写反馈", "先告诉 Koko 哪里错了，再开始复盘重做。");
        return;
      }}
      const original = button.textContent;
      button.disabled = true;
      button.textContent = "复盘中...";
      try {{
        const response = await fetch(`/api/items/${{itemId}}/review`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ feedback }}),
        }});
        const data = await response.json();
        if (!response.ok) throw new Error(data.error || "Review failed");
        if (data.job_id) {{
          activeJobId = data.job_id;
          activeReviewItemId = itemId;
          reviewTracker[itemId] = "running";
          pollJob(data.job_id);
        }}
      }} catch (error) {{
        showToast("复盘失败", String(error.message || error));
        button.disabled = false;
        button.textContent = original;
      }}
    }}

    function renderItemCard(item, idx, open = false) {{
      const title = escapeHtml(item.title || `Video ${{idx + 1}}`);
      const contentType = item.content_type ? `<span class="batch-chip">${{escapeHtml(item.content_type)}}</span>` : "";
      const editor = buildEditorMarkup(item);
      const review = buildReviewMarkup(item);
      const links = [
        item.html_url ? `<a class="action-link" href="${{item.html_url}}" target="_blank" rel="noreferrer">Open preview</a>` : "",
        item.docx_url ? `<button class="action-link" type="button" data-download-script="${{item.docx_url}}">导出脚本</button>` : "",
      ].join("");
      const iframe = item.html_url ? `<iframe src="${{item.html_url}}" loading="lazy"></iframe>` : "";
      const error = item.error ? `<code>${{escapeHtml(item.error)}}</code>` : "";
      return `
        <details class="item-card" ${{open ? "open" : ""}}>
          <summary>
            <span>${{idx + 1}}. ${{title}}</span>
            <span>${{escapeHtml(item.status || "")}}</span>
          </summary>
          <div class="item-meta">
            <span>${{escapeHtml(item.stage_message || "")}}</span>
            ${{contentType}}
          </div>
          <div class="item-body">
            <div class="item-sections">
              ${{review}}
              ${{editor}}
              <div class="item-actions-shell">
                <div class="editor-label">Preview and export</div>
                <div class="link-row">${{links}}</div>
              </div>
            </div>
            ${{error}}
            ${{iframe}}
          </div>
        </details>
      `;
    }}

    function renderBatchResults(data) {{
      const items = Array.isArray(data.items) ? data.items : [];
      const summary = `
        <div class="batch-summary">
          <span class="batch-chip">Total ${{data.total_items || items.length}}</span>
          <span class="batch-chip">Completed ${{data.completed_items || 0}}</span>
          <span class="batch-chip">Failed ${{data.failed_items || 0}}</span>
        </div>
      `;
      const cards = items.map((item, idx) => renderItemCard(item, idx, idx === 0)).join("");
      return summary + `<div class="item-stack">${{cards}}</div>`;
    }}

    function checkReviewTransitions(items) {{
      for (const item of (items || [])) {{
        if (!item || !item.id) continue;
        const prev = reviewTracker[item.id] || "";
        const next = item.review_status || "";
        if (prev === "running" && next === "completed") {{
          showToast("复盘成功", "Koko 已完成复盘重做，并更新了当前脚本。");
          if (activeReviewItemId === item.id) activeReviewItemId = "";
        }} else if (prev === "running" && next === "failed") {{
          showToast("复盘失败", item.review_message || "复盘重做没有成功完成。");
          if (activeReviewItemId === item.id) activeReviewItemId = "";
        }}
        reviewTracker[item.id] = next;
      }}
    }}

    function hasRunningReview(items) {{
      return (items || []).some((item) => item && item.review_status === "running");
    }}

    function progressMarkup(stage, stageMessage, jobId) {{
      const index = Math.max(0, STAGE_ORDER.indexOf(stage));
      const percent = stage === "completed" ? 100 : stage === "failed" ? 100 : Math.max(6, Math.round(((index + 1) / STAGE_ORDER.length) * 100));
      const displayMessage = stage === "failed" ? (stageMessage || "Analysis failed.") : (STAGE_COPY[stage] || stageMessage || "Running analysis...");
      const steps = ["download", "media_prep", "gemini_analysis", "v2_analysis", "consistency_audit", "targeted_recheck", "arbitration", "final_output"].map((key) => {{
        let cls = "step-pill";
        const keyIndex = STAGE_ORDER.indexOf(key);
        if (stage === "completed" || index > keyIndex) cls += " done";
        else if (stage === key || (stage === "starting" && key === "download")) cls += " active";
        return `<div class="${{cls}}">${{STAGE_LABELS[key]}}</div>`;
      }}).join("");
      return `
        <div class="progress-wrap">
          <div class="progress-top">
            <span>${{escapeHtml(displayMessage)}}</span>
            <span>${{percent}}%</span>
          </div>
          <div class="progress-rail"><div class="progress-fill" style="width:${{percent}}%"></div></div>
          <div class="step-list">${{steps}}</div>
        </div>
        <small>Task ID: <code>${{escapeHtml(jobId)}}</code></small>
      `;
    }}

    async function pollJob(jobId) {{
      activeJobId = jobId;
      const res = await fetch(`/api/jobs/${{jobId}}`);
      const data = await res.json();
      const batchResults = renderBatchResults(data);
      const reviewRunning = hasRunningReview(data.items);
      checkReviewTransitions(data.items);
      if (data.status === "completed") {{
        const completedMessage = reviewRunning
          ? "Analysis completed. Review is still running."
          : (data.message || "Analysis completed.");
        setStatus(`<span class="status status-completed">completed</span><br><br>${{progressMarkup("completed", completedMessage, data.id)}}${{batchResults}}`, true);
        if (reviewRunning) {{
          setTimeout(() => pollJob(jobId), 2500);
        }}
        return;
      }}
      if (data.status === "failed") {{
        const partial = Array.isArray(data.items) && data.items.length ? batchResults : "";
        setStatus(`<span class="status status-failed">failed</span><br><br>${{progressMarkup("failed", data.message || "Analysis failed.", data.id)}}<code>${{escapeHtml(data.error || "Unknown error")}}</code>${{partial}}`);
        return;
      }}
      const badge = data.status === "running" ? "status-running" : "status-queued";
      const batchSummary = Array.isArray(data.items) && data.items.length ? `
        <div class="batch-summary">
          <span class="batch-chip">Total ${{data.total_items || data.items.length}}</span>
          <span class="batch-chip">Completed ${{data.completed_items || 0}}</span>
          <span class="batch-chip">Failed ${{data.failed_items || 0}}</span>
        </div>` : "";
      setStatus(`<span class="status ${{badge}}">${{data.status}}</span><br><br>${{progressMarkup(data.stage || "queued", data.stage_message || data.message, data.id)}}${{batchSummary}}`);
      setTimeout(() => pollJob(jobId), 2500);
    }}

    submitBtn.addEventListener("click", async () => {{
      const videoUrls = collectUrls();
      if (!videoUrls.length) {{
        setStatus("Please paste at least one public video link first.");
        return;
      }}
      submitBtn.disabled = true;
      setStatus("Creating task...");
      try {{
        const res = await fetch("/api/jobs", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ video_urls: videoUrls }})
        }});
        const data = await res.json();
        if (!res.ok) {{
          throw new Error(data.error || "Task creation failed");
        }}
        activeJobId = data.id;
        setStatus(`<span class="status status-queued">queued</span><br><br>${{progressMarkup("queued", "Task created. Preparing analysis.", data.id)}}`);
        pollJob(data.id);
      }} catch (error) {{
        setStatus(`<span class="status status-failed">failed</span><br><br><code>${{escapeHtml(String(error.message || error))}}</code>`);
      }} finally {{
        submitBtn.disabled = false;
      }}
    }});

    videoInput.addEventListener("keydown", (event) => {{
      if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {{
        submitBtn.click();
      }}
    }});

    document.addEventListener("click", (event) => {{
      const saveBtn = event.target.closest("[data-save-edits]");
      if (saveBtn) {{
        persistItemEdits(saveBtn.getAttribute("data-save-edits"), "save", saveBtn);
        return;
      }}
      const reviewBtn = event.target.closest("[data-run-review]");
      if (reviewBtn) {{
        runReview(reviewBtn.getAttribute("data-run-review"), reviewBtn);
        return;
      }}
      const libraryBtn = event.target.closest("[data-save-library]");
      if (libraryBtn) {{
        persistItemEdits(libraryBtn.getAttribute("data-save-library"), "library", libraryBtn);
        return;
      }}
      const downloadBtn = event.target.closest("[data-download-script]");
      if (downloadBtn) {{
        downloadScript(downloadBtn.getAttribute("data-download-script"), downloadBtn);
      }}
    }});
  </script>
</body>
</html>"""


def library_html() -> str:
    entries = load_library_entries()
    counts = Counter(entry.get("content_type") or DEFAULT_CONTENT_TYPE for entry in entries)
    ordered_counts = [(label, counts.get(label, 0)) for label in LIBRARY_FILTER_LABELS]
    filter_options = "".join(
        f"<option value='{html_escape(label)}'>{html_escape(label)} ({count})</option>"
        for label, count in ordered_counts
    )
    chips = "".join(
        f"<span class='batch-chip'>{html_escape(label)} · {count}</span>"
        for label, count in ordered_counts
    ) or "<span class='batch-chip'>No scripts yet</span>"
    cards = []
    for entry in entries:
        source_video_url = f"/results/{entry.get('entry_id')}/source.mp4"
        created_at = str(entry.get("created_at") or "")[:19].replace("T", " ")
        cards.append(
            f"<article class='library-card' data-content-type='{html_escape(entry.get('content_type') or DEFAULT_CONTENT_TYPE)}'>"
            "<div class='library-card-top'>"
            f"<span class='batch-chip'>{html_escape(entry.get('content_type') or DEFAULT_CONTENT_TYPE)}</span>"
            f"<span class='library-time'>{html_escape(created_at or 'Unknown time')}</span>"
            "</div>"
            f"<a class='video-origin-link' href='{html_escape(entry.get('video_url') or '')}' target='_blank' rel='noreferrer'>{html_escape(entry.get('video_url') or '')}</a>"
            f"<div class='video-frame-wrap'><video class='video-frame' data-first-frame muted playsinline preload='metadata' src='{html_escape(source_video_url)}'></video></div>"
            "<div class='library-copy'>"
            f"<h3>{html_escape(entry.get('title') or 'Untitled Script')}</h3>"
            f"<p>{html_escape(entry.get('whole_video_summary') or '')}</p>"
            "</div>"
            "<div class='link-row'>"
            + (f"<button class='action-link' type='button' data-open-preview='{html_escape(entry.get('html_url') or '')}'>打开预览</button>" if entry.get("html_url") else "")
            + (f"<button class='action-link' type='button' data-download-script='{html_escape(entry.get('docx_url') or '')}'>导出脚本</button>" if entry.get("docx_url") else "")
            + f"<button class='action-link action-link-danger' type='button' data-delete-entry='{html_escape(entry.get('entry_id') or '')}'>删除</button>"
            + "</div>"
            "</article>"
        )
    cards_html = "".join(cards) or "<div class='status-empty-title'>No scripts saved yet.</div>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Koko Library</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Readex+Pro:wght@300;400;500;600;700&display=swap');
    * {{ box-sizing: border-box; font-family: 'Readex Pro', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }}
    body {{
      margin: 0;
      min-height: 100vh;
      color: #FF8200;
      background:
        radial-gradient(circle at 8% 10%, rgba(255,130,0,.46), transparent 30%),
        radial-gradient(circle at 86% 12%, rgba(249,115,0,.38), transparent 28%),
        radial-gradient(circle at 50% 42%, rgba(255,178,84,.20), transparent 36%),
        linear-gradient(180deg, #FFB15A 0%, #FFD9AF 34%, #FFF1E2 70%, #FFFFFF 100%);
    }}
    .library-shell {{ padding: 24px; }}
    .library-wrap {{
      width: min(1320px, 100%);
      margin: 0 auto;
      border-radius: 34px;
      overflow: hidden;
      border: 1px solid rgba(255,255,255,.72);
      box-shadow: 0 28px 80px rgba(249,115,0,.16);
      background:
        radial-gradient(circle at 12% 16%, rgba(255,130,0,.32), rgba(255,130,0,0) 22%),
        radial-gradient(circle at 86% 18%, rgba(249,115,0,.26), rgba(249,115,0,0) 22%),
        radial-gradient(circle at 70% 62%, rgba(255,244,232,.70), rgba(255,244,232,0) 24%),
        linear-gradient(180deg, rgba(255,207,146,.64) 0%, rgba(255,240,222,.52) 44%, rgba(255,255,255,.62) 100%);
      backdrop-filter: blur(26px);
      -webkit-backdrop-filter: blur(26px);
      padding: 28px;
    }}
    .library-topbar {{ display:flex; align-items:flex-start; justify-content:space-between; gap:20px; flex-wrap:wrap; }}
    .library-topbar h1 {{ margin:0; font-size:clamp(3rem, 7vw, 5.4rem); letter-spacing:-.08em; line-height:.88; }}
    .batch-chip {{
      display:inline-flex; align-items:center; border-radius:999px; padding:8px 12px;
      font-size:12px; font-weight:700; color:#FF8200; background:rgba(255,255,255,.58); border:1px solid rgba(255,130,0,.16);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
    }}
    .library-stats {{ display:flex; flex-wrap:wrap; gap:10px; margin-top:18px; }}
    .library-toolbar {{ display:flex; align-items:center; justify-content:space-between; gap:20px; flex-wrap:wrap; margin-top:42px; margin-bottom:14px; }}
    .filter-label {{ display:flex; flex-direction:column; gap:8px; font-size:13px; font-weight:700; }}
    .filter-select {{
      min-width:220px; border:1px solid rgba(255,130,0,.18); border-radius:16px; padding:12px 14px;
      font-size:14px; color:#FF8200; background:rgba(255,255,255,.64); outline:none;
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
    }}
    .library-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(320px, 1fr)); gap:24px; margin-top:20px; }}
    .library-card {{
      border:1px solid rgba(255,130,0,.16); border-radius:24px; background:rgba(255,255,255,.56);
      padding:22px; display:flex; flex-direction:column; gap:18px;
      box-shadow: 0 18px 42px rgba(249,115,0,.10);
      backdrop-filter: blur(18px);
      -webkit-backdrop-filter: blur(18px);
    }}
    .library-card-top {{ display:flex; align-items:center; justify-content:space-between; gap:14px; flex-wrap:wrap; }}
    .library-time {{ font-size:12px; font-weight:700; color:#FF8200; opacity:.88; }}
    .video-origin-link {{ color:#FF8200; text-decoration:none; font-size:13px; line-height:1.55; word-break:break-all; }}
    .video-frame-wrap {{
      border-radius:18px; overflow:hidden; border:1px solid rgba(255,130,0,.16);
      background:rgba(255,244,232,.78); padding:10px;
    }}
    .video-frame {{
      width:100%; height:auto; object-fit:contain; display:block; background:#FFF4E8;
      border-radius:12px;
    }}
    .library-copy {{ display:flex; flex-direction:column; gap:14px; }}
    .library-card h3 {{ margin:0; font-size:22px; line-height:1.34; letter-spacing:0; }}
    .library-card p {{ margin:0; line-height:1.72; font-size:14px; color:#FF8200; }}
    .link-row {{ display:flex; flex-wrap:wrap; gap:12px; margin-top:4px; }}
    .action-link {{
      display:inline-flex; align-items:center; justify-content:center; text-decoration:none;
      border-radius:999px; padding:10px 14px; color:#FF8200; border:1px solid rgba(255,130,0,.18); background:rgba(255,255,255,.72);
      font-weight:700; font-size:13px; cursor:pointer;
      backdrop-filter: blur(14px);
      -webkit-backdrop-filter: blur(14px);
    }}
    .action-link-danger {{ color:#F97300; }}
    .home-link {{ color:#FF8200; text-decoration:none; font-weight:700; }}
    .confirm-overlay {{
      position: fixed; inset: 0; display: none; align-items: center; justify-content: center;
      background: rgba(255, 130, 0, 0.14); backdrop-filter: blur(10px); -webkit-backdrop-filter: blur(10px);
      padding: 24px; z-index: 40;
    }}
    .confirm-overlay.open {{ display: flex; }}
    .confirm-dialog {{
      width: min(460px, 100%); border-radius: 28px; border: 1px solid rgba(255,255,255,.72);
      background:
        radial-gradient(circle at 18% 14%, rgba(255,130,0,.22), rgba(255,130,0,0) 22%),
        linear-gradient(180deg, rgba(255,233,208,.84) 0%, rgba(255,255,255,.74) 100%);
      box-shadow: 0 24px 60px rgba(249,115,0,.18);
      backdrop-filter: blur(22px); -webkit-backdrop-filter: blur(22px);
      padding: 24px;
    }}
    .confirm-dialog h3 {{ margin: 0 0 10px; font-size: 24px; line-height: 1.2; color: #FF8200; }}
    .confirm-dialog p {{ margin: 0; font-size: 14px; line-height: 1.7; color: #FF8200; opacity: .92; }}
    .confirm-actions {{ display:flex; justify-content:flex-end; gap:12px; margin-top:20px; flex-wrap:wrap; }}
  </style>
</head>
<body>
  <main class="library-shell">
    <section class="library-wrap">
      <div class="library-topbar">
        <div>
          <button class="action-link" id="back-home" type="button">← Back to Koko</button>
          <h1>Script Library</h1>
          <div class="library-stats">{chips}</div>
        </div>
      </div>
      <div class="library-toolbar">
        <label class="filter-label">
          <span>Filter by template</span>
          <select id="content-filter" class="filter-select">
            <option value="">All templates</option>
            {filter_options}
          </select>
        </label>
      </div>
      <div class="library-grid">{cards_html}</div>
    </section>
  </main>
  <div class="confirm-overlay" id="delete-confirm-overlay" aria-hidden="true">
    <div class="confirm-dialog" role="dialog" aria-modal="true" aria-labelledby="delete-confirm-title">
      <h3 id="delete-confirm-title">Delete script?</h3>
      <p>This will remove the script from the library and permanently delete its saved files.</p>
      <div class="confirm-actions">
        <button class="action-link" id="delete-confirm-cancel" type="button">Cancel</button>
        <button class="action-link action-link-danger" id="delete-confirm-approve" type="button">Delete</button>
      </div>
    </div>
  </div>
  <script>
    const backHomeButton = document.getElementById("back-home");
    const contentFilter = document.getElementById("content-filter");
    const deleteConfirmOverlay = document.getElementById("delete-confirm-overlay");
    const deleteConfirmCancel = document.getElementById("delete-confirm-cancel");
    const deleteConfirmApprove = document.getElementById("delete-confirm-approve");
    let pendingDeleteButton = null;

    function closeDeleteConfirm() {{
      if (!deleteConfirmOverlay) return;
      deleteConfirmOverlay.classList.remove("open");
      deleteConfirmOverlay.setAttribute("aria-hidden", "true");
      pendingDeleteButton = null;
    }}

    function openDeleteConfirm(button) {{
      if (!deleteConfirmOverlay) return;
      pendingDeleteButton = button;
      deleteConfirmOverlay.classList.add("open");
      deleteConfirmOverlay.setAttribute("aria-hidden", "false");
    }}

    function applyLibraryFilter() {{
      const value = contentFilter ? contentFilter.value : "";
      document.querySelectorAll(".library-card").forEach((card) => {{
        const contentType = card.getAttribute("data-content-type") || "";
        card.style.display = !value || value === contentType ? "" : "none";
      }});
    }}

    if (contentFilter) {{
      contentFilter.addEventListener("change", applyLibraryFilter);
    }}

    document.querySelectorAll("video[data-first-frame]").forEach((video) => {{
      const setFirstFrame = () => {{
        try {{
          video.currentTime = 0.05;
        }} catch (error) {{}}
      }};
      video.addEventListener("loadedmetadata", setFirstFrame, {{ once: true }});
      video.addEventListener("seeked", () => video.pause(), {{ once: true }});
      video.load();
    }});

    if (backHomeButton) {{
      backHomeButton.addEventListener("click", () => {{
        window.location.assign("/");
      }});
    }}

    if (deleteConfirmCancel) {{
      deleteConfirmCancel.addEventListener("click", closeDeleteConfirm);
    }}

    if (deleteConfirmOverlay) {{
      deleteConfirmOverlay.addEventListener("click", (event) => {{
        if (event.target === deleteConfirmOverlay) closeDeleteConfirm();
      }});
    }}

    document.addEventListener("keydown", (event) => {{
      if (event.key === "Escape") closeDeleteConfirm();
    }});

    if (deleteConfirmApprove) {{
      deleteConfirmApprove.addEventListener("click", async () => {{
        const deleteBtn = pendingDeleteButton;
        if (!deleteBtn) {{
          closeDeleteConfirm();
          return;
        }}
        const entryId = deleteBtn.getAttribute("data-delete-entry");
        if (!entryId) {{
          closeDeleteConfirm();
          return;
        }}
        const originalText = deleteBtn.textContent;
        deleteBtn.textContent = "删除中...";
        deleteBtn.disabled = true;
        closeDeleteConfirm();
        try {{
          const response = await fetch(`/api/library/${{entryId}}`, {{ method: "DELETE" }});
          if (!response.ok) throw new Error("Delete failed");
          window.location.reload();
        }} catch (error) {{
          alert("删除失败，请重试。");
          deleteBtn.textContent = originalText;
          deleteBtn.disabled = false;
        }}
      }});
    }}

    document.addEventListener("click", async (event) => {{
      const previewBtn = event.target.closest("[data-open-preview]");
      if (previewBtn) {{
        const url = previewBtn.getAttribute("data-open-preview");
        if (url) window.location.assign(url);
        return;
      }}
      const deleteBtn = event.target.closest("[data-delete-entry]");
      if (deleteBtn) {{
        openDeleteConfirm(deleteBtn);
        return;
      }}
      const downloadBtn = event.target.closest("[data-download-script]");
      if (!downloadBtn) return;
      const url = downloadBtn.getAttribute("data-download-script");
      if (!url) return;
      const originalText = downloadBtn.textContent;
      downloadBtn.textContent = "导出中...";
      downloadBtn.disabled = true;
      try {{
        const link = document.createElement("a");
        link.href = url;
        link.download = url.split("/").pop() || "script_export.docx";
        link.rel = "noopener";
        link.target = "_self";
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => {{
          alert("导出脚本已开始下载。");
        }}, 150);
      }} catch (error) {{
        alert("导出脚本失败，请重试。");
      }} finally {{
        downloadBtn.textContent = originalText;
        downloadBtn.disabled = false;
      }}
    }});
  </script>
</body>
</html>"""


class AppHandler(BaseHTTPRequestHandler):
    server_version = "VideoAnalysisV3Web/0.2"

    def send_json(self, payload: Any, status: int = 200) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def send_html(self, body: str, status: int = 200) -> None:
        raw = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def send_file(self, path: Path) -> None:
        if path.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif path.suffix == ".json":
            content_type = "application/json; charset=utf-8"
        elif path.suffix == ".mp4":
            content_type = "video/mp4"
        elif path.suffix == ".png":
            content_type = "image/png"
        elif path.suffix == ".svg":
            content_type = "image/svg+xml; charset=utf-8"
        elif path.suffix == ".docx":
            content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        else:
            content_type = "application/octet-stream"
        raw = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        if path.suffix == ".docx":
            self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def head_file(self, path: Path) -> None:
        if path.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif path.suffix == ".json":
            content_type = "application/json; charset=utf-8"
        elif path.suffix == ".mp4":
            content_type = "video/mp4"
        elif path.suffix == ".png":
            content_type = "image/png"
        elif path.suffix == ".svg":
            content_type = "image/svg+xml; charset=utf-8"
        elif path.suffix == ".docx":
            content_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        else:
            content_type = "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        if path.suffix == ".docx":
            self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
        self.send_header("Content-Length", str(path.stat().st_size))
        self.end_headers()

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw.decode("utf-8"))

    def serve_result_file(self, job_id: str, filename: str) -> None:
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        path = (RESULTS_ROOT / job_id / filename).resolve()
        base = (RESULTS_ROOT / job_id).resolve()
        if base not in path.parents and path != base:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self.send_file(path)

    def result_path(self, job_id: str, filename: str) -> Path | None:
        if not re.fullmatch(r"[0-9a-f]{32}", job_id):
            return None
        path = (RESULTS_ROOT / job_id / filename).resolve()
        base = (RESULTS_ROOT / job_id).resolve()
        if base not in path.parents and path != base:
            return None
        if not path.exists() or not path.is_file():
            return None
        return path

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            self.send_html(page_html())
            return
        if parsed.path == "/library":
            self.send_html(library_html())
            return
        if parsed.path == "/brand/kwai-wordmark.svg" and HERO_WORDMARK.exists():
            self.send_file(HERO_WORDMARK)
            return
        if parsed.path == "/healthz":
            self.send_json({"ok": True, "time": now_iso(), "skill_root": str(SKILL_ROOT)})
            return
        if parsed.path == "/api/library":
            self.send_json({"entries": load_library_entries()})
            return
        if parsed.path.startswith("/api/jobs/"):
            job_id = parsed.path.split("/")[-1]
            with job_lock:
                job = jobs.get(job_id)
            if not job:
                self.send_json({"error": "Job not found."}, status=404)
                return
            self.send_json(public_job_view(job))
            return
        if parsed.path.startswith("/results/"):
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) >= 3:
                self.serve_result_file(parts[1], "/".join(parts[2:]))
                return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_HEAD(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            body = page_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return
        if parsed.path == "/library":
            body = library_html().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return
        if parsed.path == "/brand/kwai-wordmark.svg" and HERO_WORDMARK.exists():
            self.head_file(HERO_WORDMARK)
            return
        if parsed.path == "/healthz":
            payload = json.dumps({"ok": True, "time": now_iso(), "skill_root": str(SKILL_ROOT)}, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            return
        if parsed.path.startswith("/results/"):
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) >= 3:
                path = self.result_path(parts[1], "/".join(parts[2:]))
                if not path:
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                self.head_file(path)
                return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        item_match = re.fullmatch(r"/api/items/([0-9a-f]{32})/(save|save-to-library)", parsed.path)
        if item_match:
            item_id, action = item_match.groups()
            context = find_item_context(item_id)
            if not context:
                self.send_json({"error": "Script item not found."}, status=404)
                return
            try:
                payload = self.read_json()
            except json.JSONDecodeError:
                self.send_json({"error": "Invalid JSON body."}, status=400)
                return
            parent_job_id, item_index, item = context
            if not item.get("result_json"):
                self.send_json({"error": "No script is available for editing yet."}, status=400)
                return
            try:
                updated_script = apply_script_edits(item.get("result_json") or {}, payload)
                updated_item = regenerate_item_outputs(
                    parent_job_id,
                    item_index,
                    item_id,
                    item.get("video_url") or "",
                    updated_script,
                    persist_library=(action == "save-to-library"),
                )
            except Exception as exc:
                self.send_json({"error": friendly_error(str(exc))}, status=500)
                return
            self.send_json({"ok": True, "item": updated_item, "saved_to_library": action == "save-to-library"})
            return
        review_match = re.fullmatch(r"/api/items/([0-9a-f]{32})/review", parsed.path)
        if review_match:
            item_id = review_match.group(1)
            try:
                payload = self.read_json()
            except json.JSONDecodeError:
                self.send_json({"error": "Invalid JSON body."}, status=400)
                return
            ok, result = start_review_job(item_id, payload.get("feedback") or "")
            if not ok:
                self.send_json({"error": result}, status=400)
                return
            self.send_json({"ok": True, "job_id": result, "item_id": item_id}, status=202)
            return
        if parsed.path != "/api/jobs":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        try:
            payload = self.read_json()
        except json.JSONDecodeError:
            self.send_json({"error": "Invalid JSON body."}, status=400)
            return
        raw_urls = payload.get("video_urls")
        if isinstance(raw_urls, list):
            video_urls = split_video_urls("\n".join(str(url or "") for url in raw_urls))
        else:
            video_urls = split_video_urls(str(payload.get("video_url", "")))
        if not video_urls:
            self.send_json({"error": "Please provide at least one valid public video URL."}, status=400)
            return
        if not AUTO_ANALYZE.exists():
            self.send_json({"error": f"Missing pipeline entrypoint: {AUTO_ANALYZE}"}, status=500)
            return
        job = create_job(video_urls)
        self.send_json(job, status=202)

    def do_DELETE(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/library/"):
            entry_id = parsed.path.split("/")[-1]
            if not re.fullmatch(r"[0-9a-f]{32}", entry_id):
                self.send_json({"error": "Invalid entry id."}, status=400)
                return
            if not delete_library_entry(entry_id):
                self.send_json({"error": "Library entry not found."}, status=404)
                return
            self.send_json({"ok": True, "entry_id": entry_id})
            return
        self.send_error(HTTPStatus.NOT_FOUND)


def main() -> int:
    load_jobs()
    restore_pending_jobs_to_queue()
    start_job_workers()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), AppHandler)
    print(json.dumps({"port": PORT, "data_root": str(DATA_ROOT), "skill_root": str(SKILL_ROOT)}, ensure_ascii=False))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
