#!/usr/bin/env python3
"""Render video-analysis-v3 audited JSON as the standard Chinese script-summary HTML."""
from __future__ import annotations

import argparse, base64, html, json, mimetypes, re
from datetime import datetime, timezone
from pathlib import Path


def esc(x) -> str:
    return html.escape("" if x is None else str(x))


def data_img(path: str) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    mime = mimetypes.guess_type(str(p))[0] or "image/jpeg"
    return f"data:{mime};base64," + base64.b64encode(p.read_bytes()).decode()


def load_frames(frames_dir: str | None) -> dict[int, dict]:
    if not frames_dir:
        return {}
    mf = Path(frames_dir) / "frames_manifest.json"
    if not mf.exists():
        return {}
    return {int(item["index"]): item for item in json.loads(mf.read_text())}


def frame_pair_html(item: dict) -> str:
    if not item:
        return ""
    chunks = []
    for label, zh in [("start", "首帧"), ("mid", "中帧"), ("end", "尾帧")]:
        if item.get(label):
            src = data_img(item[label])
            if src:
                chunks.append(f'<div class="frame"><img src="{src}"><div class="cap">{zh}</div></div>')
    return '<div class="frames">' + ''.join(chunks[:3]) + '</div>' if chunks else ""


def list_text(items, sep="；") -> str:
    if not items:
        return ""
    if isinstance(items, str):
        return items
    return sep.join(str(x) for x in items if str(x).strip())


def list_cards(items: list[dict], fallback: list[tuple[str, str]]) -> str:
    if not items:
        items = [{"title": t, "text": d} for t, d in fallback]
    cards = []
    for item in items[:6]:
        title = item.get("title") or item.get("name") or "要点"
        text = item.get("text") or item.get("story_question") or item.get("likelihood") or ""
        cards.append(f'<div class="insight"><b>{esc(title)}</b><div>{esc(text)}</div></div>')
    return '<div class="insight-grid">' + ''.join(cards) + '</div>'


def seg_audio(seg: dict) -> str:
    if seg.get("dialogue_or_audio"):
        return str(seg.get("dialogue_or_audio"))
    if seg.get("dialogue_text"):
        return str(seg.get("dialogue_text"))
    lines = seg.get("audio_lines") or []
    if not lines:
        return "无明确对白/旁白，主要靠画面动作推进。"
    text = list_text(lines)
    return text if text else "无明确对白/旁白，主要靠画面动作推进。"


def action_text(seg: dict) -> str:
    if seg.get("action"):
        return str(seg.get("action"))
    if seg.get("action_text"):
        return str(seg.get("action_text"))
    action = list_text(seg.get("action_chain") or [])
    if action:
        return action
    return seg.get("integrated_summary") or "以可见动作和道具变化推进。"


def visual_text(seg: dict) -> str:
    visual = seg.get("visual_content") or seg.get("objective_visual") or seg.get("visual") or "画面细节不足，按关键帧复核。"
    objs = list_text(seg.get("object_tracks") or [])
    if objs:
        return f'{esc(visual)}<br><span class="small">道具轨迹：{esc(objs)}</span>'
    return esc(visual)


def time_text(seg: dict) -> str:
    value = seg.get("time")
    if value not in (None, ""):
        return str(value)
    start = seg.get("start")
    end = seg.get("end")
    if start not in (None, "") or end not in (None, ""):
        return f"{start or ''}-{end or ''}"
    return ""


def render_humor_mechanism(data: dict) -> str:
    humor = data.get("humor_mechanism") or {}
    if not humor:
        return ""
    items = [
        ("铺垫", humor.get("setup")),
        ("违和点", humor.get("incongruity")),
        ("反转点", humor.get("reversal")),
        ("笑点落点", humor.get("punchline")),
        ("背后原因", humor.get("underlying_reason")),
    ]
    lis = []
    for title, text in items:
        if text:
            lis.append(f"<li><b>{esc(title)}：</b>{esc(text)}</li>")
    if not lis:
        return ""
    return f'<div class="card"><h2>包袱机制</h2><ul>{"".join(lis)}</ul></div>'


def render_verification(story: dict) -> str:
    hyps = story.get("mechanism_hypotheses") or []
    wins = story.get("verification_windows") or []
    if not hyps and not wins:
        return ""
    hyp_rows = []
    for h in hyps[:8]:
        hyp_rows.append(
            f"<tr><td>{esc(h.get('name'))}</td><td>{esc(h.get('likelihood'))}</td><td>{esc(h.get('story_question'))}</td><td>{esc(list_text(h.get('evidence_for') or []))}</td><td>{esc(list_text(h.get('evidence_against') or []))}</td></tr>"
        )
    win_rows = []
    for w in wins[:12]:
        win_rows.append(f"<tr><td>{esc(w.get('start'))}-{esc(w.get('end'))}</td><td>{esc(w.get('reason'))}</td></tr>")
    return f"""
<details class="card"><summary>内部机制假设与复核窗口（过程产物，默认折叠）</summary>
 <h3>机制假设</h3><table><thead><tr><th>假设</th><th>可能性</th><th>要追问的问题</th><th>支持证据</th><th>反证/缺口</th></tr></thead><tbody>{''.join(hyp_rows)}</tbody></table>
 <h3>建议密集抽帧窗口</h3><table><thead><tr><th>时间</th><th>原因</th></tr></thead><tbody>{''.join(win_rows)}</tbody></table>
</details>"""


def render_audit(data: dict) -> str:
    rows = []
    for i, seg in enumerate(data.get("synthesized_segments") or [], 1):
        rows.append(f"<tr><td>{i}</td><td>{esc(seg.get('start'))}-{esc(seg.get('end'))}</td><td>{esc(seg.get('logic_status'))}</td><td>{esc(seg.get('uncertainty'))}</td><td>{esc(list_text(seg.get('suspicion_notes') or []))}</td><td>{esc(list_text(seg.get('blocked_claims') or []))}</td></tr>")
    return f"""
<details class="card"><summary>内部审计摘要（过程产物，默认折叠）</summary>
<table><thead><tr><th>#</th><th>时间段</th><th>状态</th><th>不确定点</th><th>质疑/复核</th><th>禁止或降级的说法</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
</details>"""


def render_observations(data: dict) -> str:
    rows = []
    for o in (data.get("observations") or [])[:300]:
        people = []
        for p in o.get("people") or []:
            if isinstance(p, dict):
                people.append("/".join(str(p.get(k, "")) for k in ["id", "position", "visible_action"] if p.get(k)))
            else:
                people.append(str(p))
        objs = []
        for obj in o.get("objects") or []:
            if isinstance(obj, dict):
                objs.append("/".join(str(obj.get(k, "")) for k in ["label", "position", "state"] if obj.get(k)))
            else:
                objs.append(str(obj))
        rows.append(f"<tr><td>{esc(o.get('time'))}</td><td>{esc(o.get('visual_scene'))}</td><td>{esc('；'.join(people))}</td><td>{esc('；'.join(objs))}</td><td>{esc(o.get('audio'))}</td><td>{esc(o.get('uncertainty'))}</td></tr>")
    return f"""
<details class="card"><summary>逐秒客观观察明细（Gemini 原始观察的规范化展示）</summary>
<table><thead><tr><th>时间</th><th>画面</th><th>人物动作</th><th>道具</th><th>音频</th><th>不确定点</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
</details>"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("analysis_json")
    ap.add_argument("--metadata")
    ap.add_argument("--frames")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    data = json.loads(Path(args.analysis_json).read_text())
    meta = json.loads(Path(args.metadata).read_text()) if args.metadata and Path(args.metadata).exists() else data.get("source_metadata", {})
    frames = load_frames(args.frames)
    source = meta.get("source_url") or meta.get("input") or ""
    story = data.get("story_analysis") or {}
    segments = data.get("rows") or data.get("synthesized_segments") or data.get("timeline") or []

    title = "视频总结归纳 + 脚本表"
    route = data.get("report_route") or data.get("analysis_route") or data.get("mode") or "observation-first"
    score = "需复核" if data.get("logic_quality") in {"suspicious", "unresolved"} else "稳定"
    summary = story.get("safe_final_story") or data.get("whole_video_summary") or "未获得足够稳定的画面事实。"
    audio_score = data.get("audio_information_score")

    rows = []
    for seg in segments:
        rows.append(f"""
<tr>
<td>{esc(time_text(seg))}</td>
<td>{visual_text(seg)}</td>
<td>{esc(action_text(seg))}</td>
<td>{esc(seg_audio(seg))}</td>
</tr>""")

    core_fallback = [
        ("机制爆点", "先确认可见动作，再追问隐藏道具机制，避免停留在表面观察。"),
        ("人性动机", "整蛊/关系段子要拆出受害者为什么会相信、为什么会行动。"),
        ("反转证据", "结尾必须有可见证据支撑反转，例如标记出现、钱掉出、道具暴露。"),
    ]
    replace_fallback = [
        ("诱饵", "钱、礼物、手机、红包、优惠券等。"),
        ("机关", "露底瓶、假盖、双层杯、藏钱口袋、可擦标记。"),
        ("关系", "情侣、夫妻、朋友、路人、老板员工。"),
        ("场景", "厨房、街头、柜台、车内、办公室等单场景。"),
    ]

    meta_extra = f" · Audio information score: {esc(audio_score)}/10" if audio_score not in (None, "") else ""

    html_doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
 <meta charset="utf-8">
 <meta name="viewport" content="width=device-width,initial-scale=1">
 <title>{title}</title>
 <style>
 body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans SC',sans-serif;background:#f6f7fb;color:#222;margin:0;padding:24px}}
 .wrap{{max-width:1680px;margin:0 auto}}
 .card{{background:#fff;border-radius:16px;padding:20px 24px;box-shadow:0 2px 10px rgba(0,0,0,.06);margin-bottom:20px}}
 h1{{margin:0 0 8px;font-size:28px}} h2{{margin:0 0 12px;font-size:22px}} h3{{margin:14px 0 8px}}
 .meta{{color:#666;font-size:14px;line-height:1.7}} .summary{{font-size:17px;line-height:1.9}}
 .insight-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-top:4px}}
 .insight{{border:1px solid #e5e7eb;border-radius:10px;padding:12px 14px;background:#fbfdff}}
 .insight b{{display:block;margin-bottom:4px;color:#111827}} .insight div{{font-size:14px;line-height:1.8;color:#374151}}
 a{{color:#2563eb;text-decoration:none;word-break:break-all}}
 table{{width:100%;border-collapse:collapse;table-layout:fixed}} th,td{{border:1px solid #e5e7eb;vertical-align:top;padding:12px 10px;line-height:1.8;font-size:14px}}
 th{{background:#f3f4f6;text-align:left}}
 .script-table th{{background:#d9f0fb;font-weight:800;text-align:center}}
 .script-table td{{white-space:pre-wrap}}
 .script-table th:nth-child(1),.script-table td:nth-child(1){{width:10%;text-align:center}}
 .script-table th:nth-child(2),.script-table td:nth-child(2){{width:24%}}
 .script-table th:nth-child(3),.script-table td:nth-child(3){{width:38%}}
 .script-table th:nth-child(4),.script-table td:nth-child(4){{width:28%}}
 .frames{{display:flex;gap:8px;margin-top:10px;flex-wrap:wrap}} .frame{{flex:1;min-width:90px}} .frame img{{width:100%;border-radius:8px;border:1px solid #e5e7eb;display:block}} .cap{{font-size:12px;color:#666;margin-top:4px;text-align:center}}
 ul{{margin:8px 0 0 20px;padding:0;line-height:1.8}} .small{{font-size:12px;color:#666}} summary{{cursor:pointer;font-weight:700;color:#111827}}
 </style>
</head>
<body><div class="wrap">
 <div class="card"><h1>视频总结归纳 + 脚本表</h1><div class="meta">Route: {esc(route)}{meta_extra} · Analysis status: {esc(score)}<br>视频链接：<a href="{esc(source)}">{esc(source)}</a></div></div>
 <div class="card"><h2>视频整体内容总结</h2><div class="summary">{esc(summary)}</div></div>
 <div class="card"><h2>脚本表</h2><table class="script-table"><thead><tr><th>时间</th><th>画面内容</th><th>动作</th><th>关键对白/旁白</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
 {render_humor_mechanism(data)}
 <div class="card"><h2>核心爆点</h2>{list_cards(story.get('core_points') or [], core_fallback)}</div>
 <div class="card"><h2>可替换部分</h2>{list_cards(story.get('replaceable_parts') or [], replace_fallback)}</div>
 {render_verification(story)}
 {render_audit(data)}
 {render_observations(data)}
</div></body></html>"""
    Path(args.out).write_text(html_doc, encoding="utf-8")
    print(args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
