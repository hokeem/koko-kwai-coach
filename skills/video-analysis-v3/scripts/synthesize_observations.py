#!/usr/bin/env python3
"""Synthesize objective Gemini observations into audited video script segments."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

INTERPRETIVE_BLOCKLIST = ["假装", "偷偷", "骗", "发现", "以为", "反转", "搞笑", "目的", "动机", "因为", "所以", "被恶搞"]
PAPER_TERMS = ["纸币", "钞票", "钱", "纸巾", "纸状物", "纸币状物", "假钞"]
UNCERTAIN_TERMS = ["无法确认", "不确定", "看不清", "遮挡", "未能确认"]
NO_UNCERTAINTY = {"", "无", "无明确不确定点", "none", "null", "None"}
PRANK_TERMS = ["整蛊", "恶作剧", "骗人", "被骗", "反应", "挑战", "魔术", "套路", "假装", "prank"]
RELATIONSHIP_TERMS = ["妻子", "丈夫", "老婆", "老公", "夫妻", "情侣", "女友", "男友", "邻居", "美女", "帅哥", "电视", "游戏", "离家出走", "洗碗", "家务", "礼物", "打电话", "兄弟", "朋友", "亲戚"]
LUST_TERMS = ["美女", "帅哥", "偷看", "搭讪", "车窗", "镜子", "后视镜", "邻居", "性感", "内裤", "亲密", "电视", "看腻"]
HENPECKED_TERMS = ["离家出走", "洗碗", "做家务", "叠衣服", "送礼物", "求和", "认错", "老婆", "妻子"]
ARGUE_TERMS = ["打电话", "兄弟", "朋友", "亲戚", "帮忙", "院子", "打扫", "借钱", "拒绝", "没空"]
CONTAINER_TERMS = ["瓶", "水壶", "杯", "桶", "容器", "罐"]
STRUCTURE_TERMS = ["底", "露底", "没底", "空心", "漏", "掉出", "掉落", "洞", "透明", "遮挡"]
EXCHANGE_TERMS = ["交换", "调换", "换", "拿走", "放回", "替换", "掉包"]


def sec(t: str) -> int:
    nums = [int(float(x)) for x in re.findall(r"\d+(?:\.\d+)?", str(t))]
    if len(nums) >= 2:
        return nums[-2] * 60 + nums[-1]
    return nums[0] if nums else 0


def fmt(s: int) -> str:
    return f"{s//60:02d}:{s%60:02d}"


def obs_text(o: dict) -> str:
    return json.dumps(o, ensure_ascii=False)


def people_actions(o: dict) -> str:
    parts = []
    for p in o.get("people", []) or []:
        if isinstance(p, dict):
            parts.append(" / ".join(str(p.get(k, "")) for k in ["id", "position", "visible_action"] if p.get(k)))
        else:
            parts.append(str(p))
    return "；".join(parts)


def object_line(o: dict) -> str:
    out = []
    for obj in o.get("objects", []) or []:
        if isinstance(obj, dict):
            label = obj.get("label") or obj.get("name") or "物体"
            pos = obj.get("position", "")
            state = obj.get("state", "")
            out.append("/".join(x for x in [str(label), str(pos), str(state)] if x))
        else:
            out.append(str(obj))
    return "；".join(out)


def scene_key(o: dict) -> str:
    txt = str(o.get("visual_scene", ""))[:24]
    objs = [re.sub(r"[：:/].*", "", x) for x in object_line(o).split("；") if x]
    return txt + "|" + ",".join(objs[:4])


def has_key_action(o: dict) -> bool:
    txt = obs_text(o)
    verbs = ["拿", "放", "递", "打开", "关闭", "倒", "检查", "查看", "离开", "进入", "交给", "靠近", "伸手", "转身", "捡", "掉", "移动"]
    return any(v in txt for v in verbs)


def observation_audio(o: dict) -> str:
    return clean_text(o.get("audio") or o.get("speech") or "")


def has_uncertainty(o: dict) -> bool:
    return any(t in obs_text(o) for t in UNCERTAIN_TERMS)


def clean_text(x: object) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip(" ；。")


def is_real_uncertainty(x: object) -> bool:
    txt = clean_text(x)
    return bool(txt and txt not in NO_UNCERTAINTY and any(t in txt for t in UNCERTAIN_TERMS))


def dedupe_keep_order(items: list[str]) -> list[str]:
    return [x for x in dict.fromkeys(clean_text(i) for i in items if clean_text(i))]


def compress_adjacent_times(times: list[str]) -> str:
    secs = sorted({sec(t) for t in times if str(t).strip()})
    if not secs:
        return ""
    ranges = []
    start = prev = secs[0]
    for s in secs[1:]:
        if s == prev + 1:
            prev = s
        else:
            ranges.append((start, prev))
            start = prev = s
    ranges.append((start, prev))
    return "、".join(fmt(a) if a == b else f"{fmt(a)}–{fmt(b)}" for a, b in ranges)


def collect_people_events(seg: list[dict]) -> dict[str, list[tuple[str, str]]]:
    events: dict[str, list[tuple[str, str]]] = {}
    for o in seg:
        t = str(o.get("time", fmt(sec(o.get("time", 0)))))
        for p in o.get("people", []) or []:
            if not isinstance(p, dict):
                continue
            pid = clean_text(p.get("id") or "人物")
            pos = clean_text(p.get("position"))
            act = clean_text(p.get("visible_action"))
            if not act:
                continue
            if pos.startswith("画面"):
                pos = "在" + pos
            phrase = f"{pos}，{act}" if pos else act
            events.setdefault(pid, []).append((t, phrase))
    return events


def summarize_people(seg: list[dict]) -> list[str]:
    lines = []
    for pid, events in collect_people_events(seg).items():
        phrases = dedupe_keep_order([p for _, p in events])
        if not phrases:
            continue
        time_range = compress_adjacent_times([t for t, _ in events])
        lines.append(f"{time_range}，{pid}{'；随后'.join(phrases[:4])}。")
    return lines


def summarize_objects(seg: list[dict]) -> list[str]:
    tracks: dict[str, list[tuple[str, str]]] = {}
    for o in seg:
        t = str(o.get("time", fmt(sec(o.get("time", 0)))))
        for obj in o.get("objects", []) or []:
            if not isinstance(obj, dict):
                continue
            label = clean_text(obj.get("label") or obj.get("name") or "物体")
            pos = clean_text(obj.get("position"))
            state = clean_text(obj.get("state"))
            desc = "，".join(x for x in [pos, state] if x)
            if desc:
                tracks.setdefault(label, []).append((t, desc))
    lines = []
    priority = ["黑色水壶", "白色水壶", "水壶盖子", "另一个黑色水壶", "纸币状物", "纸状物", "纸币", "纸巾"]
    labels = sorted(tracks, key=lambda x: (priority.index(x) if x in priority else 99, x))
    for label in labels[:6]:
        events = dedupe_keep_order([p for _, p in tracks[label]])
        if events:
            lines.append(f"{label}：{' → '.join(events[:5])}。")
    return lines


def segment_event_sentence(seg: list[dict], idx: int) -> str:
    people = summarize_people(seg)
    objects = summarize_objects(seg)
    if people:
        return " ".join(people[:2])
    if objects:
        return " ".join(objects[:2])
    scenes = dedupe_keep_order([o.get("visual_scene", "") for o in seg])
    return scenes[0] + "。" if scenes else "该段缺少稳定可描述动作。"


def concise_segment_summary(seg: dict) -> str:
    text = clean_text(seg.get("integrated_summary"))
    if not text:
        return ""
    clauses = re.split(r"(?<=。)\s+|；随后", text)
    clauses = [clean_text(c) for c in clauses if clean_text(c)]
    clauses = [re.sub(r"^\d{2}:\d{2}(?:–\d{2}:\d{2})?，", "", c) for c in clauses]
    if len(clauses) <= 2:
        body = "；".join(clauses)
    else:
        body = "；".join(clauses[:2]) + "。"
    return f"{seg.get('start')}–{seg.get('end')}：{body}"


def object_identity_conflict(window: list[dict]) -> list[str]:
    labels = []
    for o in window:
        text = obs_text(o)
        hit = [t for t in PAPER_TERMS if t in text]
        labels.append((o.get("time"), hit))
    flat = [h for _, hits in labels for h in hits]
    notes = []
    if "纸巾" in flat and any(t in flat for t in ["纸币", "钞票", "钱", "纸币状物"]):
        notes.append("纸质物体身份在相邻观察中出现冲突（纸币/钱 与 纸巾），需降级为纸状物或回看关键帧。")
    if "假钞" in flat and not any("文字" in obs_text(o) or "说" in obs_text(o) for o in window):
        notes.append("出现“假钞”判断但缺少明确文字/音频/外观证据，不能作为确定事实。")
    return notes


def split_segments(observations: list[dict]) -> list[list[dict]]:
    if not observations:
        return []
    obs = sorted(observations, key=lambda o: sec(o.get("time", "0")))
    segments = []
    cur = [obs[0]]
    last_key = scene_key(obs[0])
    for prev, o in zip(obs, obs[1:]):
        gap = sec(o.get("time", 0)) - sec(prev.get("time", 0))
        key = scene_key(o)
        transition = False
        reason = []
        if gap > 2:
            transition = True
            reason.append("时间间隔变化")
        if key != last_key and (has_key_action(o) or len(cur) >= 6):
            transition = True
            reason.append("场景/道具/动作状态变化")
        if has_key_action(o) and len(cur) >= 8:
            transition = True
            reason.append("出现新的关键动作")
        if transition:
            cur[-1]["_transition_reason"] = "、".join(reason)
            segments.append(cur)
            cur = [o]
            last_key = key
        else:
            cur.append(o)
            last_key = key
    segments.append(cur)
    return segments


def synthesize_segment(seg: list[dict], idx: int) -> dict:
    start = sec(seg[0].get("time", "0"))
    end = sec(seg[-1].get("time", "0"))
    action_chain = []
    object_tracks = []
    uncertainties = []
    suspicion_notes = []
    blocked = []
    key_times = []
    visuals = []
    audios = []
    for o in seg:
        t = str(o.get("time", fmt(sec(o.get("time", 0)))))
        visual = str(o.get("visual_scene", ""))
        action = people_actions(o)
        objs = object_line(o)
        if visual:
            visuals.append(f"{t} {visual}")
        if action:
            action_chain.append(f"{t} {action}")
        if objs:
            object_tracks.append(f"{t} {objs}")
        audio = observation_audio(o)
        if audio and audio not in NO_UNCERTAINTY:
            audios.append(f"{t} {audio}")
        if has_key_action(o):
            key_times.append(t)
        if is_real_uncertainty(o.get("uncertainty")):
            uncertainties.append(f"{t} {o.get('uncertainty')}")
        elif has_uncertainty(o):
            uncertainties.append(f"{t} 存在遮挡/不确定信息")
        for bad in INTERPRETIVE_BLOCKLIST:
            if bad in obs_text(o):
                suspicion_notes.append(f"{t} observation 含解释性词语“{bad}”，合成时不能直接采用。")
    for i in range(max(1, len(seg)-2)):
        suspicion_notes.extend(object_identity_conflict(seg[i:i+3]))
    suspicion_notes = list(dict.fromkeys(suspicion_notes))
    object_tracks = list(dict.fromkeys(object_tracks))
    uncertainties = list(dict.fromkeys(uncertainties))
    integrated_action = summarize_people(seg)
    integrated_objects = summarize_objects(seg)
    integrated_event = segment_event_sentence(seg, idx)
    raw_action_chain = action_chain[:12]
    raw_object_tracks = object_tracks[:12]
    action_chain = integrated_action or raw_action_chain[:4]
    object_tracks = integrated_objects or raw_object_tracks[:4]
    visuals = visuals[:8]
    if any("假钞" in n for n in suspicion_notes):
        blocked.append("不能写成：男子发现是假钞。除非后续有明确文字/音频/外观证据。")
    if any("纸质物体身份" in n for n in suspicion_notes):
        blocked.append("不能直接写成：纸币被换成纸巾。应写纸状物/纸币状物并标注无法确认。")
    if any("遮挡" in u or "无法确认" in u for u in uncertainties):
        blocked.append("不能断言遮挡处发生了物品转移。")
    allowed = []
    if integrated_event:
        allowed.append(integrated_event)
    if object_tracks:
        allowed.append("可见道具轨迹：" + " ".join(object_tracks[:3]))
    if not allowed and visuals:
        allowed.append(visuals[0])
    role = "建立场景" if idx == 1 else ("结果状态" if idx > 1 and ("离开" in "".join(action_chain) or "查看" in "".join(action_chain)) else "动作发展")
    logic = "suspicious" if suspicion_notes else ("unresolved" if uncertainties else "consistent")
    return {
        "start": fmt(start),
        "end": fmt(max(end, start + 1)),
        "segment_role": role,
        "objective_visual": "；".join(dedupe_keep_order([v.split(" ", 1)[-1] for v in visuals])[:2]) if visuals else "未能确认",
        "integrated_summary": integrated_event,
        "action_chain": action_chain,
        "object_tracks": object_tracks,
        "raw_action_chain": raw_action_chain,
        "raw_object_tracks": raw_object_tracks,
        "transition_reason": seg[-1].get("_transition_reason", "连续观察合并"),
        "key_action_times": key_times[:8],
        "audio_lines": dedupe_keep_order(audios)[:8],
        "uncertainty": "；".join(uncertainties) if uncertainties else "无明确不确定点",
        "suspicion_notes": suspicion_notes,
        "allowed_claims": allowed,
        "blocked_claims": blocked,
        "logic_status": logic,
    }


def times_for_terms(observations: list[dict], terms: list[str], pad: int = 2) -> list[dict]:
    windows = []
    for o in observations:
        if any(t in obs_text(o) for t in terms):
            s = max(0, sec(o.get("time", 0)) - pad)
            e = sec(o.get("time", 0)) + pad
            windows.append({"start": fmt(s), "end": fmt(e), "reason": f"命中关键词：{','.join([t for t in terms if t in obs_text(o)][:4])}"})
    dedup = []
    seen = set()
    for w in windows:
        key = (w["start"], w["end"], w["reason"])
        if key not in seen:
            dedup.append(w)
            seen.add(key)
    return dedup[:10]


def derive_story_analysis(observations: list[dict], segments: list[dict]) -> dict:
    blob = json.dumps({"observations": observations, "segments": segments}, ensure_ascii=False)
    has_prank = any(t in blob for t in PRANK_TERMS)
    has_relationship = any(t in blob for t in RELATIONSHIP_TERMS)
    has_lust = any(t in blob for t in LUST_TERMS)
    has_henpecked = any(t in blob for t in HENPECKED_TERMS)
    has_argue = any(t in blob for t in ARGUE_TERMS)
    has_money = any(t in blob for t in PAPER_TERMS)
    has_container = any(t in blob for t in CONTAINER_TERMS)
    has_exchange = any(t in blob for t in EXCHANGE_TERMS) or ("黑" in blob and "白" in blob and has_container)
    has_structure = any(t in blob for t in STRUCTURE_TERMS)

    confirmed = []
    if has_money:
        confirmed.append("画面/观察中出现纸币、钱或纸状物相关线索；未清楚时统一降级为纸币状物/纸状物。")
    if has_container:
        confirmed.append("画面/观察中出现瓶、水壶或容器类道具，需要追踪其颜色、位置和归属变化。")
    if has_exchange:
        confirmed.append("观察中存在拿取、放置、换位或黑白容器并存等交换/掉包风险信号。")
    if not confirmed:
        confirmed.append("仅确认人物、场景和可见动作；机制类结论需要额外关键帧复核。")

    hypotheses = []
    if has_relationship:
        subtype = "夫妻关系喜剧"
        if has_lust:
            subtype = "夫妻好色/亲密需求反转"
        elif has_henpecked:
            subtype = "妻管严"
        elif has_argue:
            subtype = "夫妻吵架"
        hypotheses.append({
            "type": "关系喜剧",
            "name": subtype,
            "likelihood": "high" if (has_lust or has_henpecked or has_argue) else "medium",
            "story_question": "夫妻/情侣之间谁先占上风？冲突来自面子、欲望、家务、承诺还是权力关系？结尾谁被打脸/抓包/认怂？",
            "story_chain": ["关系场景建立", "冲突/诱因出现", "情绪或权力关系升级", "反转/惩罚/求和"],
            "evidence_for": ["观察中出现夫妻/情侣/亲密关系或家庭场景相关线索。"],
            "evidence_against": [] if (has_lust or has_henpecked or has_argue) else ["子类型仍需通过对白、字幕或关键动作确认。"],
            "verification_questions": ["谁是强势方？", "冲突诱因是什么？", "情绪转折点在哪？", "结尾谁赢/谁输/谁认怂？"],
        })
    if has_prank or (has_money and has_container):
        hypotheses.append({
            "type": "整蛊/道具机制",
            "name": "诱饵—误判—道具机关—揭示/获利",
            "likelihood": "high" if (has_money and has_container and has_exchange) else "medium",
            "story_question": "谁以为自己占到便宜/完成挑战？实际哪个道具状态被改变？最后谁得到好处？",
            "story_chain": ["诱饵出现", "受害者误判", "道具/遮挡机制发生", "结果揭示/获利"],
            "evidence_for": [x for x in confirmed if x],
            "evidence_against": [] if has_exchange else ["尚未稳定确认掉包/交换窗口。"],
        })
    if has_container:
        hypotheses.append({
            "type": "整蛊/道具机制",
            "name": "容器结构机关（露底/空心/漏出/透明/遮挡）",
            "likelihood": "medium" if has_structure else "candidate",
            "story_question": "容器是否真的能装住物体？钱/纸状物放入后是否从底部露出或掉出？",
            "story_chain": ["容器出现", "人物放入/拿取", "结构属性影响结果", "掉出/暴露/无法装住"],
            "evidence_for": ["存在容器类道具。"] + (["观察中出现底部/漏出/掉落类线索。"] if has_structure else []),
            "evidence_against": [] if has_structure else ["Gemini 未明确识别容器底部结构，必须密集抽帧验证。"],
        })
    if has_exchange:
        hypotheses.append({
            "type": "整蛊/道具机制",
            "name": "换瓶/换道具/掉包机制",
            "likelihood": "medium",
            "story_question": "同一时刻是否有两个相似道具？哪个道具从谁手里转到谁手里？",
            "story_chain": ["相似道具并存", "遮挡/手部动作", "归属变化", "结果反转"],
            "evidence_for": ["存在交换、换位或黑白道具并存信号。"],
            "evidence_against": ["若交换发生在遮挡处，不能直接写成事实。"],
        })

    verification = []
    verification += times_for_terms(observations, RELATIONSHIP_TERMS + LUST_TERMS + HENPECKED_TERMS + ARGUE_TERMS, pad=2)
    verification += times_for_terms(observations, EXCHANGE_TERMS + ["黑", "白"], pad=2)
    verification += times_for_terms(observations, STRUCTURE_TERMS, pad=2)
    verification += times_for_terms(observations, PAPER_TERMS, pad=1)
    verification = verification[:12]
    if not verification and observations:
        mid = sec(observations[len(observations)//2].get("time", 0))
        verification = [{"start": fmt(max(0, mid-2)), "end": fmt(mid+2), "reason": "未命中明确机制词，抽取中段动作窗口兜底复核。"}]

    safe_story = " ".join(concise_segment_summary(s) for s in segments[:4] if concise_segment_summary(s))
    if hypotheses:
        safe_story += " 机制判断：先按可见事实确认人物动作和道具轨迹，再把整蛊/喜剧解释列为候选机制；只有经过密集抽帧确认的交换、露底、掉落、获利动作，才写入最终故事。"
    else:
        safe_story += " 当前不强行归纳隐藏机制。"

    core_points = [
        {"title": "机制优先", "text": "不仅看人物做了什么，还要追问道具的物理属性：是否露底、空心、遮挡、漏出或被替换。"},
        {"title": "受害者视角", "text": "整蛊/段子类视频必须拆出“TA以为发生了什么”和“实际发生了什么”的差异。"},
        {"title": "获利闭环", "text": "故事成立需要闭环：诱饵出现 → 误判/贪念/承诺 → 机关动作 → 结果揭示 → 谁得利或谁被套住。"},
    ]
    replaceable_parts = [
        {"title": "诱饵", "text": "钱、手机、礼物、红包、优惠、挑战奖励等高吸引力物品。"},
        {"title": "机关", "text": "露底瓶、双层杯、假盖子、掉包道具、可擦标记、隐藏口袋等。"},
        {"title": "关系", "text": "情侣、夫妻、朋友、路人、老板员工均可，但必须保留清晰的利益/承诺关系。"},
        {"title": "反转证据", "text": "最后要出现可视化证据，如钱掉出、标记出现、道具暴露、对方无法反悔。"},
    ]
    return {
        "genre_guess": " / ".join(dict.fromkeys(str(h.get("type", h.get("name", "候选类型"))) for h in hypotheses)) if hypotheses else "未确认类型",
        "confirmed_facts": confirmed,
        "mechanism_hypotheses": hypotheses,
        "verification_windows": verification,
        "safe_final_story": clean_text(safe_story),
        "core_points": core_points,
        "replaceable_parts": replaceable_parts,
        "must_not_claim_without_verification": [
            "不能把遮挡处动作写成事实。",
            "不能在未复核底部结构时断言“露底/没底”。",
            "不能在未看到归属变化时断言“某人拿走/获得钱”。",
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("observations_json")
    ap.add_argument("--metadata")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    data = json.loads(Path(args.observations_json).read_text())
    obs = data.get("observations", [])
    segments = [synthesize_segment(seg, i+1) for i, seg in enumerate(split_segments(obs))]
    logic_quality = "consistent"
    if any(s["logic_status"] == "suspicious" for s in segments):
        logic_quality = "suspicious"
    elif any(s["logic_status"] == "unresolved" for s in segments):
        logic_quality = "unresolved"
    story_analysis = derive_story_analysis(obs, segments)
    summary_parts = []
    for s in segments:
        short = concise_segment_summary(s)
        if short:
            summary_parts.append(short)
    whole = "本地整合结论：" + (" ".join(summary_parts[:6]) if summary_parts else "未获得足够稳定的画面事实。")
    if logic_quality != "consistent":
        whole += " 其中部分道具身份或遮挡动作证据不足，报告保留为无法确认/需复核。"
    if story_analysis.get("safe_final_story"):
        whole = story_analysis["safe_final_story"]
    out = {
        "mode": "observation_first_audited",
        "whole_video_summary": whole,
        "story_analysis": story_analysis,
        "logic_quality": logic_quality,
        "analysis_route": data.get("analysis_route"),
        "gemini_model": data.get("gemini_model"),
        "source_metadata": json.loads(Path(args.metadata).read_text()) if args.metadata and Path(args.metadata).exists() else data.get("source_metadata", {}),
        "synthesized_segments": segments,
        "observations": obs,
    }
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"out": args.out, "segments": len(segments), "logic_quality": logic_quality}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
