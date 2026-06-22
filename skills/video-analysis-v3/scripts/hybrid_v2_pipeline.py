#!/usr/bin/env python3
"""V2-style primary workflow with optional Gemini evidence supplementation."""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from gemini_video_observe import api_key, extract_text, files_api_observe, inline_observe, parse_json_text, retry_call

SCRIPT_DIR = Path(__file__).resolve().parent
V3_SKILL_ROOT = SCRIPT_DIR.parent
SKILLS_ROOT = V3_SKILL_ROOT.parent
REPO_ROOT = SKILLS_ROOT.parent
V2_SKILL_ROOT = SKILLS_ROOT / "video-analysis-v2-skill"
LOCAL_VIDEO_BIN = REPO_ROOT / ".video_skill_bin"
FFPROBE_BIN = LOCAL_VIDEO_BIN / "ffprobe"
FFMPEG_BIN = LOCAL_VIDEO_BIN / "ffmpeg"
CONTAINER_ATOMS = {
    b"moov",
    b"trak",
    b"mdia",
    b"minf",
    b"stbl",
    b"edts",
    b"udta",
    b"meta",
}


PRIMARY_PROMPT = """你是一个严格遵循 video-analysis-v2-sop 的短视频分析器。

你的主流程目标不是逐秒导出大证据包，而是先做 v2 风格的主干分析：
1. 判断这条视频更适合 `audio-sop` 还是 `keyframe-sop`
2. 如果音频信息足够，就用“对白回合 / 音频信息单元”作为分段骨架
3. 输出最终可渲染为 `script_table.html` 的紧凑版 JSON

硬性要求：
- 必须覆盖整条视频，而不是只写前面一小段
- `whole_video_summary` 必须是完整自然语言总结，并且要说清楚背后的故事原因/情绪原因/关系原因
- `dialogue_or_audio` 必须是中文直译，只翻译，不改写，不润色，不换说法
- `core_viral_points` 要保留，并且用新的简洁格式输出：每一项只写一句短爆点，直接说明视频为什么抓人、反差点在哪，不要写成长段分析。
- `replaceable_parts` 必须给出可直接套用的替换方案，不是建议；每一项都要说明替换成什么人物/场景/道具/冲突，以及替换后故事主轴怎么变。
- `rows` 必须写成脚本库分镜表：`visual_content` 只写拍摄准备信息（地点/场景、在场人物、必要道具），`action` 写具体调度/表情/动作推进，`dialogue_or_audio` 按说话人分行
- 先判断稳定可见人物数量；如果出现多个同性可见人物，先用 `男性A/男性B/女性A/女性B/儿童A` 这类占位标签，不要直接合并成人物关系
- 只有在音频和画面都强支持时，才允许把 `男性A` 升级成丈夫/哥哥/朋友等关系角色
- 如果说话人不稳定或电话对端不可见，`dialogue_or_audio` 必须用 `人物A`、`人物B`、`电话对端`、`旁白`、`环境音`、`非人类声音` 这类保守标签
- 对高风险小物件或工具类物体，不要因为后果去反推名称。例如：鞋子被固定，不能直接写成“胶水”；只能在画面清晰看到容器/外观时再命名
- 如果物体不够清楚，请保留识别尝试，但必须降级成 `疑似工具`、`小型手持物`、`细长物`、`无法确认的容器` 等标签，并把该时间段放进 `needs_evidence_enrichment`
- 如果你对某个时间段判断不稳，可以放进 `needs_evidence_enrichment`

请输出严格 JSON，字段如下：
{
  "title": "视频总结归纳 + 脚本表",
  "route": "audio-sop 或 keyframe-sop",
  "audio_information_score": "0/10 到 10/10",
  "source_url": "原视频链接",
  "whole_video_summary": "完整自然语言总结，必须写出故事背后的原因和关系机制",
  "core_viral_points": [
    {"label": "反差点", "text": "人物的认真反应和实际真相之间形成强反差，观众会立刻被勾住。"},
    {"label": "包袱落点", "text": "前面铺垫出的紧张感，最后却落到一个生活化的小原因上，形成好笑落差。"}
  ],
  "replaceable_parts": [
    {"label": "替换方案名", "text": "直接可执行的替换方案：把哪些人物/场景/道具/冲突替换成什么，并说明替换后的故事主轴。"}
  ],
  "rows": [
    {
      "source_url": "原视频链接",
      "time": "00:00-00:15",
      "visual_content": "拍摄准备信息，例如：厨房内；丈夫、妻子；碗、抹布",
      "action": "导演分镜式动作、表情和情绪推进，例如：妻子擦碗，丈夫一边擦桌一边慢慢靠近，小心试探提出请求",
      "dialogue_or_audio": "按说话人分行的中文直译，不改写"
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
  },
  "character_registry": [
    {
      "character_id": "男性A",
      "visible_gender_guess": "male/female/unknown",
      "role_guess": "丈夫/朋友/电话对端/unknown",
      "evidence": ["支持这个判断的画面或音频证据"],
      "uncertainty_note": "如果不稳，写清楚为什么"
    }
  ],
  "object_claims": [
    {
      "time": "00:05-00:15",
      "label": "胶水/疑似工具/无法确认的手持物",
      "confidence": "high/medium/low",
      "evidence": ["画面里直接看到了什么"],
      "needs_review": true
    }
  ],
  "audio_multiview_hints": {
    "audio_source_profile": "dialogue/voiceover/event_sound/mixed_field/BGM_only/unknown",
    "stable_source_count": "0/1/2/3+",
    "speaker_guardrail": "如果说话人不稳，最终对白必须保留人物A/人物B/电话对端标签"
  },
  "analysis_limits": ["当前限制"],
  "needs_evidence_enrichment": [
    {
      "time": "00:30-00:42",
      "reason": "为什么这里判断不稳",
      "question": "需要补的具体问题"
    }
  ]
}
"""


SUPPLEMENT_PROMPT = """你现在只负责对指定窗口做局部证据补全。
请基于整条视频，但只回答下面这些时间窗。

要求：
- 如果问题涉及物体识别，必须区分“直接看见的物体”与“根据结果推测的物体”
- 没有清晰外观证据时，不要把物体写成胶水、剪刀、胶带、钉子之类的具体名词
- 可以写：`疑似工具`、`小型手持物`、`细长物`、`无法确认`

输出严格 JSON：
{
  "windows": [
    {
      "time": "00:30-00:42",
      "visual_evidence": "该窗口更准确的画面证据",
      "object_review": [
        {
          "label": "胶水/疑似工具/无法确认",
          "status": "confirmed/uncertain/rejected",
          "evidence": "为什么"
        }
      ],
      "action_evidence": "更准确的动作推进",
      "dialogue_translation": "该窗口对白的中文直译，只翻译不改写",
      "story_reason": "该窗口在故事推进里的真实作用",
      "confidence": "high/medium/low"
    }
  ]
}
"""


REFINE_PROMPT = """你是最后的 v2 成品整理器。
输入里会有：
1. 一个 v2 风格主分析 draft
1.5. 一个 Gemini 全局分析版本 gemini_primary_draft
1.6. 一个按 video-analysis-v2-sop 跑出的本地分析版本 v2_local_result
2. 一个可选的局部补证据结果 supplement
3. 一个可选的 audio_multiview sidecar
4. 一个 type_router 结果，告诉你这次更像哪种模板，或应退回通用框架
5. 一个 similar_cases 摘要，告诉你过去有哪些相似案例和常见误判
6. 一个 comparison_report，说明 Gemini 版本和 v2 版本的冲突点
7. 一个 logic_audit，说明当前候选故事在事实一致性、故事完整性和因果通顺性上是否有问题
8. 一个 arbitration_result，给出本轮仲裁后被采纳的故事主轴和禁止继续使用的错误说法

你的任务：
- 保留 v2 的输出结构
- 用 supplement 修正主分析里不稳的时间段
- 用 `audio_multiview` 约束说话人、声音来源、角色标签
- 把 `type_router` 作为解释框架使用：强命中模板时按模板组织，弱命中时只用父类模板，未命中时退回通用故事框架
- 把 `similar_cases` 作为参考，不要照抄；它们只用来避免重复犯错、提醒你关注相似的故事链和误判点
- 如果 supplement 里把某个物体标记为 `uncertain` 或 `rejected`，最终结果禁止继续使用那个具体名词
- 如果 `comparison_report` 或 `logic_audit` 明确指出 Gemini 版本和 v2 版本存在主轴冲突、人物冲突、物体冲突或因果不通顺，必须优先服从 `arbitration_result`
- `arbitration_result.accepted_story_spine` 是当前最终成品唯一允许采用的故事主轴；不要回退到被拒绝的说法
- `arbitration_result.guardrails` 里的禁令必须严格执行，例如禁止再写“借钱梗”“请朋友来吃饭”这类被判定为越界的结论
- 确保 rows 覆盖整条视频
- `whole_video_summary` 必须完整自然语言总结，并写出背后的故事原因
- `dialogue_or_audio` 是最终 HTML 里“对话”部分的直接来源，必须是中文 1:1 直译：只翻译，不改写，不润色，不概括，不补解释，不合并句子，不删减语气词，不把多句压成一句
- 如果 `audio_multiview` 只支持 `人物A/人物B/电话对端/旁白/环境音/非人类声音`，禁止你在最终结果里升级成更具体身份
- 如果画面里有多个同性稳定人物，禁止把他们合并成一个人物，除非 `character_registry` 或 `audio_multiview` 有强证据支持

成品写法 SOP（必须遵守）：
- `title` 不是泛泛概括，而是一个完整的“故事钩子句”。必须尽量写出：人物 + 核心冲突/伪装 + 结局/包袱落点。优先使用 `不料`、`却`、`反而`、`最终`、`结果` 这类连接词把结局说满。
- `title` 不能只停在前半段设定；如果视频后面出现了真正的落点、反转、站队变化、打脸、和解、掩饰、穿帮，就要在标题里点出来。
- `whole_video_summary` 要像成熟脚本编辑写的“剧情概述”，重点写：起因 -> 推进 -> 关键证据/对质 -> 最终落点。总结必须落在具体事件和最终结果上，而不是抽象拔高成“揭示了复杂关系/社会判断/人性弱点”这类空泛句。
- `whole_video_summary` 允许写背后原因，但必须嵌在具体剧情里，例如“丈夫为了维护面子而选择相信妻子”，而不是另起一句空泛说教。
- 如果人物关系有强证据支持，在 `title` 和 `whole_video_summary` 里优先使用自然角色称呼（如丈夫/妻子/邻居），可读性优先；只有证据不足时才保留 `男性A/女性A`。
- `core_viral_points` 重新保留，但必须是短句式爆点，不要写成长报告。
- `replaceable_parts` 必须是可直接点击套用的替换方案，不能只写“可以替换场景/人物”这种建议。
- `mechanism.items[*].text` 也要尽量具体，不要写成空泛议论文。尤其 `背后原因` 必须落在这条视频里具体的人物心理和关系机制上。
- 如果旧结果里已经出现过“揭示了……复杂关系”“反映了人性的弱点”“社会判断之间的复杂关系”这类抽象收束，请改写成更具体的剧情落点和人物动机。
- `rows` 必须写成脚本库分镜表，不是观察报告：
  - `time` 按剧情 beat / 动作 beat 切分，不要机械平均切；允许 `00:40-Final`。
  - `visual_content` 只写拍摄准备信息：地点/场景、在场人物、必要道具。越短越好，例如“厨房内；丈夫、妻子；碗、抹布”。不要写镜头分析、完整动作链、对话内容、情绪变化或剧情总结。
  - `action` 写导演分镜式动作：谁在画面哪里做什么、表情/情绪如何变化、这个动作如何推动冲突或笑点。
  - `dialogue_or_audio` 按说话人分行；多人对白必须换行。没有可靠对白/字幕时写“无明确对白/旁白，主要靠画面动作推进。”。

最终输出必须是严格的 v2 `script_table.json` 结构：
{
  "title": "视频总结归纳 + 脚本表",
  "route": "audio-sop 或 keyframe-sop",
  "audio_information_score": "0/10 到 10/10",
  "source_url": "原视频链接",
  "whole_video_summary": "一到两段自然语言总结",
  "core_viral_points": [
    {"label": "冲突钩子", "text": "人物一开场就进入不对劲的状态，观众会马上想知道后面为什么会这样。"},
    {"label": "结局反转", "text": "视频把观众往严重方向带，最后却用轻巧真相翻回来，包袱成立。"}
  ],
  "replaceable_parts": [
    {"label": "替换方案名", "text": "直接可执行的替换方案"}
  ],
  "rows": [
    {
      "source_url": "原视频链接",
      "time": "00:00-00:15",
      "visual_content": "拍摄准备信息，例如：厨房内；丈夫、妻子；碗、抹布",
      "action": "导演分镜式动作、表情和情绪推进，例如：妻子擦碗，丈夫一边擦桌一边慢慢靠近，小心试探提出请求",
      "dialogue_or_audio": "按说话人分行的中文 1:1 直译，不改写，不概括，不润色"
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


AUDIO_MULTIVIEW_PROMPT = """你现在只负责生成一个保守的 audio_multiview sidecar。
目标不是写故事总结，而是把“谁在说话、是什么声音、关系大概是什么”拆开。

硬性要求：
- 所有身份类判断都是 hypothesis，不是事实
- `gender_guess` 只能写 `male` / `female` / `unknown`
- `source_type` 只能优先从这些值里选：`human_speech`、`voiceover`、`phone_remote_voice`、`environment_sound`、`animal_sound`、`object_sound`、`mixed_source`、`unknown`
- 如果说话人不稳定，必须用 `人物A`、`人物B`、`电话对端`、`旁白` 这类标签
- 如果只能确认一个稳定说话源，就明确写一个，不要硬拆成两个人
- 不要仅凭刻板印象强行判断关系

输出严格 JSON：
{
  "source_url": "原视频链接",
  "whole_audio_hypothesis": {
    "audio_source_profile": "dialogue/voiceover/event_sound/mixed_field/BGM_only/unknown",
    "overall_audio_form": "对话/旁白/混合/无有效语音",
    "stable_source_count": {
      "value": 0,
      "confidence": 0.0,
      "note": "为什么这样判断"
    },
    "uncertainties": ["不确定点"]
  },
  "speakers": [
    {
      "speaker_id": "SPEAKER_00",
      "display_label": "人物A",
      "gender_guess": "unknown",
      "gender_confidence": 0.0,
      "source_type": "human_speech",
      "role_guess": "",
      "voice_characteristics": ["可选特征"],
      "evidence": ["证据"],
      "uncertainty_note": ""
    }
  ],
  "utterances": [
    {
      "utterance_id": "utt_001",
      "start": "00:00",
      "end": "00:04",
      "speaker_id": "SPEAKER_00",
      "speaker_label": "人物A",
      "text": "中文直译或原话摘要",
      "source_type": "human_speech",
      "is_overlap": false,
      "confidence": 0.0,
      "evidence_note": "为什么归给这个说话源"
    }
  ],
  "relationship_hypotheses": [
    {
      "label": "couple/friends/siblings/coworkers/unclear",
      "confidence": 0.0,
      "evidence": ["支持证据"],
      "counter_evidence": ["反证"]
    }
  ],
  "speaker_guardrails": [
    "最终 script_table 中哪些标签不能升级成更具体身份"
  ],
  "analysis_limits": [
    "gender inference is approximate",
    "relationship labels are hypotheses only"
  ]
}
"""


V2_LOCAL_PROMPT = """你是一个严格按 video-analysis-v2-sop 执行的第二分析器。

这不是主分析，而是一条“本地研究视频”的对照链。你的任务是：
1. 先根据 media_probe 和视频内容判断 route：`audio-sop` 或 `keyframe-sop`
2. 如果是 `audio-sop`，必须把音频信息单元/对话回合作为主要分段骨架；帧只是围绕这些段做首尾验证
3. 如果是 `keyframe-sop`，必须按视觉变化分段，不要假装弱音频有结构价值
4. 输出一份更保守、更强调起因-经过-结果的脚本候选版本

硬性要求：
- 不要为了讲一个“更戏剧化”的故事而超出证据
- 如果电话内容、人物关系或关键物体不够稳定，请保守表达
- 不要从一句模糊台词直接推出整条故事主轴
- `whole_video_summary` 要明确写出起因、经过、结果，重点是故事是否讲通
- `dialogue_or_audio` 继续保持中文忠实翻译
- `rows` 继续按脚本库分镜表写：`visual_content` 是拍摄准备信息（地点/场景、在场人物、必要道具），`action` 是人物调度、表情和动作推进，`dialogue_or_audio` 按说话人分行

输出严格 JSON，结构与 script_table.json 保持兼容，但额外补充：
{
  "title": "视频总结归纳 + 脚本表",
  "route": "audio-sop 或 keyframe-sop",
  "audio_information_score": "0/10 到 10/10",
  "source_url": "原视频链接",
  "whole_video_summary": "按起因-经过-结果写出的完整候选总结",
  "core_viral_points": [],
  "replaceable_parts": [{"label": "替换方案名", "text": "直接可执行的替换方案"}],
  "rows": [
    {
      "source_url": "原视频链接",
      "time": "00:00-00:15",
      "visual_content": "拍摄准备信息，例如：厨房内；丈夫、妻子；碗、抹布",
      "action": "导演分镜式动作、表情和情绪推进，例如：妻子擦碗，丈夫一边擦桌一边慢慢靠近，小心试探提出请求",
      "dialogue_or_audio": "按说话人分行的中文忠实翻译"
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
  },
  "analysis_limits": ["当前限制"],
  "must_verify_windows": [
    {"time": "00:20-00:50", "reason": "这里决定故事主轴"}
  ]
}
"""


COMPARISON_PROMPT = """你现在负责对比两条候选分析链：
1. Gemini 全局分析版本
2. 按 video-analysis-v2-sop 产生的本地分析版本

你的任务不是重写脚本，而是找出它们是否存在“关键冲突”：
- 故事主轴是否一致
- 人物关系是否一致
- 关键物体是否一致
- 关键动作是否一致
- 起因-经过-结果是否一致
- 是否存在明显的因果跳跃或结论越界

输出严格 JSON：
{
  "story_spine_alignment": {
    "status": "pass/conflict",
    "gemini_summary": "...",
    "v2_summary": "...",
    "issue": "如果冲突，写一句话说明"
  },
  "character_alignment": {"status": "pass/conflict", "issues": ["..."]},
  "object_alignment": {"status": "pass/conflict", "issues": ["..."]},
  "causal_alignment": {"status": "pass/conflict", "issues": ["..."]},
  "focus_windows": [
    {
      "time": "00:20-00:55",
      "reason": "这里是主要冲突发生的地方",
      "question": "这里到底在找人干活，还是在讲借钱/吃饭？"
    }
  ],
  "recommended_action": "proceed/challenge_required/force_recheck",
  "reasoning": "整体判断理由"
}
"""


LOGIC_AUDIT_PROMPT = """你现在负责做“故事逻辑审查”，不是总结剧情。

你会看到：
1. Gemini 版本
2. v2 本地版本
3. comparison_report

请检查：
- 基本事实是否错误：人物数量、性别、关键物体、关键动作
- 是否有莫名其妙突然出现的物品/动机/关系
- 故事有没有人物、地点、事件、起因、经过、结果
- 事件经过和结果之间是否通顺
- 是否只凭一句话就推出整个故事主轴

输出严格 JSON：
{
  "fact_consistency": {"status": "pass/fail", "issues": ["..."]},
  "story_structure": {"status": "pass/fail", "issues": ["..."]},
  "causal_coherence": {"status": "pass/fail", "issues": ["..."]},
  "recommended_action": "proceed/force_recheck/prefer_v2/prefer_gemini/conservative_merge",
  "reasoning": "为什么"
}
"""


CONFLICT_RECHECK_PROMPT = """你现在是在做一次“冲突复核”，目标不是重新分析整条视频，而是解决 Gemini 版本和 v2 版本之间的关键冲突。

你会看到：
- comparison_report
- logic_audit
- focus_windows

请带着这些冲突点重新回看视频，重点确认：
- 关键人物关系
- 关键电话内容
- 关键物体
- 故事主轴到底是什么

要求：
- 不要写超出证据的结论
- 如果无法完全证实，就保守

输出严格 JSON：
{
  "verification_result": "confirmed/partially_confirmed/inconclusive",
  "accepted_story_spine": "更稳妥的一句话故事主轴",
  "evidence_findings": [
    {
      "time": "00:20-00:55",
      "finding": "这段真正发生了什么",
      "supports": "gemini/v2/both/neither",
      "confidence": "low/medium/high"
    }
  ],
  "corrected_entities": [
    {"wrong": "旧说法", "correct": "更稳妥说法", "evidence": "..."}
  ],
  "guardrails": [
    "后续最终整理里禁止继续写的错误说法"
  ]
}
"""


ARBITRATION_PROMPT = """你现在负责做最终仲裁。

输入会有：
1. Gemini 全局分析版本
2. v2 本地分析版本
3. comparison_report
4. logic_audit
5. 可选 conflict_recheck

你的任务：
- 选出当前更可信的故事主轴
- 说明最终应该采用 gemini、v2、merged 还是 conservative 版本
- 输出后续 final_refine 必须遵守的 guardrails

输出严格 JSON：
{
  "accepted_pipeline": "gemini/v2/merged/conservative",
  "accepted_story_spine": "最终应采用的一句话故事主轴",
  "reasoning": "为什么这样裁决",
  "rejected_claims": ["被拒绝的错误说法"],
  "guardrails": ["最终整理必须遵守的禁令或保守要求"]
}
"""

TRANSLATE_DIALOGUE_PROMPT = """你是一个严格的对白翻译器。

你的任务：
- 只处理 `dialogue_or_audio`
- 把非中文对白/旁白翻译成中文
- 必须忠实 1:1 翻译
- 只翻译，不改写，不润色，不概括，不补背景解释
- 保留说话顺序、语气、停顿、换行、称呼、语气词
- 如果原文已经是中文，就原样返回
- 如果是环境音/无对白/拟声，可以保守翻译成中文描述

输出严格 JSON：
{
  "translations": [
    {
      "index": 0,
      "dialogue_or_audio": "中文忠实翻译"
    }
  ]
}
"""

PRIMARY_FALLBACK_MODELS = [
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
]

SUPPLEMENT_FALLBACK_MODELS = [
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
    "gemini-3-flash-preview",
]

AUDIO_MULTIVIEW_MAX_BYTES = 18 * 1024 * 1024

RISKY_OBJECT_PATTERNS = {
    "胶水": re.compile(r"胶水|粘胶|强力胶"),
    "剪刀": re.compile(r"剪刀"),
    "胶带": re.compile(r"胶带|透明胶|封箱带"),
    "钉子": re.compile(r"钉子|图钉"),
    "绳子": re.compile(r"绳子|细绳|鞋带"),
}

TYPE_TEMPLATE_LIBRARY = {
    "universal": {
        "id": "universal",
        "title": "通用故事框架",
        "description": "无模板或弱命中时的总兜底。先看人物关系、冲突诱因、关键载体、转折点和结果，再保守输出候选机制。",
    },
    "relationship_comedy": {
        "id": "relationship_comedy",
        "title": "关系喜剧",
        "description": "适合夫妻/情侣/亲密关系冲突。重点看谁先占上风、冲突来自面子/欲望/家务/承诺还是权力关系，结尾谁被打脸。",
    },
    "couple_argue": {
        "id": "couple_argue",
        "title": "夫妻吵架",
        "description": "围绕家务、院子、装修、打扫等现实事务展开。一方先吹嘘人脉、承诺能解决，随后通过打电话/找朋友亲戚连续验证失败，另一方用现实经验总结打脸，最后回到花钱找人或认清狐朋狗友不顶用的落点。",
    },
    "henpecked": {
        "id": "henpecked",
        "title": "妻管严",
        "description": "丈夫表面反抗被管束，想花钱、谈条件或争自主，后半段却暴露自己离不开原来的强势管束。重点是权力反转、习惯依赖和最后主动要求恢复原管教秩序。",
    },
    "lust_catch": {
        "id": "lust_catch",
        "title": "夫妻好色/抓包",
        "description": "第三方异性出现后，一方明显偷看、搭讪或欲望外露，另一方迅速察觉并当场惩罚。重点是外界诱因、欲望暴露、抓包瞬间和翻车反应。",
    },
    "couple_cheat": {
        "id": "couple_cheat",
        "title": "夫妻出轨",
        "description": "围绕隐瞒第三者、掩饰行踪、伪装身份或反侦察展开。重点看脚印、借口、修理工/来客身份伪装，以及前后脚错位进入同一家庭空间的风险。",
    },
    "couple_deceive": {
        "id": "couple_deceive",
        "title": "夫妻欺骗",
        "description": "表面状态与真实行为相反，一方被假象欺骗并据此做出付出或判断，另一方暗中进行购物、偷溜、消费或隐瞒行为。重点是睡觉/生病/乖巧等表象与真实行动的不一致。",
    },
    "couple_scheme": {
        "id": "couple_scheme",
        "title": "夫妻算计",
        "description": "围绕穿搭、资源、注意力或关系中的轻度博弈展开。一方刻意保留信息、制造悬念或防止对方模仿，重点是保留优势、嘴上拿捏和关系里的小算计。",
    },
    "couple_prank": {
        "id": "couple_prank",
        "title": "夫妻整蛊",
        "description": "一方借助道具、时机或环境声响实施整蛊，另一方沉浸在电视、手机或其他注意力目标里，最终被突然惊吓。重点是铺垫、卡点、受害者反应和滑稽后果。",
    },
    "couple_dirty_joke": {
        "id": "couple_dirty_joke",
        "title": "夫妻黄段子",
        "description": "一方故意抛出带性暗示的台词，另一方却从贫穷、消费、银行卡、衣物等现实角度误解并接招。重点是暧昧暗示与现实误读之间的反差。",
    },
    "prank_prop": {
        "id": "prank_prop",
        "title": "整蛊/道具机制",
        "description": "重点追问诱饵、误判、道具机制、受害者视角和结果证据，不允许把遮挡处动作写成事实。",
    },
}

TYPE_PATTERN_RULES = [
    {
        "subtype": "夫妻吵架",
        "parent_type": "夫妻类型",
        "template_id": "couple_argue",
        "threshold": 2,
        "keywords": ["院子", "打扫", "装修", "凉棚", "干活", "帮忙", "兄弟", "朋友", "亲戚", "打电话", "没人来", "花钱找人"],
        "reason": "命中院子/装修/打扫/找人帮忙/连续被拒这类现实事务争执信号。",
    },
    {
        "subtype": "夫妻出轨",
        "parent_type": "夫妻类型",
        "template_id": "couple_cheat",
        "threshold": 2,
        "keywords": ["抓奸", "奸夫", "脚印", "倒着走", "假脚印", "电工", "修水管", "换灯泡", "躲藏", "第三者"],
        "reason": "命中抓奸、反侦察、伪装维修工或第三者潜入的出轨题材信号。",
    },
    {
        "subtype": "夫妻好色",
        "parent_type": "夫妻类型",
        "template_id": "lust_catch",
        "threshold": 2,
        "keywords": ["美女", "帅哥", "照镜子", "车窗", "放下车窗", "偷看", "搭讪", "暴揍", "抓包"],
        "reason": "命中外界异性诱因、偷看搭讪和抓包惩罚的欲望外露信号。",
    },
    {
        "subtype": "妻管严",
        "parent_type": "夫妻类型",
        "template_id": "henpecked",
        "threshold": 2,
        "keywords": ["喝酒", "训斥", "别管", "给你钱", "疯老婆", "爱骂我的", "送酒", "受不了", "回到原样"],
        "reason": "命中反抗管束、花钱谈条件、最后又求恢复原管教秩序的权力反转信号。",
    },
    {
        "subtype": "夫妻欺骗",
        "parent_type": "夫妻类型",
        "template_id": "couple_deceive",
        "threshold": 2,
        "keywords": ["酣睡", "睡觉", "偷偷溜", "信用卡", "商场", "购物", "赶回床上", "以为", "包揽家务"],
        "reason": "命中表面睡觉/乖巧与暗中消费/外出的欺骗反差信号。",
    },
    {
        "subtype": "夫妻算计",
        "parent_type": "夫妻类型",
        "template_id": "couple_scheme",
        "threshold": 2,
        "keywords": ["穿搭", "惊喜", "模仿", "嫉妒", "聚会", "不给你看", "太傻", "风格"],
        "reason": "命中穿搭展示、保留信息、怕被模仿或嫉妒的轻度关系算计信号。",
    },
    {
        "subtype": "夫妻整蛊",
        "parent_type": "夫妻类型",
        "template_id": "couple_prank",
        "threshold": 2,
        "keywords": ["气球", "恐怖电视", "关键时候", "扎破", "吓得", "掉下沙发", "整蛊", "恶作剧"],
        "reason": "命中道具卡点、突然惊吓和夸张反应的夫妻整蛊信号。",
    },
    {
        "subtype": "夫妻黄段子",
        "parent_type": "夫妻类型",
        "template_id": "couple_dirty_joke",
        "threshold": 2,
        "keywords": ["没穿内衣", "什么都没穿", "挑逗", "性暗示", "银行卡", "买内衣", "太穷", "取钱"],
        "reason": "命中性暗示与现实误读错位的黄段子题材信号。",
    },
]


def unique_models(*names: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        value = str(name or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def is_retryable_model_error(text: str) -> bool:
    hay = (text or "").upper()
    return any(
        token in hay
        for token in [
            "HTTP 503",
            "HTTP 500",
            "HTTP 404",
            "HTTP 400",
            "INTERNAL",
            "INVALID_ARGUMENT",
            "NOT_FOUND",
            "UNAVAILABLE",
            "UNSUPPORTED",
            "MODEL_NOT_FOUND",
            "HIGH DEMAND",
            "RESOURCE_EXHAUSTED",
            "TIMED OUT",
            "REMOTE END CLOSED CONNECTION WITHOUT RESPONSE",
            "EOF OCCURRED",
            "INCOMPLETEREAD",
            "READ OPERATION TIMED OUT",
            "CONNECTION RESET",
            "BROKEN PIPE",
            "NO PARSEABLE JSON OBJECT FOUND",
            "JSON",
            "EXPECTING ',' DELIMITER",
            "EXPECTING VALUE",
            "UNTERMINATED STRING",
            "EXTRA DATA",
        ]
    )


def run_step(name: str, cmd: list[str]) -> None:
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"[{name}] failed: {detail}")


def _resolve_working_binary(candidates: list[Path | str], version_args: list[str] | None = None) -> str | None:
    args = version_args or ["-version"]
    for candidate in candidates:
        if not candidate:
            continue
        path = str(candidate)
        try:
            proc = subprocess.run(
                [path, *args],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except OSError:
            continue
        if proc.returncode == 0:
            return path
    return None


def _iter_atoms(handle, start: int, end: int):
    pos = start
    while pos + 8 <= end:
        handle.seek(pos)
        header = handle.read(8)
        if len(header) < 8:
            break
        size = int.from_bytes(header[:4], "big")
        atom_type = header[4:8]
        header_size = 8
        if size == 1:
            extended = handle.read(8)
            if len(extended) < 8:
                break
            size = int.from_bytes(extended, "big")
            header_size = 16
        elif size == 0:
            size = end - pos
        if size < header_size:
            break
        atom_start = pos + header_size
        atom_end = pos + size
        yield atom_type, atom_start, atom_end
        pos = atom_end


def _parse_mvhd(handle, start: int, end: int) -> float:
    handle.seek(start)
    data = handle.read(end - start)
    if len(data) < 24:
        return 0.0
    version = data[0]
    if version == 1 and len(data) >= 32:
        timescale = int.from_bytes(data[20:24], "big")
        duration = int.from_bytes(data[24:32], "big")
    elif len(data) >= 20:
        timescale = int.from_bytes(data[12:16], "big")
        duration = int.from_bytes(data[16:20], "big")
    else:
        return 0.0
    if not timescale:
        return 0.0
    return float(duration) / float(timescale)


def _parse_tkhd_dimensions(handle, start: int, end: int) -> tuple[int | None, int | None]:
    handle.seek(start)
    data = handle.read(end - start)
    if len(data) < 8:
        return None, None
    width_fixed = int.from_bytes(data[-8:-4], "big")
    height_fixed = int.from_bytes(data[-4:], "big")
    width = width_fixed >> 16 if width_fixed else None
    height = height_fixed >> 16 if height_fixed else None
    return width, height


def _parse_hdlr_type(handle, start: int, end: int) -> str | None:
    handle.seek(start)
    data = handle.read(end - start)
    if len(data) < 12:
        return None
    try:
        return data[8:12].decode("ascii", "ignore")
    except Exception:
        return None


def _parse_mp4_fallback(video: Path) -> dict:
    duration = 0.0
    width = None
    height = None
    audio_stream_count = 0
    audio_stream_exists = False
    try:
        with video.open("rb") as handle:
            file_size = video.stat().st_size
            for atom_type, atom_start, atom_end in _iter_atoms(handle, 0, file_size):
                if atom_type != b"moov":
                    continue
                for child_type, child_start, child_end in _iter_atoms(handle, atom_start, atom_end):
                    if child_type == b"mvhd":
                        duration = _parse_mvhd(handle, child_start, child_end)
                    elif child_type == b"trak":
                        track_type = None
                        track_width = None
                        track_height = None
                        for trak_type, trak_start, trak_end in _iter_atoms(handle, child_start, child_end):
                            if trak_type == b"tkhd":
                                track_width, track_height = _parse_tkhd_dimensions(handle, trak_start, trak_end)
                            elif trak_type == b"mdia":
                                for mdia_type, mdia_start, mdia_end in _iter_atoms(handle, trak_start, trak_end):
                                    if mdia_type == b"hdlr":
                                        track_type = _parse_hdlr_type(handle, mdia_start, mdia_end)
                                        break
                        if track_type == "vide" and (width is None or height is None):
                            width = track_width or width
                            height = track_height or height
                        elif track_type == "soun":
                            audio_stream_count += 1
            audio_stream_exists = audio_stream_count > 0
    except Exception:
        duration = 0.0
    return {
        "duration": round(duration, 3) if duration else 0,
        "resolution": {"width": width, "height": height},
        "video_codec": None,
        "audio_stream_exists": audio_stream_exists,
        "audio_codec": None,
        "audio_stream_count": audio_stream_count,
        "fallback_mode": "mp4_header_parse",
    }


def run_media_probe(video: Path, out_path: Path) -> dict:
    probe_script = V2_SKILL_ROOT / "scripts" / "probe_media.py"
    ffprobe_cmd = _resolve_working_binary(
        [FFPROBE_BIN, shutil.which("ffprobe")],
        ["-h"],
    )
    if ffprobe_cmd:
        env = os.environ.copy()
        ffprobe_dir = str(Path(ffprobe_cmd).parent)
        env["PATH"] = f"{ffprobe_dir}:{env.get('PATH', '')}"
        proc = subprocess.run(
            [sys.executable, str(probe_script), str(video), "--output", str(out_path)],
            text=True,
            capture_output=True,
            env=env,
        )
        if proc.returncode == 0 and out_path.exists():
            return json.loads(out_path.read_text(encoding="utf-8"))
    fallback = _parse_mp4_fallback(video)
    out_path.write_text(json.dumps(fallback, ensure_ascii=False, indent=2), encoding="utf-8")
    return fallback


def write_progress(out_dir: Path, stage: str, message: str) -> None:
    path = out_dir / "progress.json"
    payload = {"stage": stage, "message": message}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"progress": payload}, ensure_ascii=False), flush=True)


def normalize_script_payload(result: dict, source_url: str, title_fallback: str = "视频总结归纳 + 脚本表") -> dict:
    value = dict(result or {})
    value.setdefault("title", title_fallback)
    value["source_url"] = value.get("source_url") or source_url
    value.setdefault("rows", [])
    value.setdefault("core_viral_points", [])
    value.setdefault("replaceable_parts", [])
    value.setdefault("analysis_limits", [])
    return value


def _contains_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text or "")))


def _contains_latin(text: str) -> bool:
    return bool(re.search(r"[A-Za-zÀ-ÿ]", str(text or "")))


def needs_dialogue_translation(text: str) -> bool:
    value = str(text or "").strip()
    if not value:
        return False
    if _contains_chinese(value) and not _contains_latin(value):
        return False
    return _contains_latin(value) and not _contains_chinese(value)


def enforce_chinese_dialogue_translation(script_json: dict, key: str, models: list[str]) -> dict:
    rows = script_json.get("rows") or []
    if not isinstance(rows, list) or not rows:
        return script_json

    targets: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        text = str(row.get("dialogue_or_audio") or "").strip()
        if needs_dialogue_translation(text):
            targets.append(
                {
                    "index": index,
                    "time": row.get("time") or "",
                    "dialogue_or_audio": text,
                }
            )

    if not targets:
        return script_json

    payload = {"rows": targets}
    try:
        translated, _, model_used = run_text_json_prompt_with_fallback(
            payload,
            key,
            models,
            TRANSLATE_DIALOGUE_PROMPT,
            "dialogue translation",
        )
    except Exception:
        return script_json

    translation_items = translated.get("translations") or []
    if not isinstance(translation_items, list):
        return script_json

    translated_count = 0
    for item in translation_items:
        if not isinstance(item, dict):
            continue
        try:
            index = int(item.get("index"))
        except Exception:
            continue
        if index < 0 or index >= len(rows):
            continue
        translated_text = str(item.get("dialogue_or_audio") or "").strip()
        if not translated_text:
            continue
        rows[index]["dialogue_or_audio"] = translated_text
        translated_count += 1

    if translated_count:
        script_json["rows"] = rows
        script_json["dialogue_translation_model_used"] = model_used
    return script_json


def run_video_json_prompt(video: Path, key: str, model: str, prompt: str, mime: str = "video/mp4", inline_max_mb: float = 18.0) -> tuple[dict, dict]:
    if video.stat().st_size <= inline_max_mb * 1024 * 1024:
        return inline_observe(video, key, model, prompt, mime)
    return files_api_observe(video, key, model, prompt, mime)


def run_text_json_prompt(payload: dict, key: str, model: str, prompt: str) -> tuple[dict, dict]:
    body = {
        "contents": [
            {
                "parts": [
                    {"text": prompt},
                    {"text": json.dumps(payload, ensure_ascii=False)},
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
            raise RuntimeError(f"Gemini refine HTTP {exc.code}: {detail}") from exc

    raw = retry_call("Gemini text refine", _send, attempts=2, sleep_sec=2)
    return parse_json_text(extract_text(raw)), raw


def run_video_json_prompt_with_fallback(video: Path, key: str, models: list[str], prompt: str, label: str) -> tuple[dict, dict, str]:
    last_error: Exception | None = None
    tried: list[str] = []
    for model in unique_models(*models):
        tried.append(model)
        try:
            data, raw = run_video_json_prompt(video, key, model, prompt)
            return data, raw, model
        except Exception as exc:
            last_error = exc
            retryable = isinstance(exc, json.JSONDecodeError) or is_retryable_model_error(str(exc))
            if not retryable:
                break
    raise RuntimeError(f"{label} failed across models {tried}: {last_error}") from last_error


def run_text_json_prompt_with_fallback(payload: dict, key: str, models: list[str], prompt: str, label: str) -> tuple[dict, dict, str]:
    last_error: Exception | None = None
    tried: list[str] = []
    for model in unique_models(*models):
        tried.append(model)
        try:
            data, raw = run_text_json_prompt(payload, key, model, prompt)
            return data, raw, model
        except Exception as exc:
            last_error = exc
            retryable = isinstance(exc, json.JSONDecodeError) or is_retryable_model_error(str(exc))
            if not retryable:
                break
    raise RuntimeError(f"{label} failed across models {tried}: {last_error}") from last_error


def merge_candidate_windows(*window_lists: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for window_list in window_lists:
        for window in window_list or []:
            if not isinstance(window, dict):
                continue
            time_value = str(window.get("time") or "").strip()
            reason = str(window.get("reason") or "").strip()
            question = str(window.get("question") or "").strip()
            if not time_value:
                start = str(window.get("start") or "").strip()
                end = str(window.get("end") or "").strip()
                if start and end:
                    time_value = f"{start}-{end}"
            if not time_value:
                continue
            key = (time_value, reason, question)
            if key in seen:
                continue
            seen.add(key)
            merged.append({"time": time_value, "reason": reason, "question": question})
    return merged


def comparison_windows(report: dict) -> list[dict]:
    windows: list[dict] = []
    for item in report.get("focus_windows") or []:
        if not isinstance(item, dict):
            continue
        time_value = str(item.get("time") or "").strip()
        reason = str(item.get("reason") or "").strip()
        question = str(item.get("question") or "").strip()
        if time_value:
            windows.append({"time": time_value, "reason": reason, "question": question})
    return windows


def should_run_conflict_recheck(comparison_report: dict, logic_audit: dict) -> tuple[bool, str]:
    if str(logic_audit.get("recommended_action") or "").strip() == "force_recheck":
        return True, "Logic audit requires forced recheck."
    if str(comparison_report.get("recommended_action") or "").strip() == "force_recheck":
        return True, "Comparison report requires forced recheck."
    for key in ["story_spine_alignment", "causal_alignment"]:
        node = comparison_report.get(key) or {}
        if str(node.get("status") or "").strip() == "conflict":
            return True, f"{key} is in conflict."
    if str((logic_audit.get("fact_consistency") or {}).get("status") or "").strip() == "fail":
        return True, "Fact consistency failed."
    if str((logic_audit.get("causal_coherence") or {}).get("status") or "").strip() == "fail":
        return True, "Causal coherence failed."
    return False, "No severe Gemini-v2 conflict detected."


def parse_time_range(value: str) -> tuple[str, str]:
    text = str(value or "").strip()
    if "-" in text:
        parts = text.split("-", 1)
        return parts[0].strip(), parts[1].strip()
    return text, text


def parse_audio_score(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    head = text.split("/", 1)[0].strip()
    try:
        return int(float(head))
    except ValueError:
        return 0


def should_run_audio_multiview(primary_result: dict, video: Path) -> tuple[bool, str]:
    route = str(primary_result.get("route") or "").strip()
    audio_score = parse_audio_score(primary_result.get("audio_information_score"))
    if route != "audio-sop" and audio_score < 6:
        return False, "主分析未判定为音频主导"

    if video.exists() and video.stat().st_size > AUDIO_MULTIVIEW_MAX_BYTES:
        return False, f"视频体积较大（{video.stat().st_size // (1024 * 1024)}MB），跳过整段 audio_multiview 以避免二次全量超时"

    if any("speaker" in str(x).lower() or "说话" in str(x) or "电话对端" in str(x) or "声音来源" in str(x) for x in primary_result.get("analysis_limits") or []):
        return True, "主分析已暴露说话人/声音来源不稳定"

    for item in primary_result.get("character_registry") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("role_guess") or "").strip().lower() in {"", "unknown"}:
            return True, "角色归属仍不稳定"
        if str(item.get("uncertainty_note") or "").strip():
            return True, "人物/说话人存在不确定说明"

    for window in primary_result.get("needs_evidence_enrichment") or []:
        if not isinstance(window, dict):
            continue
        blob = " ".join(str(window.get(k) or "") for k in ["reason", "question", "time"])
        if any(term in blob for term in ["说话", "对白", "电话对端", "声音", "旁白", "speaker"]):
            return True, "补证据窗口提示说话人/对白归属存在风险"

    return False, "主分析已足够稳定，不再做第二次整段 audio_multiview"


def _keyword_hit_count(text: str, words: list[str]) -> int:
    return sum(1 for word in words if word and word in text)


def build_type_router(primary_result: dict, audio_multiview_result: dict, metadata: dict, source_url: str) -> dict:
    blob = json.dumps(
        {
            "primary": primary_result,
            "audio_multiview": audio_multiview_result,
            "metadata": metadata,
        },
        ensure_ascii=False,
    )
    route = str(primary_result.get("route") or "").strip()
    audio_score = parse_audio_score(primary_result.get("audio_information_score"))
    relationship_hits = _keyword_hit_count(blob, ["夫妻", "情侣", "老婆", "老公", "妻子", "丈夫", "女友", "男友", "伴侣"])
    prank_hits = _keyword_hit_count(blob, ["整蛊", "恶作剧", "被骗", "套路", "假装", "魔术", "换瓶", "露底", "掉包", "诱饵"])
    prop_hits = _keyword_hit_count(blob, ["瓶", "水壶", "杯", "桶", "容器", "纸币", "钱", "礼物", "红包"])

    matched: list[dict] = []
    parent_type = "通用故事"
    subtype = "未命中现成模板"
    routing_mode = "universal"
    confidence = "low"
    reasons: list[str] = []

    if relationship_hits >= 1:
        parent_type = "夫妻类型"
        matched.append(TYPE_TEMPLATE_LIBRARY["relationship_comedy"])
        reasons.append("出现夫妻/情侣/伴侣相关线索。")
        best_rule = None
        best_score = 0
        for rule in TYPE_PATTERN_RULES:
            score = _keyword_hit_count(blob, rule["keywords"])
            if score >= rule["threshold"] and score > best_score:
                best_rule = rule
                best_score = score
        if best_rule:
            subtype = best_rule["subtype"]
            matched.append(TYPE_TEMPLATE_LIBRARY[best_rule["template_id"]])
            routing_mode = "strong-template"
            confidence = "high" if best_score >= best_rule["threshold"] + 1 else "medium"
            reasons.append(best_rule["reason"])
        else:
            subtype = "关系喜剧（待细分）"
            routing_mode = "weak-parent-template"
            confidence = "medium"
            reasons.append("命中夫妻/伴侣关系类，但子类型证据还不够稳定。")
    elif prank_hits >= 1 or prop_hits >= 3:
        parent_type = "整蛊/道具机制"
        subtype = "整蛊/道具机制"
        matched.append(TYPE_TEMPLATE_LIBRARY["prank_prop"])
        routing_mode = "strong-template" if prank_hits >= 1 else "weak-parent-template"
        confidence = "high" if prank_hits >= 1 else "medium"
        reasons.append("命中整蛊/诱饵/道具容器相关信号。")
    else:
        matched.append(TYPE_TEMPLATE_LIBRARY["universal"])
        reasons.append("没有稳定命中现成模板，退回通用故事框架。")

    if route == "audio-sop":
        reasons.append(f"主流程判定为音频主导，音频信息分 {audio_score}/10。")
    elif route:
        reasons.append(f"主流程判定为视觉主导（{route}），音频信息分 {audio_score}/10。")

    review_questions = [
        "这条视频更像哪一种高频结构？",
        "是否应该只使用父类模板，而不要硬套子类型？",
        "最终结论里哪些关系、物体或机制仍需保守表达？",
    ]
    if routing_mode == "strong-template":
        review_questions.append("模板里的核心故事链是否在当前证据中完整成立？")
    elif routing_mode == "universal":
        review_questions.append("既然没命中模板，是否应该只输出候选机制而非具体题材名？")

    return {
        "source_url": source_url,
        "route": route,
        "audio_information_score": primary_result.get("audio_information_score"),
        "routing_mode": routing_mode,
        "primary_type": parent_type,
        "subtype_guess": subtype,
        "confidence": confidence,
        "matched_templates": matched,
        "reasoning_summary": " ".join(reasons),
        "review_questions": review_questions,
    }


def load_case_memory(memory_path: Path) -> list[dict]:
    if not memory_path.exists():
        return []
    try:
        data = json.loads(memory_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return data if isinstance(data, list) else []


def save_case_memory(memory_path: Path, entries: list[dict]) -> None:
    memory_path.write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")


def build_case_memory_entry(source_url: str, type_router: dict, final_result: dict, primary_result: dict) -> dict:
    story = final_result.get("story_analysis") or {}
    return {
        "id": Path(source_url).name or source_url[-24:],
        "source_url": source_url,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "route": final_result.get("route") or primary_result.get("route") or "",
        "audio_information_score": final_result.get("audio_information_score") or primary_result.get("audio_information_score") or "",
        "primary_type": type_router.get("primary_type") or "",
        "subtype_guess": type_router.get("subtype_guess") or "",
        "routing_mode": type_router.get("routing_mode") or "",
        "whole_video_summary": final_result.get("whole_video_summary") or "",
        "safe_final_story": story.get("safe_final_story") or "",
        "core_viral_points": final_result.get("core_viral_points") or [],
        "replaceable_parts": final_result.get("replaceable_parts") or [],
        "mechanism_labels": [str(item.get("label") or item.get("title") or "") for item in (final_result.get("mechanism") or {}).get("items") or []],
        "template_titles": [str(item.get("title") or item.get("id") or "") for item in type_router.get("matched_templates") or []],
        "analysis_limits": primary_result.get("analysis_limits") or [],
    }


def find_similar_cases(memory_entries: list[dict], type_router: dict, primary_result: dict, limit: int = 3) -> list[dict]:
    current_summary = " ".join(
        [
            str(primary_result.get("whole_video_summary") or ""),
            " ".join(str(row.get("visual_content") or "") for row in (primary_result.get("rows") or [])[:4]),
            " ".join(str(row.get("action") or "") for row in (primary_result.get("rows") or [])[:4]),
        ]
    )
    current_words = {w for w in re.findall(r"[\u4e00-\u9fffA-Za-z0-9_]{2,}", current_summary) if len(w) >= 2}
    ranked: list[tuple[int, dict]] = []
    for entry in memory_entries:
        if not isinstance(entry, dict):
            continue
        if entry.get("source_url") == type_router.get("source_url"):
            continue
        score = 0
        if entry.get("primary_type") == type_router.get("primary_type"):
            score += 4
        if entry.get("subtype_guess") == type_router.get("subtype_guess"):
            score += 5
        if entry.get("route") == primary_result.get("route"):
            score += 2
        hay = " ".join(
            [
                str(entry.get("whole_video_summary") or ""),
                str(entry.get("safe_final_story") or ""),
                " ".join(str(x) for x in entry.get("template_titles") or []),
            ]
        )
        words = {w for w in re.findall(r"[\u4e00-\u9fffA-Za-z0-9_]{2,}", hay) if len(w) >= 2}
        score += min(4, len(current_words & words))
        if score > 0:
            ranked.append((score, entry))
    ranked.sort(key=lambda item: item[0], reverse=True)
    picked = []
    for score, entry in ranked[:limit]:
        picked.append(
            {
                "score": score,
                "source_url": entry.get("source_url") or "",
                "primary_type": entry.get("primary_type") or "",
                "subtype_guess": entry.get("subtype_guess") or "",
                "route": entry.get("route") or "",
                "whole_video_summary": entry.get("whole_video_summary") or "",
                "safe_final_story": entry.get("safe_final_story") or "",
                "template_titles": entry.get("template_titles") or [],
            }
        )
    return picked


def infer_object_review_windows(primary_result: dict) -> list[dict]:
    windows: list[dict] = []
    rows = primary_result.get("rows") or []
    claims = primary_result.get("object_claims") or []
    claimed_times: set[str] = set()

    for claim in claims:
        if not isinstance(claim, dict):
            continue
        time_value = str(claim.get("time") or "").strip()
        label = str(claim.get("label") or "").strip()
        confidence = str(claim.get("confidence") or "").strip().lower()
        if not time_value or not label:
            continue
        if confidence in {"low", "medium"}:
            windows.append(
                {
                    "time": time_value,
                    "reason": f"高风险物体 `{label}` 识别把握不足",
                    "question": f"请确认 {time_value} 是否真的能清晰看见 `{label}`，还是只能写成疑似工具/无法确认。",
                }
            )
            claimed_times.add(time_value)

    for row in rows:
        if not isinstance(row, dict):
            continue
        time_value = str(row.get("time") or "").strip()
        if not time_value or time_value in claimed_times:
            continue
        blob = " ".join(str(row.get(key) or "") for key in ["visual_content", "action", "dialogue_or_audio"])
        for label, pattern in RISKY_OBJECT_PATTERNS.items():
            if pattern.search(blob):
                windows.append(
                    {
                        "time": time_value,
                        "reason": f"检测到高风险物体名 `{label}`，需要复核是否为结果反推",
                        "question": f"请只基于清晰可见证据复核 {time_value} 的 `{label}` 是否真实可见；若不是，请改写为疑似工具/无法确认。",
                    }
                )
                claimed_times.add(time_value)
                break

    return windows


def review_object_decisions(supplement_result: dict) -> tuple[set[str], set[str]]:
    uncertain_or_rejected: set[str] = set()
    confirmed: set[str] = set()
    for window in supplement_result.get("windows") or []:
        if not isinstance(window, dict):
            continue
        for item in window.get("object_review") or []:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            status = str(item.get("status") or "").strip().lower()
            if not label:
                continue
            if status == "confirmed":
                confirmed.add(label)
            elif status in {"uncertain", "rejected"}:
                uncertain_or_rejected.add(label)
    return uncertain_or_rejected, confirmed


def apply_object_review_to_text(text: str, uncertain_labels: set[str], confirmed_labels: set[str]) -> str:
    value = str(text or "")
    if not value:
        return value

    should_downgrade_glue = any(label in uncertain_labels for label in {"强力胶", "无法确认的粘性物质"}) and not any(
        label in confirmed_labels for label in {"强力胶", "胶水"}
    )
    if should_downgrade_glue:
        replacements = [
            (r"一支疑似胶水的软管物体", "一个白色软管状物体"),
            (r"一个疑似胶水的软管", "一个白色软管状物体"),
            (r"疑似胶水的软管", "白色软管状物体"),
            (r"疑似强力胶软管", "白色软管状物体"),
            (r"疑似胶水", "白色软管状物体"),
            (r"强力胶", "白色软管状物体"),
            (r"胶水", "白色软管状物体"),
            (r"粘胶水", "涂抹某种物质"),
            (r"粘合剂", "粘性物质"),
        ]
        for pattern, replacement in replacements:
            value = re.sub(pattern, replacement, value)

    scissors_confirmed = any("剪刀" in label for label in confirmed_labels)
    if scissors_confirmed:
        value = value.replace("疑似剪刀的金属工具", "剪刀")
        value = value.replace("疑似金属工具", "剪刀")
        value = value.replace("剪刀/疑似金属工具", "剪刀")
        value = value.replace("一个剪刀", "一把剪刀")
    return value


def enforce_object_reviews(final_result: dict, supplement_result: dict) -> dict:
    uncertain_labels, confirmed_labels = review_object_decisions(supplement_result)
    if not uncertain_labels and not confirmed_labels:
        return final_result

    for key in ["title", "whole_video_summary"]:
        if key in final_result:
            final_result[key] = apply_object_review_to_text(final_result.get(key, ""), uncertain_labels, confirmed_labels)

    for item in final_result.get("core_viral_points") or []:
        if isinstance(item, dict):
            item["label"] = apply_object_review_to_text(item.get("label", ""), uncertain_labels, confirmed_labels)
            item["text"] = apply_object_review_to_text(item.get("text", ""), uncertain_labels, confirmed_labels)

    for item in final_result.get("replaceable_parts") or []:
        if isinstance(item, dict):
            item["label"] = apply_object_review_to_text(item.get("label", ""), uncertain_labels, confirmed_labels)
            item["text"] = apply_object_review_to_text(item.get("text", ""), uncertain_labels, confirmed_labels)

    for row in final_result.get("rows") or []:
        if isinstance(row, dict):
            for key in ["visual_content", "action", "dialogue_or_audio"]:
                row[key] = apply_object_review_to_text(row.get(key, ""), uncertain_labels, confirmed_labels)

    mechanism = final_result.get("mechanism") or {}
    for item in mechanism.get("items") or []:
        if isinstance(item, dict):
            item["label"] = apply_object_review_to_text(item.get("label", ""), uncertain_labels, confirmed_labels)
            item["text"] = apply_object_review_to_text(item.get("text", ""), uncertain_labels, confirmed_labels)
    return final_result


def maybe_extract_frames(video: Path, out_dir: Path, final_json: dict) -> None:
    ffmpeg_cmd = _resolve_working_binary([FFMPEG_BIN, shutil.which("ffmpeg")])
    if not ffmpeg_cmd:
        return
    rows = final_json.get("rows") or []
    if not rows:
        return
    start_dir = out_dir / "selected_frames"
    end_dir = out_dir / "selected_frames_end"
    start_dir.mkdir(parents=True, exist_ok=True)
    end_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "frame_timestamps.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "output"])
        writer.writeheader()
        for index, row in enumerate(rows, start=1):
            start_ts, end_ts = parse_time_range(row.get("time", ""))
            start_rel = f"selected_frames/segment_{index:03d}.jpg"
            end_rel = f"selected_frames_end/segment_{index:03d}.jpg"
            writer.writerow({"timestamp": start_ts, "output": str(out_dir / start_rel)})
            writer.writerow({"timestamp": end_ts, "output": str(out_dir / end_rel)})
            row["start_frame"] = start_rel
            row["end_frame"] = end_rel
    extract_script = V2_SKILL_ROOT / "scripts" / "extract_frames.py"
    env = os.environ.copy()
    ffmpeg_dir = str(Path(ffmpeg_cmd).parent)
    env["PATH"] = f"{ffmpeg_dir}:{env.get('PATH', '')}"
    proc = subprocess.run(
        [sys.executable, str(extract_script), str(video), str(csv_path)],
        text=True,
        capture_output=True,
        env=env,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        print(json.dumps({"warning": f"[extract_frames] skipped after failure: {detail}"}, ensure_ascii=False), flush=True)
        return


STORY_RENOVATION_KEYWORDS = ["装修", "区域", "院子", "凉棚", "干活", "搭建", "建造", "打扫", "求助", "免费帮助", "花钱请人"]
STORY_SPOUSE_COMPLAINT_KEYWORDS = ["妻子不帮忙", "家务", "做家务", "没人帮忙家务", "家庭关系中付出与回报", "抱怨妻子"]
STORY_BORROW_MONEY_KEYWORDS = ["借钱", "吃饭", "朋友圈", "收租"]


def _keyword_score(text: str, words: list[str]) -> int:
    hay = str(text or "")
    return sum(1 for word in words if word in hay)


def harden_comparison_report(primary_result: dict, v2_local_result: dict, comparison_report: dict) -> dict:
    report = dict(comparison_report or {})
    gemini_summary = str((report.get("story_spine_alignment") or {}).get("gemini_summary") or primary_result.get("whole_video_summary") or "")
    v2_summary = str((report.get("story_spine_alignment") or {}).get("v2_summary") or v2_local_result.get("whole_video_summary") or "")

    gemini_renovation = _keyword_score(gemini_summary, STORY_RENOVATION_KEYWORDS)
    v2_renovation = _keyword_score(v2_summary, STORY_RENOVATION_KEYWORDS)
    gemini_spouse = _keyword_score(gemini_summary, STORY_SPOUSE_COMPLAINT_KEYWORDS)
    v2_spouse = _keyword_score(v2_summary, STORY_SPOUSE_COMPLAINT_KEYWORDS)
    gemini_money = _keyword_score(gemini_summary, STORY_BORROW_MONEY_KEYWORDS)
    v2_money = _keyword_score(v2_summary, STORY_BORROW_MONEY_KEYWORDS)

    hard_issues: list[str] = []
    if v2_renovation >= 2 and gemini_renovation == 0:
        hard_issues.append("v2 明确指向装修/找人干活主轴，但 Gemini 总结没有保留这个主轴。")
    if gemini_spouse >= 1 and v2_spouse == 0 and v2_renovation >= 2:
        hard_issues.append("Gemini 把故事偏成了配偶家务抱怨线，但 v2 主轴是装修求助/干活求助。")
    if gemini_money >= 1 and v2_money == 0 and v2_renovation >= 2:
        hard_issues.append("Gemini 出现借钱/吃饭/朋友圈类结论，但 v2 主轴不支持这一方向。")

    if hard_issues:
        story_alignment = dict(report.get("story_spine_alignment") or {})
        story_alignment["status"] = "conflict"
        issue_text = str(story_alignment.get("issue") or "").strip()
        story_alignment["issue"] = " ".join(filter(None, [issue_text, *hard_issues])).strip()
        report["story_spine_alignment"] = story_alignment

        causal_alignment = dict(report.get("causal_alignment") or {})
        causal_alignment["status"] = "conflict"
        causal_issues = list(causal_alignment.get("issues") or [])
        causal_issues.extend(x for x in hard_issues if x not in causal_issues)
        causal_alignment["issues"] = causal_issues
        report["causal_alignment"] = causal_alignment

        focus_windows = list(report.get("focus_windows") or [])
        focus_windows.append(
            {
                "time": "00:00-01:10",
                "reason": "本地硬规则判断 Gemini 与 v2 的故事主轴不一致，需要重点复核装修求助/家务抱怨/借钱笑话之间是否串线。",
                "question": "当前故事到底围绕找人干活，还是被错误带偏成配偶抱怨或借钱梗？",
            }
        )
        report["focus_windows"] = focus_windows
        report["recommended_action"] = "force_recheck"
        reasoning = str(report.get("reasoning") or "").strip()
        report["reasoning"] = " ".join(filter(None, [reasoning, "本地硬规则已把这次判定升级为主轴冲突，不能直接 proceed。"])).strip()
    return report


def harden_logic_audit(primary_result: dict, v2_local_result: dict, logic_audit: dict, comparison_report: dict) -> dict:
    audit = dict(logic_audit or {})
    if str(((comparison_report.get("story_spine_alignment") or {}).get("status") or "")).strip() == "conflict":
        causal = dict(audit.get("causal_coherence") or {})
        causal["status"] = "fail"
        causal_issues = list(causal.get("issues") or [])
        issue = str((comparison_report.get("story_spine_alignment") or {}).get("issue") or "").strip()
        if issue and issue not in causal_issues:
            causal_issues.append(issue)
        causal["issues"] = causal_issues
        audit["causal_coherence"] = causal
        audit["recommended_action"] = "force_recheck"
        reasoning = str(audit.get("reasoning") or "").strip()
        audit["reasoning"] = " ".join(filter(None, [reasoning, "本地硬规则发现 Gemini 与 v2 主轴冲突，因此逻辑审查强制要求 recheck。"])).strip()
    return audit


def main() -> int:
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("source_path")
        ap.add_argument("--out", required=True)
        ap.add_argument("--model", default="gemini-2.5-flash-lite")
        ap.add_argument("--supplement-model", default="gemini-2.5-flash-lite")
        ap.add_argument("--api-key")
        ap.add_argument("--api-key-file")
        args = ap.parse_args()

        out_dir = Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        write_progress(out_dir, "download", "正在下载视频")
        scripts_dir = Path(__file__).resolve().parent
        download_script = scripts_dir / "download_video.py"
        v2_render_script = V2_SKILL_ROOT / "scripts" / "render_script_table.py"

        run_step("download_video", [sys.executable, str(download_script), args.source_path, "--out", str(out_dir)])

        video = out_dir / "source.mp4"
        metadata_path = out_dir / "source_metadata.json"
        primary_json_path = out_dir / "primary_v2_draft.json"
        primary_raw_path = out_dir / "primary_analysis_raw_gemini.json"
        supplement_json_path = out_dir / "supplement_evidence.json"
        supplement_raw_path = out_dir / "supplement_raw_gemini.json"
        audio_multiview_path = out_dir / "audio_multiview.json"
        audio_multiview_raw_path = out_dir / "audio_multiview_raw_gemini.json"
        type_router_path = out_dir / "type_router.json"
        media_probe_path = out_dir / "media_probe.json"
        v2_local_json_path = out_dir / "v2_local_result.json"
        v2_local_raw_path = out_dir / "v2_local_raw_gemini.json"
        comparison_report_path = out_dir / "comparison_report.json"
        comparison_raw_path = out_dir / "comparison_raw_gemini.json"
        logic_audit_path = out_dir / "logic_audit.json"
        logic_audit_raw_path = out_dir / "logic_audit_raw_gemini.json"
        conflict_recheck_path = out_dir / "conflict_recheck.json"
        conflict_recheck_raw_path = out_dir / "conflict_recheck_raw_gemini.json"
        arbitration_path = out_dir / "arbitration_result.json"
        arbitration_raw_path = out_dir / "arbitration_raw_gemini.json"
        case_memory_entry_path = out_dir / "case_memory_entry.json"
        final_json_path = out_dir / "script_table.json"
        final_html_path = out_dir / "script_table.html"
        refine_raw_path = out_dir / "final_refine_raw_gemini.json"
        case_memory_path = out_dir.parent.parent / "case_memory.json"

        key, _ = api_key(args.api_key, args.api_key_file)
        primary_models = unique_models(args.model, *PRIMARY_FALLBACK_MODELS)
        supplement_models = unique_models(args.supplement_model, args.model, *SUPPLEMENT_FALLBACK_MODELS)
        refine_models = unique_models(args.model, args.supplement_model, *PRIMARY_FALLBACK_MODELS)

        write_progress(out_dir, "media_prep", "正在做媒体预处理")
        media_probe = run_media_probe(video, media_probe_path)

        write_progress(out_dir, "gemini_analysis", "正在运行 Gemini 主分析链")
        primary_result, primary_raw, primary_model_used = run_video_json_prompt_with_fallback(
            video, key, primary_models, PRIMARY_PROMPT, "primary analysis"
        )
        primary_result = normalize_script_payload(primary_result, args.source_path)
        primary_result["primary_model_used"] = primary_model_used
        primary_json_path.write_text(json.dumps(primary_result, ensure_ascii=False, indent=2), encoding="utf-8")
        primary_raw_path.write_text(json.dumps(primary_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        audio_multiview_result: dict = {}
        audio_multiview_model_used = ""
        should_audio_multiview, audio_multiview_reason = should_run_audio_multiview(primary_result, video)
        primary_result["audio_multiview_decision"] = {
            "enabled": should_audio_multiview,
            "reason": audio_multiview_reason,
        }

        metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else {}
        type_router = build_type_router(primary_result, audio_multiview_result, metadata, args.source_path)
        type_router_path.write_text(json.dumps(type_router, ensure_ascii=False, indent=2), encoding="utf-8")

        write_progress(out_dir, "v2_analysis", "正在运行 v2 本地分析链")
        v2_local_payload, v2_local_raw, v2_local_model_used = run_video_json_prompt_with_fallback(
            video,
            key,
            supplement_models,
            V2_LOCAL_PROMPT + "\n\nmedia_probe:\n" + json.dumps(media_probe, ensure_ascii=False) + "\n\ntype_router:\n" + json.dumps(type_router, ensure_ascii=False),
            "v2 local analysis",
        )
        v2_local_result = normalize_script_payload(v2_local_payload, args.source_path)
        v2_local_result["v2_local_model_used"] = v2_local_model_used
        v2_local_json_path.write_text(json.dumps(v2_local_result, ensure_ascii=False, indent=2), encoding="utf-8")
        v2_local_raw_path.write_text(json.dumps(v2_local_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        write_progress(out_dir, "consistency_audit", "正在做 Gemini 与 v2 的一致性审查")
        comparison_report, comparison_raw, comparison_model_used = run_text_json_prompt_with_fallback(
            {
                "source_metadata": metadata,
                "media_probe": media_probe,
                "gemini_result": primary_result,
                "v2_result": v2_local_result,
            },
            key,
            supplement_models,
            COMPARISON_PROMPT,
            "comparison report",
        )
        comparison_report = harden_comparison_report(primary_result, v2_local_result, comparison_report)
        comparison_report["comparison_model_used"] = comparison_model_used
        comparison_report_path.write_text(json.dumps(comparison_report, ensure_ascii=False, indent=2), encoding="utf-8")
        comparison_raw_path.write_text(json.dumps(comparison_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        logic_audit, logic_audit_raw, logic_audit_model_used = run_text_json_prompt_with_fallback(
            {
                "source_metadata": metadata,
                "gemini_result": primary_result,
                "v2_result": v2_local_result,
                "comparison_report": comparison_report,
            },
            key,
            supplement_models,
            LOGIC_AUDIT_PROMPT,
            "logic audit",
        )
        logic_audit = harden_logic_audit(primary_result, v2_local_result, logic_audit, comparison_report)
        logic_audit["logic_audit_model_used"] = logic_audit_model_used
        logic_audit_path.write_text(json.dumps(logic_audit, ensure_ascii=False, indent=2), encoding="utf-8")
        logic_audit_raw_path.write_text(json.dumps(logic_audit_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        memory_entries = load_case_memory(case_memory_path)
        similar_cases = find_similar_cases(memory_entries, type_router, primary_result)

        supplement_result: dict = {"windows": []}
        supplement_model_used = ""
        if should_audio_multiview:
            write_progress(out_dir, "targeted_recheck", "正在做目标复核与说话人校验")
            try:
                audio_multiview_result, audio_multiview_raw, audio_multiview_model_used = run_video_json_prompt_with_fallback(
                    video, key, supplement_models, AUDIO_MULTIVIEW_PROMPT, "audio multiview"
                )
                audio_multiview_result.setdefault("source_url", args.source_path)
                audio_multiview_result["model_used"] = audio_multiview_model_used
                audio_multiview_result["decision_reason"] = audio_multiview_reason
                audio_multiview_path.write_text(json.dumps(audio_multiview_result, ensure_ascii=False, indent=2), encoding="utf-8")
                audio_multiview_raw_path.write_text(json.dumps(audio_multiview_raw, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as exc:
                audio_multiview_result = {
                    "skipped": True,
                    "fallback_mode": "primary-only",
                    "reason": f"audio_multiview 降级跳过：{exc}",
                    "decision_reason": audio_multiview_reason,
                    "source_url": args.source_path,
                }
                audio_multiview_path.write_text(json.dumps(audio_multiview_result, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            audio_multiview_result = {
                "skipped": True,
                "fallback_mode": "primary-only",
                "reason": audio_multiview_reason,
                "source_url": args.source_path,
            }
            audio_multiview_path.write_text(json.dumps(audio_multiview_result, ensure_ascii=False, indent=2), encoding="utf-8")

        windows = list(primary_result.get("needs_evidence_enrichment") or [])
        windows.extend(infer_object_review_windows(primary_result))
        windows = merge_candidate_windows(
            windows,
            comparison_windows(comparison_report),
            comparison_windows({"focus_windows": v2_local_result.get("must_verify_windows") or []}),
        )
        if windows:
            write_progress(out_dir, "targeted_recheck", "正在补充关键证据")
            supplement_prompt = SUPPLEMENT_PROMPT + "\n需要重点检查的窗口如下：\n" + json.dumps(windows, ensure_ascii=False)
            supplement_result, supplement_raw, supplement_model_used = run_video_json_prompt_with_fallback(
                video, key, supplement_models, supplement_prompt, "supplement evidence"
            )
            supplement_result["supplement_model_used"] = supplement_model_used
            supplement_json_path.write_text(json.dumps(supplement_result, ensure_ascii=False, indent=2), encoding="utf-8")
            supplement_raw_path.write_text(json.dumps(supplement_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        conflict_recheck: dict = {"skipped": True, "reason": ""}
        conflict_recheck_model_used = ""
        should_recheck, recheck_reason = should_run_conflict_recheck(comparison_report, logic_audit)
        if should_recheck:
            write_progress(out_dir, "targeted_recheck", "正在复核 Gemini 与 v2 的冲突点")
            conflict_prompt = (
                CONFLICT_RECHECK_PROMPT
                + "\n\ncomparison_report:\n"
                + json.dumps(comparison_report, ensure_ascii=False)
                + "\n\nlogic_audit:\n"
                + json.dumps(logic_audit, ensure_ascii=False)
            )
            conflict_recheck, conflict_recheck_raw, conflict_recheck_model_used = run_video_json_prompt_with_fallback(
                video, key, supplement_models, conflict_prompt, "conflict recheck"
            )
            conflict_recheck["conflict_recheck_model_used"] = conflict_recheck_model_used
            conflict_recheck_path.write_text(json.dumps(conflict_recheck, ensure_ascii=False, indent=2), encoding="utf-8")
            conflict_recheck_raw_path.write_text(json.dumps(conflict_recheck_raw, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            conflict_recheck = {"skipped": True, "reason": recheck_reason}
            conflict_recheck_path.write_text(json.dumps(conflict_recheck, ensure_ascii=False, indent=2), encoding="utf-8")

        write_progress(out_dir, "arbitration", "正在仲裁 Gemini 与 v2 的差异")
        arbitration_result, arbitration_raw, arbitration_model_used = run_text_json_prompt_with_fallback(
            {
                "source_metadata": metadata,
                "gemini_result": primary_result,
                "v2_result": v2_local_result,
                "comparison_report": comparison_report,
                "logic_audit": logic_audit,
                "conflict_recheck": conflict_recheck,
            },
            key,
            refine_models,
            ARBITRATION_PROMPT,
            "arbitration",
        )
        arbitration_result["arbitration_model_used"] = arbitration_model_used
        arbitration_path.write_text(json.dumps(arbitration_result, ensure_ascii=False, indent=2), encoding="utf-8")
        arbitration_raw_path.write_text(json.dumps(arbitration_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        write_progress(out_dir, "final_output", "正在整理最终脚本并生成输出")
        final_result, final_raw, refine_model_used = run_text_json_prompt_with_fallback(
            {
                "source_metadata": json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else {},
                "primary_draft": primary_result,
                "gemini_primary_draft": primary_result,
                "v2_local_result": v2_local_result,
                "type_router": type_router,
                "similar_cases": similar_cases,
                "audio_multiview": audio_multiview_result,
                "supplement": supplement_result,
                "comparison_report": comparison_report,
                "logic_audit": logic_audit,
                "arbitration_result": arbitration_result,
                "conflict_recheck": conflict_recheck,
            },
            key,
            refine_models,
            REFINE_PROMPT,
            "final refine",
        )
        final_result = normalize_script_payload(final_result, args.source_path)
        final_result["primary_model_used"] = primary_model_used
        final_result["v2_local_model_used"] = v2_local_model_used
        final_result["supplement_model_used"] = supplement_model_used
        final_result["audio_multiview_model_used"] = audio_multiview_model_used
        final_result["comparison_model_used"] = comparison_model_used
        final_result["logic_audit_model_used"] = logic_audit_model_used
        final_result["conflict_recheck_model_used"] = conflict_recheck_model_used
        final_result["arbitration_model_used"] = arbitration_model_used
        final_result["refine_model_used"] = refine_model_used
        final_result["type_router"] = type_router
        final_result["similar_cases_used"] = similar_cases
        final_result["comparison_report"] = comparison_report
        final_result["logic_audit"] = logic_audit
        final_result["arbitration_result"] = arbitration_result
        final_result = enforce_object_reviews(final_result, supplement_result)
        final_result = enforce_chinese_dialogue_translation(final_result, key, refine_models)
        maybe_extract_frames(video, out_dir, final_result)
        final_json_path.write_text(json.dumps(final_result, ensure_ascii=False, indent=2), encoding="utf-8")
        refine_raw_path.write_text(json.dumps(final_raw, ensure_ascii=False, indent=2), encoding="utf-8")

        case_memory_entry = build_case_memory_entry(args.source_path, type_router, final_result, primary_result)
        case_memory_entry_path.write_text(json.dumps(case_memory_entry, ensure_ascii=False, indent=2), encoding="utf-8")
        memory_entries.append(case_memory_entry)
        save_case_memory(case_memory_path, memory_entries[-200:])

        run_step("render_script_table", [sys.executable, str(v2_render_script), str(final_json_path), "--output", str(final_html_path)])

        write_progress(out_dir, "completed", "分析完成")
        payload = {
            "out_dir": str(out_dir),
            "html": str(final_html_path),
            "json": str(final_json_path),
            "model": args.model,
            "supplement_model": args.supplement_model,
            "primary_model_used": primary_model_used,
            "supplement_model_used": supplement_model_used,
            "audio_multiview_model_used": audio_multiview_model_used,
            "v2_local_model_used": v2_local_model_used,
            "comparison_model_used": comparison_model_used,
            "logic_audit_model_used": logic_audit_model_used,
            "conflict_recheck_model_used": conflict_recheck_model_used,
            "arbitration_model_used": arbitration_model_used,
            "refine_model_used": refine_model_used,
            "type_router": str(type_router_path),
            "media_probe": str(media_probe_path),
            "v2_local_result": str(v2_local_json_path),
            "comparison_report": str(comparison_report_path),
            "logic_audit": str(logic_audit_path),
            "conflict_recheck": str(conflict_recheck_path),
            "arbitration_result": str(arbitration_path),
            "case_memory_entry": str(case_memory_entry_path),
            "source_metadata": str(metadata_path),
        }
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
