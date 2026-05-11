# Output Contract

## Purpose

Define the fixed final deliverable for `video-analysis-v2-skill`. This file is the single source of truth for `script_table.json`, `script_table.html`, and the user-facing table layout.

The HTML format is based on the provided sample file `script_table (15).html`.

## Required Files

Every completed analysis must produce:

- `script_table.html`
- `script_table.json`
- `selected_frames/`
- `selected_frames_end/`

Optional sidecar file when the user asks for richer audio structure:

- `audio_multiview.json`

The HTML must embed frame images as directly visible images. Prefer base64 `data:image/jpeg;base64,...` URIs in the HTML so the file can be shared without broken local image paths.

## JSON Shape

Use this shape for `script_table.json`:

```json
{
  "title": "视频总结归纳 + 脚本表",
  "route": "audio-sop",
  "audio_information_score": "9/10",
  "source_url": "https://example.com/video",
  "whole_video_summary": "一到两句话总结整条视频。",
  "core_viral_points": [
    {
      "label": "极致的对比与讽刺",
      "text": "这条视频最容易被转发、评论或记住的冲突机制。"
    }
  ],
  "replaceable_parts": [
    {
      "label": "求助事项",
      "text": "可替换为搬家、修电脑、辅导孩子功课等同结构元素。"
    }
  ],
  "rows": [
    {
      "source_url": "https://example.com/video",
      "time": "00:00-00:15",
      "visual_content": "这一段整体看到了什么。",
      "action": "这一段发生了什么变化或动作推进。",
      "dialogue_or_audio": "男：...\n女：...",
      "start_frame": "selected_frames/segment_001.jpg",
      "end_frame": "selected_frames_end/segment_001.jpg"
    }
  ],
  "mechanism": {
    "title": "包袱机制",
    "items": [
      {"label": "铺垫", "text": "..."},
      {"label": "违和点", "text": "..."},
      {"label": "反转点", "text": "..."},
      {"label": "笑点落点", "text": "..."}
    ]
  }
}
```

`core_viral_points` and `replaceable_parts` are required. If a video is weak, still write the best available hypothesis and mark uncertainty in the text.

For non-comedy videos, omit `mechanism` unless the video has a clear twist, reveal, payoff, emotional reversal, persuasion mechanism, or before-after structure worth explaining.

## HTML Layout

The final HTML must contain these cards in order:

1. Title/meta card
   - H1: `视频总结归纳 + 脚本表`
   - Meta line: `Route: {route} · Audio information score: {score}`
   - Source link: `视频链接：{source_url}`
2. Summary card
   - H2: `视频整体内容总结`
   - One concise paragraph in Chinese
3. Core viral points card
   - H2: `核心爆点`
   - Short numbered or bulleted items
   - Explain why the video works, not only what happens
4. Replaceable parts card
   - H2: `可替换部分`
   - Short items that describe reusable variables for adaptation
5. Script table card
   - H2: `脚本表`
   - Table with exactly five user-facing columns
6. Optional mechanism card
   - H2: `包袱机制` or another precise mechanism title
   - Short bullet list with bold labels

## Core Viral Points

`核心爆点` explains why the video is compelling, repeatable, or likely to spread. It is not a plot summary.

Always inspect these dimensions:

- `contrast`: strong contrast, irony, mismatch, identity reversal, before-after gap
- `reversal`: whether the audience's first interpretation is overturned
- `escalation`: whether the same conflict repeats in a three-step or progressive structure
- `relatability`: whether the scene matches a common relationship, family, workplace, or platform experience
- `payoff`: whether a final line, prop, action, expression, or visual reveal completes the joke or persuasion
- `emotional_trigger`: embarrassment, superiority, recognition, frustration, surprise, envy, relief, desire to imitate

Writing rules:

- Use concrete labels, such as `身份转换`, `三段式递进`, `极致对比`, `日常代入感`, `结尾明牌`.
- Explain the mechanism behind the point.
- Tie each point to evidence in the video: dialogue, action, visual reveal, relationship, or structure.
- Avoid generic claims like `内容有趣` or `节奏很好` unless the specific reason is stated.

Good examples:

- `极致的对比与讽刺`: 平时吃喝玩乐都在，一到关键求助全都消失，形成强烈反差。
- `身份转换`: 开头让观众以为女友是施压者，反转后发现她才是被遗忘在商场的受害者。

## Replaceable Parts

`可替换部分` identifies which elements can be swapped while preserving the same core theme and viral mechanism. This section is for adaptation, remixing, and producing new scripts with the same content engine.

Always inspect these dimensions:

- `scenario`: where the same conflict can happen
- `request_or_task`: what concrete request, mission, favor, or trigger starts the story
- `relationship`: which relationship can carry the same tension
- `excuse_or_obstacle`: what refusal, misunderstanding, or blocking reason can replace the original
- `symbolic_action_or_prop`: what action, object, facial expression, line, or visual reveal carries the payoff
- `ending_variant`: how the ending can change while preserving the same mechanism

Writing rules:

- Do not propose random replacements. Each replacement must preserve the original video's core conflict and payoff logic.
- Group replacements by element type, such as `场景`, `人物关系`, `拒绝借口`, `结局`, `标志动作`.
- Prefer practical examples that can become new short-video scripts.
- If a replacement would change the core mechanism, do not include it.

Good examples:

- `求助事项`: 原片是修整院子，可替换为搬家、辅导孩子功课、修电脑、借钱。
- `场景`: 原片是商场遗忘女友，可替换为买菜后男生自己开车走、Party 后独自打车回家、加油站女生下车买水时男生直接开走。

## Script Table Columns

Use exactly these five columns in the final user-facing table:

| 视频链接 | 时间 | 画面内容 | 动作 | 关键对白/旁白（中文忠实翻译） |
|---|---|---|---|---|

Column rules:

- `视频链接`: show a clickable `原视频链接` in every row.
- `时间`: use `MM:SS-MM:SS` or `HH:MM:SS-HH:MM:SS` consistently.
- `画面内容`: describe what is visually present across the segment. Include embedded start/end frames inside this cell when available.
- `动作`: describe what changes, progresses, is revealed, or is completed during the segment.
- `关键对白/旁白（中文忠实翻译）`: provide faithful Chinese translation of spoken content. Do not summarize or add interpretation here.

## Dialogue Rules

- Preserve the original meaning.
- Translate into faithful Chinese when the source audio is not Chinese.
- Keep line breaks for multiple utterances.
- Mark speaker/source when identifiable: `男：`, `女：`, `旁白：`, `人物A：`, `人物B：`.
- Use neutral labels when uncertain. Do not guess identities from appearance alone.
- If there is no usable audio, write a compact status such as `无有效音频信息` or `BGM only`.

## Frame Display Rules

When frames are included in a row:

- Place frames under the visual-content text.
- Use this structure:
  - `<div class="frames">`
  - two `.frame` blocks when both start and end exist
  - each `.frame` contains an image and a `.cap`
- Captions should be `首帧` and `尾帧`.
- Images must be visible in the HTML without manually opening local paths.

## Visual Style

Match the provided sample:

- `lang="zh-CN"`
- title: `视频总结归纳 + 脚本表`
- light grey page background `#f6f7fb`
- centered `.wrap` with `max-width:1680px`
- white `.card` blocks
- table uses fixed layout and five column widths:
  - 14%, 8%, 34%, 18%, 26%
- frame images have rounded corners and light borders

Use `assets/script-table-template.html` as the rendering template.
