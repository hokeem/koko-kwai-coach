# HTML output schema

The final `script_table.html` must be standalone and viewable without external network access.

## Style target

The required public format is the uploaded light-card template, not the old dark v2 table.

Visual standard:

- body background: `#f6f7fb`
- centered `.wrap` with max width around `1680px`
- white `.card` blocks with rounded corners and light shadow
- section order:
  1. `视频总结归纳 + 脚本表`
  2. `视频整体内容总结`
  3. `核心爆点`
  4. `可替换部分`
  5. `脚本表`
- `核心爆点` and `可替换部分` use `.insight-grid` / `.insight` cards
- the main script table is a five-column table

## Public/private structure

V3 is an observation-first pipeline with local story/mechanism synthesis.

Important distinction:

- Public output: concise Chinese script report in the uploaded template.
- Process artifacts: story chaining, skepticism, `allowed_claims`, `blocked_claims`, object conflicts, mechanism hypotheses, `verification_windows`, and raw Gemini observations.
- Process artifacts may appear only in collapsed appendices or JSON, never as the main report.

## Required public sections

### 1. Header card

Title must be:

```text
视频总结归纳 + 脚本表
```

Metadata line should include route/status and source video link.

### 2. 视频整体内容总结

A concise Chinese synthesis of the video.

Rules:

- It may include likely story interpretation if supported by observations and verification.
- It must preserve uncertainty for hidden mechanisms.
- It must not assert hidden actions, intent, or object structure as fact without evidence.

### 3. 核心爆点

Use 2–4 insight cards. Each card has:

- short title
- concise explanation

For 整蛊/魔术/关系博弈 videos, cards should usually include some of:

- 利益诱饵 / 贪念触发
- 道具机关
- 受害者误判
- 反转证据
- 关系博弈

### 4. 可替换部分

Use 3–5 insight cards. Each card has:

- replaceable element title
- examples or substitution logic

Typical replaceable elements:

- 诱饵：钱、手机、礼物、红包、优惠、挑战奖励
- 机关：露底瓶、双层杯、假盖、掉包道具、可擦标记、隐藏口袋
- 关系：情侣、夫妻、朋友、路人、老板员工
- 场景：厨房、街头、柜台、车内、办公室
- 反转证据：钱掉出、标记出现、道具暴露、承诺无法反悔

### 5. 脚本表

Main public table columns must be exactly:

| 视频链接 | 时间 | 画面内容 | 动作 | 关键对白/旁白（中文忠实翻译） |
|---|---|---|---|---|

Column mapping:

- `视频链接`: source link, usually only first row needs `原视频链接`.
- `时间`: segment start-end.
- `画面内容`: objective visible scene plus embedded reference frames when available.
- `动作`: local integrated action summary; do not paste raw per-second Gemini text.
- `关键对白/旁白（中文忠实翻译）`: faithful Chinese translation/transcription when audio is available. If not clear, write: `无明确对白/旁白，主要靠画面动作推进。`

## Collapsed appendices

Allowed but folded by default:

### 内部机制假设与复核窗口

Show `story_analysis.mechanism_hypotheses` and `story_analysis.verification_windows`.

### 内部审计摘要

| # | 时间段 | 状态 | 不确定点 | 质疑/复核 | 禁止或降级的说法 |
|---|---|---|---|---|---|

### 逐秒客观观察明细

| 时间 | 画面 | 人物动作 | 道具 | 音频 | 不确定点 |
|---|---|---|---|---|---|

## Mechanism reasoning display rules

For prank/整蛊/magic/prop videos:

- The public summary can say “大概率是……”, “疑似通过……”, or “从可见证据看……”.
- Only say “确认” when the action/property/result is visible or supported by audio/text.
- If the core mechanism is not fully verified, show the strongest safe version in the public report and leave the hypothesis details in collapsed appendix.

## Non-negotiable rules

- Do not display unsupported claims as facts.
- If object identity changes abruptly, downgrade labels to `纸状物/纸币状物/无法确认` and create a verification window.
- If an action occurs under occlusion, do not claim the transfer/result unless audio/text or later visible state supports it.
- If a story is not logically continuous, mark `logic_quality=suspicious/unresolved` in JSON and use cautious language in HTML.
- Do not make `suspicion_notes`, `allowed_claims`, `blocked_claims`, mechanism hypotheses, or raw observations the main public table.
- Do not assert `露底/没底/掉出/某人拿走钱` unless the relevant frames or audio support it.
