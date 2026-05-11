# Gemini video prompts

## Primary prompt: objective observations only

Ask Gemini to act as an objective video annotator, not a story analyst. It must return strict JSON only.

```text
你不是剧情分析师，你是视频客观标注器。
请不要解释剧情，不要判断人物动机，不要总结笑点，不要推断未直接可见的行为。

所有字段值必须使用简体中文输出，不要使用英文描述。

请按每 1 秒输出一条 observation。每条只包含客观可见/可听信息：
1. time：时间戳，格式 00:00
2. visual_scene：这一秒画面环境/场景，不要解释剧情
3. people：每个人的位置、外观、正在做的可见动作
4. objects：画面中可见道具及其大概位置
5. visible_text：画面文字，没有则写“无”
6. audio：这一秒可听到的声音/对白/音乐；听不清就写“未能确认”
7. uncertainty：哪些事情不能确认

禁止使用这些解释性词语或同义表达：
- 假装
- 偷偷
- 骗
- 发现
- 以为
- 反转
- 搞笑
- 目的
- 动机
- 因为
- 所以
- 被恶搞
- 假钞（除非画面文字/音频明确说明，或纸币外观清楚显示为假钞）

如果物体身份不确定，请写“纸状物/纸币状物/无法确认”，不要强行命名。
如果动作发生在遮挡区域，请明确写“被遮挡，无法确认”。

输出严格 JSON：
{
  "mode": "objective_observation",
  "observation_interval_sec": 1,
  "duration_estimate": "视频时长估计",
  "observations": [
    {
      "time": "00:00",
      "visual_scene": "客观画面，必须中文",
      "people": [
        {
          "id": "人物1",
          "position": "画面位置，必须中文",
          "appearance": "外观衣着，必须中文",
          "visible_action": "可见动作，必须中文，不要推断"
        }
      ],
      "objects": [
        {"label": "物体名或不确定标签", "position": "位置", "state": "可见状态"}
      ],
      "visible_text": "无/可见文字",
      "audio": "背景音乐/对白/未能确认",
      "uncertainty": "无法确认的事项"
    }
  ]
}
```

## Local synthesis principles

The agent/local pipeline, not Gemini, performs story synthesis after observations are returned.

Rules:

1. Apply `universal-story-framework.md` to every video.
2. Load a subtype template when signals match, such as relationship comedy or prank/prop mechanism.
3. Story must be logically continuous.
4. No object/action may appear in the final script unless supported by observation, audio, subtitle, or verified frames.
5. If object identity changes abruptly, mark suspicion and downgrade the label unless reviewed frames support a specific identity.
6. If an action has no visible causal chain, block the strong claim.
7. If a conclusion requires mental state or interpretation, allow it only as `likely_interpretation` unless visible text/audio/state strongly supports it.

## Audited output schema

`synthesize_observations.py` should produce:

```json
{
  "mode": "observation_first_audited",
  "whole_video_summary": "安全最终故事",
  "story_analysis": {
    "genre_guess": "类型判断",
    "confirmed_facts": [],
    "mechanism_hypotheses": [],
    "verification_windows": [],
    "safe_final_story": "",
    "core_points": [],
    "replaceable_parts": [],
    "must_not_claim_without_verification": []
  },
  "logic_quality": "consistent/suspicious/unresolved",
  "synthesized_segments": [
    {
      "start": "00:00",
      "end": "00:08",
      "segment_role": "建立场景/关键动作/转场/结果状态",
      "objective_visual": "观察支持的画面事实",
      "integrated_summary": "本地整合后的动作/故事段落",
      "action_chain": [],
      "object_tracks": [],
      "audio_lines": [],
      "uncertainty": "无法确认的点",
      "suspicion_notes": [],
      "allowed_claims": [],
      "blocked_claims": []
    }
  ],
  "observations": []
}
```

## HTML display contract

Final HTML must use `html-output-schema.md`:

| 视频链接 | 时间 | 画面内容 | 动作 | 关键对白/旁白（中文忠实翻译） |
|---|---|---|---|---|

Raw observations and audit details belong in collapsed appendices or JSON, not the main table.
