---
name: video-analysis-v2-skill
description: Analyze short-video URLs or local short-video files from Xiaohongshu, Kwai, TikTok, Reels, or similar platforms. Use when the user wants segmented video breakdown, short-video script analysis, audio/voiceover interpretation, keyframe-aligned visual analysis, comedy/payoff structure analysis, or a directly shareable HTML script table with embedded frames.
---

# Video Analysis V2

Use this skill to turn a short video into a directly viewable HTML script table with a concise whole-video summary, core viral hooks, replaceable adaptation elements, route metadata, segment rows, embedded start/end frames, and an optional mechanism-analysis section.

When the user asks for richer audio understanding, this skill can also produce a structured multi-speaker sidecar analysis that separates transcript text, speaker ownership, source-type judgement, and relationship hypotheses instead of treating the whole soundtrack as one blended block.

## Operating Contract

- Work from the downloaded local video or the user-provided local file, not from the link preview, title, or platform description alone.
- Do not begin with uniform frame sampling. Select frames around route-level segments.
- Decide first whether the audio contains usable information; route to `audio-sop` or `keyframe-sop`.
- Keep the final user-facing output aligned with `references/output-contract.md`.
- Always write `核心爆点` and `可替换部分`; these are first-class final outputs, not optional notes.
- When the user asks to "直接给我看", "发到群里", or receive the result, return `script_table.html` in the current chat instead of only giving local paths.

## Workflow

1. Prepare media:
   - Run `scripts/check_dependencies.py` when the environment is new or uncertain.
   - Download URL inputs with `scripts/download_video.py`, or use the local video file directly.
   - Probe media with `scripts/probe_media.py`.
2. Select route:
   - Read `references/route-selection.md`.
   - If usable audio exists, read `references/audio-analysis.md`.
   - If the user asks for speaker ownership, gender guess, or relationship inference, also read `references/audio-multiview-analysis.md`.
   - If usable audio does not exist, read `references/visual-segmentation.md`.
3. Align frames:
   - Extract candidate frames with `scripts/extract_frames.py`.
   - Analyze frames using `references/frame-analysis-guide.md`.
   - Check segment consistency using `references/multimodal-consistency.md`.
4. Render final deliverables:
   - Build `script_table.json` using `references/output-contract.md`.
   - When requested, build `audio_multiview.json` using `references/audio-multiview-analysis.md` and `scripts/build_audio_multiview.py`.
   - Write `core_viral_points` and `replaceable_parts` using `references/viral-and-replacement-analysis.md`.
   - Render `script_table.html` with `scripts/render_script_table.py`.
   - Validate outputs with `scripts/validate_outputs.py`.

## Required Outputs

Produce these files in the working output directory:

- `script_table.html`
- `script_table.json`
- `selected_frames/`
- `selected_frames_end/`

When the request explicitly asks for speaker-level or relationship-level audio analysis, also produce:

- `audio_multiview.json`

The HTML must follow the sample-backed layout in `assets/script-table-template.html`:

- title card: `视频总结归纳 + 脚本表`
- route and audio score metadata
- source video link
- `视频整体内容总结`
- `核心爆点`
- `可替换部分`
- `脚本表`
- optional `包袱机制` card when the video contains a joke, twist, reveal, payoff, or scripted conflict

## References

- `references/output-contract.md`: final JSON schema, HTML layout, table columns, and sample-backed formatting rules
- `references/route-selection.md`: audio-information judgement and route selection
- `references/audio-analysis.md`: audio-led segmentation and transcript handling
- `references/audio-multiview-analysis.md`: speaker attribution, source profiling, gender guess, and relationship-hypothesis workflow
- `references/visual-segmentation.md`: keyframe-led visual segmentation
- `references/frame-analysis-guide.md`: frame-level and start/end frame analysis
- `references/multimodal-consistency.md`: evidence-chain and re-sampling rules
- `references/viral-and-replacement-analysis.md`: how to write required `核心爆点` and `可替换部分`
- `references/evolution-rules.md`: confirmed reusable SOP-level improvements
- `references/special-video-patterns.md`: confirmed reusable special video patterns

## Evolution Rule

Only run a self-evolution pass after the agent has completed an analysis and the user provides a better human reference, correction, or comparison case. Summarize reusable learnings first. Edit `references/evolution-rules.md` or `references/special-video-patterns.md` only when the user explicitly asks to retain the learning.
