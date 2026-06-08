# video-analysis-v2-skill

`video-analysis-v2-skill` is a Codex skill for analyzing short videos from Xiaohongshu, Kwai, TikTok, Reels, and similar platforms.

It turns a short-video URL or local video file into a directly shareable HTML deliverable:

- `script_table.html`
- `script_table.json`
- `selected_frames/`
- `selected_frames_end/`

When the request needs speaker-aware audio understanding, the skill can also produce:

- `audio_multiview.json`

The final HTML format is fixed by `references/output-contract.md` and `assets/script-table-template.html`.
In addition to the whole-video summary and script table, every final output now includes:

- `核心爆点`: the video's core viral hooks, contrast, reversal, relatability, payoff, or emotional trigger
- `可替换部分`: reusable variables that can be changed to create new scripts with the same core mechanism

## Core idea

The skill first decides whether the video has usable audio information:

- `audio-sop`: use audio/voiceover/dialogue as the segment backbone, then align start/end frames to each segment.
- `keyframe-sop`: when audio is absent, BGM-only, distorted, or not useful, use visual changes as the segment backbone.

For richer audio requests, `audio-sop` can be paired with a multi-speaker sidecar workflow that keeps these layers separate:

- transcript
- speaker ownership
- source type
- gender guess
- relationship hypotheses

The final table uses exactly four user-facing script columns:

| 时间 | 画面内容 | 动作 | 关键对白/旁白 |
|---|---|---|---|

## Skill structure

```text
video-analysis-v2-skill/
├── SKILL.md
├── agents/openai.yaml
├── assets/script-table-template.html
├── scripts/
│   ├── check_dependencies.py
│   ├── download_video.py
│   ├── probe_media.py
│   ├── extract_audio.py
│   ├── build_audio_multiview.py
│   ├── extract_frames.py
│   ├── render_script_table.py
│   └── validate_outputs.py
└── references/
    ├── output-contract.md
    ├── route-selection.md
    ├── audio-analysis.md
    ├── audio-multiview-analysis.md
    ├── visual-segmentation.md
    ├── frame-analysis-guide.md
    ├── multimodal-consistency.md
    ├── evolution-rules.md
    └── special-video-patterns.md
```

## Important references

- `SKILL.md`: lightweight execution entry and resource router
- `references/output-contract.md`: the final output schema and HTML layout contract
- `references/route-selection.md`: audio-information scoring and route selection
- `references/audio-analysis.md`: audio-led segmentation rules
- `references/audio-multiview-analysis.md`: speaker attribution, source profiling, and relationship-hypothesis rules
- `references/visual-segmentation.md`: keyframe-led segmentation rules
- `references/frame-analysis-guide.md`: frame observation and start/end comparison rules
- `references/multimodal-consistency.md`: evidence-chain checks and re-sampling rules
- `references/viral-and-replacement-analysis.md`: required analysis rules for `核心爆点` and `可替换部分`

## HTML output

The HTML layout follows the provided sample:

1. Title/meta card: `视频总结归纳 + 脚本表`
2. Whole-video summary card
3. Five-column script table
4. Optional `包袱机制` card for comedy, reveal, payoff, or twist videos

Images should be embedded directly in the HTML, preferably as base64 data URIs, so the file can be opened or shared without broken local image paths.

## Self-evolution

When the agent completes an analysis and the user later provides a better human reference or correction, the skill can extract reusable improvements.

Only retain:

- general SOP improvements in `references/evolution-rules.md`
- repeatable special video patterns in `references/special-video-patterns.md`

Do not retain one-off wording preferences or single-case story details.
