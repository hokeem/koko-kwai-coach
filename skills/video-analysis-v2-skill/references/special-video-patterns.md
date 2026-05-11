# Special Video Patterns

## Purpose

Store reusable pattern cards for special video types that require non-default interpretation or segmentation strategy.

Use this file when a human good example reveals that the video belongs to a recognizable special pattern.

---

## Pattern card format

For each pattern, record:
- `pattern_name`
- `video_type`
- `core_features`
- `recommended_strategy`
- `common_mistakes`
- `output_note`

Keep each card concise.

---

## Example shape

### Pattern: Time-jump domestic comedy
- `video_type`: couple / family short skits with visible clock or time-card jumps
- `core_features`:
  - repeated spoken threat or promise
  - behavior does not match the spoken claim
  - multiple time jumps across the same domestic setting
  - payoff or emotional reversal at the end
- `recommended_strategy`:
  - detect time-card jumps first
  - regroup segments by scene block before final whole-video synthesis
  - isolate the final payoff segment instead of merging it into the conflict body
- `common_mistakes`:
  - treating the whole video as one continuous real-time event
  - merging the final gift/payoff beat into the prior argument beat
- `output_note`:
  - the summary should preserve the conflict-to-reversal structure

---

## Admission rule

Add a pattern only when it describes a repeatable class of videos.
If the issue is a general workflow weakness instead of a video class, store it in `references/evolution-rules.md` instead.
