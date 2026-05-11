# Visual Segmentation

## Purpose

Use this reference when `references/route-selection.md` selects `keyframe-sop`.

In this route, visual structure is the segmentation backbone. Do not pretend weak audio is analytically useful.

## Whole-Video Visual Scan

First judge what primarily carries meaning:

- action demonstration
- subtitle or text-card explanation
- product display
- reaction or expression
- visual rhythm or editing rhythm
- before-after comparison
- screen recording or UI operation
- joke reveal or payoff

## Segment by Visual Change

Segment by:

- scene change
- action change
- camera or shot change
- subtitle/text-card change
- object state change
- product state change
- screen-state or UI change
- reveal/payoff boundary

Each segment should record:

- `segment_id`
- `start`
- `end`
- `keyframe_start`
- `keyframe_end`
- `visual_structure_role`

Recommended `visual_structure_role` values:

- `hook`
- `setup`
- `demo`
- `comparison`
- `reaction`
- `proof`
- `reveal`
- `payoff`
- `CTA`
- `ending`

## Audio Handling

Do not produce transcript-like output when the audio has no structural value.

Use one compact status when useful:

- `无有效音频信息`
- `BGM only`
- `audio not used for structural analysis`

## Frame Sampling

For each visual segment, keep:

- one chosen start frame
- one chosen end frame
- one meaningful middle frame only when needed for a critical action or state change
