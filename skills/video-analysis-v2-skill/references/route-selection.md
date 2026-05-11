# Route Selection

## Purpose

Choose one analysis route before segmentation:

- `audio-sop`: audio contains usable information and should define the segment backbone
- `keyframe-sop`: audio is absent, unusable, or not structurally helpful; visual changes define the segment backbone

## Required Inputs

Use media metadata from `scripts/probe_media.py`:

- duration
- resolution
- video codec
- audio stream presence
- audio codec
- audio stream count

If there is no audio stream, choose `keyframe-sop`.

## Audio Information Judgement

Ask only this question:

> Does the audio contain enough usable information to help explain what the video is saying, doing, or structuring?

Use one compact score:

- `audio_information_score: 0-10`

Threshold:

- `>= 6`: `audio_information_exists: yes`, select `audio-sop`
- `< 6`: `audio_information_exists: no`, select `keyframe-sop`

Directly choose `keyframe-sop` without scoring when:

- no audio stream
- full-video BGM only
- weak environment noise with no explanatory value
- severe distortion, masking, or clipping makes audio unusable
- audio exists but does not help explain the video content

## Required Decision Block

Record this internally and carry it into `script_table.json`:

```yaml
audio_information_score: x/10
audio_information_exists: yes | no
reason: one short sentence
selected_route: audio-sop | keyframe-sop
```

The HTML title/meta card must show the selected route and score.
