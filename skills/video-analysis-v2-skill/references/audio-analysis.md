# Audio Analysis

## Purpose

Use this reference only when `references/route-selection.md` selects `audio-sop`.

In this route, audio is the main segmentation backbone. Visual frames support and verify the audio-led structure.

If the user asks for speaker-level ownership, gender guess, or relationship inference, pair this file with `references/audio-multiview-analysis.md` so the transcript layer and speaker-inference layer remain separate.

## Whole-Audio Hypothesis

Before segmentation, build a reversible whole-audio hypothesis:

- main hypothesis
- alternate hypothesis
- uncertainties

Judge:

- audio source profile: speech, voiceover, dialogue, event sound, imitation, mixed field
- overall audio form: monologue, dialogue, interview, commentary, misleading imitation, mixed structure
- stable source count: one speaker, two speakers, multi-source, one speaker plus imitation

Revise this hypothesis if later evidence contradicts it.

## Timestamped Segmentation

Convert audio into a structured timeline.

Segmentation rules:

- Voiceover or monologue: use one complete sentence or one complete information unit.
- Dialogue: use one complete exchange round when possible.
- Important non-speech audio: create standalone segments when it changes interpretation.

Each segment should record:

- `segment_id`
- `start`
- `end`
- `duration`
- `segment_type`
- `speaker_or_source`
- `text`
- `summary`
- `confidence`
- `hypothesis_note`

When multi-speaker analysis is requested, also preserve utterance-level records for later merging into `audio_multiview.json`.

## Transcript Handling

For the final table:

- Use the original transcript content as the source.
- Present faithful Chinese translation in `关键对白/旁白（中文忠实翻译）`.
- Do not summarize in the dialogue field.
- Mark speaker/source when possible.
- Use neutral speaker labels when uncertain.
- Do not silently upgrade uncertain speaker guesses into facts. Keep `人物A`, `人物B`, or `重叠人声` when the evidence is weak.

## Frame Sampling for Audio Segments

For each audio segment, extract candidate frames around:

- `start`
- `start + 0.3s`
- `start + 0.8s`
- `end - 0.2s`

Select one representative start frame and one representative end frame. Add a meaningful middle frame only if the state change would otherwise be missed.
