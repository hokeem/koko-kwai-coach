# Multimodal Consistency

## Purpose

Integrate segment evidence into stable explanations and prevent over-reading mismatched audio/visual evidence.

Use this file after frame analysis in both routes.

## Evidence Chain

For each segment, build an ordered chain:

```text
start state -> audio event or audio status -> end state
```

Then form one segment hypothesis:

- what the segment is doing
- what changes or progresses
- what the audience learns or feels
- what role the segment plays in the whole video

## Consistency Classification

Classify each segment:

- `consistent`: start frame, audio/status, and end frame support the same interpretation
- `weakly_consistent`: evidence mostly supports the interpretation but some detail is missing
- `inconsistent`: selected evidence points to different moments, subjects, actions, or meanings

## Re-Sampling Rules

If inconsistent, do targeted re-sampling before final writing.

Priority:

1. If the current end frame does not support the audio or segment hypothesis, sample additional frames before the current end.
2. If the action transition is missing, add one middle frame.
3. If the start state is unclear, adjust start-side sampling.
4. If the information carrier is text, UI, product label, or subtitle, sample the frame where that carrier is clearest.

Do not force an explanation when ordered evidence is inconsistent.

## Common Inconsistency Signals

- audio describes people, actions, or objects not supported by selected frames
- start and end frames appear to belong to different narrative moments with no visible transition
- audio implies a result or reaction that the visual evidence does not show
- selected frames miss the actual information carrier, such as subtitle, label, product detail, popup, or result screen
