# Multi-Speaker Audio Analysis

## Purpose

Use this reference only when the user asks for more than a plain transcript, such as:

- how many speaking entities exist
- which utterance belongs to which speaker
- rough male or female voice guess
- human, animal, object, environment, or mixed-source judgement
- likely relationship hypotheses such as couple, siblings, friends, coworkers, parent-child

This layer is not a replacement for the normal `audio-sop` table. It is a sidecar analysis that sits above ASR and below final story interpretation.

## Core Principle

Never collapse these judgements into one blended paragraph.

Keep four layers separate:

1. transcript layer: what was said and when
2. speaker layer: who likely produced each utterance
3. source-profile layer: what kind of source each speaker or sound is
4. interaction layer: what relationship or scene dynamic is most likely

## Required Caution

All identity-like outputs here are hypotheses, not facts.

- `gender_guess` must be written as `male`, `female`, or `unknown`
- `relationship_hypotheses` must include confidence and evidence
- do not claim legal, medical, ethnic, or uniquely identifying attributes from voice alone
- do not force a relationship label when the evidence only supports `close peers` or `unclear`

## Audio Asset Preparation

Prepare two audio assets when possible:

- `audio.wav`: 16k mono WAV for ASR compatibility
- `audio_analysis.wav`: higher-fidelity analysis copy that preserves the original channel count when possible

If only one file is available, document the limitation in `analysis_limits`.

## Expected Inputs

The workflow can merge any subset of these inputs:

- transcript JSON from Whisper or faster-whisper
- optional diarization or speaker-turn output
- optional external speaker-profile guesses
- frame evidence from the video when voices must be grounded to visible people

## Output File

Write `audio_multiview.json`.

Recommended shape:

```json
{
  "source_url": "https://example.com/video",
  "audio_assets": {
    "asr_audio": "audio.wav",
    "analysis_audio": "audio_analysis.wav"
  },
  "language": {
    "value": "pt",
    "confidence": 0.94
  },
  "whole_audio_hypothesis": {
    "audio_source_profile": "dialogue",
    "overall_audio_form": "two-person domestic conflict",
    "stable_source_count": {
      "value": 2,
      "confidence": 0.68,
      "note": "speaker turns alternate but no diarization model was available"
    },
    "uncertainties": [
      "overlapping speech in the middle section"
    ]
  },
  "speakers": [
    {
      "speaker_id": "SPEAKER_00",
      "display_label": "人物A",
      "gender_guess": "female",
      "gender_confidence": 0.61,
      "source_type": "human_speech",
      "role_guess": "complaining partner",
      "voice_characteristics": [
        "higher pitch",
        "fast speech rate"
      ],
      "evidence": [
        "appears in frames while speaking at 00:03-00:07"
      ],
      "uncertainty_note": ""
    }
  ],
  "utterances": [
    {
      "utterance_id": "utt_001",
      "start": 0.0,
      "end": 2.4,
      "speaker_id": "SPEAKER_00",
      "speaker_label": "人物A",
      "text": "……",
      "source_type": "human_speech",
      "is_overlap": false,
      "confidence": 0.72,
      "evidence_note": "speaker-turn hypothesis from diarization merge"
    }
  ],
  "relationship_hypotheses": [
    {
      "label": "romantic couple",
      "confidence": 0.58,
      "evidence": [
        "mutual second-person address",
        "domestic setting",
        "conflict tone mirrors couple skits"
      ],
      "counter_evidence": [
        "no explicit kinship or spouse title in audio"
      ]
    }
  ],
  "analysis_limits": [
    "gender inference is approximate",
    "relationship labels are hypotheses only"
  ]
}
```

## Speaker Attribution Rules

- Prefer diarization or explicit turn markers over intuition.
- If diarization is unavailable, use cautious labels such as `人物A`, `人物B`.
- If you can only support one stable speaker confidently, say so directly.
- Mark overlaps explicitly instead of forcing a single owner.

## Relationship-Hypothesis Rules

Relationship hypotheses must be evidence-based and reversible.

Good evidence sources:

- forms of address
- repeated reciprocal turns
- domestic or workplace setting seen on screen
- role-specific requests or complaints
- shared child references, money references, chores, flirting, or authority dynamics

Weak evidence that should not stand alone:

- pitch alone
- clothing alone
- one facial expression
- stereotype-based assumptions

## Recommended Processing Order

1. transcribe audio
2. split transcript into utterance-sized units
3. merge diarization or speaker-turn hints if available
4. build speaker profiles
5. assign each utterance to a speaker or mark overlap
6. write relationship hypotheses with evidence and counter-evidence
7. keep the normal `script_table.json` focused on story structure; store speaker-rich detail in `audio_multiview.json`
