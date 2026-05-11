#!/usr/bin/env python3
"""Build a normalized multi-speaker audio sidecar from transcript-first inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_json(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    candidate = Path(path)
    if not candidate.exists():
        return None
    return json.loads(candidate.read_text(encoding="utf-8"))


def infer_audio_profile(segment_count: int) -> str:
    if segment_count <= 0:
        return "unknown"
    if segment_count == 1:
        return "monologue_or_single_source"
    if segment_count <= 4:
        return "short_dialogue_or_sparse_speech"
    return "dialogue_or_mixed_speech"


def build_speakers(diarization: dict[str, Any] | None, transcript: dict[str, Any]) -> list[dict[str, Any]]:
    if diarization and diarization.get("speakers"):
        speakers = []
        for index, speaker in enumerate(diarization["speakers"], start=1):
            speakers.append(
                {
                    "speaker_id": speaker.get("speaker_id", f"SPEAKER_{index - 1:02d}"),
                    "display_label": speaker.get("display_label", f"人物{chr(64 + index)}"),
                    "gender_guess": speaker.get("gender_guess", "unknown"),
                    "gender_confidence": speaker.get("gender_confidence", 0.0),
                    "source_type": speaker.get("source_type", "human_speech"),
                    "role_guess": speaker.get("role_guess", ""),
                    "voice_characteristics": speaker.get("voice_characteristics", []),
                    "evidence": speaker.get("evidence", []),
                    "uncertainty_note": speaker.get("uncertainty_note", ""),
                }
            )
        return speakers

    return [
        {
            "speaker_id": "SPEAKER_00",
            "display_label": "人物A",
            "gender_guess": "unknown",
            "gender_confidence": 0.0,
            "source_type": "human_speech",
            "role_guess": "",
            "voice_characteristics": [],
            "evidence": ["no diarization input was provided; all transcript segments remain under a conservative single-speaker bucket"],
            "uncertainty_note": "speaker ownership requires diarization, manual annotation, or stronger multimodal evidence",
        }
    ]


def build_utterances(
    transcript: dict[str, Any],
    diarization: dict[str, Any] | None,
    speakers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    diarized_segments = []
    if diarization:
        diarized_segments = diarization.get("utterances") or diarization.get("segments") or []

    fallback_speaker_id = speakers[0]["speaker_id"] if speakers else "SPEAKER_00"
    utterances = []
    for index, segment in enumerate(transcript.get("segments", []), start=1):
        matched = None
        for item in diarized_segments:
            if abs(float(item.get("start", 0)) - float(segment.get("start", 0))) < 0.35 and abs(
                float(item.get("end", 0)) - float(segment.get("end", 0))
            ) < 0.5:
                matched = item
                break
        speaker_id = matched.get("speaker_id") if matched else fallback_speaker_id
        utterances.append(
            {
                "utterance_id": f"utt_{index:03d}",
                "start": segment.get("start", 0),
                "end": segment.get("end", 0),
                "speaker_id": speaker_id,
                "speaker_label": next((s["display_label"] for s in speakers if s["speaker_id"] == speaker_id), "人物A"),
                "text": segment.get("text", ""),
                "source_type": matched.get("source_type", "human_speech") if matched else "human_speech",
                "is_overlap": bool(matched.get("is_overlap", False)) if matched else False,
                "confidence": matched.get("confidence", max(0.0, 1 - float(segment.get("no_speech_prob", 0.5)))) if matched else max(
                    0.0, 1 - float(segment.get("no_speech_prob", 0.5))
                ),
                "evidence_note": matched.get("evidence_note", "transcript-only fallback attribution") if matched else "transcript-only fallback attribution",
            }
        )
    return utterances


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transcript", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--source-url", default="")
    parser.add_argument("--asr-audio", default="audio.wav")
    parser.add_argument("--analysis-audio", default="audio_analysis.wav")
    parser.add_argument("--diarization-json")
    parser.add_argument("--relationship-json")
    args = parser.parse_args()

    transcript = load_json(args.transcript)
    if transcript is None:
        raise SystemExit(f"missing transcript: {args.transcript}")

    diarization = load_json(args.diarization_json)
    relationship = load_json(args.relationship_json) or {}
    speakers = build_speakers(diarization, transcript)
    utterances = build_utterances(transcript, diarization, speakers)
    stable_source_count = len({item["speaker_id"] for item in utterances}) if utterances else 0

    output = {
        "source_url": args.source_url,
        "audio_assets": {
            "asr_audio": args.asr_audio,
            "analysis_audio": args.analysis_audio,
        },
        "language": {
            "value": transcript.get("language", "unknown"),
            "confidence": transcript.get("language_probability", 0.0),
        },
        "whole_audio_hypothesis": {
            "audio_source_profile": infer_audio_profile(len(transcript.get("segments", []))),
            "overall_audio_form": relationship.get("overall_audio_form", "underdetermined"),
            "stable_source_count": {
                "value": stable_source_count,
                "confidence": 0.8 if diarization else 0.25,
                "note": "count comes from diarization merge" if diarization else "count is conservative because no diarization input was provided",
            },
            "uncertainties": relationship.get(
                "uncertainties",
                ["speaker ownership, gender guess, and relationship inference need stronger audio or multimodal evidence"],
            ),
        },
        "speakers": speakers,
        "utterances": utterances,
        "relationship_hypotheses": relationship.get("relationship_hypotheses", []),
        "analysis_limits": relationship.get(
            "analysis_limits",
            [
                "gender inference is approximate",
                "relationship labels are hypotheses only",
                "transcript-only mode should not be treated as speaker-verified diarization",
            ],
        ),
    }

    Path(args.output).write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
