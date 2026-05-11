# Fallback frame analysis

Use this only when Gemini native video understanding fails.

## Minimal fallback

1. Use `ffprobe` to get duration.
2. Use `ffmpeg` to sample 1 frame per second or one frame at regular intervals for long videos.
3. Run available image/OCR analysis on sampled frames in batches.
4. If audio matters and ASR is available, transcribe audio; otherwise mark audio as unavailable.
5. Build a coarse timeline and mark route as `fallback-frame-analysis`.

## Important distinction

In v3, frame extraction is not the primary analysis engine. It is used for:

- HTML thumbnails
- verification of uncertain Gemini claims
- fallback when Gemini cannot analyze the video