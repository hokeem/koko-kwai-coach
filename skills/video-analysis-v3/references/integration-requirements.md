# Integration requirements

Use this when installing `video-analysis-v3` in another bot or runtime.

## Required capabilities

- Python 3.10+.
- Network access to public video pages/CDN URLs.
- Google Gemini API access for native video understanding.
- A writable temp/output directory for downloaded videos and generated HTML.

## Required model/API configuration

Set one of:

```bash
export GOOGLE_API_KEY="..."
```

or provide an equivalent Google provider API key in the host bot configuration.

Default model used by `gemini_video_observe.py`:

```text
gemini-2.5-flash-lite
```

Acceptable fallbacks:

```text
gemini-3-flash-preview
gemini-2.0-flash
```

The model must support video input. The prompt intentionally asks Gemini for objective per-second observations only; story interpretation happens locally.

## Recommended system tools

- `ffprobe` for duration/resolution/audio metadata.
- `ffmpeg` for frame extraction and verification thumbnails.
- `yt-dlp` optional for broader platform download support; Kwai can often be resolved by direct HTML `.mp4` parsing.

## Input/output contract

Input:

- public video URL, or
- local video path.

Standard output directory:

```text
tmp/video_analysis_v3/<id>/
```

Expected artifacts:

- `source.mp4`
- `source_metadata.json`
- `observations.json`
- `observations_raw_gemini.json`
- `script_table.json`
- `script_table.html`
- optional `selected_frames/`

## Porting notes

- The host bot should deliver `script_table.html` as a file attachment when the user asks to see/share the report.
- Do not expose raw Gemini observations as the main user-facing output.
- Do not replace Gemini observation with story generation; that breaks v3's safety/accuracy design.
