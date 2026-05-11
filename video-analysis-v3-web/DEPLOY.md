# Video Analysis V3 Web Deploy

## What this app does

- Public webpage for `video-analysis-v3`
- User pastes a short-video URL
- Server runs the v3 pipeline
- Final result is served as `script_table.html`

## Required environment variables

- `GOOGLE_API_KEY`
  - Required for Gemini video observation
- `VIDEO_ANALYSIS_WEB_DATA_DIR`
  - Recommended: `/var/data`
- `VIDEO_ANALYSIS_MAX_CONCURRENT_JOBS`
  - Recommended for low-cost single-service deploy: `1`
- `VIDEO_ANALYSIS_PIPELINE_TIMEOUT_SEC`
  - Recommended: `480`

## Local run

```bash
cd video-analysis-v3-web
python3 app.py
```

Then open:

```text
http://localhost:8310
```

## Render deploy

1. Push the repo to GitHub.
2. Create a new Render Blueprint.
3. Point it at this repository.
4. Render will read the repository-root `render.yaml`.
5. Set `GOOGLE_API_KEY` in the Render dashboard.

## Recommended low-cost production mode

For a `$7.5/month` style deploy:

- Use a single `Starter` web service
- Use a persistent disk mounted at `/var/data`
- Set `VIDEO_ANALYSIS_MAX_CONCURRENT_JOBS=1`

This keeps the app stable for light multi-user usage by:

- queuing jobs inside the app
- allowing only one heavy analysis/review pipeline at a time
- restoring queued/incomplete jobs after a service restart

## Notes

- This app assumes the repository also contains `skills/video-analysis-v3/`.
- Video downloading relies on `yt-dlp`, which is installed via `requirements.txt`.
- Frame extraction is optional. If the deploy target has no `ffmpeg`, leave the UI checkbox off.
- For a truly public production app, you may later want rate limits, authentication, and queue persistence.
