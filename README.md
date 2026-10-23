# Koko content center

This repository contains Koko's internal content-production and operations service for a Portuguese-language, Brazil-focused creator product. The public creator site, `kokocomedy.com`, is a **separate repository and Render service**: [hokeem/koko-creator](https://github.com/hokeem/koko-creator). Read both repositories before changing a cross-product workflow.

## Product map

1. Internal operators ingest short videos, run analysis/translation/storyboard jobs, edit scripts and taxonomy, and publish eligible entries from the content center.
2. The content center exposes the library at `/api/library`. The Creator service consumes and caches it; publication and sync are not the same thing as rendering a card in the author UI.
3. Brazilian creators enter `kokocomedy.com`, bind a Kwai profile, choose content preferences, browse Portuguese script recommendations, save or plan scripts, and submit links to their own filmed videos.
4. Submissions and creator accounts live in the Creator service. This service's operations UI proxies author, analytics, submission, and review requests to Creator. **Only Creator's `approved` submission status counts toward reward progress and wallet estimates.**

## Where to start

| Task | Start here |
| --- | --- |
| Video ingestion, analysis, queues, storage | `video-analysis-v3-web/app.py`, `video-analysis-v3-web/content_radar.py`, `skills/video-analysis-v3/` |
| Studio UI and script editing | `video-analysis-v3-web/app.py` (`/studio`) |
| Library, taxonomy, publish/unpublish, Telekwai | `video-analysis-v3-web/app.py` (`/library`, `/api/library`), `config/script_taxonomy_v2.json` |
| Creator operations UI | `video-analysis-v3-web/app.py` (`creator_admin_html`, `/koko_admin_2026-kwai/...`) |
| Creator accounts, submissions, analytics, review | The proxy routes `/api/creator-admin/...` here **and** the corresponding `/api/admin/...` routes in `koko-creator/app.py` |
| Author-facing recommendations, onboarding, wallet | `koko-creator/app.py` (`page_html`, `/creator-portal`) |

The operations UI has tabs for creators, analytics, video review, submissions, and creator intake. The review page is `/koko_admin_2026-kwai/reviews`; it does not own the submission records. The admin-path name is not an authentication mechanism.

## Local development

From `video-analysis-v3-web/`, install `requirements.txt`, run `python3 -m unittest discover -s tests`, then start `PORT=8310 VIDEO_ANALYSIS_WEB_DATA_DIR=/tmp/koko-coach-dev python3 app.py`. Open `http://127.0.0.1:8310/studio`. Use a separate data directory and **do not copy production `/var/data` into a test server**. Background analysis and content-radar work may call external services; do not provide production API credentials for routine UI tests.

The repository-root [`render.yaml`](render.yaml) describes a Render web service with a persistent `/var/data` disk. Key configuration names include `VIDEO_ANALYSIS_WEB_DATA_DIR`, `CREATOR_CENTER_BASE_URL`, `CREATOR_CENTER_SYNC_URL`, `KOKO_CREATOR_ADMIN_PASSWORD`, `GOOGLE_API_KEY`, and `APIFY_TOKEN`. The nested `video-analysis-v3-web/render.yaml` and [`DEPLOY.md`](video-analysis-v3-web/DEPLOY.md) contain older deployment guidance; verify the actual production service, branch, env vars, disk mount, and auto-deploy setting in Render before releasing. A code checkout is not a backup of the disk.

See [`AGENTS.md`](AGENTS.md) for agent-specific guardrails and cross-service verification.
