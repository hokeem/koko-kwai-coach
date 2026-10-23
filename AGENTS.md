# Agent handoff: Koko content center

Read `README.md`, `render.yaml`, and the relevant code/tests before editing. This repo is the internal content center; the public Creator app is [hokeem/koko-creator](https://github.com/hokeem/koko-creator). Do not infer current behavior from older PRDs or screenshots when code and live responses disagree.

## Ownership and contracts

- Main service: `video-analysis-v3-web/app.py`. Internal pages: `/studio`, `/library`, `/koko_admin_2026-kwai/creators`, `/analytics`, `/reviews`, `/submissions`, `/intakes` (the latter five under the same admin prefix).
- Much of the admin UI is JavaScript embedded in a Python f-string (`creator_admin_html`). Keep Python/JS interpolation and doubled braces intact; render the HTML and syntax-check its scripts after edits. New tab routes must be added both to `CREATOR_ADMIN_TAB_ROUTES` and the `creator_admin_html` allowed-tab set.
- The content center's `/api/library` is a script source. The Creator service caches it using `CREATOR_LIBRARY_SOURCE_URL`; updates may also trigger `CREATOR_CENTER_SYNC_URL`. Check publication, `telekwai`, `reference_video_enabled`, effective content, cache, and sync before blaming recommendation UI.
- `creator_admin_remote_json` and `/api/creator-admin/...` proxy data to the Creator service's `/api/admin/...`. Creator owns account, author-state, analytics, submission, and review records. A proxy-only change cannot repair Creator-side persistence or reward rules.
- Video review is `pending_review` / `approved` / `rejected`; rejection needs a reason. Never count pending/rejected submissions as earned cash. Do not silently bulk-approve historical submissions.
- Production data is on the Render disk configured by `VIDEO_ANALYSIS_WEB_DATA_DIR` (normally `/var/data`), not in Git. Treat job files, script libraries, exports, and caches according to their actual role before cleanup; never delete unknown disk contents to address a 502.

## Workflow

1. Inspect `git status` first. The user's primary checkout may be dirty; use a clean clone/worktree and preserve all unrelated changes.
2. Identify whether a request touches this repo, `koko-creator`, or both. For cross-service work, trace browser -> API -> persisted data -> response, and test a compatible pair of revisions.
3. Run focused tests, then `cd video-analysis-v3-web && python3 -m unittest discover -s tests`. For inline HTML/JS changes, also render the Python HTML function, run a JS syntax check, and inspect the affected mobile/desktop page in a browser.
4. Before any production release, confirm the remote branch, Render service mapping, secrets configuration, disk health, and rollback point. Deploy the dependency service first (typically Creator API before content-center proxy), then verify `/studio`, `/api/library`, the changed admin page, and authenticated read-only flows on production. Do not mutate real accounts/submissions merely to smoke-test.
5. Report exactly what was changed, tests run, deployment status, production verification, and remaining uncertainty. Never claim a push alone proves Render deployed successfully.

Never put passwords, GitHub tokens, API keys, or user data in prompts, commits, fixtures, logs, or documentation. Use scoped access and Render secret env vars. The code currently has fallback admin-secret values; treat them as security debt, not safe credentials to reuse. Avoid adding more such defaults.
