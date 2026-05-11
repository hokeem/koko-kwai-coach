# Platform download notes

## Generic order

1. If input is a local file, copy/link it into the output directory and probe metadata.
2. If `yt-dlp` is installed, try it first for broad platform support.
3. If `yt-dlp` fails, fetch page HTML with a desktop User-Agent and parse direct video URLs.
4. Save `page.html` for debugging.
5. Use `ffprobe` when available to collect duration/resolution/audio metadata.

## Kwai

Kwai public pages often embed direct `.mp4` CDN URLs in HTML, e.g. `cdn.kwai.net` or `ak-*-cdn.kwai.net`. Parse `.mp4` URLs and download the first viable candidate with `Referer: https://www.kwai.com/`.

## Xiaohongshu

Xiaohongshu video pages may require page-state parsing or `yt-dlp`. If only image carousel URLs are found, use the xiaohongshu content extraction skill instead.

## TikTok/Reels

Prefer `yt-dlp`. If blocked, report the blocker and ask for uploaded mp4.

## Safety

Do not attempt login, CAPTCHA bypass, private content scraping, or access-control circumvention.
