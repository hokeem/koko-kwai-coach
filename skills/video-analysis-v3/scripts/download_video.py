#!/usr/bin/env python3
"""Resolve/download a public short-video URL or local video for video-analysis-v3."""
from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import re
import shutil
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123 Safari/537.36"
RETRYABLE_ERRORS = (
    urllib.error.URLError,
    ssl.SSLError,
    ConnectionError,
    TimeoutError,
)


def run(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, text=True, capture_output=True)
    return p.returncode, p.stdout, p.stderr


def run_bytes(cmd: list[str]) -> tuple[int, bytes, bytes]:
    p = subprocess.run(cmd, capture_output=True)
    return p.returncode, p.stdout, p.stderr


def ffprobe(path: Path) -> dict:
    if not shutil.which("ffprobe"):
        return {}
    code, out, _ = run([
        "ffprobe", "-v", "error", "-show_entries",
        "format=duration,size:stream=codec_type,width,height,codec_name",
        "-of", "json", str(path)
    ])
    if code != 0:
        return {}
    try:
        data = json.loads(out)
    except Exception:
        return {}
    meta = {}
    fmt = data.get("format", {})
    if fmt.get("duration"):
        meta["duration"] = float(fmt["duration"])
    if fmt.get("size"):
        meta["size"] = int(fmt["size"])
    streams = data.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), {})
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if video:
        meta.update({"width": video.get("width"), "height": video.get("height"), "video_codec": video.get("codec_name")})
    meta["has_audio"] = bool(audio)
    if audio:
        meta["audio_codec"] = audio.get("codec_name")
    return meta


def fetch_with_urllib(url: str, referer: str, timeout: int) -> tuple[bytes, str]:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": referer})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        charset = "utf-8"
        if hasattr(resp.headers, "get_content_charset"):
            charset = resp.headers.get_content_charset() or "utf-8"
    return data, charset


def post_json_with_urllib(url: str, payload: dict, referer: str, timeout: int) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "User-Agent": UA,
            "Referer": referer,
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "ignore")
    return json.loads(body)


def fetch_with_curl(url: str, referer: str, timeout: int) -> bytes:
    if not shutil.which("curl"):
        raise RuntimeError("curl not found")
    code, out, err = run_bytes([
        "curl",
        "-L",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        str(timeout),
        "-A",
        UA,
        "-e",
        referer,
        url,
    ])
    if code != 0:
        raise RuntimeError(err.decode("utf-8", "replace").strip() or f"curl exited with {code}")
    return out


def retry_fetch_text(url: str, referer: str, timeout: int = 30, attempts: int = 3) -> str:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            data, charset = fetch_with_urllib(url, referer, timeout)
            return data.decode(charset, "ignore")
        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(min(2 * attempt, 5))
    try:
        return fetch_with_curl(url, referer, timeout).decode("utf-8", "ignore")
    except Exception as exc:
        if last_error:
            raise RuntimeError(f"html fetch failed after retries: {last_error}; curl fallback failed: {exc}") from exc
        raise RuntimeError(f"html fetch failed: {exc}") from exc


def fetch_html(url: str, out: Path) -> str:
    text = retry_fetch_text(url, url)
    (out / "page.html").write_text(text, encoding="utf-8")
    return text


def extract_title(page: str) -> str | None:
    m = re.search(r"<title[^>]*>(.*?)</title>", page, flags=re.I | re.S)
    if not m:
        return None
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", m.group(1)))).strip()


def direct_mp4_urls(page: str) -> list[str]:
    urls = []
    for raw in re.findall(r"https?://[^\"\\<> ]+\.mp4[^\"\\<> ]*", page):
        u = html.unescape(raw).replace(r"\/", "/").rstrip("');,}")
        if u not in urls:
            urls.append(u)
    return urls


def extract_kuaishou_photo_id(url: str) -> str | None:
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower()
    if not any(domain in host for domain in ("gifshow.com", "kuaishou.com")):
        return None
    patterns = (
        r"/fw/photo/([^/?#]+)",
        r"/short-video/([^/?#]+)",
        r"/fw/long-video/([^/?#]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, parsed.path)
        if match:
            return urllib.parse.unquote(match.group(1))
    return None


def iter_kuaishou_video_urls(photo: dict) -> list[dict]:
    candidates = []

    def add(url: str | None, height: int = 0, bitrate: int = 0, source: str = "") -> None:
        if not url or ".mp4" not in url:
            return
        candidates.append({"url": url, "height": height or 0, "bitrate": bitrate or 0, "source": source})

    manifest = photo.get("manifest") or {}
    for adaptation in manifest.get("adaptationSet") or []:
        for rep in adaptation.get("representation") or []:
            height = int(rep.get("height") or 0)
            bitrate = int(rep.get("avgBitrate") or rep.get("maxBitrate") or 0)
            add(rep.get("url"), height, bitrate, "manifest.url")
            for backup in rep.get("backupUrl") or []:
                add(backup, height, bitrate, "manifest.backupUrl")

    for key in ("photoUrl", "photoH265Url", "croppedPhotoUrl", "croppedPhotoH265Url"):
        add(photo.get(key), int(photo.get("height") or 0), 0, key)

    seen = set()
    unique = []
    for item in candidates:
        if item["url"] in seen:
            continue
        seen.add(item["url"])
        unique.append(item)
    return sorted(unique, key=lambda item: (item["height"], item["bitrate"]), reverse=True)


def try_kuaishou_h5(url: str, source: Path, meta: dict) -> bool:
    photo_id = extract_kuaishou_photo_id(url)
    if not photo_id:
        return False

    endpoint = "https://m.gifshow.com/rest/wd/ugH5App/photo/simple/info"
    data = post_json_with_urllib(endpoint, {"photoId": photo_id}, url, 30)
    result = data.get("result")
    if result != 1:
        message = data.get("error_msg") or data.get("error") or f"result={result}"
        raise SystemExit(f"Kuaishou H5 detail failed: {message}")

    photo = data.get("photo") or {}
    urls = iter_kuaishou_video_urls(photo)
    if not urls:
        raise SystemExit("Kuaishou H5 detail returned no downloadable mp4 URL")

    best = urls[0]
    size = download_url(best["url"], source, url)
    meta.update({
        "local_video": str(source),
        "route": "kuaishou-h5",
        "video_url": best["url"],
        "downloaded_bytes": size,
        "photo_id": photo_id,
        "title": photo.get("caption") or photo.get("photoId") or photo_id,
        "author": photo.get("userName"),
        "cover_url": ((photo.get("coverUrls") or [{}])[0] or {}).get("url"),
        "source_height": best.get("height"),
        "source_bitrate": best.get("bitrate"),
        "source_url_type": best.get("source"),
    })
    return True


def download_url(url: str, dest: Path, referer: str, timeout: int = 120, attempts: int = 3) -> int:
    last_error: Exception | None = None
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, attempts + 1):
        try:
            data, _ = fetch_with_urllib(url, referer, timeout)
            tmp.write_bytes(data)
            tmp.replace(dest)
            return len(data)
        except Exception as exc:
            last_error = exc
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            if attempt < attempts:
                time.sleep(min(2 * attempt, 5))
    try:
        data = fetch_with_curl(url, referer, timeout)
        tmp.write_bytes(data)
        tmp.replace(dest)
        return len(data)
    except Exception as exc:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        if last_error:
            raise RuntimeError(f"mp4 download failed after retries: {last_error}; curl fallback failed: {exc}") from exc
        raise RuntimeError(f"mp4 download failed: {exc}") from exc


def try_ytdlp(url: str, dest: Path) -> bool:
    if not shutil.which("yt-dlp"):
        return False
    tmpl = str(dest / "source.%(ext)s")
    code, _, _ = run([
        "yt-dlp",
        "--no-playlist",
        "--retries",
        "3",
        "--fragment-retries",
        "3",
        "-f",
        "mp4/best",
        "-o",
        tmpl,
        url,
    ])
    if code != 0:
        return False
    files = sorted(dest.glob("source.*"), key=lambda p: p.stat().st_size if p.exists() else 0, reverse=True)
    if not files:
        return False
    if files[0].name != "source.mp4":
        files[0].rename(dest / "source.mp4")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="public video URL or local mp4 path")
    ap.add_argument("--out", required=True, help="output directory")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    inp = args.input
    source = out / "source.mp4"
    meta = {"input": inp, "source_url": inp if inp.startswith(("http://", "https://")) else None}

    if Path(inp).exists():
        src = Path(inp)
        if src.resolve() != source.resolve():
            shutil.copyfile(src, source)
        meta.update({"local_video": str(source), "title": src.name, "route": "local-file"})
    else:
        if try_kuaishou_h5(inp, source, meta):
            pass
        elif try_ytdlp(inp, out):
            meta.update({"local_video": str(source), "route": "yt-dlp"})
        else:
            page = fetch_html(inp, out)
            meta["title"] = extract_title(page)
            urls = direct_mp4_urls(page)
            if not urls:
                raise SystemExit("No direct mp4 URL found and yt-dlp failed/unavailable")
            size = download_url(urls[0], source, inp)
            meta.update({"local_video": str(source), "route": "html-mp4", "video_url": urls[0], "downloaded_bytes": size})

    meta.update(ffprobe(source))
    if "size" not in meta and source.exists():
        meta["size"] = source.stat().st_size
    meta.setdefault("mime_type", mimetypes.guess_type(str(source))[0] or "video/mp4")
    (out / "source_metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
