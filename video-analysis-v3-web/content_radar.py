#!/usr/bin/env python3
"""Daily TikTok creator monitoring and metadata-first content ranking."""
from __future__ import annotations

import json
import os
import re
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo


DEFAULT_CREATORS = [
    "leobagarolo",
    "joselohn",
    "camargoevelin",
    "nossaloucuraaoficial",
    "guipolemicoo",
]
DEFAULT_ACTOR_ID = "clockworks~tiktok-profile-scraper"
VALID_DECISIONS = {"pending", "selected", "rejected"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def clean_username(value: str) -> str:
    value = str(value or "").strip().rstrip("/")
    if "tiktok.com/@" in value:
        value = value.split("tiktok.com/@", 1)[1].split("/", 1)[0]
    return value.lstrip("@").lower()


def nested_value(item: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        current: Any = item
        for key in path.split("."):
            if not isinstance(current, dict) or key not in current:
                current = None
                break
            current = current[key]
        if current not in (None, ""):
            return current
    return None


def iso_from_epoch(value: Any) -> str:
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return ""


def parse_datetime(value: Any) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def number(value: Any) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "").lower())
    return "".join(ch for ch in text if not unicodedata.combining(ch))


SIGNALS: dict[str, tuple[str, list[str]]] = {
    "couple": ("夫妻/情侣", ["casal", "marido", "esposa", "namorado", "namorada", "mozão", "mozão", "amor", "casamento", "casado", "casada"]),
    "prank": ("整蛊/反转", ["pegadinha", "trollagem", "trollei", "peguei", "flagra", "flagrante", "brincadeira", "vinganca", "vingança", "desafio"]),
    "innuendo": ("暧昧双关", ["duplo sentido", "safado", "safada", "safadeza", "cama", "motel", "cueca", "calcinha", "amante", "traição", "traicao", "beijo", "sentada", "proposta indecente"]),
    "family": ("家庭日常", ["familia", "família", "mae", "mãe", "pai", "filho", "filha", "sogra", "sogro", "cunhado", "cunhada", "em casa"]),
}
NEGATIVE_SIGNALS: dict[str, tuple[str, list[str]]] = {
    "school": ("校园场景", ["escola", "colegio", "colégio", "professor", "professora", "aluno", "aluna"]),
    "dance": ("偏舞蹈", ["dancinha", "dança", "danca", "coreografia", "trend dance"]),
    "series": ("连续短剧", ["episodio", "episódio", "capitulo", "capítulo", "parte 1", "parte 2", "ep. ", "ep "]),
}


def metadata_analysis(post: dict[str, Any]) -> dict[str, Any]:
    caption = str(post.get("caption") or "").strip()
    searchable = normalize_text(" ".join([caption, " ".join(post.get("hashtags") or [])]))
    score = 34
    reasons: list[str] = []
    categories: list[str] = []

    for key, (label, words) in SIGNALS.items():
        matched = [word for word in words if normalize_text(word) in searchable]
        if not matched:
            continue
        categories.append(label)
        score += {"couple": 18, "prank": 18, "innuendo": 15, "family": 12}[key]
        reasons.append(f"标题信号：{label}")

    for key, (label, words) in NEGATIVE_SIGNALS.items():
        if any(normalize_text(word) in searchable for word in words):
            score -= {"school": 24, "dance": 22, "series": 17}[key]
            reasons.append(f"需留意：{label}")

    duration = number(post.get("duration_seconds"))
    if 8 <= duration <= 60:
        score += 14
        reasons.append("短频快时长")
    elif 61 <= duration <= 90:
        score += 7
    elif duration > 120:
        score -= 12
        reasons.append("时长偏长")

    metrics = post.get("metrics") if isinstance(post.get("metrics"), dict) else {}
    views = number(metrics.get("views"))
    likes = number(metrics.get("likes"))
    if views >= 1_000_000:
        score += 9
        reasons.append("百万级播放")
    elif views >= 100_000:
        score += 6
        reasons.append("10万+播放")
    elif views >= 20_000:
        score += 3
    if views and likes / views >= 0.08:
        score += 5
        reasons.append("点赞率较高")

    if not caption:
        score -= 8
        reasons.append("无标题，需人工试看")
    score = max(0, min(100, score))
    fit = "high" if score >= 68 else "medium" if score >= 48 else "low"
    if categories:
        summary = f"标题/标签疑似涉及{'、'.join(categories[:3])}，建议打开原视频确认剧情、场景和可翻拍性。"
    elif caption:
        summary = "标题未出现强匹配词，建议结合画面与对白快速复核。"
    else:
        summary = "缺少可判断的标题信息，当前排序主要参考时长与互动数据。"
    return {
        "score": score,
        "fit": fit,
        "categories": categories,
        "reasons": reasons[:5],
        "summary_zh": summary,
        "basis": "metadata",
    }


def normalize_apify_item(item: dict[str, Any]) -> dict[str, Any] | None:
    if item.get("errorCode"):
        return None
    post_id = nested_value(item, "id", "post_id", "aweme_id")
    username = nested_value(item, "authorMeta.name", "authorMeta.uniqueId", "author.uniqueId", "username")
    if post_id is None or username is None:
        return None
    username = clean_username(str(username))
    published_at = nested_value(item, "createTimeISO", "create_time_iso", "published_at") or iso_from_epoch(
        nested_value(item, "createTime", "create_time", "timestamp")
    )
    post_url = nested_value(item, "webVideoUrl", "url", "post_url") or f"https://www.tiktok.com/@{username}/video/{post_id}"
    raw_hashtags = item.get("hashtags") or item.get("hashtagNames") or []
    hashtags: list[str] = []
    if isinstance(raw_hashtags, list):
        for value in raw_hashtags[:30]:
            if isinstance(value, dict):
                value = value.get("name") or value.get("title") or ""
            text = str(value or "").strip().lstrip("#")
            if text:
                hashtags.append(text)
    caption = str(nested_value(item, "text", "description", "title") or "").strip()
    post = {
        "id": f"tiktok:{post_id}",
        "platform": "tiktok",
        "creator_username": username,
        "creator_name": str(nested_value(item, "authorMeta.nickName", "authorMeta.nickname", "author.nickname") or username),
        "creator_avatar_url": str(nested_value(item, "authorMeta.avatar", "author.avatarThumb", "author.avatar") or ""),
        "post_id": str(post_id),
        "caption": caption,
        "hashtags": hashtags,
        "published_at": str(published_at or ""),
        "duration_seconds": number(nested_value(item, "videoMeta.duration", "video.duration", "duration")),
        "post_url": str(post_url),
        "thumbnail_url": str(nested_value(item, "videoMeta.coverUrl", "videoMeta.originalCoverUrl", "video.cover", "cover") or ""),
        "metrics": {
            "views": number(nested_value(item, "playCount", "stats.playCount", "view_count")),
            "likes": number(nested_value(item, "diggCount", "stats.diggCount", "like_count")),
            "comments": number(nested_value(item, "commentCount", "stats.commentCount", "comment_count")),
            "shares": number(nested_value(item, "shareCount", "stats.shareCount", "share_count")),
        },
        "fetched_at": iso_now(),
    }
    post["analysis"] = metadata_analysis(post)
    return post


class ContentRadar:
    def __init__(self, state_path: Path, *, logger: Callable[..., Any] | None = None) -> None:
        self.state_path = state_path
        self.logger = logger
        self.lock = threading.RLock()
        self.refresh_lock = threading.Lock()
        self._refreshing = False
        creators = os.environ.get("CONTENT_RADAR_TIKTOK_CREATORS", ",".join(DEFAULT_CREATORS))
        self.creators = [clean_username(value) for value in creators.split(",") if clean_username(value)]
        self.per_creator_limit = max(1, min(50, int(os.environ.get("CONTENT_RADAR_POSTS_PER_CREATOR", "12"))))
        self.actor_id = os.environ.get("APIFY_TIKTOK_ACTOR_ID", DEFAULT_ACTOR_ID).strip() or DEFAULT_ACTOR_ID
        self.timezone_name = os.environ.get("CONTENT_RADAR_TIMEZONE", "Asia/Shanghai")
        self.daily_hour = max(0, min(23, int(os.environ.get("CONTENT_RADAR_DAILY_HOUR", "8"))))

    def _default_state(self) -> dict[str, Any]:
        return {"version": 1, "posts": {}, "runs": [], "last_run": None}

    def _read(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._default_state()
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("posts"), dict):
                return data
        except (OSError, ValueError, TypeError):
            pass
        return self._default_state()

    def _write(self, state: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.parent / f"{self.state_path.name}.{uuid4().hex}.tmp"
        temporary.write_text(json.dumps(state, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        temporary.replace(self.state_path)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            state = self._read()
        posts = list(state.get("posts", {}).values())
        posts.sort(
            key=lambda post: (
                number((post.get("analysis") or {}).get("score")),
                parse_datetime(post.get("published_at")),
            ),
            reverse=True,
        )
        counts = {decision: sum(1 for post in posts if post.get("decision", "pending") == decision) for decision in VALID_DECISIONS}
        return {
            "ok": True,
            "creators": self.creators,
            "posts": posts,
            "last_run": state.get("last_run"),
            "runs": (state.get("runs") or [])[:10],
            "refreshing": self._refreshing,
            "counts": counts,
            "timezone": self.timezone_name,
            "daily_hour": self.daily_hour,
            "ranking_note": "匹配分基于标题、标签、时长和互动量，仅用于运营初筛；最终判断需打开原视频确认。",
        }

    def set_decision(self, post_id: str, decision: str, note: str = "") -> dict[str, Any]:
        if decision not in VALID_DECISIONS:
            raise ValueError("decision must be pending, selected, or rejected")
        with self.lock:
            state = self._read()
            post = state.get("posts", {}).get(post_id)
            if not isinstance(post, dict):
                raise KeyError(post_id)
            post["decision"] = decision
            post["operator_note"] = str(note or "").strip()[:500]
            post["decision_updated_at"] = iso_now()
            self._write(state)
            return post

    def _call_apify(self, token: str) -> list[dict[str, Any]]:
        actor = urllib.parse.quote(self.actor_id, safe="~")
        url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items?{urllib.parse.urlencode({'token': token})}"
        payload = {
            "profiles": self.creators,
            "profileScrapeSections": ["videos"],
            "profileSorting": "latest",
            "resultsPerPage": self.per_creator_limit,
            "excludePinnedPosts": True,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSlideshowImages": False,
            "maxFollowersPerProfile": 0,
            "maxFollowingPerProfile": 0,
        }
        request = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:800]
            raise RuntimeError(f"Apify HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"无法连接 Apify：{exc.reason}") from exc
        if not isinstance(result, list):
            raise RuntimeError("Apify 返回格式异常")
        return result

    def search_tiktok(self, queries: list[str], *, limit_per_query: int = 15) -> dict[str, Any]:
        """Run a bounded TikTok keyword search without changing the daily feed."""
        cleaned = [str(query or "").strip()[:180] for query in queries if str(query or "").strip()][:6]
        if not cleaned:
            raise ValueError("至少需要一个搜索词")
        limit_per_query = max(1, min(25, int(limit_per_query)))
        token = os.environ.get("APIFY_TOKEN", "").strip()
        if not token:
            raise RuntimeError("服务尚未配置 APIFY_TOKEN")
        actor = urllib.parse.quote("clockworks~tiktok-scraper", safe="~")
        url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items?{urllib.parse.urlencode({'token': token})}"
        payload = {
            "searchQueries": cleaned,
            "searchSection": "/video",
            "videoSearchSorting": "MOST_RELEVANT",
            "videoSearchDateFilter": "ALL_TIME",
            "resultsPerPage": limit_per_query,
            "shouldDownloadVideos": False,
            "shouldDownloadCovers": False,
            "shouldDownloadSubtitles": False,
            "shouldDownloadSlideshowImages": False,
        }
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                raw_items = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:800]
            raise RuntimeError(f"Apify HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"无法连接 Apify：{exc.reason}") from exc
        if not isinstance(raw_items, list):
            raise RuntimeError("Apify 返回格式异常")
        posts = [post for item in raw_items if (post := normalize_apify_item(item)) is not None]
        unique: dict[str, dict[str, Any]] = {}
        for post in posts:
            unique[post["id"]] = post
        ranked = list(unique.values())
        ranked.sort(
            key=lambda post: (
                number((post.get("analysis") or {}).get("score")),
                number((post.get("metrics") or {}).get("views")),
            ),
            reverse=True,
        )
        return {"ok": True, "queries": cleaned, "posts": ranked, "raw_count": len(raw_items)}

    def refresh(self, *, reason: str = "manual") -> dict[str, Any]:
        if not self.refresh_lock.acquire(blocking=False):
            return {"ok": True, "started": False, "message": "采集正在进行中"}
        self._refreshing = True
        started_at = iso_now()
        try:
            token = os.environ.get("APIFY_TOKEN", "").strip()
            if not token:
                raise RuntimeError("服务尚未配置 APIFY_TOKEN")
            raw_items = self._call_apify(token)
            normalized = [post for item in raw_items if (post := normalize_apify_item(item)) is not None]
            tracked = set(self.creators)
            normalized = [post for post in normalized if post.get("creator_username") in tracked]
            normalized.sort(key=lambda post: parse_datetime(post.get("published_at")), reverse=True)
            counts: dict[str, int] = {}
            limited: list[dict[str, Any]] = []
            for post in normalized:
                username = str(post.get("creator_username") or "")
                if counts.get(username, 0) >= self.per_creator_limit:
                    continue
                counts[username] = counts.get(username, 0) + 1
                limited.append(post)
            with self.lock:
                state = self._read()
                existing = state.setdefault("posts", {})
                new_count = 0
                for post in limited:
                    previous = existing.get(post["id"], {})
                    if not previous:
                        new_count += 1
                    post["decision"] = previous.get("decision", "pending")
                    post["operator_note"] = previous.get("operator_note", "")
                    post["decision_updated_at"] = previous.get("decision_updated_at", "")
                    post["first_seen_at"] = previous.get("first_seen_at", iso_now())
                    existing[post["id"]] = post
                run = {
                    "started_at": started_at,
                    "finished_at": iso_now(),
                    "status": "success",
                    "reason": reason,
                    "items_received": len(raw_items),
                    "posts_saved": len(limited),
                    "new_posts": new_count,
                }
                state["last_run"] = run
                state["runs"] = [run, *(state.get("runs") or [])][:30]
                self._write(state)
            return {"ok": True, "started": True, "run": run}
        except Exception as exc:
            run = {"started_at": started_at, "finished_at": iso_now(), "status": "error", "reason": reason, "error": str(exc)[:1000]}
            with self.lock:
                state = self._read()
                state["last_run"] = run
                state["runs"] = [run, *(state.get("runs") or [])][:30]
                self._write(state)
            if self.logger:
                try:
                    self.logger("content_radar_refresh_failed", "Content Radar Apify refresh failed.", error=str(exc))
                except Exception:
                    pass
            return {"ok": False, "started": True, "run": run, "error": str(exc)}
        finally:
            self._refreshing = False
            self.refresh_lock.release()

    def trigger_refresh(self, *, reason: str = "manual") -> dict[str, Any]:
        if self._refreshing:
            return {"ok": True, "started": False, "message": "采集正在进行中"}
        threading.Thread(target=self.refresh, kwargs={"reason": reason}, name="content-radar-refresh", daemon=True).start()
        return {"ok": True, "started": True, "message": "已开始调用 Apify，通常需要 1–3 分钟"}

    def _ran_today(self, local_now: datetime) -> bool:
        with self.lock:
            last_run = self._read().get("last_run") or {}
        finished = parse_datetime(last_run.get("finished_at"))
        if finished == datetime.min.replace(tzinfo=timezone.utc):
            return False
        same_day = finished.astimezone(local_now.tzinfo).date() == local_now.date()
        return same_day and (last_run.get("reason") == "daily" or last_run.get("status") == "success")

    def scheduler_loop(self) -> None:
        try:
            local_tz = ZoneInfo(self.timezone_name)
        except Exception:
            local_tz = ZoneInfo("Asia/Shanghai")
        time.sleep(20)
        while True:
            local_now = utc_now().astimezone(local_tz)
            if os.environ.get("APIFY_TOKEN", "").strip() and local_now.hour >= self.daily_hour and not self._ran_today(local_now):
                self.refresh(reason="daily")
            time.sleep(600)

    def start_scheduler(self) -> None:
        threading.Thread(target=self.scheduler_loop, name="content-radar-scheduler", daemon=True).start()
