#!/usr/bin/env python3
"""Manual TikTok keyword discovery and lightweight human review."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4
DEFAULT_KEYWORDS = [
    "couple comedy",
    "relationship comedy",
    "funny couple",
    "husband wife comedy",
    "couple prank",
    "relationship skit",
    "marriage humor",
    "couple skit",
    "humor de casal",
    "comédia de casal",
    "casal engraçado",
    "pegadinha de casal",
    "marido e mulher comédia",
    "relacionamento com humor",
    "esquete de casal",
]

REFRESH_TIMEOUT_SECONDS = 600
APIFY_POLL_SECONDS = 2


class RefreshCancelled(RuntimeError):
    pass


class RefreshTimedOut(RuntimeError):
    pass
V2_KEYWORDS = [
    "humor de casal",
    "couple comedy",
    "couples comedy",
    "husband wife comedy",
    "husband wife humor",
    "boyfriend girlfriend comedy",
    "couple skit",
    "couple daily life comedy",
    "funny couple at home",
    "casal engraçado",
    "comédia de casal",
    "marido e mulher humor",
    "humor de pareja",
    "comedia de pareja",
    "matrimonio humor",
    "funny couple prank",
    "wife husband reaction",
    "couple prank reaction",
]
PROMPT_KEYWORD_SETS = {"v1": DEFAULT_KEYWORDS, "v2": V2_KEYWORDS}
RELAXED_KEYWORDS = [
    "funny relationship",
    "married couple funny",
    "husband wife funny",
    "funny marriage",
    "boyfriend girlfriend funny",
    "couple jokes",
    "relationship prank",
    "casal divertido",
    "relacionamento engraçado",
    "pegadinha casal",
]
CHEATING_KEYWORDS = [
    "cheating husband comedy",
    "cheating wife comedy",
    "cheating boyfriend skit",
    "cheating girlfriend skit",
    "caught cheating comedy",
    "caught in the act couple skit",
    "affair comedy skit",
    "side chick comedy skit",
    "other woman comedy skit",
    "husband caught cheating prank",
    "wife caught cheating prank",
    "cheating prank on boyfriend",
    "cheating prank on girlfriend",
    "marido traindo comédia",
    "esposa traindo comédia",
    "traição de casal comédia",
    "pegadinha de traição",
    "flagrante de traição humor",
    "amante escondida comédia",
    "amante do marido humor",
    "infidelidade casal humor",
]
CHEATING_RELAXED_KEYWORDS = [
    "cheating husband",
    "cheating wife",
    "caught cheating",
    "side chick drama",
    "other woman skit",
    "marido infiel humor",
    "esposa infiel humor",
    "traição relacionamento",
    "amante pegadinha",
]
FRIEND_PRANK_KEYWORDS = [
    "pranking my roommate",
    "roommate prank reaction",
    "funny roommate prank",
    "best friend prank reaction",
    "prank on my best friend",
    "friends pranking each other",
    "friend prank at home",
    "hidden camera friend prank",
    "harmless prank on friend",
    "housemate prank",
    "pegadinha com amigo",
    "pegadinha com melhor amigo",
    "pegadinha com colega de quarto",
    "trollagem com amigo",
    "reação pegadinha amigo",
    "amigos trolando",
    "pegadinha entre amigos em casa",
    "susto no amigo pegadinha",
]
FRIEND_PRANK_RELAXED_KEYWORDS = [
    "friend prank",
    "roommate prank",
    "best friend reaction",
    "prank wars friends",
    "housemate funny prank",
    "pegadinha amigo",
    "trollagem amigo",
    "colega de quarto pegadinha",
]
DEFAULT_CONTENT_TYPE = "couple_comedy"
CONTENT_TYPE_CONFIG = {
    "couple_comedy": {
        "label": "夫妻情侣喜剧",
        "short_label": "情侣",
        "versions": PROMPT_KEYWORD_SETS,
        "relaxed_keywords": RELAXED_KEYWORDS,
    },
    "cheating_comedy": {
        "label": "出轨 / 抓包喜剧",
        "short_label": "出轨",
        "versions": {"v1": CHEATING_KEYWORDS},
        "relaxed_keywords": CHEATING_RELAXED_KEYWORDS,
        "min_views": 100_000,
    },
    "friend_prank": {
        "label": "朋友整蛊",
        "short_label": "朋友整蛊",
        "versions": {"v1": FRIEND_PRANK_KEYWORDS},
        "relaxed_keywords": FRIEND_PRANK_RELAXED_KEYWORDS,
        "min_views": 1_000_000,
    },
}
MANUAL_REFRESH_LIMIT = 50
DEFAULT_ACTOR_ID = "coregent~tiktok-keyword-search-scraper"
VALID_DECISIONS = {"pending", "selected", "produced", "rejected"}
CURATED_BATCH_ID = "2026-09-03-apify-tiktok-shortlist"
CURATED_DATASET_ID = "v09ZyrDkrBEaovxOL"
FRIEND_PRANK_REFERENCE_BATCH_ID = "2026-09-21-friend-prank-reference"
FRIEND_PRANK_REFERENCE_POST = {
    "username": "anthonyriveras",
    "post_id": "6717348453673946373",
    "caption": "Pranking My Roommate 😂😂😂 #foryou #foryoupage #prank #challenge",
    "published_at": "2019-07-24T00:00:00Z",
    "duration_seconds": 25,
    "views": 5_400_000,
    "likes": 836_700,
    "comments": 1_896,
}
DEFAULT_REFRESH_PASSWORD_SHA256 = "65fea9f52c567036ccee405d09f214764054fe64c451b0b4d7f30afdd49a77e4"
CURATED_TIKTOK_POSTS = [
    ("texasbaz", 36_800_000, "7675481187808300319"),
    ("chris978462", 14_800_000, "7676058284406754590"),
    ("cobyandashley", 10_900_000, "7676564075467246861"),
    ("colbyandceleste", 7_550_000, "7678820562625400095"),
    ("mccall_girl76", 7_280_000, "7679964637508488479"),
    ("therealbeaufords", 6_000_000, "7679980069015538958"),
    ("linneamullen", 4_720_000, "7676230229463141650"),
    ("noelle.cefola", 4_120_000, "7678824104165690637"),
    ("jilliangerhardt", 3_780_000, "7674316587377184030"),
    ("bellagraceslife", 3_570_000, "7674698769425796383"),
    ("helloginadarling", 3_310_000, "7674439076400909582"),
    ("therealhammytv", 2_840_000, "7671743815505562894"),
    ("deal_family", 2_810_000, "7680007917969411342"),
    ("miranda_maeee", 2_630_000, "7676322356247203102"),
    ("jackiemitchellll", 2_390_000, "7679900945559391502"),
    ("thevaglefamily", 2_340_000, "7672780771047820575"),
    ("rickandcarly", 2_290_000, "7670237528611474702"),
    ("theblondebrewer", 2_210_000, "7679181892385590559"),
    ("josephjamestiktok", 1_710_000, "7678301119537319190"),
    ("drefiggysmalls", 1_580_000, "7678093976842292493"),
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def verify_refresh_password(value: str) -> bool:
    expected = os.environ.get("CONTENT_RADAR_REFRESH_PASSWORD_SHA256", DEFAULT_REFRESH_PASSWORD_SHA256).strip().lower()
    candidate = hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()
    return bool(value) and hmac.compare_digest(candidate, expected)


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


def contains_term(text: str, term: str) -> bool:
    normalized = normalize_text(term)
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])", text))


SIGNALS: dict[str, tuple[str, list[str]]] = {
    "couple": ("夫妻/情侣", ["couple", "relationship", "husband", "wife", "boyfriend", "girlfriend", "marriage", "married", "casal", "marido", "esposa", "namorado", "namorada", "amor", "casamento", "casado", "casada"]),
    "prank": ("整蛊/反转", ["prank", "caught", "reaction", "plot twist", "pegadinha", "trollagem", "trollei", "peguei", "flagra", "flagrante", "brincadeira", "vinganca", "vingança", "desafio"]),
    "innuendo": ("暧昧双关", ["innuendo", "naughty", "bed", "motel", "cheating", "kiss", "duplo sentido", "safado", "safada", "safadeza", "cama", "cueca", "calcinha", "amante", "traição", "traicao", "beijo", "sentada", "proposta indecente"]),
    "family": ("家庭日常", ["family", "mom", "dad", "son", "daughter", "mother in law", "at home", "familia", "família", "mae", "mãe", "pai", "filho", "filha", "sogra", "sogro", "cunhado", "cunhada", "em casa"]),
}
NEGATIVE_SIGNALS: dict[str, tuple[str, list[str]]] = {
    "school": ("校园场景", ["school", "teacher", "student", "classroom", "escola", "colegio", "colégio", "professor", "professora", "aluno", "aluna"]),
    "dance": ("偏舞蹈", ["dance", "dancing", "choreography", "dancinha", "dança", "danca", "coreografia", "trend dance"]),
    "series": ("连续短剧", ["episode", "part 1", "part 2", "episodio", "episódio", "capitulo", "capítulo", "parte 1", "parte 2", "ep. ", "ep "]),
}
CHEATING_TOPIC_TERMS = [
    "cheating", "affair", "unfaithful", "infidelity", "side chick", "mistress", "other woman", "other man", "caught in the act",
    "traicao", "traindo", "traiu", "infiel", "amante", "outra mulher", "outro homem", "chifre", "corno", "corna",
]
PERFORMANCE_TERMS = [
    "comedy", "comedic", "skit", "sketch", "prank", "funny", "humor", "acting", "acted", "pov", "parody",
    "comedia", "pegadinha", "trollagem", "encenacao", "cena", "parodia", "interpretacao",
]
NON_PERFORMANCE_TERMS = [
    "news", "breaking news", "podcast", "interview", "storytime", "confession", "true story", "documentary", "reddit story",
    "noticia", "noticias", "entrevista", "desabafo", "historia real", "relato real", "documentario", "fofoca de famosos",
]
FRIEND_RELATIONSHIP_TERMS = [
    "friend", "friends", "best friend", "bestie", "roommate", "room mate", "housemate", "buddy", "bro",
    "amigo", "amiga", "amigos", "amigas", "melhor amigo", "melhor amiga", "colega de quarto",
]
FRIEND_PRANK_TERMS = [
    "prank", "pranking", "pranked", "reaction", "hidden camera", "prank war", "scare prank", "harmless prank",
    "pegadinha", "trollagem", "trollei", "trolando", "reacao", "reação", "camera escondida", "câmera escondida", "susto",
]
FRIEND_PRANK_EXCLUSIONS = [
    "stranger prank", "prank on strangers", "random people", "public prank", "social experiment", "prank compilation",
    "prank fails", "school prank", "teacher prank", "student prank", "pegadinha com desconhecido", "experimento social",
    "pegadinha na escola", "compilacao de pegadinhas", "compilação de pegadinhas",
]


def validate_content_type(post: dict[str, Any], content_type: str) -> dict[str, Any]:
    """Apply high-precision metadata gates before a post enters a specialized queue."""
    if content_type not in {"cheating_comedy", "friend_prank"}:
        return {"eligible": True, "mode": "default"}
    searchable = normalize_text(" ".join([
        str(post.get("caption") or ""),
        " ".join(str(value) for value in (post.get("hashtags") or [])),
    ]))
    if content_type == "friend_prank":
        topic_hits = [term for term in FRIEND_RELATIONSHIP_TERMS if contains_term(searchable, term)]
        performance_hits = [term for term in FRIEND_PRANK_TERMS if contains_term(searchable, term)]
        excluded_hits = [term for term in FRIEND_PRANK_EXCLUSIONS if contains_term(searchable, term)]
        eligible = bool(topic_hits and performance_hits and not excluded_hits)
        if not topic_hits:
            reason = "标题或标签没有明确朋友、室友或同伴关系"
        elif not performance_hits:
            reason = "标题或标签没有明确整蛊或真实反应语义"
        elif excluded_hits:
            reason = "疑似陌生人、街头、校园或整蛊合集内容"
        else:
            reason = "同时命中朋友关系与整蛊反应信号"
        return {
            "eligible": eligible,
            "mode": "strict_metadata",
            "reason": reason,
            "topic_hits": topic_hits[:5],
            "performance_hits": performance_hits[:5],
            "excluded_hits": excluded_hits[:5],
        }
    topic_hits = [term for term in CHEATING_TOPIC_TERMS if normalize_text(term) in searchable]
    performance_hits = [term for term in PERFORMANCE_TERMS if normalize_text(term) in searchable]
    excluded_hits = [term for term in NON_PERFORMANCE_TERMS if normalize_text(term) in searchable]
    eligible = bool(topic_hits and performance_hits and not excluded_hits)
    if not topic_hits:
        reason = "标题或标签没有明确出轨语义"
    elif not performance_hits:
        reason = "标题或标签没有剧情演绎、喜剧或整蛊语义"
    elif excluded_hits:
        reason = "疑似新闻、播客、真人倾诉或故事口播"
    else:
        reason = "同时命中出轨与剧情演绎信号"
    return {
        "eligible": eligible,
        "mode": "strict_metadata",
        "reason": reason,
        "topic_hits": topic_hits[:5],
        "performance_hits": performance_hits[:5],
        "excluded_hits": excluded_hits[:5],
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
    post_id = nested_value(item, "videoId", "id", "post_id", "aweme_id")
    username = nested_value(item, "authorUniqueId", "authorMeta.name", "authorMeta.uniqueId", "author.uniqueId", "username")
    if post_id is None or username is None:
        return None
    username = clean_username(str(username))
    published_at = nested_value(item, "createTimeISO", "create_time_iso", "published_at") or iso_from_epoch(
        nested_value(item, "createTime", "create_time", "timestamp")
    )
    post_url = nested_value(item, "videoUrl", "shareUrl", "webVideoUrl", "url", "post_url") or f"https://www.tiktok.com/@{username}/video/{post_id}"
    raw_hashtags = item.get("hashtags") or item.get("hashtagNames") or []
    hashtags: list[str] = []
    if isinstance(raw_hashtags, list):
        for value in raw_hashtags[:30]:
            if isinstance(value, dict):
                value = value.get("name") or value.get("title") or ""
            text = str(value or "").strip().lstrip("#")
            if text:
                hashtags.append(text)
    caption = str(nested_value(item, "caption", "text", "description", "title") or "").strip()
    post = {
        "id": f"tiktok:{post_id}",
        "platform": "tiktok",
        "creator_username": username,
        "creator_name": str(nested_value(item, "authorNickname", "authorMeta.nickName", "authorMeta.nickname", "author.nickname") or username),
        "creator_avatar_url": str(nested_value(item, "authorAvatarUrl", "authorMeta.avatar", "author.avatarThumb", "author.avatar") or ""),
        "creator_tags": [],
        "post_id": str(post_id),
        "caption": caption,
        "hashtags": hashtags,
        "published_at": str(published_at or ""),
        "duration_seconds": number(nested_value(item, "duration", "videoMeta.duration", "video.duration")),
        "post_url": str(post_url),
        "thumbnail_url": str(nested_value(item, "coverUrl", "originCoverUrl", "dynamicCoverUrl", "videoMeta.coverUrl", "videoMeta.originalCoverUrl", "video.cover", "cover") or ""),
        "metrics": {
            "views": number(nested_value(item, "views", "playCount", "stats.playCount", "view_count")),
            "likes": number(nested_value(item, "likes", "diggCount", "stats.diggCount", "like_count")),
            "comments": number(nested_value(item, "comments", "commentCount", "stats.commentCount", "comment_count")),
            "shares": number(nested_value(item, "shares", "shareCount", "stats.shareCount", "share_count")),
        },
        "matched_keyword": str(item.get("keyword") or ""),
        "discovery_mode": "keyword",
        "fetched_at": iso_now(),
    }
    post["analysis"] = metadata_analysis(post)
    post["thumbnail_source_url"] = post["thumbnail_url"]
    return post


class ContentRadar:
    def __init__(self, state_path: Path, *, logger: Callable[..., Any] | None = None) -> None:
        self.state_path = state_path
        self.logger = logger
        self.lock = threading.RLock()
        self.refresh_lock = threading.Lock()
        self.thumbnail_lock = threading.Lock()
        self._refreshing = False
        self._refresh_progress: dict[str, Any] | None = None
        self._cancel_event = threading.Event()
        self._active_apify_run_id = ""
        self._active_apify_token = ""
        self._refresh_deadline = 0.0
        self._refresh_started_monotonic = 0.0
        self._thumbnail_thread: threading.Thread | None = None
        self.cover_dir = state_path.parent / "content_radar_covers"
        configured_version = os.environ.get("CONTENT_RADAR_PROMPT_VERSION", "v1").strip().lower()
        self.prompt_version = configured_version if configured_version in PROMPT_KEYWORD_SETS else "v1"
        self.keyword_sets = {version: list(keywords) for version, keywords in PROMPT_KEYWORD_SETS.items()}
        self.keywords = self.keyword_sets[self.prompt_version]
        self.max_results = max(10, min(120, int(os.environ.get("CONTENT_RADAR_MAX_RESULTS", "40"))))
        self.min_views = max(1_000_000, int(os.environ.get("CONTENT_RADAR_MIN_VIEWS", "1000000")))
        lookback = os.environ.get("CONTENT_RADAR_LOOKBACK", "last30Days").strip()
        self.lookback = lookback if lookback in {"last24Hours", "last7Days", "last30Days", "last90Days"} else "last30Days"
        self.actor_id = os.environ.get("APIFY_TIKTOK_ACTOR_ID", DEFAULT_ACTOR_ID).strip() or DEFAULT_ACTOR_ID
        self.daily_enabled = False

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
            progress = dict(self._refresh_progress) if self._refresh_progress else None
            if progress is not None and self._refreshing and self._refresh_started_monotonic:
                progress["elapsed_seconds"] = max(0, int(time.monotonic() - self._refresh_started_monotonic))
        posts = [post for post in state.get("posts", {}).values() if post.get("discovery_mode") == "keyword"]
        for post in posts:
            post["prompt_version"] = str(post.get("prompt_version") or "v1")
            post["content_type"] = str(post.get("content_type") or DEFAULT_CONTENT_TYPE)
            config = CONTENT_TYPE_CONFIG.get(post["content_type"], CONTENT_TYPE_CONFIG[DEFAULT_CONTENT_TYPE])
            post["content_type_label"] = str(post.get("content_type_label") or config["short_label"])
        posts.sort(
            key=lambda post: (
                parse_datetime(post.get("fetched_at")),
                parse_datetime(post.get("first_seen_at")),
                parse_datetime(post.get("published_at")),
            ),
            reverse=True,
        )
        counts = {decision: sum(1 for post in posts if post.get("decision", "pending") == decision) for decision in VALID_DECISIONS}
        return {
            "ok": True,
            "keywords": self.keywords,
            "prompt_version": self.prompt_version,
            "keyword_versions": {
                version: {"keywords": keywords, "keyword_count": len(keywords)}
                for version, keywords in self.keyword_sets.items()
            },
            "content_types": {
                content_type: {
                    "label": config["label"],
                    "short_label": config["short_label"],
                    "versions": {
                        version: {"keywords": list(words), "keyword_count": len(words)}
                        for version, words in config["versions"].items()
                    },
                }
                for content_type, config in CONTENT_TYPE_CONFIG.items()
            },
            "min_views": self.min_views,
            "lookback": self.lookback,
            "max_results": self.max_results,
            "posts": posts,
            "last_run": state.get("last_run"),
            "runs": (state.get("runs") or [])[:10],
            "refreshing": self._refreshing,
            "progress": progress,
            "counts": counts,
            "daily_enabled": self.daily_enabled,
            "collection_mode": "manual",
        }

    def set_decision(self, post_id: str, decision: str, note: str = "") -> dict[str, Any]:
        if decision not in VALID_DECISIONS:
            raise ValueError("decision must be pending, selected, produced, or rejected")
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

    def set_decisions(self, post_ids: list[str], decision: str) -> list[dict[str, Any]]:
        """Apply one workflow decision to multiple posts in a single state write."""
        if decision not in VALID_DECISIONS:
            raise ValueError("decision must be pending, selected, produced, or rejected")
        cleaned = list(dict.fromkeys(str(post_id or "").strip() for post_id in post_ids if str(post_id or "").strip()))[:500]
        if not cleaned:
            raise ValueError("post_ids must contain at least one post id")
        with self.lock:
            state = self._read()
            posts = state.get("posts", {})
            missing = [post_id for post_id in cleaned if not isinstance(posts.get(post_id), dict)]
            if missing:
                raise KeyError(missing[0])
            updated_at = iso_now()
            updated: list[dict[str, Any]] = []
            for post_id in cleaned:
                post = posts[post_id]
                post["decision"] = decision
                post["decision_updated_at"] = updated_at
                updated.append(post)
            self._write(state)
            return updated

    def import_friend_prank_reference(self) -> int:
        """Seed the user-provided roommate-prank reference without calling Apify."""
        reference = FRIEND_PRANK_REFERENCE_POST
        post_id = str(reference["post_id"])
        key = f"tiktok:{post_id}"
        local_cover_url = f"/content-radar-cover/{post_id}.jpg"
        try:
            bundled_cover = Path(__file__).resolve().parent / "assets" / f"content-radar-friend-prank-{post_id}.jpg"
            if bundled_cover.is_file():
                self.cover_dir.mkdir(parents=True, exist_ok=True)
                target = self.cover_dir / f"{post_id}.jpg"
                if not target.exists():
                    target.write_bytes(bundled_cover.read_bytes())
        except OSError:
            local_cover_url = ""
        with self.lock:
            state = self._read()
            imported_batches = state.setdefault("imported_batches", [])
            if FRIEND_PRANK_REFERENCE_BATCH_ID in imported_batches:
                return 0
            posts = state.setdefault("posts", {})
            previous = posts.get(key, {})
            now = iso_now()
            post = {
                "id": key,
                "platform": "tiktok",
                "creator_username": reference["username"],
                "creator_name": previous.get("creator_name") or "Anthony Rivera",
                "creator_avatar_url": previous.get("creator_avatar_url", ""),
                "creator_tags": ["朋友整蛊", "室友", "隐藏拍摄"],
                "post_id": post_id,
                "caption": reference["caption"],
                "hashtags": ["foryou", "foryoupage", "prank", "challenge"],
                "published_at": reference["published_at"],
                "duration_seconds": reference["duration_seconds"],
                "post_url": f"https://www.tiktok.com/@{reference['username']}/video/{post_id}",
                "thumbnail_url": local_cover_url,
                "thumbnail_source_url": local_cover_url,
                "metrics": {
                    "views": reference["views"],
                    "likes": reference["likes"],
                    "comments": reference["comments"],
                    "shares": number((previous.get("metrics") or {}).get("shares")),
                },
                "matched_keyword": "pranking my roommate",
                "prompt_version": "v1",
                "content_type": "friend_prank",
                "content_type_label": "朋友整蛊",
                "content_validation": validate_content_type({"caption": reference["caption"], "hashtags": ["prank"]}, "friend_prank"),
                "discovery_mode": "keyword",
                "fetched_at": previous.get("fetched_at") or now,
                "decision": previous.get("decision", "pending"),
                "operator_note": previous.get("operator_note", "案例视频：室友卫生间透明胶带整蛊"),
                "decision_updated_at": previous.get("decision_updated_at", ""),
                "first_seen_at": previous.get("first_seen_at") or now,
                "search_stage": "reference_seed",
                "search_stage_label": "用户案例",
            }
            post["analysis"] = metadata_analysis(post)
            posts[key] = post
            imported_batches.append(FRIEND_PRANK_REFERENCE_BATCH_ID)
            self._write(state)
            return 0 if previous else 1

    def import_curated_batch(self) -> int:
        """Import the already-paid September 3 shortlist once, without calling Apify."""
        with self.lock:
            state = self._read()
            imported_batches = state.setdefault("imported_batches", [])
            backfilled = False
            for existing_post in state.setdefault("posts", {}).values():
                if existing_post.get("discovery_mode") == "keyword" and not existing_post.get("prompt_version"):
                    existing_post["prompt_version"] = "v1"
                    backfilled = True
            if CURATED_BATCH_ID in imported_batches:
                if backfilled:
                    self._write(state)
                return 0
            posts = state.setdefault("posts", {})
            imported = 0
            for username, views, post_id in CURATED_TIKTOK_POSTS:
                key = f"tiktok:{post_id}"
                previous = posts.get(key, {})
                post = {
                    "id": key,
                    "platform": "tiktok",
                    "creator_username": username,
                    "creator_name": previous.get("creator_name") or username,
                    "creator_avatar_url": previous.get("creator_avatar_url", ""),
                    "creator_tags": [],
                    "post_id": post_id,
                    "caption": previous.get("caption", ""),
                    "hashtags": previous.get("hashtags", []),
                    "published_at": previous.get("published_at", ""),
                    "duration_seconds": number(previous.get("duration_seconds")),
                    "post_url": f"https://www.tiktok.com/@{username}/video/{post_id}",
                    "thumbnail_url": previous.get("thumbnail_url", ""),
                    "metrics": {**(previous.get("metrics") or {}), "views": views},
                    "matched_keyword": "curated test batch",
                    "prompt_version": previous.get("prompt_version") or "v1",
                    "discovery_mode": "keyword",
                    "fetched_at": previous.get("fetched_at") or "2026-09-03T00:00:00Z",
                    "decision": previous.get("decision", "pending"),
                    "operator_note": previous.get("operator_note", ""),
                    "decision_updated_at": previous.get("decision_updated_at", ""),
                    "first_seen_at": previous.get("first_seen_at") or "2026-09-03T00:00:00Z",
                }
                post["analysis"] = metadata_analysis(post)
                posts[key] = post
                if not previous:
                    imported += 1
            imported_batches.append(CURATED_BATCH_ID)
            self._write(state)
            return imported

    def hydrate_curated_metadata(self) -> int:
        """Restore captions and short-lived cover URLs from the already-paid dataset."""
        curated_ids = {post_id for _, _, post_id in CURATED_TIKTOK_POSTS}
        with self.lock:
            current = self._read().get("posts", {})
            if all(
                (post := current.get(f"tiktok:{post_id}"))
                and post.get("caption")
                and self.cached_cover_url(post_id)
                for post_id in curated_ids
            ):
                return 0
        try:
            url = f"https://api.apify.com/v2/datasets/{CURATED_DATASET_ID}/items?clean=true&format=json&limit=100"
            request = urllib.request.Request(url, headers={"User-Agent": "Koko-Content-Radar/1.0"})
            with urllib.request.urlopen(request, timeout=12) as response:
                raw_items = json.load(response)
        except Exception as exc:
            if self.logger:
                self.logger("content_radar_metadata_restore_failed", "Could not restore curated TikTok metadata.", error=str(exc))
            return 0
        normalized = [post for item in raw_items if isinstance(item, dict) and str(item.get("videoId")) in curated_ids if (post := normalize_apify_item(item))]
        with self.lock:
            state = self._read()
            posts = state.setdefault("posts", {})
            restored = 0
            for fresh in normalized:
                previous = posts.get(fresh["id"], {})
                if not previous:
                    continue
                fresh["decision"] = previous.get("decision", "pending")
                fresh["operator_note"] = previous.get("operator_note", "")
                fresh["decision_updated_at"] = previous.get("decision_updated_at", "")
                fresh["first_seen_at"] = previous.get("first_seen_at") or "2026-09-03T00:00:00Z"
                fresh["fetched_at"] = previous.get("fetched_at") or fresh.get("fetched_at") or "2026-09-03T00:00:00Z"
                fresh["prompt_version"] = previous.get("prompt_version") or "v1"
                cached = self.cached_cover_url(fresh["post_id"])
                if cached:
                    fresh["thumbnail_url"] = cached
                posts[fresh["id"]] = fresh
                restored += 1
            if restored:
                self._write(state)
            return restored

    def cached_cover_url(self, post_id: str) -> str:
        for suffix in ("jpg", "png", "webp", "gif"):
            if (self.cover_dir / f"{post_id}.{suffix}").is_file():
                return f"/content-radar-cover/{post_id}.{suffix}"
        return ""

    def _cache_thumbnail(self, post: dict[str, Any]) -> tuple[str, str] | None:
        post_id = str(post.get("post_id") or "")
        existing = self.cached_cover_url(post_id)
        if existing:
            return post_id, existing
        source = str(post.get("thumbnail_source_url") or post.get("thumbnail_url") or "")
        if not re.fullmatch(r"https://[^\s]+", source):
            return None
        request = urllib.request.Request(
            source,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.tiktok.com/", "Accept": "image/avif,image/webp,image/*"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                content_type = str(response.headers.get_content_type() or "").lower()
                raw = response.read(5_000_001)
        except Exception:
            return None
        suffixes = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif"}
        suffix = suffixes.get(content_type)
        if not suffix or not raw or len(raw) > 5_000_000:
            return None
        self.cover_dir.mkdir(parents=True, exist_ok=True)
        target = self.cover_dir / f"{post_id}.{suffix}"
        temporary = self.cover_dir / f"{post_id}.{uuid4().hex}.tmp"
        temporary.write_bytes(raw)
        temporary.replace(target)
        return post_id, f"/content-radar-cover/{target.name}"

    def _cache_thumbnails(self) -> None:
        try:
            with self.lock:
                state = self._read()
                candidates = [
                    dict(post)
                    for post in state.get("posts", {}).values()
                    if post.get("discovery_mode") == "keyword" and not self.cached_cover_url(str(post.get("post_id") or ""))
                ]
            updates: dict[str, str] = {}
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(self._cache_thumbnail, post) for post in candidates]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except Exception:
                        continue
                    if result:
                        updates[result[0]] = result[1]
            if updates:
                with self.lock:
                    state = self._read()
                    for post in state.get("posts", {}).values():
                        local_url = updates.get(str(post.get("post_id") or ""))
                        if local_url:
                            post["thumbnail_url"] = local_url
                    self._write(state)
        finally:
            self.thumbnail_lock.release()

    def start_thumbnail_cache(self) -> bool:
        if not self.thumbnail_lock.acquire(blocking=False):
            return False
        self._thumbnail_thread = threading.Thread(target=self._cache_thumbnails, name="content-radar-thumbnail-cache", daemon=True)
        self._thumbnail_thread.start()
        return True

    def content_type_config(self, content_type: str) -> dict[str, Any]:
        normalized = str(content_type or DEFAULT_CONTENT_TYPE).strip().lower()
        config = CONTENT_TYPE_CONFIG.get(normalized)
        if config is None:
            raise ValueError("内容类型必须是 couple_comedy、cheating_comedy 或 friend_prank")
        return config

    def keywords_for(self, prompt_version: str, content_type: str = DEFAULT_CONTENT_TYPE) -> list[str]:
        version = str(prompt_version or "").strip().lower()
        config = self.content_type_config(content_type)
        versions = config["versions"]
        if version not in versions:
            allowed = "、".join(item.upper() for item in versions)
            raise ValueError(f"该内容类型只支持关键词版本：{allowed}")
        return list(versions[version])

    def _update_progress(self, **changes: Any) -> None:
        with self.lock:
            current = dict(self._refresh_progress or {})
            current.update(changes)
            if self._refresh_started_monotonic:
                current["elapsed_seconds"] = max(0, int(time.monotonic() - self._refresh_started_monotonic))
            self._refresh_progress = current

    def _request_json(self, url: str, token: str, *, method: str = "GET", payload: dict[str, Any] | None = None, timeout: int = 30) -> Any:
        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8") if payload is not None else None,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:800]
            raise RuntimeError(f"Apify HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"无法连接 Apify：{exc.reason}") from exc

    def _abort_apify_run(self, run_id: str, token: str) -> None:
        if not run_id or not token:
            return
        encoded = urllib.parse.quote(run_id, safe="")
        try:
            self._request_json(f"https://api.apify.com/v2/actor-runs/{encoded}/abort", token, method="POST", payload={}, timeout=20)
        except Exception:
            pass

    def cancel_refresh(self) -> dict[str, Any]:
        with self.lock:
            if not self._refreshing:
                return {"ok": True, "cancelled": False, "message": "当前没有抓取任务"}
            self._cancel_event.set()
            run_id = self._active_apify_run_id
            token = self._active_apify_token
        self._update_progress(status="stopping", stage_label="正在停止抓取…", cancel_requested=True)
        if run_id and token:
            threading.Thread(target=self._abort_apify_run, args=(run_id, token), name="content-radar-abort", daemon=True).start()
        return {"ok": True, "cancelled": True, "message": "已发送停止请求，将保留本轮已找到的视频"}

    def _call_apify(self, token: str, *, keywords: list[str], max_results: int, lookback: str, min_views: int) -> list[dict[str, Any]]:
        actor = urllib.parse.quote(self.actor_id, safe="~")
        url = f"https://api.apify.com/v2/acts/{actor}/runs"
        payload = {
            "keywords": keywords,
            "searchType": "video",
            "maxItemsPerKeyword": 30,
            "maxTotalResults": max_results,
            "sort": "mostViewed",
            "datePosted": lookback,
            "deduplicateAcrossKeywords": True,
            "minViews": min_views,
            "includeKeywordInsights": False,
            "includeDownloadUrl": False,
        }
        created = self._request_json(url, token, method="POST", payload=payload)
        run = created.get("data") if isinstance(created, dict) else None
        if not isinstance(run, dict) or not run.get("id"):
            raise RuntimeError("Apify 未返回任务编号")
        run_id = str(run["id"])
        dataset_id = str(run.get("defaultDatasetId") or "")
        with self.lock:
            self._active_apify_run_id = run_id
            self._active_apify_token = token
        terminal_failures = {"FAILED", "TIMING-OUT", "TIMED-OUT", "ABORTING", "ABORTED"}
        while True:
            if self._cancel_event.is_set():
                self._abort_apify_run(run_id, token)
                raise RefreshCancelled("用户已停止抓取")
            if self._refresh_deadline and time.monotonic() >= self._refresh_deadline:
                self._abort_apify_run(run_id, token)
                raise RefreshTimedOut("抓取超过10分钟，已自动停止")
            status_payload = self._request_json(f"https://api.apify.com/v2/actor-runs/{urllib.parse.quote(run_id, safe='')}", token)
            current = status_payload.get("data") if isinstance(status_payload, dict) else None
            if not isinstance(current, dict):
                raise RuntimeError("Apify 任务状态格式异常")
            status = str(current.get("status") or "RUNNING").upper()
            dataset_id = str(current.get("defaultDatasetId") or dataset_id)
            stats = current.get("stats") if isinstance(current.get("stats"), dict) else {}
            stage_items = 0
            if dataset_id:
                try:
                    dataset_meta = self._request_json(f"https://api.apify.com/v2/datasets/{urllib.parse.quote(dataset_id, safe='')}", token, timeout=15)
                    stage_items = int(((dataset_meta.get("data") or {}).get("itemCount") or 0))
                except Exception:
                    stage_items = int((self._refresh_progress or {}).get("stage_items") or 0)
            self._update_progress(
                status="running",
                apify_status=status,
                stage_items=stage_items,
                run_time_seconds=int(stats.get("runTimeSecs") or 0),
            )
            if status == "SUCCEEDED":
                break
            if status in terminal_failures:
                if self._cancel_event.is_set() or status in {"ABORTING", "ABORTED"}:
                    raise RefreshCancelled("用户已停止抓取")
                if status in {"TIMING-OUT", "TIMED-OUT"}:
                    raise RefreshTimedOut("Apify 任务运行超时")
                raise RuntimeError(f"Apify 任务失败：{status}")
            time.sleep(APIFY_POLL_SECONDS)
        if not dataset_id:
            return []
        query = urllib.parse.urlencode({"clean": "true", "format": "json", "limit": max_results})
        result = self._request_json(f"https://api.apify.com/v2/datasets/{urllib.parse.quote(dataset_id, safe='')}/items?{query}", token, timeout=60)
        with self.lock:
            self._active_apify_run_id = ""
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

    def inspect_tiktok_posts(self, post_urls: list[str]) -> dict[str, Any]:
        """Fetch low-resolution review copies for a few explicit public posts."""
        urls = [str(value or "").strip() for value in post_urls if str(value or "").strip()][:5]
        if not urls or any(not re.fullmatch(r"https://www\.tiktok\.com/@[^/]+/video/\d+", value) for value in urls):
            raise ValueError("请提供 1–5 个标准 TikTok 视频链接")
        token = os.environ.get("APIFY_TOKEN", "").strip()
        if not token:
            raise RuntimeError("服务尚未配置 APIFY_TOKEN")
        actor = urllib.parse.quote("coregent~tiktok-video-scraper", safe="~")
        url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items?{urllib.parse.urlencode({'token': token})}"
        payload = {
            "videos": urls,
            "maxVideos": len(urls),
            "shouldDownloadVideos": True,
            "videoDownloadQuality": "low",
            "downloadSubtitlesOptions": "transcript",
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
        items = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            raw_media = item.get("mediaUrls") or []
            if isinstance(raw_media, str):
                raw_media = [raw_media]
            media_urls = [str(value) for value in raw_media if str(value or "").startswith("http")]
            download_addr = str(item.get("downloadAddr") or nested_value(item, "videoMeta.downloadAddr") or "")
            if download_addr.startswith("http") and download_addr not in media_urls:
                media_urls.insert(0, download_addr)
            items.append({
                "post_url": str(item.get("webVideoUrl") or item.get("url") or ""),
                "post_id": str(item.get("id") or ""),
                "creator_username": str(nested_value(item, "authorMeta.name", "author.uniqueId") or ""),
                "caption": str(item.get("text") or item.get("description") or ""),
                "duration_seconds": number(nested_value(item, "videoMeta.duration", "video.duration", "duration")),
                "media_urls": media_urls,
                "transcript": str(item.get("transcript") or "")[:12000],
            })
        return {"ok": True, "items": items, "raw_count": len(raw_items)}

    def _search_stages(self, content_type: str, version: str, target_count: int) -> list[dict[str, Any]]:
        config = self.content_type_config(content_type)
        strict_keywords = self.keywords_for(version, content_type)
        relaxed_keywords = list(dict.fromkeys([*strict_keywords, *config["relaxed_keywords"]]))
        return [
            {"id": "strict_30", "label": "原关键词 · 近30天", "lookback": "last30Days", "max_age_days": 30, "keywords": strict_keywords, "max_results": max(80, target_count * 2)},
            {"id": "strict_50", "label": "原关键词 · 近50天", "lookback": "last90Days", "max_age_days": 50, "keywords": strict_keywords, "max_results": max(100, target_count * 2)},
            {"id": "strict_100", "label": "原关键词 · 近100天", "lookback": "any", "max_age_days": 100, "keywords": strict_keywords, "max_results": max(120, target_count * 3)},
            {"id": "strict_300", "label": "原关键词 · 近300天", "lookback": "any", "max_age_days": 300, "keywords": strict_keywords, "max_results": max(150, target_count * 3)},
            {"id": "relaxed_300", "label": "放宽关键词 · 近300天", "lookback": "any", "max_age_days": 300, "keywords": relaxed_keywords, "max_results": max(180, target_count * 4), "keywords_relaxed": True},
        ]

    def refresh(self, *, reason: str = "manual", content_type: str = DEFAULT_CONTENT_TYPE, prompt_version: str | None = None, max_results: int | None = None) -> dict[str, Any]:
        content_type = str(content_type or DEFAULT_CONTENT_TYPE).strip().lower()
        content_config = self.content_type_config(content_type)
        version = str(prompt_version or self.prompt_version).strip().lower()
        keywords = self.keywords_for(version, content_type)
        category_min_views = int(content_config.get("min_views") or self.min_views)
        target_count = max(1, min(120, int(max_results or self.max_results)))
        if not self.refresh_lock.acquire(blocking=False):
            return {"ok": True, "started": False, "message": "采集正在进行中"}
        triggered = bool(self._refresh_progress and self._refresh_progress.get("status") == "starting")
        self._refreshing = True
        started_at = iso_now()
        if not triggered:
            self._cancel_event.clear()
        self._refresh_started_monotonic = time.monotonic()
        self._refresh_deadline = self._refresh_started_monotonic + REFRESH_TIMEOUT_SECONDS
        stages = self._search_stages(content_type, version, target_count)
        if not self._refresh_progress or self._refresh_progress.get("status") not in {"starting", "running"}:
            self._refresh_progress = {
                "status": "starting",
                "content_type": content_type,
                "content_type_label": content_config["label"],
                "prompt_version": version,
                "target_count": target_count,
                "found_count": 0,
                "stage_index": 0,
                "stage_count": len(stages),
                "stage_label": "准备启动 Apify",
                "stage_items": 0,
                "percent": 1,
                "started_at": started_at,
                "elapsed_seconds": 0,
                "cancel_requested": False,
            }
        stage_reports: list[dict[str, Any]] = []
        cancelled = False
        timed_out = False
        try:
            token = os.environ.get("APIFY_TOKEN", "").strip()
            if not token:
                raise RuntimeError("服务尚未配置 APIFY_TOKEN")
            with self.lock:
                existing_ids = set(self._read().get("posts", {}))
            collected: dict[str, dict[str, Any]] = {}
            items_received = 0
            for stage_index, stage in enumerate(stages, start=1):
                if len(collected) >= target_count:
                    break
                if self._cancel_event.is_set():
                    cancelled = True
                    break
                if time.monotonic() >= self._refresh_deadline:
                    timed_out = True
                    break
                self._update_progress(
                    status="running",
                    stage_index=stage_index,
                    stage_count=len(stages),
                    stage_label=stage["label"],
                    stage_items=0,
                    apify_status="READY",
                    found_count=len(collected),
                    percent=min(94, max(2, int(((stage_index - 1) / len(stages)) * 90))),
                )
                report = {
                    "id": stage["id"],
                    "label": stage["label"],
                    "max_age_days": stage["max_age_days"],
                    "keyword_count": len(stage["keywords"]),
                    "keywords_relaxed": bool(stage.get("keywords_relaxed")),
                    "min_views": category_min_views,
                }
                try:
                    raw_items = self._call_apify(
                        token,
                        keywords=stage["keywords"],
                        max_results=stage["max_results"],
                        lookback=stage["lookback"],
                        min_views=category_min_views,
                    )
                except RefreshCancelled as exc:
                    cancelled = True
                    report.update({"status": "cancelled", "error": str(exc), "received": 0, "added": 0})
                    stage_reports.append(report)
                    break
                except RefreshTimedOut as exc:
                    timed_out = True
                    report.update({"status": "timed_out", "error": str(exc), "received": 0, "added": 0})
                    stage_reports.append(report)
                    break
                except Exception as exc:
                    report.update({"status": "error", "error": str(exc)[:500], "received": 0, "added": 0})
                    stage_reports.append(report)
                    break
                items_received += len(raw_items)
                normalized = [post for item in raw_items if (post := normalize_apify_item(item)) is not None]
                invalid_count = len(raw_items) - len(normalized)
                below_views = [post for post in normalized if number((post.get("metrics") or {}).get("views")) < category_min_views]
                view_eligible = [post for post in normalized if number((post.get("metrics") or {}).get("views")) >= category_min_views]
                content_eligible = []
                content_mismatch = 0
                for post in view_eligible:
                    validation = validate_content_type(post, content_type)
                    post["content_validation"] = validation
                    if validation["eligible"]:
                        content_eligible.append(post)
                    else:
                        content_mismatch += 1
                cutoff = utc_now() - timedelta(days=stage["max_age_days"])
                missing_published_at = [post for post in content_eligible if not post.get("published_at")]
                age_eligible = [
                    post for post in content_eligible
                    if parse_datetime(post.get("published_at")) >= cutoff
                    or (not post.get("published_at") and stage["lookback"] != "any")
                ]
                outside_age = len(content_eligible) - len(age_eligible) - (len(missing_published_at) if stage["lookback"] == "any" else 0)
                stage_unique = {post["id"]: post for post in age_eligible}
                duplicate_existing = sum(1 for post_id in stage_unique if post_id in existing_ids)
                duplicate_batch = sum(1 for post_id in stage_unique if post_id in collected)
                candidates = [
                    post for post_id, post in stage_unique.items()
                    if post_id not in existing_ids and post_id not in collected
                ]
                candidates.sort(
                    key=lambda post: (
                        number((post.get("analysis") or {}).get("score")),
                        number((post.get("metrics") or {}).get("views")),
                    ),
                    reverse=True,
                )
                remaining = target_count - len(collected)
                added = candidates[:remaining]
                for post in added:
                    post["search_stage"] = stage["id"]
                    post["search_stage_label"] = stage["label"]
                    collected[post["id"]] = post
                report.update({
                    "status": "success",
                    "received": len(raw_items),
                    "invalid": invalid_count,
                    "below_min_views": len(below_views),
                    "content_mismatch": content_mismatch,
                    "outside_time_window": outside_age,
                    "missing_published_at": len(missing_published_at),
                    "duplicate_existing": duplicate_existing,
                    "duplicate_this_run": duplicate_batch,
                    "eligible_new": len(candidates),
                    "added": len(added),
                    "total_new": len(collected),
                })
                stage_reports.append(report)
                self._update_progress(
                    found_count=len(collected),
                    stage_items=len(raw_items),
                    percent=min(95, int((stage_index / len(stages)) * 90)),
                )
            ranked = list(collected.values())
            with self.lock:
                state = self._read()
                existing = state.setdefault("posts", {})
                for post in ranked:
                    post["decision"] = "pending"
                    post["operator_note"] = ""
                    post["decision_updated_at"] = ""
                    post["first_seen_at"] = iso_now()
                    post["prompt_version"] = version
                    post["content_type"] = content_type
                    post["content_type_label"] = content_config["short_label"]
                    existing[post["id"]] = post
                new_count = len(ranked)
                target_met = new_count >= target_count
                duplicate_total = sum(int(stage.get("duplicate_existing") or 0) + int(stage.get("duplicate_this_run") or 0) for stage in stage_reports)
                below_views_total = sum(int(stage.get("below_min_views") or 0) for stage in stage_reports)
                content_mismatch_total = sum(int(stage.get("content_mismatch") or 0) for stage in stage_reports)
                outside_time_total = sum(int(stage.get("outside_time_window") or 0) for stage in stage_reports)
                if cancelled:
                    shortfall_reason = "用户主动停止抓取，已保留停止前找到的合格视频。"
                elif timed_out:
                    shortfall_reason = "抓取达到10分钟资源上限，已自动停止并保留已有结果。"
                elif target_met:
                    shortfall_reason = ""
                elif any(stage.get("status") == "error" for stage in stage_reports):
                    shortfall_reason = "Apify抓取阶段发生错误，流程提前停止。"
                elif content_type == "cheating_comedy" and content_mismatch_total:
                    shortfall_reason = "部分结果没有同时满足“明确出轨语义”和“剧情演绎语义”，已按准确性要求剔除。"
                elif content_type == "friend_prank" and content_mismatch_total:
                    shortfall_reason = "部分结果没有同时满足“朋友/室友关系”和“整蛊/反应语义”，已剔除陌生人、街头、校园及合集内容。"
                elif duplicate_total:
                    shortfall_reason = "搜索结果中已有视频较多，去重后不足50条新内容。"
                elif below_views_total or outside_time_total:
                    shortfall_reason = "部分结果未达到100万播放量或超出300天时间范围。"
                else:
                    shortfall_reason = "TikTok在当前关键词和时间范围内返回的合格视频不足。"
                run = {
                    "started_at": started_at,
                    "finished_at": iso_now(),
                    "status": "cancelled" if cancelled else "timed_out" if timed_out else "success",
                    "reason": reason,
                    "items_received": items_received,
                    "posts_saved": len(ranked),
                    "new_posts": new_count,
                    "updated_posts": 0,
                    "prompt_version": version,
                    "content_type": content_type,
                    "content_type_label": content_config["label"],
                    "min_views": category_min_views,
                    "keywords": keywords,
                    "target_count": target_count,
                    "target_met": target_met,
                    "shortfall": max(0, target_count - new_count),
                    "shortfall_reason": shortfall_reason,
                    "stages": stage_reports,
                    "relaxed_keywords_used": any(stage.get("keywords_relaxed") for stage in stage_reports),
                    "cancelled": cancelled,
                    "timed_out": timed_out,
                }
                state["last_run"] = run
                state["runs"] = [run, *(state.get("runs") or [])][:30]
                self._write(state)
            self.start_thumbnail_cache()
            self._update_progress(
                status="cancelled" if cancelled else "timed_out" if timed_out else "completed",
                stage_label="已停止" if cancelled else "达到时限，已停止" if timed_out else "抓取完成",
                found_count=len(ranked),
                percent=100,
                finished_at=run["finished_at"],
            )
            return {"ok": True, "started": True, "run": run}
        except Exception as exc:
            run = {"started_at": started_at, "finished_at": iso_now(), "status": "error", "reason": reason, "error": str(exc)[:1000], "prompt_version": version, "content_type": content_type, "content_type_label": content_config["label"], "min_views": category_min_views, "keywords": keywords, "target_count": target_count, "target_met": False, "shortfall": target_count, "shortfall_reason": "抓取服务运行失败。", "stages": stage_reports}
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
            self._update_progress(status="error", stage_label="抓取失败", error=str(exc)[:500], percent=100, finished_at=run["finished_at"])
            return {"ok": False, "started": True, "run": run, "error": str(exc)}
        finally:
            self._refreshing = False
            with self.lock:
                self._active_apify_run_id = ""
                self._active_apify_token = ""
            self._refresh_deadline = 0.0
            self.refresh_lock.release()

    def trigger_refresh(self, *, reason: str = "manual", content_type: str = DEFAULT_CONTENT_TYPE, prompt_version: str = "v1", max_results: int = MANUAL_REFRESH_LIMIT) -> dict[str, Any]:
        content_type = str(content_type or DEFAULT_CONTENT_TYPE).strip().lower()
        content_config = self.content_type_config(content_type)
        version = str(prompt_version or "v1").strip().lower()
        self.keywords_for(version, content_type)
        if self._refreshing:
            return {"ok": True, "started": False, "message": "采集正在进行中"}
        target_count = max(1, min(120, int(max_results)))
        self._cancel_event.clear()
        self._refreshing = True
        self._refresh_progress = {
            "status": "starting",
            "content_type": content_type,
            "content_type_label": content_config["label"],
            "prompt_version": version,
            "target_count": target_count,
            "found_count": 0,
            "stage_index": 0,
            "stage_count": 5,
            "stage_label": "正在创建抓取任务",
            "stage_items": 0,
            "percent": 1,
            "started_at": iso_now(),
            "elapsed_seconds": 0,
            "cancel_requested": False,
        }
        threading.Thread(
            target=self.refresh,
            kwargs={"reason": reason, "content_type": content_type, "prompt_version": version, "max_results": max_results},
            name="content-radar-refresh",
            daemon=True,
        ).start()
        return {"ok": True, "started": True, "content_type": content_type, "prompt_version": version, "message": f"已开始抓取{content_config['label']} {version.upper()}，最长运行10分钟，可随时停止"}

    def start_scheduler(self) -> None:
        """Kept for app startup compatibility; collection is manual-only."""
        return
