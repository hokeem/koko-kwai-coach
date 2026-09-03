import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from content_radar import ContentRadar, metadata_analysis, normalize_apify_item


class ContentRadarTests(unittest.TestCase):
    def test_couple_prank_metadata_ranks_above_school_dance(self):
        strong = metadata_analysis({
            "caption": "Pegadinha com meu marido na cama 😂 #casal",
            "duration_seconds": 31,
            "hashtags": ["trollagem"],
            "metrics": {"views": 420_000, "likes": 48_000},
        })
        weak = metadata_analysis({
            "caption": "Dancinha na escola com os alunos",
            "duration_seconds": 31,
            "hashtags": [],
            "metrics": {"views": 420_000, "likes": 48_000},
        })
        self.assertEqual(strong["fit"], "high")
        self.assertGreater(strong["score"], weak["score"])
        self.assertIn("夫妻/情侣", strong["categories"])
        self.assertIn("整蛊/反转", strong["categories"])

    def test_normalizes_clockworks_item(self):
        post = normalize_apify_item({
            "id": "7412345",
            "text": "Pegadinha com a esposa",
            "createTime": 1_725_000_000,
            "authorMeta": {"name": "Creator", "nickName": "Creator BR", "avatar": "https://example.com/a.jpg"},
            "videoMeta": {"duration": 27, "coverUrl": "https://example.com/c.jpg"},
            "playCount": 120_000,
            "diggCount": 13_000,
            "commentCount": 350,
            "shareCount": 900,
            "hashtags": [{"name": "casal"}],
        })
        self.assertIsNotNone(post)
        self.assertEqual(post["id"], "tiktok:7412345")
        self.assertEqual(post["creator_username"], "creator")
        self.assertEqual(post["metrics"]["views"], 120_000)
        self.assertEqual(post["hashtags"], ["casal"])

    def test_normalizes_coregent_keyword_result(self):
        post = normalize_apify_item({
            "videoId": "7512345",
            "caption": "Funny couple prank at home",
            "authorUniqueId": "homecouple",
            "authorNickname": "Home Couple",
            "coverUrl": "https://example.com/cover.jpg",
            "videoUrl": "https://www.tiktok.com/@homecouple/video/7512345",
            "duration": 29,
            "views": 1_500_000,
            "likes": 120_000,
            "comments": 1_200,
            "shares": 4_500,
            "keyword": "couple prank",
        })
        self.assertIsNotNone(post)
        self.assertEqual(post["id"], "tiktok:7512345")
        self.assertEqual(post["creator_username"], "homecouple")
        self.assertEqual(post["metrics"]["views"], 1_500_000)
        self.assertEqual(post["matched_keyword"], "couple prank")
        self.assertEqual(post["discovery_mode"], "keyword")

    def test_human_decision_persists_when_metadata_refreshes(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.json"
            path.write_text(json.dumps({
                "version": 1,
                "posts": {"tiktok:1": {"id": "tiktok:1", "decision": "pending", "analysis": {"score": 50}}},
                "runs": [],
                "last_run": None,
            }), encoding="utf-8")
            radar = ContentRadar(path)
            radar.set_decision("tiktok:1", "selected", "适合翻拍")
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["posts"]["tiktok:1"]["decision"], "selected")
            self.assertEqual(saved["posts"]["tiktok:1"]["operator_note"], "适合翻拍")

    def test_daily_collection_is_paused_by_default(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.dict(os.environ, {"CONTENT_RADAR_DAILY_ENABLED": "1"}):
                radar = ContentRadar(Path(folder) / "state.json")
            self.assertFalse(radar.daily_enabled)
            self.assertFalse(radar.snapshot()["daily_enabled"])
            self.assertEqual(radar.snapshot()["collection_mode"], "manual")
            self.assertEqual(radar.snapshot()["max_results"], 40)

    def test_keyword_refresh_deduplicates_and_preserves_decision(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.json"
            radar = ContentRadar(path)
            item = {
                "videoId": "9001",
                "caption": "Couple comedy",
                "authorUniqueId": "couple",
                "videoUrl": "https://www.tiktok.com/@couple/video/9001",
                "views": 2_000_000,
            }
            with patch.dict(os.environ, {"APIFY_TOKEN": "test-token"}):
                with patch.object(radar, "_call_apify", return_value=[item, dict(item)]):
                    first = radar.refresh()
            self.assertTrue(first["ok"])
            self.assertEqual(first["run"]["new_posts"], 1)
            self.assertEqual(len(radar.snapshot()["posts"]), 1)
            radar.set_decision("tiktok:9001", "selected")
            with patch.dict(os.environ, {"APIFY_TOKEN": "test-token"}):
                with patch.object(radar, "_call_apify", return_value=[{**item, "views": 2_500_000}]):
                    second = radar.refresh()
            self.assertEqual(second["run"]["new_posts"], 0)
            self.assertEqual(second["run"]["updated_posts"], 1)
            saved = radar.snapshot()["posts"][0]
            self.assertEqual(saved["decision"], "selected")
            self.assertEqual(saved["metrics"]["views"], 2_500_000)

    def test_curated_batch_imports_once_without_overwriting_decisions(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.json"
            radar = ContentRadar(path)
            self.assertEqual(radar.import_curated_batch(), 20)
            self.assertEqual(radar.import_curated_batch(), 0)
            snapshot = radar.snapshot()
            self.assertEqual(len(snapshot["posts"]), 20)
            self.assertEqual(snapshot["counts"]["pending"], 20)
            radar.set_decision("tiktok:7675481187808300319", "selected")
            self.assertEqual(radar.import_curated_batch(), 0)
            selected = next(post for post in radar.snapshot()["posts"] if post["id"] == "tiktok:7675481187808300319")
            self.assertEqual(selected["decision"], "selected")

    def test_dashboard_defers_single_player_until_cover_click(self):
        html = (WEB_ROOT / "content-radar.html").read_text(encoding="utf-8")
        self.assertIn('class="cover-image"', html)
        self.assertIn("data-play-id", html)
        self.assertIn("www.tiktok.com/player/v1/", html)
        self.assertIn("activePlayer:null", html)
        self.assertIn("autoplay=1", html)
        self.assertNotIn("IntersectionObserver", html)
        self.assertNotIn('target="_blank"', html)

    def test_thumbnail_cache_saves_a_stable_local_cover(self):
        with tempfile.TemporaryDirectory() as folder:
            radar = ContentRadar(Path(folder) / "state.json")
            response = MagicMock()
            response.__enter__.return_value = response
            response.headers.get_content_type.return_value = "image/jpeg"
            response.read.return_value = b"jpeg-data"
            with patch("urllib.request.urlopen", return_value=response):
                result = radar._cache_thumbnail({"post_id": "123", "thumbnail_source_url": "https://example.com/cover.jpg"})
            self.assertEqual(result, ("123", "/content-radar-cover/123.jpg"))
            self.assertEqual((radar.cover_dir / "123.jpg").read_bytes(), b"jpeg-data")


if __name__ == "__main__":
    unittest.main()
