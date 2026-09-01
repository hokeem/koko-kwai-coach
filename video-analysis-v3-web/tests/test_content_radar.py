import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


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

    def test_known_multi_role_creator_gets_format_tag(self):
        post = normalize_apify_item({
            "id": "7512345",
            "text": "Tipos de pessoas em casa",
            "authorMeta": {"name": "edsontheodoro1"},
            "videoMeta": {"duration": 29},
        })
        self.assertIsNotNone(post)
        self.assertEqual(post["creator_tags"], ["一人分饰多角"])

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


if __name__ == "__main__":
    unittest.main()
