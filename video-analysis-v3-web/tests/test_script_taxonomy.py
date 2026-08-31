from __future__ import annotations

import sys
import unittest
from pathlib import Path


WEB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_ROOT))

import app  # noqa: E402


class ScriptTaxonomyTests(unittest.TestCase):
    def test_twenty_seconds_is_first_bucket(self) -> None:
        self.assertEqual(app.creator_duration_bucket(20), "dur_1_20")
        self.assertEqual(app.creator_duration_bucket(20.01), "dur_20_60")

    def test_multi_role_and_story_acting_can_coexist(self) -> None:
        entry = {
            "format_tags": ["multi_role", "story_acting"],
            "taxonomy_source": "manual",
            "duration_bucket": "dur_20_60",
        }
        app.ensure_script_taxonomy_fields(entry, source="test")
        self.assertEqual(entry["format_tags"], ["multi_role", "story_acting"])

    def test_boss_employee_does_not_imply_construction_site(self) -> None:
        entry = {
            "title": "老板在办公室批评员工",
            "relationship_tags": ["boss_employee"],
            "taxonomy_source": "manual",
            "duration_bucket": "dur_20_60",
        }
        app.ensure_script_taxonomy_fields(entry, source="test")
        self.assertNotIn("construction_site", entry["location_tags"])

    def test_adult_humor_portuguese_label(self) -> None:
        self.assertEqual(app.taxonomy_tag_labels("content", ["adult_humor"], "pt"), ["Humor adulto"])

    def test_payload_accepts_ids_and_labels(self) -> None:
        parsed, errors = app.parse_taxonomy_tags_payload(
            {
                "relationship_tags": ["couple", "家庭"],
                "format_tags": ["剧情演绎", "multi_role"],
                "location_tags": ["家里"],
                "content_tags": ["Humor adulto"],
            }
        )
        self.assertFalse(errors)
        self.assertEqual(parsed["relationship"], ["couple", "family"])
        self.assertEqual(parsed["format"], ["story_acting", "multi_role"])
        self.assertEqual(parsed["location"], ["home"])
        self.assertEqual(parsed["content"], ["adult_humor"])

    def test_manual_taxonomy_is_not_expanded_by_keyword_inference(self) -> None:
        entry = {
            "title": "朋友在工地整蛊老板",
            "summary": "一场家庭争吵和游戏挑战",
            "relationship_tags": ["couple"],
            "format_tags": [],
            "location_tags": [],
            "content_tags": [],
            "duration_bucket": "dur_20_60",
            "taxonomy_source": "manual",
        }

        app.ensure_script_taxonomy_fields(entry, source="test")

        self.assertEqual(entry["relationship_tags"], ["couple"])
        self.assertEqual(entry["format_tags"], [])
        self.assertEqual(entry["location_tags"], [])
        self.assertEqual(entry["content_tags"], [])


if __name__ == "__main__":
    unittest.main()
