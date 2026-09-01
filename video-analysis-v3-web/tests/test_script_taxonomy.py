from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock


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

    def test_manual_empty_dimensions_are_not_repopulated_by_llm(self) -> None:
        entry = {
            "relationship_tags": ["couple"],
            "format_tags": [],
            "location_tags": [],
            "content_tags": [],
            "duration_bucket": "dur_20_60",
            "taxonomy_source": "manual",
        }
        classified = {
            "relationship_tags": ["family"],
            "format_tags": ["story_acting"],
            "location_tags": ["home"],
            "content_tags": ["prank"],
            "taxonomy_source": "llm",
        }

        with mock.patch.object(app, "classify_script_taxonomy_with_llm", return_value=classified):
            app.apply_script_taxonomy_classification(entry, {}, {}, use_llm=True)

        self.assertEqual(entry["relationship_tags"], ["couple"])
        self.assertEqual(entry["format_tags"], [])
        self.assertEqual(entry["location_tags"], [])
        self.assertEqual(entry["content_tags"], [])
        self.assertEqual(entry["taxonomy_source"], "manual")

    def test_studio_uses_taxonomy_v2_manual_editor(self) -> None:
        markup = app.studio_html()
        self.assertIn('data-manual-taxonomy="${dimension}"', markup)
        self.assertIn('relationship: "人物关系"', markup)
        self.assertIn('duration: "时间"', markup)
        self.assertIn("标签已修改，保存或入库后生效", markup)
        self.assertIn(".manual-taxonomy-chip {\n      width: auto;", markup)
        self.assertNotIn("data-manual-content-type", markup)
        self.assertIn("data-manual-telekwai", markup)
        self.assertIn("Telekwai 脚本已隔离", markup)

    def test_script_admin_has_save_feedback(self) -> None:
        markup = app.creator_admin_html("scripts", library_mode=True)
        self.assertIn('id="script-taxonomy-feedback"', markup)
        self.assertIn("保存成功，新标签已同步到脚本库", markup)
        self.assertIn('key:"telekwai",label:"Telekwai"', markup)
        self.assertIn("data-telekwai-edit", markup)

    def test_telekwai_is_exclusive_and_unpublished(self) -> None:
        entry = {
            "telekwai": True,
            "published": True,
            "relationship_tags": ["couple"],
            "format_tags": ["story_acting"],
            "location_tags": ["home"],
            "content_tags": ["prank"],
            "duration_bucket": "dur_20_60",
        }

        app.ensure_script_taxonomy_fields(entry, source="test")

        self.assertTrue(entry["telekwai"])
        self.assertEqual(entry["script_type"], "telekwai")
        self.assertFalse(entry["published"])
        self.assertEqual(entry["duration_bucket"], "")
        for dimension in app.SCRIPT_TAG_DIMENSIONS:
            self.assertEqual(entry[f"{dimension}_tags"], [])

    def test_agent_job_can_create_telekwai_without_taxonomy(self) -> None:
        with mock.patch.object(app, "ensure_capacity_for_new_job"), mock.patch.object(app, "save_jobs"), mock.patch.object(app, "enqueue_job"):
            created = app.create_job(
                ["https://example.com/video"],
                source="agent_api",
                telekwai=True,
                taxonomy_tags={"relationship": ["couple"]},
            )
        try:
            item = app.jobs[created["id"]]["items"][0]
            self.assertTrue(item["telekwai"])
            self.assertEqual(item["script_type"], "telekwai")
            self.assertFalse(item["published"])
            self.assertEqual(item["relationship_tags"], [])
        finally:
            app.jobs.pop(created["id"], None)

    def test_manual_item_taxonomy_updates_all_dimensions(self) -> None:
        job_id = "test-job"
        original_jobs = app.jobs
        app.jobs = {
            job_id: {
                "id": job_id,
                "items": [{"id": "item-1", "status": "completed"}],
            }
        }
        try:
            with mock.patch.object(app, "save_jobs"):
                app.apply_manual_item_taxonomy(
                    job_id,
                    0,
                    {
                        "relationship_tags": ["couple"],
                        "format_tags": ["story_acting", "multi_role"],
                        "location_tags": ["home"],
                        "content_tags": ["prank"],
                        "duration_bucket": "dur_1_20",
                    },
                )
            item = app.jobs[job_id]["items"][0]
            self.assertEqual(item["format_tags"], ["story_acting", "multi_role"])
            self.assertEqual(item["duration_bucket"], "dur_1_20")
            self.assertEqual(item["taxonomy_source"], "manual")
        finally:
            app.jobs = original_jobs

    def test_manual_item_telekwai_clears_other_tags(self) -> None:
        job_id = "test-telekwai-job"
        original_jobs = app.jobs
        app.jobs = {
            job_id: {
                "id": job_id,
                "items": [{"id": "item-1", "status": "completed", "relationship_tags": ["couple"]}],
            }
        }
        try:
            with mock.patch.object(app, "save_jobs"):
                app.apply_manual_item_taxonomy(job_id, 0, {"telekwai": True})
            item = app.jobs[job_id]["items"][0]
            self.assertTrue(item["telekwai"])
            self.assertEqual(item["relationship_tags"], [])
            self.assertFalse(item["published"])
        finally:
            app.jobs = original_jobs


if __name__ == "__main__":
    unittest.main()
