import copy
import unittest
from unittest import mock

import app


class ReferenceVideoApiTests(unittest.TestCase):
    def test_hidden_entry_has_stable_replacement_metadata(self) -> None:
        entry = {
            "entry_id": "a" * 32,
            "video_url": "https://www.kwai.com/@creator/video/123",
            "reference_video_enabled": False,
        }

        state = app.reference_video_state(entry)

        self.assertEqual(state["reference_video_status"], "hidden_pending_replacement")
        self.assertEqual(state["reference_video_replacement_key"], "a" * 32)
        self.assertFalse(state["reference_video_enabled"])

    def test_replacement_updates_url_state_and_audit_history(self) -> None:
        entry_id = "b" * 32
        source = [{
            "entry_id": entry_id,
            "video_url": "https://www.kwai.com/@creator/video/old",
            "reference_video_enabled": False,
            "reference_video_status": "hidden_pending_replacement",
        }]
        saved = []

        def save_entries(entries):
            saved.extend(copy.deepcopy(entries))
            return True

        with mock.patch.object(app, "load_library_entries", return_value=copy.deepcopy(source)), \
             mock.patch.object(app, "save_library_entries", side_effect=save_entries), \
             mock.patch.object(app, "save_jobs"):
            original_jobs = app.jobs
            try:
                app.jobs = {}
                result = app.update_library_reference_video(entry_id, {
                    "video_url": "https://www.tiktok.com/@creator/video/999",
                    "reference_video_enabled": True,
                    "request_id": "replace-999",
                    "source": "batch_followup",
                })
            finally:
                app.jobs = original_jobs

        self.assertIsNotNone(result)
        self.assertEqual(saved[0]["video_url"], "https://www.tiktok.com/@creator/video/999")
        self.assertTrue(saved[0]["reference_video_enabled"])
        self.assertEqual(saved[0]["reference_video_status"], "active")
        self.assertEqual(saved[0]["reference_video_replacement_key"], entry_id)
        self.assertEqual(saved[0]["reference_video_original_url"], "https://www.kwai.com/@creator/video/old")
        self.assertEqual(saved[0]["reference_video_request_id"], "replace-999")
        self.assertEqual(len(saved[0]["reference_video_history"]), 1)


if __name__ == "__main__":
    unittest.main()
