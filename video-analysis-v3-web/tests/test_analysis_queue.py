from __future__ import annotations

import sys
import unittest
from collections import deque
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app  # noqa: E402


class AnalysisQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_jobs = app.jobs
        self.original_queue = app.job_queue
        self.original_queued_ids = app.queued_job_ids
        self.original_active_ids = app.active_job_ids
        app.jobs = {
            "old": {"id": "old", "items": [{"id": "old-item", "status": "running", "stage": "gemini_analysis", "video_url": "https://example.com/old"}]},
            "live": {"id": "live", "items": [{"id": "live-item", "status": "running", "stage": "download", "video_url": "https://example.com/live"}]},
            "waiting": {"id": "waiting", "items": [{"id": "waiting-item", "status": "queued", "stage": "queued", "video_url": "https://example.com/waiting"}]},
        }
        app.job_queue = deque(["waiting"])
        app.queued_job_ids = {"waiting"}
        app.active_job_ids = {"live"}

    def tearDown(self) -> None:
        app.jobs = self.original_jobs
        app.job_queue = self.original_queue
        app.queued_job_ids = self.original_queued_ids
        app.active_job_ids = self.original_active_ids
        for item_id in ("old-item", "live-item", "waiting-item"):
            app.clear_item_cancelled(item_id)

    def test_snapshot_ignores_stale_running_record_and_shows_ahead_progress(self) -> None:
        snapshot = app.build_system_queue_snapshot("waiting")
        self.assertEqual(snapshot["running_count"], 1)
        self.assertEqual(snapshot["queued_count"], 1)
        self.assertEqual(snapshot["current_job_ahead"], 1)
        self.assertEqual(snapshot["ahead_workloads"][0]["job_id"], "live")
        self.assertGreater(snapshot["ahead_workloads"][0]["progress_percent"], 0)

    def test_stop_clears_queue_and_prevents_pipeline_start(self) -> None:
        with mock.patch.object(app, "save_jobs"):
            result = app.stop_all_tasks()
        self.assertEqual(result["stopped_jobs"], 3)
        self.assertEqual(len(app.job_queue), 0)
        self.assertTrue(app.jobs["waiting"]["stop_requested"])
        with mock.patch.object(app.subprocess, "Popen") as popen:
            app.execute_single_pipeline("waiting", 0, app.jobs["waiting"]["items"][0])
            popen.assert_not_called()
        self.assertEqual(app.build_system_queue_snapshot("waiting")["queued_count"], 0)


if __name__ == "__main__":
    unittest.main()
