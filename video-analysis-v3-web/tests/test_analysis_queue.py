from __future__ import annotations

import sys
import threading
import unittest
import urllib.request
from collections import deque
from http.server import ThreadingHTTPServer
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
        self.assertEqual(result["remaining_running"], 0)
        self.assertEqual(result["remaining_queued"], 0)
        with mock.patch.object(app.subprocess, "Popen") as popen:
            app.execute_single_pipeline("waiting", 0, app.jobs["waiting"]["items"][0])
            popen.assert_not_called()
        self.assertEqual(app.build_system_queue_snapshot("waiting")["queued_count"], 0)

    def test_active_batch_between_items_is_not_requeued_or_counted_twice(self) -> None:
        app.jobs["live"]["items"] = [
            {"id": "live-item", "status": "completed", "stage": "completed", "video_url": "https://example.com/live"},
            {"id": "live-next", "status": "queued", "stage": "queued", "video_url": "https://example.com/next"},
        ]
        app.job_queue.append("live")  # A duplicate left by an earlier watchdog run.
        snapshot = app.build_system_queue_snapshot("waiting")
        self.assertEqual(snapshot["running_count"], 1)
        self.assertEqual(snapshot["queued_count"], 1)
        self.assertEqual([entry["job_id"] for entry in snapshot["queued_jobs"]], ["waiting"])
        app.job_queue.remove("live")
        with mock.patch.object(app, "item_output_ready", return_value=False), \
             mock.patch.object(app, "enqueue_job") as enqueue, \
             mock.patch.object(app, "save_jobs"):
            app.reconcile_stale_jobs()
        enqueue.assert_not_called()

    def test_polling_queued_item_does_not_read_entire_library(self) -> None:
        with mock.patch.object(app, "library_entry_exists", side_effect=AssertionError("library read")):
            result = app.public_item_view(app.jobs["waiting"]["items"][0])
        self.assertFalse(result["in_library"])

    def test_job_poll_does_not_run_global_reconciliation(self) -> None:
        app.jobs["waiting"]["status"] = "queued"
        server = ThreadingHTTPServer(("127.0.0.1", 0), app.AppHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with mock.patch.object(app, "reconcile_stale_jobs", side_effect=AssertionError("global scan")), \
                 mock.patch.object(app, "recompute_job_status", side_effect=AssertionError("full recompute")):
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                with opener.open(f"http://127.0.0.1:{server.server_port}/api/jobs/waiting", timeout=3) as response:
                    self.assertEqual(response.status, 200)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
