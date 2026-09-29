import unittest
from unittest.mock import patch

import app


class AgentResourceCleanupTests(unittest.TestCase):
    def test_cleanup_reports_capacity_and_preserves_cleanup_boundaries(self) -> None:
        snapshots = [
            {"disk_free_mb": 250.0},
            {"disk_free_mb": 500.0},
        ]
        with (
            patch.object(app, "runtime_resource_snapshot", side_effect=snapshots),
            patch.object(app, "cleanup_finished_understanding_jobs", return_value={"freed_mb": 10.0}),
            patch.object(app, "cleanup_finished_heavy_artifacts", return_value={"freed_mb": 200.0}) as heavy,
            patch.object(app, "compact_finished_storyboard_images", return_value={"freed_mb": 25.0}) as storyboards,
            patch.object(app, "cleanup_orphan_result_dirs", return_value={"freed_mb": 40.0}),
            patch.object(app, "compact_completed_job_history", return_value=0),
        ):
            result = app.run_agent_resource_cleanup(aggressive=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["after"]["disk_free_mb"], 500.0)
        heavy.assert_called_once_with(aggressive=True)
        storyboards.assert_called_once_with(max_dirs=250)


if __name__ == "__main__":
    unittest.main()
