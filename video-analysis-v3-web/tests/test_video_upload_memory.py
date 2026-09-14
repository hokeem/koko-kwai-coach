from __future__ import annotations

import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock


SCRIPTS_ROOT = Path(__file__).resolve().parents[2] / "skills" / "video-analysis-v3" / "scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

import gemini_video_observe as observe  # noqa: E402
import hybrid_v2_pipeline as pipeline  # noqa: E402


class VideoUploadMemoryTests(unittest.TestCase):
    def test_streamlined_pipeline_uses_latest_two_video_models(self) -> None:
        self.assertEqual(pipeline.DEFAULT_PRIMARY_VIDEO_MODEL, "gemini-3.8-flash")
        self.assertEqual(pipeline.DEFAULT_SECONDARY_VIDEO_MODEL, "gemini-3.7-flash")
        self.assertEqual(pipeline.PRIMARY_FALLBACK_MODELS[:2], ["gemini-3.8-flash", "gemini-3.7-flash"])
        self.assertEqual(pipeline.SUPPLEMENT_FALLBACK_MODELS[:2], ["gemini-3.7-flash", "gemini-3.8-flash"])

    def setUp(self) -> None:
        observe._ACTIVE_FILE_CACHE.clear()

    def test_default_video_route_uses_files_api(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "source.mp4"
            video.write_bytes(b"small-video")
            with mock.patch.object(pipeline, "files_api_observe", return_value=({}, {})) as files_api:
                pipeline.run_video_json_prompt(video, "key", "model", "prompt")

            files_api.assert_called_once()

    def test_explicit_legacy_inline_limit_still_uses_files_api(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "source.mp4"
            video.write_bytes(b"small-video")
            with mock.patch.object(pipeline, "files_api_observe", return_value=({}, {})) as files_api:
                pipeline.run_video_json_prompt(video, "key", "model", "prompt", inline_max_mb=999)

            files_api.assert_called_once()

    def test_files_api_reuses_uploaded_video_in_same_process(self) -> None:
        active_file = {"name": "files/123", "uri": "https://files.example/123", "state": "ACTIVE"}
        generate = {"candidates": [{"content": {"parts": [{"text": "{}"}]}}]}
        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "source.mp4"
            video.write_bytes(b"video-content")
            with mock.patch.object(observe, "upload_file", return_value={"file": active_file}) as upload, \
                 mock.patch.object(observe, "get_file", return_value={"file": active_file}), \
                 mock.patch.object(observe, "retry_call", return_value=generate):
                _, first_raw = observe.files_api_observe(video, "key", "model-a", "prompt-a", "video/mp4")
                _, second_raw = observe.files_api_observe(video, "key", "model-b", "prompt-b", "video/mp4")

            upload.assert_called_once()
            self.assertFalse(first_raw["upload_reused"])
            self.assertTrue(second_raw["upload_reused"])

    def test_parallel_video_analysts_share_one_upload(self) -> None:
        active_file = {"name": "files/parallel", "uri": "https://files.example/parallel", "state": "ACTIVE"}
        generate = {"candidates": [{"content": {"parts": [{"text": "{}"}]}}]}

        def slow_upload(*_args, **_kwargs):
            time.sleep(0.05)
            return {"file": active_file}

        with tempfile.TemporaryDirectory() as temp_dir:
            video = Path(temp_dir) / "source.mp4"
            video.write_bytes(b"video-content")
            with mock.patch.object(observe, "upload_file", side_effect=slow_upload) as upload, \
                 mock.patch.object(observe, "get_file", return_value={"file": active_file}), \
                 mock.patch.object(observe, "retry_call", return_value=generate):
                with ThreadPoolExecutor(max_workers=2) as executor:
                    first = executor.submit(observe.files_api_observe, video, "key", "model-a", "prompt-a", "video/mp4")
                    second = executor.submit(observe.files_api_observe, video, "key", "model-b", "prompt-b", "video/mp4")
                    first.result()
                    second.result()

            upload.assert_called_once()

    def test_consolidated_review_preserves_legacy_artifacts(self) -> None:
        review = {
            "story_spine_alignment": {"status": "pass"},
            "fact_consistency": {"status": "pass", "issues": []},
            "causal_coherence": {"status": "pass", "issues": []},
            "recommended_action": "proceed",
            "accepted_pipeline": "merged",
            "accepted_story_spine": "人物误会后完成反转。",
            "guardrails": ["不要补写人物关系"],
        }

        comparison, logic_audit, arbitration = pipeline.build_legacy_review_views(review)

        self.assertEqual(comparison["story_spine_alignment"]["status"], "pass")
        self.assertEqual(logic_audit["fact_consistency"]["status"], "pass")
        self.assertEqual(arbitration["accepted_pipeline"], "merged")
        self.assertEqual(arbitration["accepted_story_spine"], "人物误会后完成反转。")


if __name__ == "__main__":
    unittest.main()
