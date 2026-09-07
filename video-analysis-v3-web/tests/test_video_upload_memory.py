from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_ROOT = Path(__file__).resolve().parents[2] / "skills" / "video-analysis-v3" / "scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

import gemini_video_observe as observe  # noqa: E402
import hybrid_v2_pipeline as pipeline  # noqa: E402


class VideoUploadMemoryTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
