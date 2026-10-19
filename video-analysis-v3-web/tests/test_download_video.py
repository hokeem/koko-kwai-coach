from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]
DOWNLOAD_SCRIPT = REPO_ROOT / "skills" / "video-analysis-v3" / "scripts" / "download_video.py"
SPEC = importlib.util.spec_from_file_location("download_video", DOWNLOAD_SCRIPT)
assert SPEC and SPEC.loader
download_video = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(download_video)


class DownloadVideoTests(unittest.TestCase):
    def test_ytdlp_missing_records_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(download_video.shutil, "which", return_value=None):
            meta: dict[str, str] = {}

            self.assertFalse(download_video.try_ytdlp("https://www.tiktok.com/@demo/video/1", Path(tmp), meta))
            self.assertEqual(meta["yt_dlp_error"], "yt-dlp executable is not installed")

    def test_ytdlp_failure_redacts_sensitive_query_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            meta: dict[str, str] = {}
            with (
                mock.patch.object(download_video.shutil, "which", return_value="/usr/bin/yt-dlp"),
                mock.patch.object(
                    download_video,
                    "run",
                    return_value=(1, "", "ERROR token=secret signature=private-key"),
                ),
            ):
                self.assertFalse(download_video.try_ytdlp("https://www.tiktok.com/@demo/video/1", Path(tmp), meta))

            self.assertNotIn("secret", meta["yt_dlp_error"])
            self.assertNotIn("private-key", meta["yt_dlp_error"])
            self.assertIn("token=***", meta["yt_dlp_error"])
            self.assertIn("signature=***", meta["yt_dlp_error"])

    def test_ytdlp_normalizes_downloaded_file_to_mp4_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp)
            destination.mkdir(exist_ok=True)

            def fake_run(_command: list[str]) -> tuple[int, str, str]:
                (destination / "source.webm").write_bytes(b"video")
                return 0, "", ""

            with (
                mock.patch.object(download_video.shutil, "which", return_value="/usr/bin/yt-dlp"),
                mock.patch.object(download_video, "run", side_effect=fake_run),
            ):
                self.assertTrue(download_video.try_ytdlp("https://www.tiktok.com/@demo/video/1", destination))

            self.assertTrue((destination / "source.mp4").is_file())
            self.assertFalse((destination / "source.webm").exists())


if __name__ == "__main__":
    unittest.main()
