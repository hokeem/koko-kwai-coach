from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

import app


class StorageSafetyTests(unittest.TestCase):
    def test_redacts_embedded_binary_without_dropping_metadata(self) -> None:
        payload = {
            "model": "gemini-image",
            "candidates": [{"parts": [{"inlineData": {"mimeType": "image/jpeg", "data": "A" * 4096}}]}],
        }

        cleaned = app.redact_large_binary_payloads(payload)

        self.assertEqual(cleaned["model"], "gemini-image")
        inline = cleaned["candidates"][0]["parts"][0]["inlineData"]
        self.assertEqual(inline["mimeType"], "image/jpeg")
        self.assertEqual(inline["data"], "[binary payload omitted: 4096 chars]")

    def test_link_or_copy_keeps_both_public_names(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            preview = root / "storyboard_preview.jpg"
            cover = root / "storyboard_cover.jpg"
            preview.write_bytes(b"image-bytes")

            app.link_or_copy_file(preview, cover)

            self.assertEqual(cover.read_bytes(), b"image-bytes")
            if os.name != "nt":
                self.assertEqual(preview.stat().st_ino, cover.stat().st_ino)


if __name__ == "__main__":
    unittest.main()
