from __future__ import annotations

import http.client
import io
import threading
import unittest
import urllib.parse
from email.message import Message
from unittest.mock import patch

from PIL import Image

import app


class Response(io.BytesIO):
    def __init__(self, data: bytes, url: str) -> None:
        super().__init__(data)
        self.url = url
        self.headers = Message()
        self.headers["Content-Type"] = "text/html; charset=utf-8"

    def geturl(self) -> str:
        return self.url


class ReviewCoverTests(unittest.TestCase):
    def test_enriches_script_cover_and_registers_video_source(self) -> None:
        submission_id = "a" * 32
        entry_id = "b" * 32
        payload = {"submissions": [{"submission_id": submission_id, "entry_id": entry_id,
                                    "video_url": "https://kwai-video.com/p/example"}]}
        cache = {"entries": [{"entry_id": entry_id, "storyboard_image_url": "/results/example/cover.png"}]}
        with patch.object(app, "load_creator_admin_scripts_cache", return_value=cache), \
             patch.dict(app.REVIEW_VIDEO_SOURCES, {}, clear=True):
            app.enrich_review_script_covers(payload)
            self.assertEqual(payload["submissions"][0]["review_script_cover_url"], "/results/example/cover.png")
            self.assertEqual(app.REVIEW_VIDEO_SOURCES[submission_id], "https://kwai-video.com/p/example")

    def test_recovers_valid_image_from_video_page_and_caches_it(self) -> None:
        submission_id = "c" * 32
        video_url = "https://kwai-video.com/p/example"
        image_url = "https://p16-kimg.kwai.net/cover.webp"
        page = f'<meta property="og:image" content="{image_url}">'.encode()
        image = io.BytesIO()
        Image.new("RGB", (2, 2), "orange").save(image, format="PNG")
        responses = [Response(page, video_url), Response(image.getvalue(), image_url)]
        with patch.dict(app.REVIEW_VIDEO_SOURCES, {submission_id: video_url}, clear=True), \
             patch.dict(app.REVIEW_COVER_CACHE, {}, clear=True), \
             patch.object(app.urllib.request, "build_opener") as build_opener:
            opener = build_opener.return_value
            opener.open.side_effect = responses
            result = app.fetch_review_video_cover(submission_id)
            self.assertEqual(result, ("image/png", image.getvalue()))
            self.assertEqual(app.fetch_review_video_cover(submission_id), result)
            self.assertEqual(opener.open.call_count, 2)

    def test_only_expected_domains_and_safe_redirects_are_allowed(self) -> None:
        self.assertTrue(app.review_media_url_allowed("https://kwai-video.com/p/example"))
        for url in ("http://127.0.0.1/private", "https://kwai.com.evil.test/", "https://kwai.com:8080/"):
            self.assertFalse(app.review_media_url_allowed(url))
        redirect = app.ReviewMediaRedirectHandler()
        request = app.urllib.request.Request("https://kwai.com/video")
        with self.assertRaises(ValueError):
            redirect.redirect_request(request, None, 302, "", {}, "http://127.0.0.1/private")

    def test_cover_endpoint_requires_admin(self) -> None:
        server = app.ThreadingHTTPServer(("127.0.0.1", 0), app.AppHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with patch.object(app, "fetch_review_video_cover", return_value=("image/png", b"image")) as fetch:
                connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    path = "/api/creator-admin/review-video-cover/" + "d" * 32
                    connection.request("GET", path)
                    response = connection.getresponse()
                    self.assertEqual(response.status, 401)
                    response.read()
                    fetch.assert_not_called()
                    headers = {"Cookie": f"{app.CREATOR_ADMIN_AUTH_COOKIE}={urllib.parse.quote(app.CREATOR_ADMIN_PASSWORD)}"}
                    connection.request("GET", path, headers=headers)
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    self.assertEqual(response.getheader("Content-Type"), "image/png")
                    self.assertEqual(response.read(), b"image")
                finally:
                    connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
