from __future__ import annotations

import http.client
import io
import json
import threading
import unittest
import urllib.parse
import zipfile

import app


class DashboardXlsxExportTests(unittest.TestCase):
    def test_return_export_keeps_independent_video_url_and_is_real_xlsx(self) -> None:
        server = app.ThreadingHTTPServer(("127.0.0.1", 0), app.AppHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
            try:
                headers = {
                    "Cookie": f"{app.CREATOR_ADMIN_AUTH_COOKIE}={urllib.parse.quote(app.CREATOR_ADMIN_PASSWORD)}",
                    "Content-Type": "application/json",
                }
                video_url = "https://www.kwai.com/short-video/example?share=1"
                row = ["2026-11-03T12:00:00Z", "author-1", "kwai-1", "Creator", "自主回传", video_url, "", "pending_review"]
                connection.request("POST", "/api/creator-admin/dashboard-export", json.dumps({"kind": "returns", "rows": [row]}), headers)
                response = connection.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(response.getheader("Content-Type"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with zipfile.ZipFile(io.BytesIO(response.read())) as workbook:
                    shared = workbook.read("xl/sharedStrings.xml").decode("utf-8")
                    self.assertIn(video_url, shared)
                    self.assertIn("自主回传", shared)
                    self.assertIn("视频链接", shared)
            finally:
                connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
