from __future__ import annotations

import io
import http.client
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
from pathlib import Path
from unittest.mock import patch

import app


class CreatorAdminOverviewUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = app.creator_admin_html("creators")

    def test_summary_cards_have_no_explanatory_microcopy(self) -> None:
        start = self.html.index("function creatorSummaryCardsHtml")
        end = self.html.index("function refreshCreatorSummaryCards", start)
        summary_source = self.html[start:end]
        self.assertIn('analyticsSummaryCard("注册用户",data.registered)', summary_source)
        self.assertIn('analyticsSummaryCard("脚本打开次数",data.scriptCount)', summary_source)
        self.assertIn('analyticsSummaryCard("回传数",data.returnCount)', summary_source)
        self.assertNotIn("原始事件", summary_source)
        self.assertNotIn("实时查询", summary_source)

    def test_registration_sort_filters_before_paginating(self) -> None:
        self.assertIn('data-creator-sort="registered_desc"', self.html)
        self.assertIn("creatorRegistrationTime(b)-creatorRegistrationTime(a)", self.html)
        self.assertIn("return creatorSortedRows(rows)", self.html)
        self.assertIn('creatorPage=1;selectedCreatorId="";renderCreatorPocFilters();renderCreators()', self.html)

    def test_monthly_details_start_collapsed_after_reload(self) -> None:
        self.assertIn('.filter(key=>key!=="daily:root")', self.html)
        self.assertIn('creatorDetailOpenAttr("daily:root")', self.html)

    def test_campaign_dashboard_appears_in_creator_overview(self) -> None:
        self.assertIn('data-campaign-view', self.html)
        self.assertIn('data-campaign-back', self.html)
        self.assertIn('list.innerHTML=campaignDashboardHtml()', self.html)
        self.assertNotIn('creatorSummaryCardsHtml()}${campaignDashboardHtml()', self.html)
        self.assertIn('data-campaign-copy', self.html)
        self.assertIn('data-campaign-refresh', self.html)
        self.assertIn('/api/creator-admin/campaign-dashboard?month=', self.html)
        self.assertIn('推广渠道数据', self.html)
        self.assertIn('正在查询最新数据', self.html)
        self.assertIn('campaign-spinner', self.html)
        self.assertIn('setInterval(()=>{if(creatorCampaignView', self.html)
        self.assertIn('当前显示上次成功查询的数据', self.html)
        self.assertIn('data.from_cache?', self.html)
        self.assertIn('loadCampaignDashboard()},60000)', self.html)
        self.assertIn('const CREATOR_REALTIME_MS=60000', self.html)
        self.assertIn('if(document.hidden||creatorCampaignView||creatorRealtimeBusy', self.html)

    def test_campaign_dashboard_serves_last_good_result_during_upstream_502(self) -> None:
        first = {"ok": True, "month": "2026-10", "rows": [{"source": "inbox", "visits": 3}]}
        server = app.ThreadingHTTPServer(("127.0.0.1", 0), app.AppHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory, patch.object(app, "DATA_ROOT", Path(directory)), patch.object(
                app, "creator_admin_remote_json", side_effect=[(200, first), (502, {"error": "upstream failed"})]
            ):
                connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
                try:
                    headers = {"Cookie": f"{app.CREATOR_ADMIN_AUTH_COOKIE}={urllib.parse.quote(app.CREATOR_ADMIN_PASSWORD)}"}
                    results = []
                    for _ in range(2):
                        connection.request("GET", "/api/creator-admin/campaign-dashboard?month=2026-10", headers=headers)
                        response = connection.getresponse()
                        results.append((response.status, json.loads(response.read())))
                finally:
                    connection.close()
            self.assertEqual([status for status, _ in results], [200, 200])
            self.assertEqual(results[1][1]["rows"], first["rows"])
            self.assertTrue(results[1][1]["from_cache"])
            self.assertIn("cached_at", results[1][1])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)

    def test_upstream_html_502_is_not_returned_as_error_text(self) -> None:
        upstream = urllib.error.HTTPError(
            "https://example.test/api/admin/campaign-dashboard", 502, "Bad Gateway",
            {"Content-Type": "text/html"}, io.BytesIO(b"<html>huge Render error page</html>"),
        )
        with patch.object(app.urllib.request, "build_opener") as build_opener:
            build_opener.return_value.open.side_effect = upstream
            status, payload = app.creator_admin_remote_json("/api/admin/campaign-dashboard", attempts=1)
        self.assertEqual(status, 502)
        self.assertNotIn("<html>", payload["error"])
        self.assertIn("暂时不可用", payload["error"])

    def test_campaign_link_picker_uses_simple_channels(self) -> None:
        start = self.html.index('function campaignDashboardHtml()')
        end = self.html.index('function creatorOverviewHtml()', start)
        picker = self.html[start:end]
        self.assertIn('const options=["email","inbox","social","other"]', picker)
        self.assertIn('inbox:"站内信"', picker)
        self.assertNotIn('const options=["email","instagram"', picker)
        self.assertIn('data-campaign-month', picker)
        self.assertIn('访问账号', picker)
        self.assertIn('campaignAccountsHtml(r)', picker)
        self.assertIn('未识别', self.html)
        self.assertIn('campaignSource==="inbox"', self.html)
        self.assertIn(app.CREATOR_CENTER_BASE_URL + '/inbox', self.html)


if __name__ == "__main__":
    unittest.main()
