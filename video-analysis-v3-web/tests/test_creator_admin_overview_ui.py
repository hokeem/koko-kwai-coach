from __future__ import annotations

import unittest

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
