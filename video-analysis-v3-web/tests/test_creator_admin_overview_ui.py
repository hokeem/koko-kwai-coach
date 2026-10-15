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


if __name__ == "__main__":
    unittest.main()
