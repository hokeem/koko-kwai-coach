from __future__ import annotations

import unittest

import app


class CreatorExportTests(unittest.TestCase):
    def test_detailed_export_uses_only_standard_creator_fields(self) -> None:
        html = app.creator_admin_html("creators")
        start = html.rfind("async function downloadCreatorDetailCsv")
        end = html.find('const CREATOR_USAGE_CUTOFF_DAY="2026-08-07"', start)

        self.assertGreater(start, 0)
        self.assertGreater(end, start)
        export_source = html[start:end]
        self.assertIn(
            '["作者名称","UID","电话","POC","投喂数量","回传数量","投喂脚本链接","回传视频链接"]',
            export_source,
        )
        self.assertIn(
            '["creator_name","uid","phone","poc","feed_count","return_count","script_url","return_video_url"]',
            export_source,
        )
        for old_field in (
            "record_type",
            "platform_open_count",
            "script_open_count",
            "total_duration_seconds",
        ):
            self.assertNotIn(old_field, export_source)

    def test_export_preserves_creator_script_and_return_relationships(self) -> None:
        html = app.creator_admin_html("creators")
        start = html.find("function creatorStandardDetailRows")
        end = html.find('const CREATOR_USAGE_CUTOFF_DAY="2026-08-07"', start)
        export_source = html[start:end]

        self.assertIn("for(const item of returns)", export_source)
        self.assertIn("script_url:scriptUrl", export_source)
        self.assertIn("return_video_url:item.video_url||item.return_url", export_source)
        self.assertIn('details.length?details:[{script_url:"",return_video_url:""}]', export_source)


if __name__ == "__main__":
    unittest.main()
