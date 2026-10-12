from __future__ import annotations

import unittest

import app


class CreatorQuickAccountTests(unittest.TestCase):
    def test_creator_admin_has_phone_only_account_flow(self) -> None:
        html = app.creator_admin_html("creators")

        self.assertIn('id="open-quick-account-modal"', html)
        self.assertIn('id="quick-account-form"', html)
        self.assertIn("添加新账号", html)
        self.assertIn("手机号后四位", html)
        self.assertIn("/api/creator-admin/accounts/quick-create", html)
        self.assertIn("作者卡片已创建", html)

    def test_quick_account_backend_marks_account_active(self) -> None:
        source = app.Path(app.__file__).read_text("utf-8")
        start = source.index('if parsed.path == "/api/creator-admin/accounts/quick-create"')
        end = source.index('if parsed.path == "/api/creator-admin/accounts":', start)
        handler = source[start:end]

        self.assertIn('"activate": True', handler)
        self.assertIn('"initial_password": phone[-4:]', handler)
        self.assertIn('"creator_card_created": True', handler)


if __name__ == "__main__":
    unittest.main()
