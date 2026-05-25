#!/usr/bin/env python3
import json
import os
import sys
import urllib.request


def main() -> int:
    webhook = os.environ.get("LARK_BOT_WEBHOOK")
    if not webhook:
        print("LARK_BOT_WEBHOOK is not set; skipping Lark delivery.")
        return 0

    if len(sys.argv) < 4:
        print("Usage: send_lark.py <date> <summary_md_path> <public_url>", file=sys.stderr)
        return 2

    report_date, summary_path, public_url = sys.argv[1:4]
    with open(summary_path, "r", encoding="utf-8") as f:
        text = f.read().strip()

    preview = text[:3200]
    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "green",
                "title": {"tag": "plain_text", "content": f"世界杯2026巴西区热点播报 · {report_date}"},
            },
            "elements": [
                {"tag": "markdown", "content": preview},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "打开图文版日报"},
                            "url": public_url,
                            "type": "primary",
                        }
                    ],
                },
            ],
        },
    }

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook,
        data=data,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", "ignore")
        print(f"Lark response {resp.status}: {body[:500]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
