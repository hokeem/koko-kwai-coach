#!/usr/bin/env python3
import json
import os
import re
import sys
import urllib.request


def _section(text: str, name: str) -> str:
    pattern = rf"## {re.escape(name)}\n(?P<body>.*?)(?=\n## |\Z)"
    match = re.search(pattern, text, flags=re.S)
    return match.group("body").strip() if match else ""


def _items(section_text: str) -> list[dict[str, str]]:
    chunks = re.split(r"\n(?=\d+\. 葡语标题：)", section_text.strip())
    parsed = []
    for chunk in chunks:
        if not chunk.strip():
            continue
        fields = {}
        for key in ["葡语标题", "中文标题", "发布时间", "背景", "热度等级", "证据", "适合传播", "链接"]:
            match = re.search(rf"{key}：(?P<value>.*?)(?=\n(?:葡语标题|中文标题|发布时间|背景|热度等级|证据|适合传播|链接)：|\Z)", chunk, flags=re.S)
            if match:
                fields[key] = " ".join(match.group("value").strip().split())
        if fields:
            parsed.append(fields)
    return parsed


def _module_markdown(title: str, items: list[dict[str, str]], limit: int = 2) -> str:
    if not items:
        return f"**{title}**\n暂无符合条件内容"

    lines = [f"**{title}**"]
    for idx, item in enumerate(items[:limit], 1):
        cn = item.get("中文标题", "未命名")
        pt = item.get("葡语标题", "")
        heat = item.get("热度等级", "")
        evidence = item.get("证据") or item.get("适合传播") or ""
        link = item.get("链接", "")
        lines.append(f"{idx}. **{cn}**")
        if pt:
            lines.append(f"   葡语：{pt}")
        if heat:
            lines.append(f"   🔥 {heat}")
        if evidence:
            lines.append(f"   证据/玩法：{evidence}")
        if link:
            lines.append(f"   来源：{link}")
    return "\n".join(lines)


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

    report_url = public_url.rstrip("/") + f"/reports/{report_date}.html"
    text_url = public_url.rstrip("/") + f"/reports/{report_date}.md"
    module_a = _items(_section(text, "模块A：世界杯重要资讯"))
    module_b = _items(_section(text, "模块B：巴西全网热点"))
    module_c = _items(_section(text, "模块C：站内热点预测"))

    intro = (
        f"**更新时间：{report_date}**\n"
        "仅推送过去3天内、真实可访问、权威来源的世界杯相关内容。\n"
        f"今日入选：资讯 **{len(module_a)}** 条｜全网热点 **{len(module_b)}** 条｜站内预测 **{len(module_c)}** 条"
    )

    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "green",
                "title": {"tag": "plain_text", "content": f"世界杯2026巴西区热点播报 · {report_date}"},
            },
            "elements": [
                {"tag": "markdown", "content": intro},
                {"tag": "hr"},
                {"tag": "markdown", "content": _module_markdown("模块A｜世界杯重要资讯", module_a)},
                {"tag": "hr"},
                {"tag": "markdown", "content": _module_markdown("模块B｜巴西全网热点", module_b)},
                {"tag": "hr"},
                {"tag": "markdown", "content": _module_markdown("模块C｜站内热点预测", module_c)},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "打开图文版日报"},
                            "url": report_url,
                            "type": "primary",
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "查看日报首页"},
                            "url": public_url,
                            "type": "default",
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "文字版"},
                            "url": text_url,
                            "type": "default",
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
