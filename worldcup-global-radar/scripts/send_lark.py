#!/usr/bin/env python3
import json
import os
import re
import sys
import urllib.request


def section(text: str, name: str) -> str:
    m = re.search(rf"## {re.escape(name)}\n(?P<body>.*?)(?=\n## |\Z)", text, flags=re.S)
    return m.group("body").strip() if m else ""


def items(section_text: str) -> list[dict[str, str]]:
    chunks = re.split(r"\n(?=\d+\. (?:原文标题|中文标题)：)", section_text.strip())
    out = []
    for chunk in chunks:
        if not chunk.strip():
            continue
        item = {}
        for key in ["原文标题", "中文标题", "发布时间", "背景", "热度等级", "证据", "建议玩法", "链接"]:
            m = re.search(rf"{key}：(?P<value>.*?)(?=\n(?:原文标题|中文标题|发布时间|背景|热度等级|证据|建议玩法|链接)：|\Z)", chunk, flags=re.S)
            if m:
                item[key] = " ".join(m.group("value").strip().split())
        if item:
            out.append(item)
    return out


def block(title: str, rows: list[dict[str, str]], limit: int = 3) -> str:
    if not rows:
        return f"**{title}**\n暂无符合条件内容"
    lines = [f"**{title}**"]
    for idx, row in enumerate(rows[:limit], 1):
        lines.append(f"{idx}. **{row.get('中文标题', '未命名')}**")
        if row.get("原文标题"):
            lines.append(f"   原文：{row['原文标题']}")
        if row.get("热度等级"):
            lines.append(f"   🔥 {row['热度等级']}")
        detail = row.get("建议玩法") or row.get("证据") or row.get("背景", "")
        if detail:
            lines.append(f"   重点：{detail[:180]}")
        if row.get("链接"):
            lines.append(f"   链接：{row['链接']}")
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
    text = open(summary_path, "r", encoding="utf-8").read()
    a = items(section(text, "模块A｜全球权威要闻"))
    b = items(section(text, "模块B｜全球社媒/舆情热点"))
    c = items(section(text, "模块C｜今日运营机会"))
    base = public_url.rstrip("/")
    report_url = f"{base}/reports/{report_date}.html"
    text_url = f"{base}/reports/{report_date}.md"
    important = a[0].get("链接") if a else report_url
    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": "blue", "title": {"tag": "plain_text", "content": f"世界杯2026全球热点雷达 · {report_date}"}},
            "elements": [
                {"tag": "markdown", "content": f"**更新时间：{report_date}**\n过去3天窗口｜全球源筛选｜真实可访问链接优先。\n今日入选：权威要闻 **{len(a)}** 条｜舆情热点 **{len(b)}** 条｜运营机会 **{len(c)}** 条"},
                {"tag": "hr"},
                {"tag": "markdown", "content": block("模块A｜全球权威要闻", a)},
                {"tag": "hr"},
                {"tag": "markdown", "content": block("模块B｜全球社媒/舆情热点", b)},
                {"tag": "hr"},
                {"tag": "markdown", "content": block("模块C｜今日运营机会", c)},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "打开全球图文版"}, "url": report_url, "type": "primary"},
                    {"tag": "button", "text": {"tag": "plain_text", "content": "全球雷达首页"}, "url": base + "/", "type": "default"},
                    {"tag": "button", "text": {"tag": "plain_text", "content": "文字版"}, "url": text_url, "type": "default"},
                    {"tag": "button", "text": {"tag": "plain_text", "content": "最重要来源"}, "url": important, "type": "default"},
                ]},
            ],
        },
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(webhook, data=data, headers={"Content-Type": "application/json; charset=utf-8"}, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        print(f"Lark response {resp.status}: {resp.read().decode('utf-8', 'ignore')[:500]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
