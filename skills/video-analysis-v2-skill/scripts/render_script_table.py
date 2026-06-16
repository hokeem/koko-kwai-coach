#!/usr/bin/env python3
"""Render sample-compatible script_table.html from script_table.json."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "assets" / "script-table-template.html"

LOCALE_COPY = {
    "zh": {
        "html_lang": "zh-CN",
        "default_title": "视频总结归纳 + 脚本表",
        "source_link": "视频链接",
        "summary_title": "视频整体内容总结",
        "core_points_title": "",
        "replaceable_title": "替换方案",
        "table_title": "脚本表",
        "time_col": "时间",
        "visual_col": "画面内容",
        "action_col": "动作",
        "dialogue_col": "关键对白/旁白",
        "pending_label": "待分析",
        "pending_text": "本视频未提供足够信息，需要结合片段结构补充。",
    },
    "pt": {
        "html_lang": "pt-BR",
        "default_title": "Resumo do vídeo + Tabela do roteiro",
        "source_link": "Link do vídeo",
        "summary_title": "Resumo geral do vídeo",
        "core_points_title": "",
        "replaceable_title": "Planos de substituição",
        "table_title": "Tabela do roteiro",
        "time_col": "Tempo",
        "visual_col": "Conteúdo visual",
        "action_col": "Ação",
        "dialogue_col": "Diálogos",
        "pending_label": "Pendente",
        "pending_text": "Este vídeo não forneceu informação suficiente; é preciso complementar com a estrutura dos trechos.",
    },
}


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def render_rows(data: dict) -> str:
    rows = []
    for row in data.get("rows", []):
        visual = esc(row.get("visual_content", "")).replace("\n", "<br>")
        dialogue = esc(row.get("dialogue_or_audio", "")).replace("\n", "<br>")
        rows.append(
            "<tr>"
            f"<td>{esc(row.get('time', ''))}</td>"
            f"<td>{visual}</td>"
            f"<td>{esc(row.get('action', '')).replace(chr(10), '<br>')}</td>"
            f"<td>{dialogue}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def normalize_items(value: object) -> list[dict]:
    if not value:
        return []
    if isinstance(value, list):
        items = []
        for item in value:
            if isinstance(item, dict):
                items.append(item)
            else:
                items.append({"label": "", "text": str(item)})
        return items
    if isinstance(value, dict):
        return [{"label": key, "text": text} for key, text in value.items()]
    return [{"label": "", "text": str(value)}]


def render_insight_card(title: str, items: object, locale: str) -> str:
    blocks = []
    for item in normalize_items(items):
        label = item.get("label") or item.get("title") or item.get("name") or "要点"
        text = item.get("text") or item.get("description") or item.get("value") or ""
        if not text:
            continue
        blocks.append(f'<div class="insight"><b>{esc(label)}</b><div>{esc(text).replace(chr(10), "<br>")}</div></div>')
    if not blocks:
        copy = LOCALE_COPY.get(locale, LOCALE_COPY["zh"])
        blocks.append(f'<div class="insight"><b>{esc(copy["pending_label"])}</b><div>{esc(copy["pending_text"])}</div></div>')
    return f'<div class="card"><h2>{esc(title)}</h2><div class="insight-grid">{"".join(blocks)}</div></div>'


def render(data: dict, base_dir: Path, locale: str = "zh") -> str:
    template = TEMPLATE.read_text(encoding="utf-8")
    copy = LOCALE_COPY.get(locale, LOCALE_COPY["zh"])
    template = template.replace('lang="zh-CN"', f'lang="{copy["html_lang"]}"')
    title = data.get("title", copy["default_title"])
    source_url = data.get("source_url", "")
    title_card = (
        '<div class="card">'
        f"<h1>{esc(title)}</h1>"
        f'<div class="meta">Route: {esc(data.get("route", ""))} · Audio information score: {esc(data.get("audio_information_score", ""))}<br>'
        f'{esc(copy["source_link"])}：<a href="{esc(source_url)}">{esc(source_url)}</a></div>'
        "</div>"
    )
    summary_card = (
        f'<div class="card"><h2>{esc(copy["summary_title"])}</h2>'
        f'<div class="summary">{esc(data.get("whole_video_summary", ""))}</div></div>'
    )
    storyboard_cover = str(data.get("storyboard_cover_url") or "").strip()
    storyboard_card = (
        f'<div class="card"><h2>{"分解示意图" if locale == "zh" else "Capa em storyboard"}</h2>'
        f'<div class="video-frame-wrap"><img class="video-frame" src="{esc(storyboard_cover)}" alt=""></div></div>'
        if storyboard_cover
        else ""
    )
    table_card = (
        f'<div class="card"><h2>{esc(copy["table_title"])}</h2><table class="script-table"><thead><tr>'
        f"<th>{esc(copy['time_col'])}</th><th>{esc(copy['visual_col'])}</th><th>{esc(copy['action_col'])}</th><th>{esc(copy['dialogue_col'])}</th>"
        "</tr></thead><tbody>"
        + render_rows(data)
        + "</tbody></table></div>"
    )
    return (
        template.replace("{{ title }}", esc(title))
        .replace("{{ title_card }}", title_card)
        .replace("{{ summary_card }}", summary_card)
        .replace("{{ video_card }}", storyboard_card)
        .replace("{{ core_viral_points_card }}", "")
        .replace("{{ replaceable_parts_card }}", render_insight_card(copy["replaceable_title"], data.get("replaceable_parts"), locale))
        .replace("{{ table_card }}", table_card)
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("json_file")
    parser.add_argument("--output", default="script_table.html")
    parser.add_argument("--locale", default="zh", choices=["zh", "pt"])
    args = parser.parse_args()

    json_path = Path(args.json_file)
    data = json.loads(json_path.read_text(encoding="utf-8"))
    html_text = render(data, json_path.parent, args.locale)
    Path(args.output).write_text(html_text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
