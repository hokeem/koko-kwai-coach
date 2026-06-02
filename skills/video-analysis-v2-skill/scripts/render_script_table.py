#!/usr/bin/env python3
"""Render sample-compatible script_table.html from script_table.json."""

from __future__ import annotations

import argparse
import base64
import html
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "assets" / "script-table-template.html"

LOCALE_COPY = {
    "zh": {
        "html_lang": "zh-CN",
        "default_title": "视频总结归纳 + 脚本表",
        "source_link": "视频链接",
        "summary_title": "视频整体内容总结",
        "core_points_title": "核心爆点",
        "replaceable_title": "可替换部分",
        "table_title": "脚本表",
        "mechanism_title": "包袱机制",
        "video_preview_title": "视频预览",
        "video_preview_hint": "点击脚本表的时间，可以跳到对应视频片段。",
        "video_preview_current": "当前",
        "video_link_col": "视频链接",
        "time_col": "时间",
        "visual_col": "画面内容",
        "action_col": "动作",
        "dialogue_col": "关键对白/旁白（中文忠实翻译）",
        "start_frame": "首帧",
        "end_frame": "尾帧",
        "pending_label": "待分析",
        "pending_text": "本视频未提供足够信息，需要结合片段结构补充。",
    },
    "pt": {
        "html_lang": "pt-BR",
        "default_title": "Resumo do vídeo + Tabela do roteiro",
        "source_link": "Link do vídeo",
        "summary_title": "Resumo geral do vídeo",
        "core_points_title": "Pontos centrais",
        "replaceable_title": "Partes substituíveis",
        "table_title": "Tabela do roteiro",
        "mechanism_title": "Mecanismo da piada",
        "video_preview_title": "Prévia do vídeo",
        "video_preview_hint": "Clique no tempo da tabela para pular para o trecho correspondente.",
        "video_preview_current": "Atual",
        "video_link_col": "Link do vídeo",
        "time_col": "Tempo",
        "visual_col": "Conteúdo visual",
        "action_col": "Ação",
        "dialogue_col": "Diálogo/narração (tradução fiel em português)",
        "start_frame": "Quadro inicial",
        "end_frame": "Quadro final",
        "pending_label": "Pendente",
        "pending_text": "Este vídeo não forneceu informação suficiente; é preciso complementar com a estrutura dos trechos.",
    },
}

CLOCK_TIME_PATTERN = re.compile(r"(?P<minute>\d{1,2}):(?P<second>\d{2})")
SECOND_RANGE_PATTERN = re.compile(
    r"(?P<start>\d+(?:\.\d+)?)\s*[-–—~至到]\s*(?P<end>\d+(?:\.\d+)?)\s*(?:s|秒)",
    re.IGNORECASE,
)
SECOND_PATTERN = re.compile(r"(?P<second>\d+(?:\.\d+)?)\s*(?:s|秒)", re.IGNORECASE)


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def parse_time_seconds(value: object) -> tuple[float | None, float | None]:
    text = "" if value is None else str(value)
    clock_matches = list(CLOCK_TIME_PATTERN.finditer(text))
    if clock_matches:
        seconds = [int(match.group("minute")) * 60 + int(match.group("second")) for match in clock_matches[:2]]
        start = float(seconds[0])
        end = float(seconds[1]) if len(seconds) > 1 and seconds[1] > start else None
        return (start, end)

    range_match = SECOND_RANGE_PATTERN.search(text)
    if range_match:
        start = float(range_match.group("start"))
        end = float(range_match.group("end"))
        return (start, end if end > start else None)

    second_match = SECOND_PATTERN.search(text)
    if second_match:
        return (float(second_match.group("second")), None)

    return (None, None)


def render_time_cell(value: object, has_video: bool) -> str:
    label = esc(value)
    if not has_video:
        return label
    start, _end = parse_time_seconds(value)
    if start is None:
        return label
    return f'<button class="time-jump" type="button" data-seek="{start:g}">{label}</button>'


def as_data_uri(path: str | None, base_dir: Path) -> str | None:
    if not path:
        return None
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    if not candidate.exists():
        return None
    mime = "image/png" if candidate.suffix.lower() == ".png" else "image/jpeg"
    encoded = base64.b64encode(candidate.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def render_frames(row: dict, base_dir: Path, locale: str) -> str:
    copy = LOCALE_COPY.get(locale, LOCALE_COPY["zh"])
    frames = [
        (copy["start_frame"], as_data_uri(row.get("start_frame"), base_dir)),
        (copy["end_frame"], as_data_uri(row.get("end_frame"), base_dir)),
    ]
    visible = [(cap, uri) for cap, uri in frames if uri]
    if not visible:
        return ""
    blocks = []
    for caption, uri in visible:
        blocks.append(
            f'<div class="frame"><img src="{uri}" alt="{esc(caption)}"><div class="cap">{esc(caption)}</div></div>'
        )
    return '<div class="frames">' + "".join(blocks) + "</div>"


def render_rows(data: dict, base_dir: Path, locale: str) -> str:
    rows = []
    source_url = data.get("source_url", "")
    has_video = (base_dir / "source.mp4").exists()
    for row in data.get("rows", []):
        row_url = row.get("source_url") or source_url
        start, end = parse_time_seconds(row.get("time", ""))
        row_attrs = ""
        if start is not None:
            row_attrs += f' data-start="{start:g}"'
            row_attrs += f' data-end="{(end if end is not None else start + 1):g}"'
        visual = esc(row.get("visual_content", "")).replace("\n", "<br>")
        visual += render_frames(row, base_dir, locale)
        dialogue = esc(row.get("dialogue_or_audio", "")).replace("\n", "<br>")
        rows.append(
            f"<tr{row_attrs}>"
            f'<td><a href="{esc(row_url)}">{esc(LOCALE_COPY.get(locale, LOCALE_COPY["zh"])["video_link_col"])}</a></td>'
            f"<td>{render_time_cell(row.get('time', ''), has_video)}</td>"
            f"<td>{visual}</td>"
            f"<td>{esc(row.get('action', '')).replace(chr(10), '<br>')}</td>"
            f"<td>{dialogue}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def render_video_card(base_dir: Path, locale: str) -> str:
    if not (base_dir / "source.mp4").exists():
        return ""
    copy = LOCALE_COPY.get(locale, LOCALE_COPY["zh"])
    return (
        '<div class="card video-card" id="video-preview">'
        f"<h2>{esc(copy['video_preview_title'])}</h2>"
        '<div class="video-review-layout">'
        '<video id="source-video" controls preload="metadata" src="source.mp4"></video>'
        '<div class="video-side">'
        f'<div class="video-hint">{esc(copy["video_preview_hint"])}</div>'
        f'<div class="video-current" id="video-current" data-label="{esc(copy["video_preview_current"])}">'
        f'{esc(copy["video_preview_current"])} 00:00</div>'
        "</div>"
        "</div>"
        "</div>"
    )


def render_mechanism(data: dict, locale: str) -> str:
    mechanism = data.get("mechanism")
    if not mechanism:
        return ""
    items = []
    for item in mechanism.get("items", []):
        items.append(f"<li><b>{esc(item.get('label', ''))}：</b>{esc(item.get('text', ''))}</li>")
    if not items:
        return ""
    title = mechanism.get("title", LOCALE_COPY.get(locale, LOCALE_COPY["zh"])["mechanism_title"])
    return f'<div class="card"><h2>{esc(title)}</h2><ul>{"".join(items)}</ul></div>'


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
    table_card = (
        f'<div class="card"><h2>{esc(copy["table_title"])}</h2><table><thead><tr>'
        f"<th>{esc(copy['video_link_col'])}</th><th>{esc(copy['time_col'])}</th><th>{esc(copy['visual_col'])}</th><th>{esc(copy['action_col'])}</th><th>{esc(copy['dialogue_col'])}</th>"
        "</tr></thead><tbody>"
        + render_rows(data, base_dir, locale)
        + "</tbody></table></div>"
    )
    return (
        template.replace("{{ title }}", esc(title))
        .replace("{{ title_card }}", title_card)
        .replace("{{ summary_card }}", summary_card)
        .replace("{{ video_card }}", render_video_card(base_dir, locale))
        .replace("{{ core_viral_points_card }}", render_insight_card(copy["core_points_title"], data.get("core_viral_points"), locale))
        .replace("{{ replaceable_parts_card }}", render_insight_card(copy["replaceable_title"], data.get("replaceable_parts"), locale))
        .replace("{{ table_card }}", table_card)
        .replace("{{ mechanism_card }}", render_mechanism(data, locale))
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
