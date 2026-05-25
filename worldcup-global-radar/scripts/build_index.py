#!/usr/bin/env python3
import html
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def read_title(md_text: str) -> str:
    match = re.search(r"^#\s+(.+)$", md_text, flags=re.M)
    return match.group(1).strip() if match else "世界杯2026全球热点雷达"


def read_summary(md_text: str) -> str:
    match = re.search(r"背景：(?P<body>.*?)(?=\n热度等级：|\n链接：|\Z)", md_text, flags=re.S)
    if not match:
        return "全球权威新闻、社媒舆情和今日运营机会。"
    return " ".join(match.group("body").strip().split())[:180]


def read_cover(html_text: str) -> str:
    match = re.search(r"url\('(?P<url>https?://[^']+)'\)", html_text)
    return match.group("url") if match else ""


def collect_reports() -> list[dict[str, str]]:
    reports = []
    for md_path in sorted(REPORTS.glob("*.md"), reverse=True):
        date = md_path.stem
        html_path = REPORTS / f"{date}.html"
        md_text = md_path.read_text(encoding="utf-8")
        html_text = html_path.read_text(encoding="utf-8") if html_path.exists() else ""
        reports.append({
            "date": date,
            "title": read_title(md_text),
            "summary": read_summary(md_text),
            "cover": read_cover(html_text),
            "html": f"reports/{date}.html",
            "md": f"reports/{date}.md",
        })
    return reports


def main() -> int:
    reports = collect_reports()
    latest = reports[0] if reports else None
    latest_style = (
        f"background-image:linear-gradient(90deg,rgba(10,22,38,.9),rgba(10,22,38,.35)),url('{html.escape(latest['cover'])}')"
        if latest and latest["cover"] else
        "background:linear-gradient(135deg,#111c2c,#175c9b 55%,#e7b83f)"
    )
    cards = []
    for report in reports:
        cover_style = (
            f"background-image:linear-gradient(180deg,rgba(10,22,38,.08),rgba(10,22,38,.62)),url('{html.escape(report['cover'])}')"
            if report["cover"] else
            "background:linear-gradient(135deg,#175c9b,#e7b83f)"
        )
        cards.append(f"""
        <article class="card">
          <a class="thumb" style="{cover_style}" href="{html.escape(report['html'])}"><span>{html.escape(report['date'])}</span></a>
          <div class="body">
            <p class="eyebrow">Global Radar</p>
            <h3>{html.escape(report['title'])}</h3>
            <p>{html.escape(report['summary'])}</p>
            <div class="actions"><a href="{html.escape(report['html'])}">图文版</a><a href="{html.escape(report['md'])}">文字版</a></div>
          </div>
        </article>""")

    latest_block = ""
    if latest:
        latest_block = f"""
      <section class="latest" style="{latest_style}">
        <div>
          <p class="eyebrow light">Latest · {html.escape(latest['date'])}</p>
          <h2>{html.escape(latest['title'])}</h2>
          <p>{html.escape(latest['summary'])}</p>
          <div class="hero-actions"><a href="{html.escape(latest['html'])}">打开最新图文版</a><a href="{html.escape(latest['md'])}">查看文字版</a></div>
        </div>
      </section>"""

    page = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>世界杯2026全球热点雷达</title>
  <style>
    :root {{ --ink:#151d29; --muted:#647280; --line:#dbe3eb; --paper:#f6f8fb; --blue:#175c9b; --gold:#e7b83f; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",Arial,sans-serif; color:var(--ink); background:var(--paper); line-height:1.55; }}
    a {{ color:inherit; text-decoration:none; }}
    .top {{ border-bottom:1px solid var(--line); background:#fff; position:sticky; top:0; z-index:2; }}
    .top-inner {{ max-width:1180px; margin:0 auto; padding:14px 22px; display:flex; justify-content:space-between; gap:16px; }}
    .brand {{ font-weight:900; color:var(--blue); }}
    .top span {{ color:var(--muted); font-size:13px; }}
    .wrap {{ max-width:1180px; margin:0 auto; padding:28px 22px 72px; }}
    .intro {{ display:grid; grid-template-columns:1.2fr .8fr; gap:18px; margin-bottom:24px; align-items:end; }}
    .eyebrow {{ margin:0 0 8px; color:var(--blue); font-size:12px; font-weight:900; letter-spacing:.04em; text-transform:uppercase; }}
    .light {{ color:rgba(255,255,255,.82); }}
    h1 {{ margin:0 0 16px; font-size:clamp(34px,5vw,68px); line-height:1.04; letter-spacing:0; }}
    h2 {{ margin:0 0 12px; font-size:clamp(32px,5vw,60px); line-height:1.05; letter-spacing:0; }}
    h3 {{ margin:0 0 8px; font-size:22px; line-height:1.25; }}
    p {{ margin:0; }}
    .intro p:not(.eyebrow), .body p:not(.eyebrow), .archive-note {{ color:var(--muted); }}
    .stat {{ display:grid; grid-template-columns:repeat(3,1fr); gap:10px; }}
    .stat div {{ background:#fff; border:1px solid var(--line); padding:14px; }}
    .stat b {{ display:block; font-size:24px; color:var(--blue); }}
    .latest {{ min-height:430px; background-size:cover !important; background-position:center !important; color:#fff; display:flex; align-items:end; padding:32px; margin-bottom:30px; }}
    .latest div {{ max-width:760px; }}
    .latest p:not(.eyebrow) {{ color:rgba(255,255,255,.88); max-width:680px; font-size:17px; }}
    .hero-actions, .actions {{ display:flex; flex-wrap:wrap; gap:10px; margin-top:18px; }}
    .hero-actions a {{ border:1px solid rgba(255,255,255,.8); padding:10px 14px; font-weight:900; background:rgba(255,255,255,.12); }}
    .archive-head {{ border-top:3px solid var(--ink); padding-top:18px; display:flex; justify-content:space-between; align-items:end; margin-bottom:16px; }}
    .reports {{ display:grid; grid-template-columns:repeat(3,1fr); gap:16px; }}
    .card {{ background:#fff; border:1px solid var(--line); display:flex; flex-direction:column; min-height:100%; }}
    .thumb {{ min-height:210px; background-size:cover !important; background-position:center !important; color:#fff; display:flex; align-items:end; padding:14px; font-weight:900; }}
    .thumb span {{ background:rgba(10,22,38,.84); padding:6px 9px; }}
    .body {{ padding:18px; display:flex; flex-direction:column; flex:1; }}
    .actions {{ margin-top:auto; padding-top:18px; }}
    .actions a {{ border:1px solid var(--ink); padding:8px 11px; font-size:13px; font-weight:900; }}
    .footer {{ margin-top:40px; background:#111c2c; color:rgba(255,255,255,.84); padding:20px; font-size:14px; }}
    @media (max-width:900px) {{ .intro,.stat,.reports {{ grid-template-columns:1fr; }} .archive-head {{ display:block; }} }}
  </style>
</head>
<body>
  <header class="top"><div class="top-inner"><a class="brand" href="./">世界杯2026全球热点雷达</a><span>每日 08:10 更新 · Lark 图文版归档</span></div></header>
  <main class="wrap">
    <section class="intro">
      <div><p class="eyebrow">Global Watch Desk</p><h1>全球世界杯热点资讯站</h1><p>面向 Lark 群的全球热点图文版归档，覆盖权威要闻、全球舆情和运营机会。</p></div>
      <div class="stat"><div><b>{len(reports)}</b><span>归档</span></div><div><b>08:10</b><span>Lark 推送</span></div><div><b>HTML+MD</b><span>双版本</span></div></div>
    </section>
    {latest_block}
    <section><div class="archive-head"><div><p class="eyebrow">Archive</p><h3>最近全球雷达</h3></div><p class="archive-note">按日期倒序排列。</p></div><div class="reports">{''.join(cards)}</div></section>
    <footer class="footer">全球雷达用于快速消费：先验证权威事实，再叠加趋势和社区热度。无法验证的社媒不作为唯一证据。</footer>
  </main>
</body>
</html>"""
    (ROOT / "index.html").write_text(page, encoding="utf-8")
    print(f"Built global index with {len(reports)} report(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
