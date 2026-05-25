# World Cup 2026 Brazil Report

Static daily report site for the World Cup 2026 Brazil watch workflow.

- `index.html`: archive-style portal listing recent reports.
- `reports/YYYY-MM-DD.html`: archived visual report.
- `reports/YYYY-MM-DD.md`: archived text report.
- `scripts/build_index.py`: rebuilds the archive portal from `reports/`.
- `scripts/send_lark.py`: optional Lark webhook sender with a structured card.

Suggested GitHub Pages URL:

`https://hokeem.github.io/koko-kwai-coach/worldcup-brazil-report/`

Lark delivery requires a group bot webhook URL stored as `LARK_BOT_WEBHOOK`.
