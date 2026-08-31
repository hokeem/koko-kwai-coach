#!/usr/bin/env python3
"""Preview or apply the Koko script taxonomy v2 migration."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path


WEB_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_ROOT))

import app  # noqa: E402


def summarize(entries: list[dict]) -> dict:
    dimensions = (*app.SCRIPT_TAG_DIMENSIONS, "duration")
    counts = {dimension: Counter() for dimension in dimensions}
    missing = Counter()
    for entry in entries:
        entry_id = str(entry.get("entry_id") or "")
        script_json = app.library_script_json(entry_id) if entry_id else {}
        if not entry.get("duration_bucket"):
            entry.update(app.creator_duration_fields(entry_id, script_json))
        app.ensure_script_taxonomy_fields(entry, script_json, source="migration_v2")
        for dimension in app.SCRIPT_TAG_DIMENSIONS:
            values = entry.get(f"{dimension}_tags") or []
            if not values:
                missing[dimension] += 1
            counts[dimension].update(values)
        duration = str(entry.get("duration_bucket") or "")
        if duration:
            counts["duration"][duration] += 1
        else:
            missing["duration"] += 1
    return {
        "total": len(entries),
        "taxonomy_version": app.SCRIPT_TAXONOMY_VERSION,
        "counts": {key: dict(value) for key, value in counts.items()},
        "missing": dict(missing),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Persist migrated fields after creating a backup.")
    parser.add_argument("--report", type=Path, help="Optional JSON report path.")
    args = parser.parse_args()

    raw_entries = app.read_json_file(app.LIBRARY_FILE, default=[])
    if not isinstance(raw_entries, list):
        raise SystemExit(f"Invalid library data: {app.LIBRARY_FILE}")
    entries = [dict(entry) for entry in raw_entries if isinstance(entry, dict)]
    report = summarize(entries)
    report["mode"] = "apply" if args.apply else "dry-run"
    report["library_file"] = str(app.LIBRARY_FILE)

    if args.apply:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup = app.LIBRARY_FILE.with_name(f"{app.LIBRARY_FILE.stem}.pre-taxonomy-v2-{stamp}.json")
        shutil.copy2(app.LIBRARY_FILE, backup)
        if not app.save_library_entries(entries):
            raise SystemExit("Could not persist migrated library.")
        report["backup"] = str(backup)

    payload = json.dumps(report, ensure_ascii=False, indent=2)
    print(payload)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(payload + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
