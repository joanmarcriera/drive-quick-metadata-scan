"""HTML report generation."""

from __future__ import annotations

from html import escape
from pathlib import Path

from gdrive_dedupe.dedupe.duplicate_files import get_duplicate_file_groups
from gdrive_dedupe.dedupe.duplicate_folders import get_duplicate_folder_groups
from gdrive_dedupe.reports.stats import collect_stats
from gdrive_dedupe.storage.database import Database


def format_bytes(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"

    units = ["KB", "MB", "GB", "TB", "PB"]
    value = float(num_bytes)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.2f} {unit}"
    return f"{value:.2f} PB"


def generate_html_report(
    database: Database, output_path: str | Path, limit_per_section: int = 100
) -> Path:
    stats = collect_stats(database)
    duplicate_files = get_duplicate_file_groups(database, limit=limit_per_section)
    duplicate_folders = get_duplicate_folder_groups(database, limit=limit_per_section)
    duplicate_file_summary = (
        f"{stats.duplicate_file_groups} " f"({stats.duplicate_file_items} files)"
    )
    duplicate_folder_summary = (
        f"{stats.duplicate_folder_groups} " f"({stats.duplicate_folder_items} folders)"
    )

    file_groups_html = (
        "\n".join(
            (
                f"<li><strong>{escape(group.md5)}</strong> "
                f"({group.count} files, total {format_bytes(group.total_size)})</li>"
            )
            for group in duplicate_files
        )
        or "<li>No duplicate file groups detected.</li>"
    )

    folder_groups_html = (
        "\n".join(
            (
                f"<li><strong>{escape(group.hash_value[:16])}...</strong> "
                f"({group.count} folders)</li>"
            )
            for group in duplicate_folders
        )
        or "<li>No duplicate folder groups detected.</li>"
    )

    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"UTF-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
  <title>gdrive-dedupe report</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem; }}
    h1, h2 {{ margin-bottom: 0.5rem; }}
    .card {{ border: 1px solid #ddd; border-radius: 8px; padding: 1rem; margin: 1rem 0; }}
    .meta {{ color: #555; }}
  </style>
</head>
<body>
  <h1>gdrive-dedupe report</h1>
  <p class=\"meta\">Metadata-only analysis of Google Drive duplicates.</p>

  <div class=\"card\">
    <h2>Drive statistics</h2>
    <ul>
      <li>Total files: {stats.file_count}</li>
      <li>Total folders: {stats.folder_count}</li>
      <li>Duplicate file groups: {duplicate_file_summary}</li>
      <li>Duplicate folder groups: {duplicate_folder_summary}</li>
      <li>Estimated reclaimable storage: {format_bytes(stats.estimated_reclaimable_bytes)}</li>
    </ul>
  </div>

  <div class=\"card\">
    <h2>Duplicate file sets</h2>
    <ul>{file_groups_html}</ul>
  </div>

  <div class=\"card\">
    <h2>Duplicate folder trees</h2>
    <ul>{folder_groups_html}</ul>
  </div>
</body>
</html>
"""

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    return out
