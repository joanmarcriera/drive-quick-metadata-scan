"""HTML report generation."""

from __future__ import annotations

from html import escape
from pathlib import Path
from typing import TypeAlias

from gdrive_dedupe.dedupe.duplicate_files import FileRecord, get_duplicate_file_groups
from gdrive_dedupe.dedupe.duplicate_folders import FolderRecord, get_duplicate_folder_groups
from gdrive_dedupe.reports.stats import collect_stats
from gdrive_dedupe.storage.database import Database

FolderNode: TypeAlias = tuple[str, str | None, str]
GOOGLE_DRIVE_FILE_URL = "https://drive.google.com/file/d/{item_id}/view"
GOOGLE_DRIVE_FOLDER_URL = "https://drive.google.com/drive/folders/{item_id}"


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
    database: Database,
    output_path: str | Path,
    limit_per_section: int = 100,
    file_samples_per_group: int = 8,
    folder_samples_per_group: int = 8,
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

    folder_cache: dict[str, FolderNode | None] = {}
    path_cache: dict[str, str] = {}
    file_groups_html_parts: list[str] = []
    for group in duplicate_files:
        example_name = group.files[0].name if group.files else "-"
        samples = group.files[:file_samples_per_group]
        samples_html = "\n".join(
            _render_file_sample(
                database,
                file_item,
                folder_cache=folder_cache,
                path_cache=path_cache,
            )
            for file_item in samples
        )

        remaining = group.count - len(samples)
        remaining_html = ""
        if remaining > 0:
            remaining_html = f"<li>... and {remaining} more files in this group.</li>"

        file_groups_html_parts.append(
            f"<li><strong>{escape(group.md5)}</strong> "
            f"({group.count} files, total {format_bytes(group.total_size)}, "
            f"example: {escape(example_name)})"
            f"<ul>{samples_html}{remaining_html}</ul></li>"
        )

    file_groups_html = (
        "\n".join(file_groups_html_parts) or "<li>No duplicate file groups detected.</li>"
    )

    folder_groups_html_parts: list[str] = []
    for group in duplicate_folders:
        example_name = group.folders[0].name if group.folders else "-"
        samples = group.folders[:folder_samples_per_group]
        samples_html = "\n".join(
            _render_folder_sample(
                database,
                folder,
                folder_cache=folder_cache,
                path_cache=path_cache,
            )
            for folder in samples
        )

        remaining = group.count - len(samples)
        remaining_html = ""
        if remaining > 0:
            remaining_html = f"<li>... and {remaining} more folders in this group.</li>"

        folder_groups_html_parts.append(
            f"<li><strong>{escape(group.hash_value[:16])}...</strong> "
            f"({group.count} folders, example: {escape(example_name)})"
            f"<ul>{samples_html}{remaining_html}</ul></li>"
        )

    folder_groups_html = (
        "\n".join(folder_groups_html_parts) or "<li>No duplicate folder groups detected.</li>"
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


def _render_folder_sample(
    database: Database,
    folder: FolderRecord,
    *,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> str:
    path = _resolve_folder_path(
        database,
        folder.id,
        folder_cache=folder_cache,
        path_cache=path_cache,
    )
    return (
        f"<li><code>{escape(folder.id)}</code> - {escape(path)} - "
        f"{_external_link(_drive_folder_url(folder.id), 'Open folder in Drive')}</li>"
    )


def _render_file_sample(
    database: Database,
    file_item: FileRecord,
    *,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> str:
    parent_path = (
        _resolve_folder_path(
            database,
            file_item.parent,
            folder_cache=folder_cache,
            path_cache=path_cache,
        )
        if file_item.parent
        else ""
    )
    file_path = f"{parent_path}/{file_item.name}" if parent_path else f"/{file_item.name}"
    size_text = format_bytes(file_item.size) if file_item.size is not None else "unknown size"
    return (
        f"<li><code>{escape(file_item.id)}</code> - {escape(file_path)} ({size_text}) - "
        f"{_external_link(_drive_file_url(file_item.id), 'Open file in Drive')}</li>"
    )


def _resolve_folder_path(
    database: Database,
    folder_id: str,
    *,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> str:
    if folder_id in path_cache:
        return path_cache[folder_id]

    cursor: str | None = folder_id
    lineage: list[str] = []
    seen: set[str] = set()
    prefix = ""

    while cursor is not None:
        if cursor in path_cache:
            prefix = path_cache[cursor]
            break
        if cursor in seen:
            break
        seen.add(cursor)

        node = _get_folder_node(database, cursor, folder_cache)
        if node is None:
            break

        lineage.append(cursor)
        cursor = node[1]

    for lineage_folder_id in reversed(lineage):
        node = _get_folder_node(database, lineage_folder_id, folder_cache)
        if node is None:
            break

        folder_name = node[2]
        if prefix:
            prefix = f"{prefix}/{folder_name}"
        else:
            prefix = f"/{folder_name}"
        path_cache[lineage_folder_id] = prefix

    return path_cache.get(folder_id, f"/{folder_id}")


def _get_folder_node(
    database: Database,
    folder_id: str,
    folder_cache: dict[str, FolderNode | None],
) -> FolderNode | None:
    if folder_id in folder_cache:
        return folder_cache[folder_id]

    row = database.execute(
        "SELECT id, parent, name FROM folders WHERE id = ?",
        (folder_id,),
    ).fetchone()
    if row is None:
        folder_cache[folder_id] = None
        return None

    node: FolderNode = (str(row["id"]), row["parent"], str(row["name"]))
    folder_cache[folder_id] = node
    return node


def _drive_file_url(item_id: str) -> str:
    return GOOGLE_DRIVE_FILE_URL.format(item_id=item_id)


def _drive_folder_url(item_id: str) -> str:
    return GOOGLE_DRIVE_FOLDER_URL.format(item_id=item_id)


def _external_link(url: str, label: str) -> str:
    safe_url = escape(url, quote=True)
    safe_label = escape(label)
    return f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer">{safe_label}</a>'
