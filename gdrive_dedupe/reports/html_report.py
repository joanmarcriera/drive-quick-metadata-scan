"""HTML report generation."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import TypeAlias

from gdrive_dedupe.dedupe.duplicate_files import FileRecord, get_duplicate_file_groups
from gdrive_dedupe.dedupe.duplicate_folders import (
    ActionableDuplicateRootGroup,
    FolderRecord,
    FolderSubtreeStats,
    compute_all_folder_subtree_stats,
    get_actionable_duplicate_root_groups,
    get_duplicate_folder_groups,
    get_folder_subtree_stats,
)
from gdrive_dedupe.reports.stats import collect_stats
from gdrive_dedupe.storage.database import Database

FolderNode: TypeAlias = tuple[str, str | None, str]
GOOGLE_DRIVE_FILE_URL = "https://drive.google.com/file/d/{item_id}/view"
GOOGLE_DRIVE_FOLDER_URL = "https://drive.google.com/drive/folders/{item_id}"


@dataclass(slots=True)
class HotspotEntry:
    folder: FolderRecord
    stats: FolderSubtreeStats
    duplicate_count: int | None


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
    actionable_candidates_per_group: int | None = 25,
) -> Path:
    stats = collect_stats(database)
    duplicate_files = get_duplicate_file_groups(database, limit=limit_per_section)
    duplicate_folders = get_duplicate_folder_groups(database, limit=limit_per_section)
    actionable_root_groups = get_actionable_duplicate_root_groups(database, limit=None)

    all_folders = _load_all_folders(database)
    subtree_stats_by_folder = compute_all_folder_subtree_stats(database)

    duplicate_file_summary = (
        f"{stats.duplicate_file_groups} " f"({stats.duplicate_file_items} files)"
    )
    duplicate_folder_summary = (
        f"{stats.duplicate_folder_groups} " f"({stats.duplicate_folder_items} folders)"
    )

    folder_cache: dict[str, FolderNode | None] = {
        folder.id: (folder.id, folder.parent, folder.name) for folder in all_folders
    }
    path_cache: dict[str, str] = {}

    file_groups_html = _render_duplicate_file_groups(
        database,
        duplicate_files,
        file_samples_per_group=file_samples_per_group,
        folder_cache=folder_cache,
        path_cache=path_cache,
    )

    folder_groups_html = _render_duplicate_folder_groups(
        database,
        duplicate_folders,
        folder_samples_per_group=folder_samples_per_group,
        folder_cache=folder_cache,
        path_cache=path_cache,
    )

    actionable_groups_html = _render_actionable_root_groups(
        database,
        actionable_root_groups,
        subtree_stats_by_folder=subtree_stats_by_folder,
        folder_cache=folder_cache,
        path_cache=path_cache,
        sample_size=actionable_candidates_per_group,
        max_groups=limit_per_section,
    )

    actionable_meta = _build_actionable_meta_map(actionable_root_groups)
    size_hotspots_html, file_hotspots_html, git_hotspots_html = _render_hotspots(
        database,
        folders=all_folders,
        subtree_stats_by_folder=subtree_stats_by_folder,
        actionable_meta=actionable_meta,
        folder_cache=folder_cache,
        path_cache=path_cache,
        limit=20,
    )

    actionable_note = (
        "Nested duplicate folders are collapsed to top-level duplicate sets "
        "to make manual cleanup faster."
    )
    size_hotspot_note = (
        "Largest folder trees by total contained bytes. "
        "Bars flagged in orange are actionable duplicate roots."
    )
    file_hotspot_note = (
        "Folder trees with the highest number of files, " "useful for surfacing noisy directories."
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
    .hbar-list {{ margin-top: 0.75rem; }}
    .hbar-row {{
      display: grid;
      grid-template-columns: minmax(280px, 2fr) minmax(220px, 3fr) minmax(220px, 2fr);
      gap: 0.75rem;
      align-items: center;
      margin: 0.5rem 0;
    }}
    .hbar-label {{ font-size: 0.9rem; line-height: 1.3; }}
    .hbar-track {{ background: #eef2ff; border-radius: 6px; overflow: hidden; height: 14px; }}
    .hbar-fill {{ height: 100%; background: linear-gradient(90deg, #2563eb, #60a5fa); }}
    .hbar-fill.dup {{ background: linear-gradient(90deg, #b45309, #f59e0b); }}
    .hbar-meta {{ font-size: 0.85rem; color: #444; }}
    .badge {{
      display: inline-block;
      margin-left: 0.4rem;
      padding: 0.1rem 0.4rem;
      border-radius: 999px;
      font-size: 0.75rem;
      background: #fff7ed;
      color: #9a3412;
      border: 1px solid #fdba74;
    }}
    code {{ font-size: 0.85rem; }}
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
    <h2>Folder Size Hotspots</h2>
    <p class=\"meta\">{size_hotspot_note}</p>
    {size_hotspots_html}
  </div>

  <div class=\"card\">
    <h2>File Count Hotspots</h2>
    <p class=\"meta\">{file_hotspot_note}</p>
    {file_hotspots_html}
  </div>

  <div class=\"card\">
    <h2>Repository Metadata Hotspots (.git)</h2>
    <p class=\"meta\">Folders named <code>.git</code>, ranked by file count and size.</p>
    {git_hotspots_html}
  </div>

  <div class=\"card\">
    <h2>Duplicate file sets</h2>
    <ul>{file_groups_html}</ul>
  </div>

  <div class=\"card\">
    <h2>Actionable Duplicate Roots</h2>
    <p class=\"meta\">{actionable_note}</p>
    <ul>{actionable_groups_html}</ul>
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


def _load_all_folders(database: Database) -> list[FolderRecord]:
    rows = database.execute("SELECT id, name, parent FROM folders ORDER BY id").fetchall()
    return [
        FolderRecord(id=str(row["id"]), name=str(row["name"]), parent=row["parent"]) for row in rows
    ]


def _build_actionable_meta_map(
    groups: list[ActionableDuplicateRootGroup],
) -> dict[str, int]:
    metadata: dict[str, int] = {}
    for group in groups:
        for folder in group.folders:
            metadata[folder.id] = group.count
    return metadata


def _render_duplicate_file_groups(
    database: Database,
    groups: list,
    *,
    file_samples_per_group: int,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> str:
    parts: list[str] = []
    for group in groups:
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

        parts.append(
            f"<li><strong>{escape(group.md5)}</strong> "
            f"({group.count} files, total {format_bytes(group.total_size)}, "
            f"example: {escape(example_name)})"
            f"<ul>{samples_html}{remaining_html}</ul></li>"
        )

    return "\n".join(parts) or "<li>No duplicate file groups detected.</li>"


def _render_duplicate_folder_groups(
    database: Database,
    groups: list,
    *,
    folder_samples_per_group: int,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> str:
    parts: list[str] = []
    for group in groups:
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

        parts.append(
            f"<li><strong>{escape(group.hash_value[:16])}...</strong> "
            f"({group.count} folders, example: {escape(example_name)})"
            f"<ul>{samples_html}{remaining_html}</ul></li>"
        )

    return "\n".join(parts) or "<li>No duplicate folder groups detected.</li>"


def _render_hotspots(
    database: Database,
    *,
    folders: list[FolderRecord],
    subtree_stats_by_folder: dict[str, FolderSubtreeStats],
    actionable_meta: dict[str, int],
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
    limit: int,
) -> tuple[str, str, str]:
    entries: list[HotspotEntry] = []
    for folder in folders:
        stats = subtree_stats_by_folder.get(folder.id)
        if stats is None:
            continue
        if stats.total_size <= 0 and stats.file_count <= 0:
            continue
        entries.append(
            HotspotEntry(
                folder=folder,
                stats=stats,
                duplicate_count=actionable_meta.get(folder.id),
            )
        )

    if not entries:
        empty = '<p class="meta">No folder hotspot data available.</p>'
        return empty, empty, empty

    size_hotspots = sorted(
        entries,
        key=lambda entry: (entry.stats.total_size, entry.stats.file_count),
        reverse=True,
    )[:limit]
    file_hotspots = sorted(
        entries,
        key=lambda entry: (entry.stats.file_count, entry.stats.total_size),
        reverse=True,
    )[:limit]
    git_hotspots = sorted(
        [entry for entry in entries if entry.folder.name == ".git"],
        key=lambda entry: (entry.stats.file_count, entry.stats.total_size),
        reverse=True,
    )[:limit]

    size_html = _render_hotspot_bar_list(
        database,
        size_hotspots,
        metric="size",
        folder_cache=folder_cache,
        path_cache=path_cache,
    )
    file_html = _render_hotspot_bar_list(
        database,
        file_hotspots,
        metric="files",
        folder_cache=folder_cache,
        path_cache=path_cache,
    )

    if git_hotspots:
        git_html = _render_hotspot_bar_list(
            database,
            git_hotspots,
            metric="files",
            folder_cache=folder_cache,
            path_cache=path_cache,
        )
    else:
        git_html = '<p class="meta">No .git-heavy candidates found.</p>'

    return size_html, file_html, git_html


def _render_hotspot_bar_list(
    database: Database,
    entries: list[HotspotEntry],
    *,
    metric: str,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> str:
    if not entries:
        return '<p class="meta">No entries.</p>'

    max_metric = max(
        entry.stats.total_size if metric == "size" else entry.stats.file_count for entry in entries
    )

    rows: list[str] = []
    for entry in entries:
        path = _resolve_folder_path(
            database,
            entry.folder.id,
            folder_cache=folder_cache,
            path_cache=path_cache,
        )
        value = entry.stats.total_size if metric == "size" else entry.stats.file_count
        width = 4 if max_metric <= 0 else max(4, int((value / max_metric) * 100))
        fill_class = "hbar-fill dup" if entry.duplicate_count else "hbar-fill"

        badge = ""
        if entry.duplicate_count:
            badge = f'<span class="badge">dup-root x{entry.duplicate_count}</span>'
        if entry.folder.name == ".git":
            badge += '<span class="badge">.git</span>'

        if metric == "size":
            metric_text = (
                f"{format_bytes(entry.stats.total_size)} · "
                f"{entry.stats.file_count:,} files · "
                f"{entry.stats.folder_count:,} folders"
            )
        else:
            metric_text = (
                f"{entry.stats.file_count:,} files · "
                f"{format_bytes(entry.stats.total_size)} · "
                f"{entry.stats.folder_count:,} folders"
            )

        rows.append(
            '<div class="hbar-row">'
            f'<div class="hbar-label"><code>{escape(entry.folder.id)}</code><br/>'
            f"{escape(path)} {badge}</div>"
            f'<div class="hbar-track"><div class="{fill_class}" '
            f'style="width:{width}%"></div></div>'
            f'<div class="hbar-meta">{metric_text}<br/>'
            f"{_external_link(_drive_folder_url(entry.folder.id), 'Open folder in Drive')}</div>"
            "</div>"
        )

    return f"<div class=\"hbar-list\">{''.join(rows)}</div>"


def _render_actionable_root_groups(
    database: Database,
    groups: list[ActionableDuplicateRootGroup],
    *,
    subtree_stats_by_folder: dict[str, FolderSubtreeStats],
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
    sample_size: int | None,
    max_groups: int | None,
) -> str:
    if not groups:
        return "<li>No actionable top-level duplicate root groups detected.</li>"

    ranked_groups: list[tuple[int, ActionableDuplicateRootGroup, FolderRecord, str]] = []
    for group in groups:
        keep_folder = _choose_keep_candidate(
            database,
            group.folders,
            folder_cache=folder_cache,
            path_cache=path_cache,
        )
        keep_path = _resolve_folder_path(
            database,
            keep_folder.id,
            folder_cache=folder_cache,
            path_cache=path_cache,
        )

        subtree_stats = subtree_stats_by_folder.get(keep_folder.id)
        if subtree_stats is None:
            subtree_stats = get_folder_subtree_stats(database, keep_folder.id)

        estimated_reclaimable = max(group.count - 1, 0) * subtree_stats.total_size
        if estimated_reclaimable <= 0:
            continue

        ranked_groups.append((estimated_reclaimable, group, keep_folder, keep_path))

    ranked_groups.sort(
        key=lambda item: (item[0], item[1].count, len(item[3])),
        reverse=True,
    )
    if max_groups is not None:
        ranked_groups = ranked_groups[:max_groups]

    if not ranked_groups:
        return "<li>No actionable top-level duplicate root groups detected.</li>"

    parts: list[str] = []
    for estimated_reclaimable, group, keep_folder, keep_path in ranked_groups:
        delete_candidates = [folder for folder in group.folders if folder.id != keep_folder.id]
        if sample_size is None:
            sampled_delete_candidates = delete_candidates
        else:
            sampled_delete_candidates = delete_candidates[:sample_size]
        delete_candidates_html = "\n".join(
            _render_delete_candidate(
                database,
                candidate,
                folder_cache=folder_cache,
                path_cache=path_cache,
            )
            for candidate in sampled_delete_candidates
        )

        remaining_candidates = len(delete_candidates) - len(sampled_delete_candidates)
        remaining_candidates_html = ""
        if remaining_candidates > 0:
            remaining_candidates_html = (
                f"<li>... and {remaining_candidates} more review candidates.</li>"
            )

        parts.append(
            "<li>"
            f"<strong>{escape(group.hash_value[:16])}...</strong> "
            f"({group.count} copies, "
            f"approx reclaimable {format_bytes(estimated_reclaimable)})"
            "<ul>"
            f"<li><strong>Keep:</strong> <code>{escape(keep_folder.id)}</code> - "
            f"{escape(keep_path)} - "
            f"{_external_link(_drive_folder_url(keep_folder.id), 'Open folder in Drive')}</li>"
            f"<li><strong>Review delete candidates:</strong>"
            f"<ul>{delete_candidates_html}{remaining_candidates_html}</ul></li>"
            "</ul>"
            "</li>"
        )

    return "\n".join(parts)


def _render_delete_candidate(
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


def _choose_keep_candidate(
    database: Database,
    folders: list[FolderRecord],
    *,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> FolderRecord:
    ranked = sorted(
        folders,
        key=lambda folder: _folder_rank_key(
            database,
            folder,
            folder_cache=folder_cache,
            path_cache=path_cache,
        ),
    )
    return ranked[0]


def _folder_rank_key(
    database: Database,
    folder: FolderRecord,
    *,
    folder_cache: dict[str, FolderNode | None],
    path_cache: dict[str, str],
) -> tuple[int, int, str]:
    path = _resolve_folder_path(
        database,
        folder.id,
        folder_cache=folder_cache,
        path_cache=path_cache,
    )
    depth = path.count("/")
    return (depth, len(path), path)


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
