"""Typer CLI entrypoint."""

from __future__ import annotations

import webbrowser
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from gdrive_dedupe.auth.oauth import build_drive_service
from gdrive_dedupe.dedupe.duplicate_files import get_duplicate_file_groups
from gdrive_dedupe.dedupe.duplicate_folders import (
    get_actionable_root_recommendations,
    get_duplicate_folder_groups,
)
from gdrive_dedupe.drive.scanner import DriveScanner
from gdrive_dedupe.hashing.folder_hash_engine import FolderHashEngine
from gdrive_dedupe.reports.html_report import format_bytes, generate_html_report
from gdrive_dedupe.reports.stats import collect_stats
from gdrive_dedupe.storage.database import Database

app = typer.Typer(help="Metadata-only Google Drive deduplication tool")
duplicates_app = typer.Typer(help="Duplicate detection commands")
app.add_typer(duplicates_app, name="duplicates")
console = Console()
DEFAULT_DB_PATH = Path.home() / ".config" / "gdrive-dedupe" / "metadata.db"
DEFAULT_CREDENTIALS_PATH = Path("credentials.json")
DEFAULT_REPORT_PATH = Path("report.html")
GOOGLE_DRIVE_FOLDER_URL = "https://drive.google.com/drive/folders/{item_id}"


class BrowserOpenMode(StrEnum):
    tabs = "tabs"
    windows = "windows"


@app.command()
def scan(
    db: Annotated[Path, typer.Option(help="SQLite metadata DB path")] = DEFAULT_DB_PATH,
    credentials: Annotated[
        Path, typer.Option(help="Path to Google OAuth credentials.json")
    ] = DEFAULT_CREDENTIALS_PATH,
    query: Annotated[str, typer.Option(help="Drive files.list query filter")] = "trashed = false",
    drive_id: Annotated[str | None, typer.Option(help="Optional shared drive ID")] = None,
    resume: Annotated[
        bool, typer.Option("--resume/--no-resume", help="Resume from previous token")
    ] = True,
    page_size: Annotated[int, typer.Option(min=100, max=1000, help="Drive API page size")] = 1000,
) -> None:
    database = Database(db)
    database.initialize()

    service = build_drive_service(credentials)
    scanner = DriveScanner(service, database)
    scanned = scanner.scan(resume=resume, page_size=page_size, query=query, drive_id=drive_id)
    console.print(f"[green]Scan complete.[/green] Items scanned: {scanned}")


@duplicates_app.command("files")
def duplicates_files(
    db: Annotated[Path, typer.Option(help="SQLite metadata DB path")] = DEFAULT_DB_PATH,
    limit: Annotated[int | None, typer.Option(help="Max duplicate groups to display")] = 50,
) -> None:
    database = Database(db)
    database.initialize()

    groups = get_duplicate_file_groups(database, limit=limit)
    if not groups:
        console.print("No duplicate files found.")
        return

    table = Table(title="Duplicate Files")
    table.add_column("MD5")
    table.add_column("Count", justify="right")
    table.add_column("Total Size", justify="right")

    for group in groups:
        table.add_row(group.md5, str(group.count), format_bytes(group.total_size))

    console.print(table)


@duplicates_app.command("folders")
def duplicates_folders(
    db: Annotated[Path, typer.Option(help="SQLite metadata DB path")] = DEFAULT_DB_PATH,
    recompute: Annotated[
        bool,
        typer.Option(
            "--recompute/--no-recompute",
            help="Recompute folder hashes before listing duplicates",
        ),
    ] = True,
    workers: Annotated[
        int, typer.Option(min=1, max=16, help="Worker threads for hash computation")
    ] = 1,
    limit: Annotated[int | None, typer.Option(help="Max duplicate groups to display")] = 50,
) -> None:
    database = Database(db)
    database.initialize()

    if recompute:
        engine = FolderHashEngine(database)
        hashed = engine.compute_all(workers=workers)
        console.print(f"Computed hashes for {hashed} folders.")

    groups = get_duplicate_folder_groups(database, limit=limit)
    if not groups:
        console.print("No duplicate folders found.")
        return

    table = Table(title="Duplicate Folders")
    table.add_column("Tree Hash")
    table.add_column("Count", justify="right")
    table.add_column("Example Folder")

    for group in groups:
        example = group.folders[0].name if group.folders else ""
        table.add_row(group.hash_value[:16] + "...", str(group.count), example)

    console.print(table)


@duplicates_app.command("waste")
def duplicates_waste(
    db: Annotated[Path, typer.Option(help="SQLite metadata DB path")] = DEFAULT_DB_PATH,
    recompute: Annotated[
        bool,
        typer.Option(
            "--recompute/--no-recompute",
            help="Recompute folder hashes before ranking reclaimable duplicate roots",
        ),
    ] = False,
    workers: Annotated[
        int, typer.Option(min=1, max=16, help="Worker threads for folder hash computation")
    ] = 1,
    limit: Annotated[int, typer.Option(min=1, help="Rows to show")] = 15,
    offset: Annotated[int, typer.Option(min=0, help="Row offset for pagination")] = 0,
    min_reclaimable: Annotated[
        str,
        typer.Option(help="Minimum reclaimable size threshold (e.g. 500MB, 2GB, 0)"),
    ] = "1MB",
    sample_candidates: Annotated[
        int, typer.Option(min=1, max=10, help="Candidate folders shown per row")
    ] = 3,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive/--no-interactive",
            help="Interactive review loop (largest duplicate roots first)",
        ),
    ] = False,
    open_links: Annotated[
        int, typer.Option(min=1, max=20, help="Links to open per interactive action")
    ] = 5,
    open_mode: Annotated[
        BrowserOpenMode,
        typer.Option(case_sensitive=False, help="Open links as browser tabs or windows"),
    ] = BrowserOpenMode.windows,
) -> None:
    database = Database(db)
    database.initialize()

    if recompute:
        engine = FolderHashEngine(database)
        hashed = engine.compute_all(workers=workers)
        console.print(f"Computed hashes for {hashed} folders.")

    min_reclaimable_bytes = _parse_size_to_bytes(min_reclaimable)
    recommendation_limit = None if interactive else limit
    recommendations = get_actionable_root_recommendations(
        database,
        limit=recommendation_limit,
        offset=offset,
        min_reclaimable_bytes=min_reclaimable_bytes,
    )
    if not recommendations:
        console.print("No actionable duplicate roots match this threshold/page.")
        return

    folder_map = _load_folder_map(database)
    path_cache: dict[str, str] = {}

    if interactive:
        _run_interactive_waste_review(
            recommendations,
            offset=offset,
            folder_map=folder_map,
            path_cache=path_cache,
            open_links=open_links,
            open_mode=open_mode,
        )
        return

    table = Table(title="Duplicate Waste Ranking (Largest First)")
    table.add_column("#", justify="right")
    table.add_column("Reclaimable", justify="right")
    table.add_column("Copies", justify="right")
    table.add_column("Delete Cands", justify="right")
    table.add_column("Keep")
    table.add_column("Keep Link")
    table.add_column("Review Candidate Preview")

    for idx, recommendation in enumerate(recommendations, start=offset + 1):
        keep_path = _resolve_folder_path(recommendation.keep.id, folder_map, path_cache)
        keep_desc = f"{recommendation.keep.id} - {keep_path}"
        keep_url = GOOGLE_DRIVE_FOLDER_URL.format(item_id=recommendation.keep.id)

        candidate_parts: list[str] = []
        sampled_candidates = recommendation.delete_candidates[:sample_candidates]
        for candidate in sampled_candidates:
            candidate_path = _resolve_folder_path(candidate.id, folder_map, path_cache)
            candidate_parts.append(f"{candidate.id} - {candidate_path}")
        remaining_candidates = len(recommendation.delete_candidates) - len(sampled_candidates)
        if remaining_candidates > 0:
            candidate_parts.append(f"... (+{remaining_candidates} more)")
        candidate_text = "\n".join(candidate_parts) if candidate_parts else "-"

        table.add_row(
            str(idx),
            format_bytes(recommendation.estimated_reclaimable_bytes),
            str(recommendation.copies),
            str(len(recommendation.delete_candidates)),
            keep_desc,
            keep_url,
            candidate_text,
        )

    console.print(table)
    console.print(
        "Next page: " f"`gdrive-dedupe duplicates waste --offset {offset + limit} --limit {limit}`"
    )


@app.command()
def report(
    db: Annotated[Path, typer.Option(help="SQLite metadata DB path")] = DEFAULT_DB_PATH,
    output: Annotated[Path, typer.Option(help="HTML output path")] = DEFAULT_REPORT_PATH,
    limit: Annotated[int, typer.Option(min=1, help="Max duplicate groups per section")] = 100,
    actionable_candidates: Annotated[
        int,
        typer.Option(
            min=0,
            help=("Delete-candidate links per actionable root in HTML report " "(0 = show all)"),
        ),
    ] = 25,
    recompute: Annotated[
        bool,
        typer.Option(
            "--recompute/--no-recompute",
            help="Recompute folder hashes before generating report",
        ),
    ] = True,
    workers: Annotated[
        int, typer.Option(min=1, max=16, help="Worker threads for folder hash computation")
    ] = 1,
) -> None:
    database = Database(db)
    database.initialize()

    if recompute:
        engine = FolderHashEngine(database)
        hashed = engine.compute_all(workers=workers)
        console.print(f"Computed hashes for {hashed} folders.")

    candidate_sample_size: int | None = (
        None if actionable_candidates == 0 else actionable_candidates
    )
    output_file = generate_html_report(
        database,
        output_path=output,
        limit_per_section=limit,
        actionable_candidates_per_group=candidate_sample_size,
    )
    console.print(f"[green]Report generated:[/green] {output_file}")


@app.command()
def stats(
    db: Annotated[Path, typer.Option(help="SQLite metadata DB path")] = DEFAULT_DB_PATH,
) -> None:
    database = Database(db)
    database.initialize()
    data = collect_stats(database)

    table = Table(title="Drive Metadata Stats")
    table.add_column("Metric")
    table.add_column("Value", justify="right")

    table.add_row("Files", str(data.file_count))
    table.add_row("Folders", str(data.folder_count))
    table.add_row("Duplicate file groups", str(data.duplicate_file_groups))
    table.add_row("Duplicate file items", str(data.duplicate_file_items))
    table.add_row("Duplicate folder groups", str(data.duplicate_folder_groups))
    table.add_row("Duplicate folder items", str(data.duplicate_folder_items))
    table.add_row("Estimated reclaimable storage", format_bytes(data.estimated_reclaimable_bytes))

    console.print(table)


def _parse_size_to_bytes(raw: str) -> int:
    value = raw.strip().upper()
    if value in {"0", "0B"}:
        return 0

    multipliers = [
        ("PB", 1024**5),
        ("TB", 1024**4),
        ("GB", 1024**3),
        ("MB", 1024**2),
        ("KB", 1024),
        ("B", 1),
    ]
    for suffix, multiplier in multipliers:
        if value.endswith(suffix):
            number = value[: -len(suffix)].strip()
            if not number:
                raise typer.BadParameter(f"Invalid size value: {raw}")
            return int(float(number) * multiplier)

    # If no suffix, assume bytes.
    try:
        return int(float(value))
    except ValueError as exc:
        raise typer.BadParameter(
            f"Invalid size value '{raw}'. Use formats like 500MB, 2GB, 0."
        ) from exc


def _load_folder_map(database: Database) -> dict[str, tuple[str | None, str]]:
    rows = database.execute("SELECT id, parent, name FROM folders").fetchall()
    return {
        str(row["id"]): (
            str(row["parent"]) if row["parent"] is not None else None,
            str(row["name"]),
        )
        for row in rows
    }


def _resolve_folder_path(
    folder_id: str,
    folder_map: dict[str, tuple[str | None, str]],
    path_cache: dict[str, str],
) -> str:
    if folder_id in path_cache:
        return path_cache[folder_id]

    cursor: str | None = folder_id
    seen: set[str] = set()
    lineage: list[str] = []
    prefix = ""
    while cursor is not None:
        if cursor in path_cache:
            prefix = path_cache[cursor]
            break
        if cursor in seen:
            break
        seen.add(cursor)

        node = folder_map.get(cursor)
        if node is None:
            break
        lineage.append(cursor)
        cursor = node[0]

    for lineage_folder_id in reversed(lineage):
        node = folder_map.get(lineage_folder_id)
        if node is None:
            break
        name = node[1]
        prefix = f"{prefix}/{name}" if prefix else f"/{name}"
        path_cache[lineage_folder_id] = prefix

    return path_cache.get(folder_id, f"/{folder_id}")


def _run_interactive_waste_review(
    recommendations: list,
    *,
    offset: int,
    folder_map: dict[str, tuple[str | None, str]],
    path_cache: dict[str, str],
    open_links: int,
    open_mode: BrowserOpenMode,
) -> None:
    total = len(recommendations)
    cursor = 0
    console.print(
        "Interactive mode: largest reclaimable duplicate roots first. "
        "Actions: [bold]o[/bold]=open keep + delete links, "
        "[bold]d[/bold]=open delete links only, [bold]k[/bold]=open keep link, "
        "[bold]n[/bold]=next, [bold]p[/bold]=previous, [bold]q[/bold]=quit."
    )

    while 0 <= cursor < total:
        recommendation = recommendations[cursor]
        row_number = offset + cursor + 1
        _print_interactive_recommendation(
            row_number=row_number,
            recommendation=recommendation,
            folder_map=folder_map,
            path_cache=path_cache,
            open_links=open_links,
        )

        action = typer.prompt("Action", default="n").strip().lower()

        if action == "q":
            break
        if action == "n":
            cursor = min(cursor + 1, total - 1)
            continue
        if action == "p":
            cursor = max(cursor - 1, 0)
            continue

        if action in {"o", "d", "k"}:
            include_keep = action in {"o", "k"}
            include_deletes = action in {"o", "d"}
            urls = _build_review_urls(
                recommendation,
                include_keep=include_keep,
                include_delete_candidates=include_deletes,
                delete_limit=open_links,
            )
            if not urls:
                console.print("[yellow]No links available for this selection.[/yellow]")
                continue
            _open_urls(urls, open_mode=open_mode)
            console.print(
                f"[green]Opened {len(urls)} link(s) in browser ({open_mode.value}).[/green]"
            )
            continue

        if action.startswith("j "):
            jump_raw = action[2:].strip()
            if jump_raw.isdigit():
                jump_index = int(jump_raw) - 1 - offset
                if 0 <= jump_index < total:
                    cursor = jump_index
                    continue
            console.print("[yellow]Invalid jump target. Use: j <rank_number>[/yellow]")
            continue

        console.print("[yellow]Unknown action. Use o/d/k/n/p/q or j <rank_number>.[/yellow]")


def _print_interactive_recommendation(
    *,
    row_number: int,
    recommendation,
    folder_map: dict[str, tuple[str | None, str]],
    path_cache: dict[str, str],
    open_links: int,
) -> None:
    keep_path = _resolve_folder_path(recommendation.keep.id, folder_map, path_cache)
    keep_url = GOOGLE_DRIVE_FOLDER_URL.format(item_id=recommendation.keep.id)
    console.rule(f"Rank #{row_number} - {format_bytes(recommendation.estimated_reclaimable_bytes)}")
    console.print(f"Copies: {recommendation.copies}")
    console.print(f"Keep: {recommendation.keep.id} - {keep_path}")
    console.print(f"Keep Link: {keep_url}")

    preview = recommendation.delete_candidates[:open_links]
    if not preview:
        console.print("Delete candidates: none")
        return

    candidate_table = Table(title=f"Delete Candidates (showing first {len(preview)})")
    candidate_table.add_column("#", justify="right")
    candidate_table.add_column("Folder")
    candidate_table.add_column("Drive Link")
    for idx, candidate in enumerate(preview, start=1):
        candidate_path = _resolve_folder_path(candidate.id, folder_map, path_cache)
        candidate_url = GOOGLE_DRIVE_FOLDER_URL.format(item_id=candidate.id)
        candidate_table.add_row(
            str(idx),
            f"{candidate.id} - {candidate_path}",
            candidate_url,
        )
    remaining = len(recommendation.delete_candidates) - len(preview)
    if remaining > 0:
        candidate_table.caption = (
            f"... and {remaining} more delete candidate(s). "
            f"Increase --open-links to include more."
        )
    console.print(candidate_table)


def _build_review_urls(
    recommendation,
    *,
    include_keep: bool,
    include_delete_candidates: bool,
    delete_limit: int,
) -> list[str]:
    urls: list[str] = []
    if include_keep:
        urls.append(GOOGLE_DRIVE_FOLDER_URL.format(item_id=recommendation.keep.id))
    if include_delete_candidates:
        for candidate in recommendation.delete_candidates[:delete_limit]:
            urls.append(GOOGLE_DRIVE_FOLDER_URL.format(item_id=candidate.id))
    return urls


def _open_urls(urls: list[str], *, open_mode: BrowserOpenMode) -> None:
    for url in urls:
        if open_mode == BrowserOpenMode.windows:
            webbrowser.open_new(url)
        else:
            webbrowser.open_new_tab(url)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
