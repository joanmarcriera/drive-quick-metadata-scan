"""Typer CLI entrypoint."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from gdrive_dedupe.auth.oauth import build_drive_service
from gdrive_dedupe.dedupe.duplicate_files import get_duplicate_file_groups
from gdrive_dedupe.dedupe.duplicate_folders import get_duplicate_folder_groups
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


@app.command()
def report(
    db: Annotated[Path, typer.Option(help="SQLite metadata DB path")] = DEFAULT_DB_PATH,
    output: Annotated[Path, typer.Option(help="HTML output path")] = DEFAULT_REPORT_PATH,
    limit: Annotated[int, typer.Option(min=1, help="Max duplicate groups per section")] = 100,
) -> None:
    database = Database(db)
    database.initialize()
    output_file = generate_html_report(database, output_path=output, limit_per_section=limit)
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
