"""Database statistics helpers."""

from __future__ import annotations

from dataclasses import dataclass

from gdrive_dedupe.dedupe.duplicate_files import estimate_reclaimable_bytes
from gdrive_dedupe.storage.database import Database


@dataclass(slots=True)
class DriveStats:
    file_count: int
    folder_count: int
    duplicate_file_groups: int
    duplicate_file_items: int
    duplicate_folder_groups: int
    duplicate_folder_items: int
    estimated_reclaimable_bytes: int


def collect_stats(database: Database) -> DriveStats:
    file_count = int(database.execute("SELECT COUNT(*) AS c FROM files").fetchone()["c"])
    folder_count = int(database.execute("SELECT COUNT(*) AS c FROM folders").fetchone()["c"])

    duplicate_file_groups = int(
        database.execute(
            "SELECT COUNT(*) AS c FROM ("
            "SELECT md5 FROM files WHERE md5 IS NOT NULL GROUP BY md5 HAVING COUNT(*) > 1"
            ")"
        ).fetchone()["c"]
    )

    duplicate_file_items = int(
        database.execute(
            "SELECT COALESCE(SUM(cnt), 0) AS c FROM ("
            "SELECT COUNT(*) AS cnt FROM files "
            "WHERE md5 IS NOT NULL "
            "GROUP BY md5 HAVING COUNT(*) > 1"
            ")"
        ).fetchone()["c"]
    )

    duplicate_folder_groups = int(
        database.execute(
            "SELECT COUNT(*) AS c FROM ("
            "SELECT hash FROM folder_hash GROUP BY hash HAVING COUNT(*) > 1"
            ")"
        ).fetchone()["c"]
    )

    duplicate_folder_items = int(
        database.execute(
            "SELECT COALESCE(SUM(cnt), 0) AS c FROM ("
            "SELECT COUNT(*) AS cnt FROM folder_hash GROUP BY hash HAVING COUNT(*) > 1"
            ")"
        ).fetchone()["c"]
    )

    return DriveStats(
        file_count=file_count,
        folder_count=folder_count,
        duplicate_file_groups=duplicate_file_groups,
        duplicate_file_items=duplicate_file_items,
        duplicate_folder_groups=duplicate_folder_groups,
        duplicate_folder_items=duplicate_folder_items,
        estimated_reclaimable_bytes=estimate_reclaimable_bytes(database),
    )
