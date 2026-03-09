"""Duplicate file detection."""

from __future__ import annotations

from dataclasses import dataclass

from gdrive_dedupe.storage.database import Database


@dataclass(slots=True)
class FileRecord:
    id: str
    name: str
    parent: str | None
    size: int | None


@dataclass(slots=True)
class DuplicateFileGroup:
    md5: str
    count: int
    total_size: int
    files: list[FileRecord]


def get_duplicate_file_groups(
    database: Database, limit: int | None = None
) -> list[DuplicateFileGroup]:
    sql = (
        "SELECT md5, COUNT(*) AS cnt "
        "FROM files "
        "WHERE md5 IS NOT NULL "
        "GROUP BY md5 "
        "HAVING COUNT(*) > 1 "
        "ORDER BY cnt DESC"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    groups = []
    for row in database.execute(sql).fetchall():
        md5 = str(row["md5"])
        files_rows = database.execute(
            "SELECT id, name, parent, size FROM files WHERE md5 = ? ORDER BY parent, name",
            (md5,),
        ).fetchall()
        file_records = [
            FileRecord(
                id=str(f_row["id"]),
                name=str(f_row["name"]),
                parent=f_row["parent"],
                size=int(f_row["size"]) if f_row["size"] is not None else None,
            )
            for f_row in files_rows
        ]
        total_size = sum(item.size or 0 for item in file_records)
        groups.append(
            DuplicateFileGroup(
                md5=md5,
                count=int(row["cnt"]),
                total_size=total_size,
                files=file_records,
            )
        )

    return groups


def estimate_reclaimable_bytes(database: Database) -> int:
    row = database.execute("""
        SELECT COALESCE(SUM((bucket.cnt - 1) * bucket.size), 0) AS reclaimable
        FROM (
            SELECT md5, COUNT(*) AS cnt, MAX(size) AS size
            FROM files
            WHERE md5 IS NOT NULL AND size IS NOT NULL
            GROUP BY md5
            HAVING COUNT(*) > 1
        ) AS bucket
        """).fetchone()
    return int(row["reclaimable"]) if row else 0
