"""Duplicate folder detection based on deterministic folder hashes."""

from __future__ import annotations

from dataclasses import dataclass

from gdrive_dedupe.storage.database import Database


@dataclass(slots=True)
class FolderRecord:
    id: str
    name: str
    parent: str | None


@dataclass(slots=True)
class DuplicateFolderGroup:
    hash_value: str
    count: int
    folders: list[FolderRecord]


def get_duplicate_folder_groups(
    database: Database, limit: int | None = None
) -> list[DuplicateFolderGroup]:
    sql = (
        "SELECT hash, COUNT(*) AS cnt "
        "FROM folder_hash "
        "GROUP BY hash "
        "HAVING COUNT(*) > 1 "
        "ORDER BY cnt DESC"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    groups = []
    for row in database.execute(sql).fetchall():
        hash_value = str(row["hash"])
        folder_rows = database.execute(
            """
            SELECT f.id, f.name, f.parent
            FROM folder_hash AS fh
            JOIN folders AS f ON f.id = fh.folder_id
            WHERE fh.hash = ?
            ORDER BY f.parent, f.name
            """,
            (hash_value,),
        ).fetchall()
        folders = [
            FolderRecord(id=str(f["id"]), name=str(f["name"]), parent=f["parent"])
            for f in folder_rows
        ]
        groups.append(
            DuplicateFolderGroup(hash_value=hash_value, count=int(row["cnt"]), folders=folders)
        )

    return groups
