"""File hash index helpers."""

from __future__ import annotations

from dataclasses import dataclass

from gdrive_dedupe.storage.database import Database


@dataclass(slots=True)
class Md5Bucket:
    md5: str
    count: int


class FileHashIndex:
    """Queries hash buckets directly from SQLite without loading all files in memory."""

    def __init__(self, database: Database):
        self.database = database

    def duplicate_buckets(self, limit: int | None = None) -> list[Md5Bucket]:
        sql = (
            "SELECT md5, COUNT(*) as cnt FROM files "
            "WHERE md5 IS NOT NULL "
            "GROUP BY md5 "
            "HAVING COUNT(*) > 1 "
            "ORDER BY cnt DESC"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"

        rows = self.database.execute(sql).fetchall()
        return [Md5Bucket(md5=row["md5"], count=int(row["cnt"])) for row in rows]
