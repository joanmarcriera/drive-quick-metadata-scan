"""SQLite database layer for metadata ingestion and analysis."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Sequence
from pathlib import Path

FileRow = tuple[str, str, str | None, int | None, str | None, str]
FolderRow = tuple[str, str | None, str]


class Database:
    """Encapsulates low-level access to the metadata database."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._configure_connection()

    def _configure_connection(self) -> None:
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")
        self.conn.execute("PRAGMA foreign_keys = OFF;")

    def initialize(self) -> None:
        schema_path = Path(__file__).with_name("schema.sql")
        script = schema_path.read_text(encoding="utf-8")
        self.conn.executescript(script)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def clear_scan_data(self) -> None:
        self.conn.execute("DELETE FROM files")
        self.conn.execute("DELETE FROM folders")
        self.conn.execute("DELETE FROM folder_hash")
        self.conn.execute("DELETE FROM scan_state")
        self.conn.commit()

    def set_scan_state(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO scan_state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def get_scan_state(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM scan_state WHERE key = ?", (key,)).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def delete_scan_state(self, key: str) -> None:
        self.conn.execute("DELETE FROM scan_state WHERE key = ?", (key,))
        self.conn.commit()

    def upsert_files(self, rows: Sequence[FileRow]) -> None:
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT INTO files(id, name, parent, size, md5, mimeType)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                parent=excluded.parent,
                size=excluded.size,
                md5=excluded.md5,
                mimeType=excluded.mimeType
            """,
            rows,
        )

    def upsert_folders(self, rows: Sequence[FolderRow]) -> None:
        if not rows:
            return
        self.conn.executemany(
            """
            INSERT INTO folders(id, parent, name)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                parent=excluded.parent,
                name=excluded.name
            """,
            rows,
        )

    def execute(self, sql: str, params: Iterable[object] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, tuple(params))

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[object]]) -> sqlite3.Cursor:
        return self.conn.executemany(sql, seq_of_params)

    def commit(self) -> None:
        self.conn.commit()
