"""Deterministic bottom-up folder hashing engine."""

from __future__ import annotations

import hashlib
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from tqdm import tqdm

from gdrive_dedupe.storage.database import Database


class FolderHashEngine:
    """Computes deterministic folder tree hashes and persists them in folder_hash."""

    def __init__(self, database: Database, batch_size: int = 512):
        self.database = database
        self.batch_size = batch_size
        self._local = threading.local()

    def compute_all(self, workers: int = 1, show_progress: bool = True) -> int:
        conn = self.database.conn
        folder_count = int(conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0])
        if folder_count == 0:
            conn.execute("DELETE FROM folder_hash")
            conn.commit()
            return 0

        conn.execute("DELETE FROM folder_hash")
        conn.execute("DROP TABLE IF EXISTS folder_work")
        conn.execute("DROP TABLE IF EXISTS queue")
        conn.execute(
            "CREATE TEMP TABLE folder_work(folder_id TEXT PRIMARY KEY, pending_children INTEGER)"
        )
        conn.execute("CREATE TEMP TABLE queue(folder_id TEXT PRIMARY KEY)")

        conn.execute("""
            INSERT INTO folder_work(folder_id, pending_children)
            SELECT f.id, COALESCE(c.child_count, 0)
            FROM folders AS f
            LEFT JOIN (
                SELECT parent, COUNT(*) AS child_count
                FROM folders
                WHERE parent IS NOT NULL
                GROUP BY parent
            ) AS c
            ON f.id = c.parent
            """)
        conn.execute(
            "INSERT INTO queue(folder_id) "
            "SELECT folder_id FROM folder_work WHERE pending_children = 0"
        )
        conn.commit()

        processed = 0
        progress = tqdm(
            total=folder_count, desc="Hashing folders", unit="folders", disable=not show_progress
        )

        executor: ThreadPoolExecutor | None = None
        if workers > 1 and self.database.db_path != Path(":memory:"):
            executor = ThreadPoolExecutor(max_workers=workers)

        try:
            while True:
                batch_rows = conn.execute(
                    "SELECT folder_id FROM queue ORDER BY folder_id LIMIT ?", (self.batch_size,)
                ).fetchall()
                if not batch_rows:
                    break

                batch_ids = [str(row[0]) for row in batch_rows]
                conn.executemany(
                    "DELETE FROM queue WHERE folder_id = ?", [(fid,) for fid in batch_ids]
                )

                if executor is None:
                    hashes = [(fid, self._compute_folder_hash(conn, fid)) for fid in batch_ids]
                else:
                    hashes = list(executor.map(self._compute_folder_hash_threadsafe, batch_ids))

                conn.executemany(
                    "INSERT OR REPLACE INTO folder_hash(folder_id, hash) VALUES(?, ?)",
                    hashes,
                )

                for folder_id in batch_ids:
                    parent_row = conn.execute(
                        "SELECT parent FROM folders WHERE id = ?", (folder_id,)
                    ).fetchone()
                    if parent_row is None or parent_row[0] is None:
                        continue

                    parent_id = str(parent_row[0])
                    conn.execute(
                        "UPDATE folder_work "
                        "SET pending_children = pending_children - 1 "
                        "WHERE folder_id = ?",
                        (parent_id,),
                    )
                    pending_row = conn.execute(
                        "SELECT pending_children FROM folder_work WHERE folder_id = ?",
                        (parent_id,),
                    ).fetchone()
                    if pending_row is not None and int(pending_row[0]) == 0:
                        conn.execute(
                            "INSERT OR IGNORE INTO queue(folder_id) VALUES(?)", (parent_id,)
                        )

                conn.commit()
                processed += len(batch_ids)
                progress.update(len(batch_ids))
        finally:
            progress.close()
            if executor:
                executor.shutdown(wait=True)

        conn.execute("DROP TABLE IF EXISTS folder_work")
        conn.execute("DROP TABLE IF EXISTS queue")
        conn.commit()
        return processed

    def _get_thread_connection(self) -> sqlite3.Connection:
        if getattr(self._local, "conn", None) is None:
            self._local.conn = sqlite3.connect(str(self.database.db_path))
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _compute_folder_hash_threadsafe(self, folder_id: str) -> tuple[str, str]:
        conn = self._get_thread_connection()
        return folder_id, self._compute_folder_hash(conn, folder_id)

    @staticmethod
    def _compute_folder_hash(conn: sqlite3.Connection, folder_id: str) -> str:
        file_md5s = [
            str(row[0])
            for row in conn.execute(
                "SELECT md5 FROM files WHERE parent = ? AND md5 IS NOT NULL ORDER BY md5",
                (folder_id,),
            ).fetchall()
        ]

        child_hashes = [
            str(row[0])
            for row in conn.execute(
                """
                SELECT fh.hash
                FROM folders AS c
                JOIN folder_hash AS fh ON fh.folder_id = c.id
                WHERE c.parent = ?
                ORDER BY fh.hash
                """,
                (folder_id,),
            ).fetchall()
        ]

        payload_parts = sorted(file_md5s + child_hashes)
        payload = "\n".join(payload_parts).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()
