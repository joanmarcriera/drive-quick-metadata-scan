from pathlib import Path

from gdrive_dedupe.dedupe.duplicate_files import (
    estimate_reclaimable_bytes,
    get_duplicate_file_groups,
)
from gdrive_dedupe.dedupe.duplicate_folders import get_duplicate_folder_groups
from gdrive_dedupe.storage.database import Database


def test_duplicate_files_grouping(tmp_path: Path) -> None:
    db = Database(tmp_path / "dup_files.db")
    db.initialize()

    db.upsert_files(
        [
            ("1", "a.txt", "p1", 10, "aaa", "text/plain"),
            ("2", "b.txt", "p2", 10, "aaa", "text/plain"),
            ("3", "c.txt", "p3", 10, "aaa", "text/plain"),
            ("4", "d.txt", "p4", 99, "bbb", "text/plain"),
        ]
    )
    db.commit()

    groups = get_duplicate_file_groups(db)
    assert len(groups) == 1
    assert groups[0].md5 == "aaa"
    assert groups[0].count == 3
    assert groups[0].total_size == 30
    assert estimate_reclaimable_bytes(db) == 20


def test_duplicate_folders_grouping(tmp_path: Path) -> None:
    db = Database(tmp_path / "dup_folders.db")
    db.initialize()

    db.upsert_folders(
        [
            ("a", None, "A"),
            ("b", None, "B"),
            ("c", None, "C"),
        ]
    )
    db.executemany(
        "INSERT INTO folder_hash(folder_id, hash) VALUES(?, ?)",
        [("a", "h1"), ("b", "h1"), ("c", "h2")],
    )
    db.commit()

    groups = get_duplicate_folder_groups(db)
    assert len(groups) == 1
    assert groups[0].hash_value == "h1"
    assert groups[0].count == 2
    assert {f.id for f in groups[0].folders} == {"a", "b"}
