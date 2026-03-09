from pathlib import Path

from gdrive_dedupe.dedupe.duplicate_folders import get_duplicate_folder_groups
from gdrive_dedupe.hashing.folder_hash_engine import FolderHashEngine
from gdrive_dedupe.storage.database import Database


def _seed_tree(db: Database) -> None:
    db.upsert_folders(
        [
            ("root1", None, "root1"),
            ("a1", "root1", "a1"),
            ("a2", "root1", "a2"),
            ("root2", None, "root2"),
            ("b1", "root2", "b1"),
            ("b2", "root2", "b2"),
        ]
    )
    db.upsert_files(
        [
            ("f1", "f1.txt", "a1", 10, "md5-a", "text/plain"),
            ("f2", "f2.txt", "a2", 20, "md5-b", "text/plain"),
            ("f3", "f3.txt", "b1", 10, "md5-a", "text/plain"),
            ("f4", "f4.txt", "b2", 20, "md5-b", "text/plain"),
        ]
    )
    db.commit()


def test_folder_hashing_detects_duplicate_subtrees(tmp_path: Path) -> None:
    db = Database(tmp_path / "test.db")
    db.initialize()
    _seed_tree(db)

    engine = FolderHashEngine(db, batch_size=2)
    processed = engine.compute_all(show_progress=False)

    assert processed == 6

    duplicate_groups = get_duplicate_folder_groups(db)
    counts = sorted(group.count for group in duplicate_groups)
    assert counts == [2, 2, 2]


def test_folder_hashing_is_deterministic(tmp_path: Path) -> None:
    db = Database(tmp_path / "deterministic.db")
    db.initialize()
    _seed_tree(db)

    engine = FolderHashEngine(db, batch_size=3)
    engine.compute_all(show_progress=False)
    first = db.execute("SELECT hash FROM folder_hash WHERE folder_id = 'root1'").fetchone()["hash"]

    db.execute("DELETE FROM folder_hash")
    db.commit()

    engine.compute_all(show_progress=False)
    second = db.execute("SELECT hash FROM folder_hash WHERE folder_id = 'root1'").fetchone()["hash"]

    assert first == second
