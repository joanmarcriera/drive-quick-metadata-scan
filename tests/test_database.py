from pathlib import Path

from gdrive_dedupe.storage.database import Database


def test_database_schema_has_required_indexes(tmp_path: Path) -> None:
    db = Database(tmp_path / "schema.db")
    db.initialize()

    file_indexes = {row[1] for row in db.execute("PRAGMA index_list('files')").fetchall()}
    folder_indexes = {row[1] for row in db.execute("PRAGMA index_list('folders')").fetchall()}
    hash_indexes = {row[1] for row in db.execute("PRAGMA index_list('folder_hash')").fetchall()}

    assert "idx_files_md5" in file_indexes
    assert "idx_files_parent" in file_indexes
    assert "idx_folders_parent" in folder_indexes
    assert "idx_folder_hash_hash" in hash_indexes


def test_scan_state_roundtrip(tmp_path: Path) -> None:
    db = Database(tmp_path / "state.db")
    db.initialize()

    db.set_scan_state("next_page_token", "abc123")
    assert db.get_scan_state("next_page_token") == "abc123"

    db.delete_scan_state("next_page_token")
    assert db.get_scan_state("next_page_token") is None
