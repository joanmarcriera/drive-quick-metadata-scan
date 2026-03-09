from pathlib import Path

from gdrive_dedupe.dedupe.duplicate_files import (
    estimate_reclaimable_bytes,
    get_duplicate_file_groups,
)
from gdrive_dedupe.dedupe.duplicate_folders import (
    compute_all_folder_subtree_stats,
    get_actionable_duplicate_root_groups,
    get_actionable_root_recommendations,
    get_duplicate_folder_groups,
    get_folder_subtree_stats,
)
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


def test_actionable_root_groups_collapse_nested_duplicates(tmp_path: Path) -> None:
    db = Database(tmp_path / "actionable_roots.db")
    db.initialize()

    db.upsert_folders(
        [
            ("root_a", None, "root_a"),
            ("child_a", "root_a", "child"),
            ("root_b", None, "root_b"),
            ("child_b", "root_b", "child"),
        ]
    )
    db.upsert_files(
        [
            ("f1", "file.txt", "child_a", 100, "md5x", "text/plain"),
            ("f2", "file.txt", "child_b", 100, "md5x", "text/plain"),
        ]
    )
    db.executemany(
        "INSERT INTO folder_hash(folder_id, hash) VALUES(?, ?)",
        [
            ("root_a", "hash_root"),
            ("root_b", "hash_root"),
            ("child_a", "hash_child"),
            ("child_b", "hash_child"),
        ],
    )
    db.commit()

    groups = get_actionable_duplicate_root_groups(db)
    assert len(groups) == 1
    assert groups[0].hash_value == "hash_root"
    assert {folder.id for folder in groups[0].folders} == {"root_a", "root_b"}

    subtree = get_folder_subtree_stats(db, "root_a")
    assert subtree.folder_count == 2
    assert subtree.file_count == 1
    assert subtree.total_size == 100

    all_subtrees = compute_all_folder_subtree_stats(db)
    assert all_subtrees["root_a"].folder_count == 2
    assert all_subtrees["root_a"].file_count == 1
    assert all_subtrees["root_a"].total_size == 100


def test_actionable_recommendations_sorted_and_paginated(tmp_path: Path) -> None:
    db = Database(tmp_path / "recommendations.db")
    db.initialize()

    db.upsert_folders(
        [
            ("r1a", None, "R1A"),
            ("r1b", None, "R1B"),
            ("r2a", None, "R2A"),
            ("r2b", None, "R2B"),
        ]
    )
    db.upsert_files(
        [
            ("fr1a", "big.bin", "r1a", 1000, "m1", "application/octet-stream"),
            ("fr1b", "big.bin", "r1b", 1000, "m1", "application/octet-stream"),
            ("fr2a", "small.bin", "r2a", 100, "m2", "application/octet-stream"),
            ("fr2b", "small.bin", "r2b", 100, "m2", "application/octet-stream"),
        ]
    )
    db.executemany(
        "INSERT INTO folder_hash(folder_id, hash) VALUES(?, ?)",
        [
            ("r1a", "hash_big"),
            ("r1b", "hash_big"),
            ("r2a", "hash_small"),
            ("r2b", "hash_small"),
        ],
    )
    db.commit()

    recs = get_actionable_root_recommendations(db, limit=10, offset=0, min_reclaimable_bytes=1)
    assert len(recs) == 2
    assert recs[0].hash_value == "hash_big"
    assert recs[0].estimated_reclaimable_bytes == 1000
    assert recs[1].hash_value == "hash_small"
    assert recs[1].estimated_reclaimable_bytes == 100

    page_2 = get_actionable_root_recommendations(db, limit=1, offset=1, min_reclaimable_bytes=1)
    assert len(page_2) == 1
    assert page_2[0].hash_value == "hash_small"


def test_actionable_root_groups_ignore_nested_descendant_duplicates(tmp_path: Path) -> None:
    db = Database(tmp_path / "nested_descendant_duplicates.db")
    db.initialize()

    db.upsert_folders(
        [
            ("root_a", None, "root_a"),
            ("mid_a", "root_a", "mid_a"),
            ("leaf_a", "mid_a", "leaf"),
            ("root_b", None, "root_b"),
            ("mid_b", "root_b", "mid_b"),
            ("leaf_b", "mid_b", "leaf"),
        ]
    )
    db.upsert_files(
        [
            ("x1", "video.mov", "leaf_a", 1000, "mleaf", "video/quicktime"),
            ("x2", "video.mov", "leaf_b", 1000, "mleaf", "video/quicktime"),
        ]
    )
    db.executemany(
        "INSERT INTO folder_hash(folder_id, hash) VALUES(?, ?)",
        [
            ("root_a", "h_root"),
            ("root_b", "h_root"),
            ("mid_a", "h_mid_a"),
            ("mid_b", "h_mid_b"),
            ("leaf_a", "h_leaf"),
            ("leaf_b", "h_leaf"),
        ],
    )
    db.commit()

    groups = get_actionable_duplicate_root_groups(db)
    assert len(groups) == 1
    assert groups[0].hash_value == "h_root"
    assert {f.id for f in groups[0].folders} == {"root_a", "root_b"}
