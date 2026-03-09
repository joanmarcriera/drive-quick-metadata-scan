from gdrive_dedupe.cli.app import _build_review_urls, _parse_size_to_bytes
from gdrive_dedupe.dedupe.duplicate_folders import (
    ActionableRootRecommendation,
    FolderRecord,
    FolderSubtreeStats,
)


def test_parse_size_to_bytes_units() -> None:
    assert _parse_size_to_bytes("0") == 0
    assert _parse_size_to_bytes("512") == 512
    assert _parse_size_to_bytes("1KB") == 1024
    assert _parse_size_to_bytes("1.5MB") == int(1.5 * 1024 * 1024)
    assert _parse_size_to_bytes("2GB") == 2 * 1024 * 1024 * 1024


def test_build_review_urls_keep_and_delete_candidates() -> None:
    recommendation = ActionableRootRecommendation(
        hash_value="h",
        copies=3,
        keep=FolderRecord(id="keep1", name="keep", parent=None),
        delete_candidates=[
            FolderRecord(id="del1", name="del1", parent=None),
            FolderRecord(id="del2", name="del2", parent=None),
            FolderRecord(id="del3", name="del3", parent=None),
        ],
        subtree=FolderSubtreeStats(folder_count=1, file_count=1, total_size=1024),
        estimated_reclaimable_bytes=2048,
    )

    urls = _build_review_urls(
        recommendation,
        include_keep=True,
        include_delete_candidates=True,
        delete_limit=2,
    )

    assert urls == [
        "https://drive.google.com/drive/folders/keep1",
        "https://drive.google.com/drive/folders/del1",
        "https://drive.google.com/drive/folders/del2",
    ]
