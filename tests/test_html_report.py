from pathlib import Path

from gdrive_dedupe.reports.html_report import generate_html_report
from gdrive_dedupe.storage.database import Database


def test_html_report_includes_duplicate_folder_samples(tmp_path: Path) -> None:
    db = Database(tmp_path / "report.db")
    db.initialize()

    db.upsert_folders(
        [
            ("fa", None, "A"),
            ("fb", None, "B"),
            ("fc", None, "C"),
            ("git1", "fa", ".git"),
            ("git2", "fb", ".git"),
        ]
    )
    db.executemany(
        "INSERT INTO folder_hash(folder_id, hash) VALUES(?, ?)",
        [("fa", "h1"), ("fb", "h1"), ("fc", "h1")],
    )
    db.upsert_files(
        [
            ("f1", "dup.txt", "fa", 10, "md5dup", "text/plain"),
            ("f2", "dup.txt", "fb", 10, "md5dup", "text/plain"),
            ("f3", "dup.txt", "fc", 10, "md5dup", "text/plain"),
            ("f4", "index", "git1", 5, "md5git1", "application/octet-stream"),
            ("f5", "pack", "git2", 5, "md5git2", "application/octet-stream"),
        ]
    )
    db.commit()

    report_path = generate_html_report(
        db,
        output_path=tmp_path / "report.html",
        limit_per_section=10,
        file_samples_per_group=2,
        folder_samples_per_group=2,
    )

    html = report_path.read_text(encoding="utf-8")
    assert "Duplicate file sets" in html
    assert "Open file in Drive" in html
    assert "https://drive.google.com/file/d/f1/view" in html
    assert "... and 1 more files in this group." in html
    assert "Actionable Duplicate Roots" in html
    assert "Keep:" in html
    assert "Review delete candidates:" in html
    assert "Folder Size Hotspots" in html
    assert "File Count Hotspots" in html
    assert "Repository Metadata Hotspots (.git)" in html
    assert "/A/.git" in html
    assert "dup-root x3" in html
    assert "Duplicate folder trees" in html
    assert "example: A" in html
    assert "<code>fa</code> - /A - " in html
    assert "<code>fb</code> - /B - " in html
    assert "Open folder in Drive" in html
    assert "https://drive.google.com/drive/folders/fa" in html
    assert "... and 1 more folders in this group." in html
