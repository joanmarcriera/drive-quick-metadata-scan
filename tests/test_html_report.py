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
        ]
    )
    db.executemany(
        "INSERT INTO folder_hash(folder_id, hash) VALUES(?, ?)",
        [("fa", "h1"), ("fb", "h1"), ("fc", "h1")],
    )
    db.commit()

    report_path = generate_html_report(
        db,
        output_path=tmp_path / "report.html",
        limit_per_section=10,
        folder_samples_per_group=2,
    )

    html = report_path.read_text(encoding="utf-8")
    assert "Duplicate folder trees" in html
    assert "example: A" in html
    assert "<code>fa</code> - /A" in html
    assert "<code>fb</code> - /B" in html
    assert "... and 1 more folders in this group." in html
