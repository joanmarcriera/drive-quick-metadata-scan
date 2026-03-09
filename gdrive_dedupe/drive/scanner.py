"""Google Drive metadata scanner."""

from __future__ import annotations

from typing import Any

from tqdm import tqdm

from gdrive_dedupe.drive.models import DriveItem
from gdrive_dedupe.storage.database import Database, FileRow, FolderRow


class DriveScanner:
    """Streams Drive API metadata into SQLite in large batches."""

    def __init__(self, service: Any, database: Database):
        self.service = service
        self.database = database

    def scan(
        self,
        *,
        resume: bool = True,
        page_size: int = 1000,
        query: str = "trashed = false",
        drive_id: str | None = None,
    ) -> int:
        if not resume:
            self.database.clear_scan_data()

        page_token = self.database.get_scan_state("next_page_token") if resume else None

        scanned_items = 0
        progress = tqdm(desc="Scanning metadata", unit="items")

        while True:
            params: dict[str, Any] = {
                "q": query,
                "pageSize": page_size,
                "pageToken": page_token,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
                "fields": "nextPageToken, files(id, name, parents, size, md5Checksum, mimeType)",
            }

            if drive_id:
                params["corpora"] = "drive"
                params["driveId"] = drive_id
            else:
                params["corpora"] = "allDrives"

            response = self.service.files().list(**params).execute()
            files = response.get("files", [])

            file_rows: list[FileRow] = []
            folder_rows: list[FolderRow] = []

            for file_data in files:
                item = DriveItem.from_api(file_data)
                if item.is_folder:
                    folder_rows.append((item.id, item.parent, item.name))
                else:
                    file_rows.append(
                        (item.id, item.name, item.parent, item.size, item.md5, item.mime_type)
                    )

            with self.database.conn:
                self.database.upsert_folders(folder_rows)
                self.database.upsert_files(file_rows)

            count = len(files)
            scanned_items += count
            progress.update(count)

            page_token = response.get("nextPageToken")
            if page_token:
                self.database.set_scan_state("next_page_token", str(page_token))
            else:
                self.database.delete_scan_state("next_page_token")
                self.database.set_scan_state("scan_complete", "1")
                break

            self.database.set_scan_state("scan_complete", "0")

        progress.close()
        return scanned_items
