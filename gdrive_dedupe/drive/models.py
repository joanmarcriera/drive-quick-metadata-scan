"""Drive metadata models."""

from __future__ import annotations

from dataclasses import dataclass

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


@dataclass(slots=True)
class DriveItem:
    id: str
    name: str
    parent: str | None
    size: int | None
    md5: str | None
    mime_type: str

    @property
    def is_folder(self) -> bool:
        return self.mime_type == FOLDER_MIME_TYPE

    @classmethod
    def from_api(cls, data: dict[str, object]) -> DriveItem:
        parents = data.get("parents")
        parent = parents[0] if isinstance(parents, list) and parents else None

        raw_size = data.get("size")
        size = int(raw_size) if raw_size is not None else None

        return cls(
            id=str(data["id"]),
            name=str(data.get("name", "")),
            parent=parent,
            size=size,
            md5=data.get("md5Checksum") if isinstance(data.get("md5Checksum"), str) else None,
            mime_type=str(data.get("mimeType", "")),
        )
