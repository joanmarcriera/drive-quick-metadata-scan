"""OAuth helpers for Google Drive metadata access."""

from __future__ import annotations

from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build

SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


def get_config_dir() -> Path:
    config_dir = Path.home() / ".config" / "gdrive-dedupe"
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir


def get_default_token_path() -> Path:
    return get_config_dir() / "token.json"


def load_credentials(credentials_path: Path, token_path: Path | None = None) -> Credentials:
    token_path = token_path or get_default_token_path()

    creds: Credentials | None = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
        creds = flow.run_local_server(port=0)

    token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def build_drive_service(credentials_path: str | Path = "credentials.json") -> Resource:
    credentials_file = Path(credentials_path).expanduser().resolve()
    if not credentials_file.exists():
        raise FileNotFoundError(
            f"credentials.json not found at: {credentials_file}. "
            "See scripts/bootstrap_credentials.md for setup instructions."
        )

    creds = load_credentials(credentials_file)
    return build("drive", "v3", credentials=creds, cache_discovery=False)
