# gdrive-dedupe

`gdrive-dedupe` is a metadata-only Google Drive deduplication tool for very large drives.
It detects both duplicate files and duplicate folder trees without downloading file contents.

## Features

- Streams Google Drive metadata with paginated `files.list` calls
- Stores normalized metadata in indexed SQLite tables
- Detects duplicate files by MD5 checksum
- Detects duplicate folders via deterministic bottom-up tree hashing
- Supports resumable scans
- Produces CLI tables and HTML reports with Google Drive links for manual review

## Architecture

Designed for large datasets (500k+ files, multi-TB drives):

- Streaming ingestion into SQLite (no full dataset in memory)
- Batched upserts and indexed queries
- Bottom-up folder hashing with queue-based processing
- Persistent scan state for resume support

## Installation

Choose one workflow:

### Option A: `pip` + `venv`

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install .
```

### Option B: `uv` (faster dependency management)

Install `uv`: [https://docs.astral.sh/uv/](https://docs.astral.sh/uv/)

```bash
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
uv pip install -e .
```

## Google API and OAuth setup

Follow the official Google Drive quickstart flow for desktop OAuth apps:

- [Google Drive API Python quickstart](https://developers.google.com/workspace/drive/api/quickstart/python)

### 1) Create or select a Google Cloud project

- Open [Google Cloud Console](https://console.cloud.google.com/)
- Select an existing project or create a new one

### 2) Enable the Google Drive API

- Open the API page directly: [Google Drive API in API Library](https://console.cloud.google.com/apis/library/drive.googleapis.com)
- Click `Enable` (or `Manage` if already enabled)

### 3) Configure OAuth consent

- Open [Google Auth Platform](https://console.cloud.google.com/auth)
- Configure app details (name, support email, developer contact)
- Choose audience:
  - `Internal` for Google Workspace org-only usage
  - `External` for personal Gmail/multi-user usage
- If using `External` in testing mode, add your account under test users

### 4) Create Desktop OAuth client credentials

- Open [Credentials page](https://console.cloud.google.com/apis/credentials)
- Click `Create credentials` -> `OAuth client ID`
- Application type: `Desktop app`
- Download the JSON file
- Save it as `credentials.json` in your project root (or any path you prefer)

### 5) Run the first scan and complete OAuth login

```bash
gdrive-dedupe scan --credentials /path/to/credentials.json
```

On first run, the tool opens a browser for consent. After approval, the local token is stored at:

- `~/.config/gdrive-dedupe/token.json`

If you change scopes or OAuth client, delete that token file and authenticate again.

## Usage

```bash
gdrive-dedupe scan
gdrive-dedupe duplicates files
gdrive-dedupe duplicates folders
gdrive-dedupe report --output report.html
gdrive-dedupe stats
```

Optional shared-drive scan:

```bash
gdrive-dedupe scan --drive-id <DRIVE_ID>
```

## Review workflow (no CLI deletion)

`gdrive-dedupe` is intentionally non-destructive. It does not delete files/folders from the command line.

Recommended workflow:

1. Run `gdrive-dedupe report --output report.html`
2. Open the generated report in your browser
3. Use `Open file in Drive` / `Open folder in Drive` links to inspect duplicates
4. Decide and delete manually in Google Drive UI

## Database schema

```sql
files(id, name, parent, size, md5, mimeType)
folders(id, parent, name)
folder_hash(folder_id, hash)
```

Indexes:

- `files(md5)`
- `files(parent)`
- `folders(parent)`
- `folder_hash(hash)`

## Duplicate detection logic

### Duplicate files

```sql
SELECT md5, COUNT(*)
FROM files
WHERE md5 IS NOT NULL
GROUP BY md5
HAVING COUNT(*) > 1;
```

### Duplicate folders

For each folder:

- Leaf hash: `SHA256(sorted(file_md5_list))`
- Non-leaf hash: `SHA256(sorted(file_md5_list + child_folder_hashes))`

Folders with identical hash values are duplicates of the same tree shape/content.

## Performance notes

- Uses `pageSize=1000` for Drive API pagination
- Avoids loading all files/folders into RAM
- Uses WAL-mode SQLite for better write/read concurrency
- Supports thread workers for folder hash computation
- Real scan example from a user drive:
  - `Scanning metadata: 489742items [23:48, 342.79items/s]`
  - `Scan complete. Items scanned: 489742`

## Security considerations

- Uses read-only metadata scope: `drive.metadata.readonly`
- Never downloads file content
- Stores OAuth token locally under `~/.config/gdrive-dedupe/`
- Do not commit credentials or token files

## Development

`pip` workflow:

```bash
ruff check .
black --check .
pytest
```

`uv` workflow:

```bash
uv run ruff check .
uv run black --check .
uv run pytest
```

CI runs lint, format checks, tests, and install validation.

## License

MIT
