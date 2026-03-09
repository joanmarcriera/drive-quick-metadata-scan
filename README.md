# gdrive-dedupe

`gdrive-dedupe` is a metadata-only Google Drive deduplication tool for very large drives.
It detects both duplicate files and duplicate folder trees without downloading file contents.

## Features

- Streams Google Drive metadata with paginated `files.list` calls
- Stores normalized metadata in indexed SQLite tables
- Detects duplicate files by MD5 checksum
- Detects duplicate folders via deterministic bottom-up tree hashing
- Supports resumable scans
- Produces CLI tables and HTML reports

## Architecture

Designed for large datasets (500k+ files, multi-TB drives):

- Streaming ingestion into SQLite (no full dataset in memory)
- Batched upserts and indexed queries
- Bottom-up folder hashing with queue-based processing
- Persistent scan state for resume support

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install .
```

## Google API setup

1. Enable Google Drive API in Google Cloud Console.
2. Create OAuth Desktop credentials.
3. Download `credentials.json`.
4. Run scan:

```bash
gdrive-dedupe scan --credentials /path/to/credentials.json
```

OAuth token is stored in:

`~/.config/gdrive-dedupe/token.json`

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

## Security considerations

- Uses read-only metadata scope: `drive.metadata.readonly`
- Never downloads file content
- Stores OAuth token locally under `~/.config/gdrive-dedupe/`
- Do not commit credentials or token files

## Development

```bash
ruff check .
black --check .
pytest
```

CI runs lint, format checks, tests, and install validation.

## License

MIT
