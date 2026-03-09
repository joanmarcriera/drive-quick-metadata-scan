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
- Highlights actionable top-level duplicate root sets with keep-vs-review candidates
- Visualizes largest folder trees, highest file-count folders, and `.git` hotspots

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
gdrive-dedupe duplicates waste --limit 20 --offset 0
gdrive-dedupe report --output report.html
gdrive-dedupe stats
```

Optional shared-drive scan:

```bash
gdrive-dedupe scan --drive-id <DRIVE_ID>
```

Largest duplicate waste first (then next page):

```bash
gdrive-dedupe duplicates waste --limit 20 --offset 0
gdrive-dedupe duplicates waste --limit 20 --offset 20
gdrive-dedupe duplicates waste --limit 20 --offset 40
```

Filter out small savings:

```bash
gdrive-dedupe duplicates waste --min-reclaimable 2GB --limit 20
```

Interactive cleanup review (largest duplicate roots first), opening up to 5 Drive folders at once:

```bash
gdrive-dedupe duplicates waste --interactive --open-links 5 --open-mode windows
```

Generate an HTML report with all candidate links for actionable duplicate roots:

```bash
gdrive-dedupe report --output report.html --actionable-candidates 0
```

## CLI options reference

Full command tree:

```bash
gdrive-dedupe [OPTIONS] COMMAND [ARGS]...
```

Top-level options:

- `--install-completion` Install completion for the current shell.
- `--show-completion` Print completion script for the current shell.
- `--help` Show help.

Top-level commands:

- `scan`
- `report`
- `stats`
- `duplicates` (`files`, `folders`, `waste`)

### `scan`

```bash
gdrive-dedupe scan [OPTIONS]
```

- `--db PATH` SQLite metadata DB path. Default: `~/.config/gdrive-dedupe/metadata.db`
- `--credentials PATH` OAuth client JSON path. Default: `credentials.json`
- `--query TEXT` Drive `files.list` filter. Default: `trashed = false`
- `--drive-id TEXT` Optional shared drive ID.
- `--resume / --no-resume` Resume from stored scan page token. Default: `--resume`
- `--page-size INTEGER` Range `100..1000`. Default: `1000`
- `--help`

### `report`

```bash
gdrive-dedupe report [OPTIONS]
```

- `--db PATH` SQLite metadata DB path. Default: `~/.config/gdrive-dedupe/metadata.db`
- `--output PATH` Output HTML path. Default: `report.html`
- `--limit INTEGER` Max duplicate groups per section (`>=1`). Default: `100`
- `--actionable-candidates INTEGER` Delete-candidate links per actionable root (`>=0`, `0 = all`). Default: `25`
- `--recompute / --no-recompute` Recompute folder hashes before report. Default: `--recompute`
- `--workers INTEGER` Hash worker threads (`1..16`). Default: `1`
- `--help`

### `stats`

```bash
gdrive-dedupe stats [OPTIONS]
```

- `--db PATH` SQLite metadata DB path. Default: `~/.config/gdrive-dedupe/metadata.db`
- `--help`

### `duplicates`

```bash
gdrive-dedupe duplicates [OPTIONS] COMMAND [ARGS]...
```

- `--help`

Subcommands:

- `files`
- `folders`
- `waste`

### `duplicates files`

```bash
gdrive-dedupe duplicates files [OPTIONS]
```

- `--db PATH` SQLite metadata DB path. Default: `~/.config/gdrive-dedupe/metadata.db`
- `--limit INTEGER` Max duplicate groups to display. Default: `50`
- `--help`

### `duplicates folders`

```bash
gdrive-dedupe duplicates folders [OPTIONS]
```

- `--db PATH` SQLite metadata DB path. Default: `~/.config/gdrive-dedupe/metadata.db`
- `--recompute / --no-recompute` Recompute folder hashes before listing. Default: `--recompute`
- `--workers INTEGER` Hash worker threads (`1..16`). Default: `1`
- `--limit INTEGER` Max duplicate groups to display. Default: `50`
- `--help`

### `duplicates waste`

```bash
gdrive-dedupe duplicates waste [OPTIONS]
```

- `--db PATH` SQLite metadata DB path. Default: `~/.config/gdrive-dedupe/metadata.db`
- `--recompute / --no-recompute` Recompute folder hashes before ranking reclaimable roots. Default: `--no-recompute`
- `--workers INTEGER` Hash worker threads (`1..16`). Default: `1`
- `--limit INTEGER` Rows to show (`>=1`). Default: `15`
- `--offset INTEGER` Row offset for pagination (`>=0`). Default: `0`
- `--min-reclaimable TEXT` Threshold like `500MB`, `2GB`, `0`. Default: `1MB`
- `--sample-candidates INTEGER` Candidate folders per row (`1..10`). Default: `3`
- `--interactive / --no-interactive` Start interactive ranking review loop. Default: `--no-interactive`
- `--open-links INTEGER` Links opened per interactive action (`1..20`). Default: `5`
- `--open-mode [tabs|windows]` Browser open target style. Default: `windows`
- `--help`

## Review workflow (no CLI deletion)

`gdrive-dedupe` is intentionally non-destructive. It does not delete files/folders from the command line.

Recommended workflow:

1. Run `gdrive-dedupe report --output report.html`
2. Open the generated report in your browser
3. Start with `Actionable Duplicate Roots` (top-level duplicate sets) to remove noise
   - Nested duplicate descendants are suppressed so you only review root duplicate folders
4. Review `Folder Size Hotspots` and `File Count Hotspots` for high-impact cleanup
5. Check `Repository Metadata Hotspots (.git)` for repo artifacts to remove
6. Use `Open file in Drive` / `Open folder in Drive` links to inspect duplicates
7. Decide and delete manually in Google Drive UI

Interactive shortcuts (`gdrive-dedupe duplicates waste --interactive`):

- `o`: open keep folder + top delete candidates in browser
- `d`: open only delete-candidate folders in browser
- `k`: open only keep folder in browser
- `n`: next ranked candidate
- `p`: previous ranked candidate
- `j <rank>`: jump to a rank number
- `q`: quit interactive mode

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
