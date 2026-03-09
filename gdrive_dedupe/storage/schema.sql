PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;

CREATE TABLE IF NOT EXISTS files (
    id TEXT PRIMARY KEY,
    name TEXT,
    parent TEXT,
    size INTEGER,
    md5 TEXT,
    mimeType TEXT
);

CREATE TABLE IF NOT EXISTS folders (
    id TEXT PRIMARY KEY,
    parent TEXT,
    name TEXT
);

CREATE TABLE IF NOT EXISTS folder_hash (
    folder_id TEXT PRIMARY KEY,
    hash TEXT
);

CREATE TABLE IF NOT EXISTS scan_state (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_md5 ON files(md5);
CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent);
CREATE INDEX IF NOT EXISTS idx_folders_parent ON folders(parent);
CREATE INDEX IF NOT EXISTS idx_folder_hash_hash ON folder_hash(hash);
