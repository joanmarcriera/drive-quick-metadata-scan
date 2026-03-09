# Bootstrap Google OAuth credentials

1. Open Google Cloud Console and create/select a project.
2. Enable the Google Drive API.
3. Create OAuth Client ID credentials (Desktop app).
4. Download the OAuth client JSON and save it as `credentials.json`.
5. Place `credentials.json` in your working directory, or pass its path with:

```bash
gdrive-dedupe scan --credentials /path/to/credentials.json
```

On first run, gdrive-dedupe opens an OAuth consent flow in your browser and stores
an access token at:

`~/.config/gdrive-dedupe/token.json`
