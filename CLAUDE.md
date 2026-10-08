# notebooklm-gateway working rules

- Never log or put in errors/audit: URLs, upload filenames, tokens, credentials. Counts and `digest()` hashes only.
- `state/` holds credentials: never commit it, never `cat` it.
- The master token lives under `~/.notebooklm/`; never read it in our code, never put it in `.env`.
- Restart the service after changes: `launchctl kickstart -k gui/$(id -u)/com.notebooklm-gateway.http`.
- Run `codegraph index --force` after creating files.
- Tests: `uv run pytest`. Python 3.12, uv; notebooklm-py pinned exactly; mcp is 2.x (`>=2.3,<3`).
- Every route lives under `/notebooklm/`; Caddy must not strip the prefix.
