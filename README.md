# notebooklm-gateway

A narrow, token-authenticated MCP gateway that lets AI agents (Claude Code, Codex, [NanoClaw](https://github.com/qwibitai/nanoclaw) groups, any MCP client) turn sources (URLs, uploaded files) into a NotebookLM **audio podcast**, **written report**, **slide deck** or **infographic**, and (per scope) list sources, ask questions of a notebook, and read or add notes. One Python process holds one long-lived `NotebookLMClient` (notebooklm-py 0.8.4, pinned) logged in as the operator's Google account via [notebooklm-py](https://github.com/teng-lin/notebooklm-py). Binds `127.0.0.1:8773`; every route lives under `/notebooklm/`.


> **Read this first.**
> - This is **unofficial**: it drives NotebookLM through [notebooklm-py](https://github.com/teng-lin/notebooklm-py), which uses undocumented Google endpoints. It can break when Google changes things, and automated use may conflict with Google's terms. Not affiliated with or endorsed by Google.
> - The host holds a **master token for the Google account** (`notebooklm login --master-token`): a durable, full-account credential that survives password changes until the device is removed in Google Account → Security. Upstream recommends a dedicated account. Run the gateway only on a machine you control, keep `~/.notebooklm/` and `state/` private, and expose it only behind TLS.
> - Agents never see the Google credential; they get narrow tools and a per-consumer bearer token you can revoke at any time.

## MCP tools (13)

`podcast_from_sources`, `report_from_sources`, `job_status`, `quota_status`, `share_link`, `list_notebooks`, `slides_from_sources`, `infographic_from_sources`, `list_sources`, `ask_notebook`, `read_note`, `create_note`, `get_source_text`. Podcast `format` is deep_dive|brief|critique|debate, `length` short|default|long; report `kind` is briefing|study_guide|blog; `language` defaults to `en`. Uploads go to a bearer-authed upload route, then are referenced by upload id (200 MB max, 2 h TTL).

Both generation tools take an optional `notebook` (exact notebook title, 1-100 printable characters) and an optional `title` (1-100 printable characters) for the result; invalid values return `bad_request` without echoing the value.

`list_notebooks` (scope `list`, granted per consumer) returns `{"notebooks": [{"title", "sources", "url"}]}` for every notebook in the account, newest first by creation time (upstream order when missing). No source names or contents are returned; the audit records only the count. Consumers without `list` neither see nor can call it.

### Scopes

| Scope | Tools |
|---|---|
| `podcast` | `podcast_from_sources` |
| `report` | `report_from_sources` |
| `list` | `list_notebooks` |
| `read` | `list_sources`, `ask_notebook` |
| `studio` | `slides_from_sources`, `infographic_from_sources` |
| `notes` | `read_note`, `create_note`, `get_source_text` |

`job_status`, `quota_status` and `share_link` are always visible. A tool outside the consumer's scopes is hidden from `tools/list` and refused as `not_found`.

### Studio tools (`studio`)

`slides_from_sources` (`format` pdf|pptx, default pdf) and `infographic_from_sources` (PNG) take the same `urls`, `upload_ids`, `notebook`, `title`, `instructions`, `language` as the podcast tool and run as the same kind of job (queued → running → done/failed, same rollback rules, `job_status`, `download_url`, `share_url` over 45 MB, rename to `title` or the default `"<first source title> · Slides|Infographic · <Mon D>"`). Downloads are `slides-<id>.pdf|.pptx` (`application/pdf`, `application/vnd.openxmlformats-officedocument.presentationml.presentation`) and `infographic-<id>.png` (`image/png`).

### Read and notes tools

These are synchronous (answer within 60 s) and take `notebook` = the exact title of an **existing** notebook; a missing notebook is `not_found` and is never created.

- `list_sources(notebook)` → `{"notebook", "sources": [{"title", "kind", "status"}]}` (no source text).
- `ask_notebook(notebook, question, source_titles?)` → `{"answer", "citations": [{"source_title", "excerpt"?}]}`. `question` 1-2000 characters (CRLF/CR become LF; newlines and tabs allowed; other control characters and the bidi controls U+202A-202E, U+2066-2069 are refused; everything else, e.g. NBSP or emoji, is accepted); `source_titles` limits the answer to sources with those exact titles (an unknown title is `bad_request` naming only its index). Bounded at 50 s (`generation_timeout`). Each answered question counts against the account-wide chat cap.
- `read_note(notebook, title?)` → without `title` `{"notes": [{"title", "chars"}]}`; with an exact title `{"title", "text"}` (cut at 20 KB with `truncated: true`; when several notes share the title the most recent is returned with `ambiguous: true`). Unknown title → `not_found`.
- `create_note(notebook, title, text)` → `{"title", "created": true}`. `title` 1-100 printable characters, `text` 1-100000 characters (same rules as the `ask_notebook` question). Only ever creates: it never edits or deletes a note. A timeout (`generation_timeout`, "NotebookLM did not confirm the note within 50 s; check with read_note before retrying.") is ambiguous: the note may exist, may be missing, or may exist blank as "New Note", so check with `read_note` before retrying.
- `get_source_text(notebook, source_title)` → `{"title", "chars", "text"}` up to 20 KB; longer text returns the first 20 KB with `truncated: true` and a `download_url` for `GET /notebooklm/sources/text/<ref>`. The ref is opaque (no titles in the URL): an HMAC with the link key over notebook id, source id, consumer and expiry (1 h), in its own domain so it cannot be used as a `/dl/` token. The route needs the same bearer (scope `notes`) and answers 404 for another consumer's, expired or altered refs and for a source deleted since; HEAD is 405. The text is `text/plain; charset=utf-8`, fetched from Google on demand into a temp file that is always deleted, never stored. Upstream rate limits answer 429. Duplicate source titles: the first match, with `ambiguous: true`.

Existing note and source titles may be up to 1000 characters when looked up. None of these tools log or audit questions, answers, titles or text: the audit records only event, consumer, notebook id and counts/lengths (`list_sources`, `ask` with `question_chars`, `read_note`, `create_note` with `text_chars`, `get_source_text`, `source_text_download`).

## Jobs and rollback

A job reserves quota, adds its sources to the named notebook, requests generation, polls (30 min timeout) and records the result. Rollback rules:

- Sources are removed only if the job fails **before generation was requested**.
- Once generation was requested the outcome is ambiguous (Google may have accepted it), so sources are kept; they are removed only if generation explicitly returned a failed status.
- Rollback is per-source and retryable: undeleted sources stay on the job with `rollback_pending`, and the maintenance loop retries them when auth is ok. After 50 failed retries it stops (audit event `rollback_abandoned`, counts only); those sources stay in the notebook and keep their slots reserved.
- On restart, jobs Google already accepted are re-attached and finish normally. If auth or the network is down at that point (or while an accepted job waits for its result), the job stays `running` and the maintenance loop re-attaches it once auth is back; only `generation_timeout` and `generation_failed` end an accepted job.
- Only jobs whose generation was requested but not yet accepted fail as `gateway_restarted` on restart, without rolling back (Google may still be generating). Jobs interrupted while being submitted also fail as `gateway_restarted` and their uploads are released.
- Each consumer may hold at most 10 unused, unexpired uploads; more return HTTP 429 (`bad_request`).

Error codes: `auth_expired`, `quota_exceeded`, `rate_limited`, `source_rejected`, `generation_timeout`, `generation_failed`, `upstream_changed`, `gateway_restarted`, `bad_request`, `not_found`.

## Notebooks by name

Agents add sources to any of the operator's notebooks by **exact, case-sensitive title**, including notebooks made by hand in the app. If no notebook has that title, it is created. Without `notebook`, the consumer's default notebook (its label, normally `To Review`) is used. When a notebook would exceed `NLM_NOTEBOOK_SOURCE_CAP`, the next one is `"<name> (2)"`, `"<name> (3)"`, and so on. A notebook deleted in the app is re-found or recreated on the next job.

## Titles

When a job is done, the podcast/report is renamed in NotebookLM to the caller's `title`, or by default to `"<first source title> · <Label> · <Mon D>"` (Label is Deep dive, Brief, Critique, Debate, Briefing, Study guide or Blog; the date is the finish date in `NLM_TIMEZONE`; the source title is cut to 60 characters, `Sources` if unavailable). A failed rename never fails the job: `job_status` then returns `title: null`, and the audit records `rename_failed` with the job id only (titles never reach logs or the audit).

## Signed download links

`/notebooklm/dl/...` is a public route (GET/HEAD) authorised by an HMAC signature only. Default TTL 24 h, max 168 h. Revoking a consumer invalidates all of its links. HEAD is answered from stored size and mime without contacting Google. Each GET re-fetches the file from Google (no host copy is kept), so a link serves one fetch at a time, and at most two Google downloads run at once across all download routes.

## Quotas

All quotas are rolling 24 h. Per consumer: audio and report caps. Account-wide: podcasts `NLM_ACCOUNT_AUDIO_DAILY` minus `NLM_ACCOUNT_AUDIO_RESERVE` (audio left for your own manual use); reports `NLM_ACCOUNT_REPORT_DAILY` (95); slide decks `NLM_ACCOUNT_SLIDES_DAILY` (10); infographics `NLM_ACCOUNT_INFOGRAPHIC_DAILY` (10); chat questions `NLM_ACCOUNT_CHAT_DAILY` (480). Slides and infographics have no per-consumer cap. Jobs that fail before Google accepted them are not counted; chat questions are counted when answered (an `events` table). Over cap returns `quota_exceeded`. `quota_status` returns `audio`, `report` (this consumer) and `account_audio`, `account_report`, `account_slides`, `account_infographic`, `account_chat` (`used`, `cap`).

## `.env` keys

Copy `.env.example` to `.env` (mode 600):

| Key | Meaning |
|---|---|
| `NLM_PUBLIC_BASE_URL` | Public Caddy base, no trailing slash (used in signed links) |
| `NLM_ACCOUNT_AUDIO_DAILY` | Tier's daily Audio Overview limit (free 3, Plus 6, Pro 20) |
| `NLM_ACCOUNT_AUDIO_RESERVE` | Audio per day kept for manual use |
| `NLM_ACCOUNT_REPORT_DAILY` | Account-wide reports per 24 h (default 95) |
| `NLM_ACCOUNT_SLIDES_DAILY` | Account-wide slide decks per 24 h (default 10) |
| `NLM_ACCOUNT_INFOGRAPHIC_DAILY` | Account-wide infographics per 24 h (default 10) |
| `NLM_ACCOUNT_CHAT_DAILY` | Account-wide `ask_notebook` questions per 24 h (default 480) |
| `NLM_NOTEBOOK_SOURCE_CAP` | Sources per notebook (free 50, Pro 300) |
| `NLM_TIMEZONE` | Timezone for the date in default titles (default America/New_York) |
| `NLM_STATE_DIR` | State directory (default `state`; credentials, never commit) |
| `NLM_HOST` | Listen address (default 127.0.0.1; keep it loopback) |
| `NLM_PORT` | Listen port (default 8773) |
| `NLM_WORKERS` | Concurrent jobs (default 2) |

## Operator steps

1. **Login (once):** `notebooklm login --master-token`. The master token lives under `~/.notebooklm/`; the gateway never reads it into `.env` or logs.
2. **Configure:** `cp .env.example .env && chmod 600 .env`; set the tier values.
3. **Pair consumers:** `uv run python scripts/consumers.py pair --id <id> --scopes podcast,report,list,read --label "To Review" --audio-cap N --report-cap N --note "..."` (prints the token once; `--label` is the default notebook name and defaults to `To Review`). Change a consumer without changing its token: `update --id <id> [--scopes a,b] [--label NAME] [--audio-cap N] [--report-cap N]`. `list`, `check` and `revoke --id <id>` also exist.
4. **Install under launchd:**
   ```bash
   sed -e "s#REPO_PATH#$PWD#g" -e "s#UV_PATH#$(which uv)#g" -e "s#HOME_PATH#$HOME#g" \
     launchd/com.notebooklm-gateway.http.plist > ~/Library/LaunchAgents/com.notebooklm-gateway.http.plist
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.notebooklm-gateway.http.plist
   ```
   Restart after any change: `launchctl kickstart -k gui/$(id -u)/com.notebooklm-gateway.http`. Logs: `~/Library/Logs/notebooklm-gateway.{out,err}.log`.
5. **Probe:** `NLM_PROBE_TOKEN=nlm_... uv run python scripts/mcp_probe.py` (healthz, tool list, quota; creates no jobs). Use a zero-cap consumer.
6. **Caddy:** route `/notebooklm/dl/*` to `127.0.0.1:8773` publicly, **GET and HEAD only**. Route everything else under `/notebooklm/*` (MCP, uploads, `/notebooklm/jobs/*/download`, `/notebooklm/sources/text/*`, `/notebooklm/healthz`) to the same upstream (every route there requires a consumer bearer token; source-pinning it to known clients is an optional extra layer). Do **not** strip the prefix. Allow a 200 MB request body on the upload route.

## Revocation

Remove the device in the Google account's security settings (this kills the master token), and run `uv run python scripts/consumers.py revoke --id <id>` for each consumer (this also invalidates their signed links).

## Upgrade check

Read the notebooklm-py release notes, bump the pin in `pyproject.toml`, `uv lock`, `uv run pytest`, `uv run python scripts/live_checks.py`, then kickstart the service.

- After any tool-schema change (new tool or argument): NanoClaw groups on pi-mcp-adapter cache tool schemas in `~/.pi/agent/mcp-cache.json` for up to 7 days; a group restart is not enough. Stop the group's container, drop the `notebooklm` cache entry, and restart it (until pi's built-in MCP replaces the adapter).
