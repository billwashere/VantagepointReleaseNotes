# Deltek Vantagepoint Release Notes Explorer

A self-updating static website that lets you search and browse all Deltek Vantagepoint release notes — defects, enhancements, regulatory changes, and security updates — across every version from 2.0 onward. Runs entirely in the browser using SQLite compiled to WebAssembly. No server required after the initial download.

**Live demo:** deploy to GitHub Pages (see below).

---

## Features

| | |
|---|---|
| 🔍 **Full-text search** | Debounced, non-blocking — queries run in a Web Worker |
| 🏷️ **Filter by type** | Defects · Enhancements · Regulatory · Security · Ported Defects |
| 🗂️ **Application Area tree** | Three-level drill-down matching the in-app navigation breadcrumbs |
| 🔀 **Ported defect detection** | Same defect number across multiple versions → "⇢ N versions" badge |
| 📦 **Filter by version** | Drill into any patch release |
| 📄 **Paginated results** | 250 rows/page — no DOM storm on large result sets |
| ⬇️ **Streaming download** | Progress bar during initial DB load, main thread never blocked |
| 🌑 **Dark theme** | |

### Application Area hierarchy

Breadcrumbs are parsed directly from the release notes pages and stored as three nav levels:

| Level | Example |
|---|---|
| nav_level1 | `Hubs` · `Billing` · `My Stuff` · `Transaction Center` |
| nav_level2 | `Projects` · `Interactive Billing` · `Reporting` |
| nav_level3 | `Planning` · `AP Vouchers` |

**Regulatory and Security** issues are grouped under synthetic top-level nodes (`Regulatory`, `Security`) so that jurisdiction names (`Federal`, `California`, `Payroll`) don't pollute the Application Area sidebar alongside app module names.

```
Application Area
├── Billing
│   ├── Batch Billing
│   └── Interactive Billing
├── Hubs
│   └── Projects
│       ├── Planning
│       └── Project
├── Regulatory          ← all regulatory items grouped here
│   ├── Federal
│   └── Payroll
│       ├── California
│       └── Federal
└── Security            ← all security items grouped here
    └── API
```

---

## Quick start

### Run locally (demo data — no scraping needed)

```bash
git clone https://github.com/YOUR_USER/vantagepoint-release-notes.git
cd vantagepoint-release-notes

pip install -r scraper/requirements.txt
python scraper/seed_demo.py       # populates db/release_notes.db with sample data
cp db/release_notes.db web/

python serve.py                   # http://localhost:8001  (opens browser automatically)
```

> **Why `serve.py` instead of `python -m http.server`?**
> Python's built-in HTTP server does not handle `Range:` requests — the progress bar works but shows no percentage. `serve.py` adds proper `206 Partial Content` support so the progress bar is accurate and local dev matches GitHub Pages behaviour exactly.

```bash
python serve.py 8080              # custom port
python serve.py 8001 /path/to/web # custom port + directory
```

### Scrape all real data

```bash
# Full fresh scrape (all versions, ~30–60 min, ~200+ pages)
python scraper/scraper.py

# Resume — skip old releases, always re-check last 10 (detects retroactive amendments)
python scraper/scraper.py --resume --recheck 10

# Only specific major versions
python scraper/scraper.py --versions 7.0 2025.1

# Preview without writing to DB
python scraper/scraper.py --dry-run --versions 2025.1

# All options
python scraper/scraper.py --help
```

After scraping:
```bash
cp db/release_notes.db web/
python serve.py
```

---

## Deploy to GitHub Pages

### One-time setup

1. Fork / push this repo to GitHub
2. **Settings → Pages → Source:** set to "GitHub Actions"
3. **Settings → Actions → General → Workflow permissions:** "Read and write permissions"

The included workflow (`.github/workflows/scrape-deploy.yml`) will:
- Run the parser unit tests first — deploy only if they pass
- Scrape with `--resume --recheck 10 --concurrency 4` (fast on subsequent runs)
- Deploy `web/` to the `gh-pages` branch
- Repeat every Monday at 06:00 UTC

### Manual trigger

Go to **Actions → Scrape & Deploy Release Notes → Run workflow**.

---

## Project structure

```
├── scraper/
│   ├── scraper.py          Main scraper — fetch, parse, persist
│   ├── seed_demo.py        Seed a working demo DB without network access
│   └── requirements.txt    requests, beautifulsoup4, lxml, pytest
├── tests/
│   └── test_parser.py      30 offline unit tests for parse_html and utilities
├── web/
│   ├── index.html          Single-file static site (sql.js via Web Worker)
│   └── release_notes.db    Generated SQLite database (committed for GH Pages)
├── serve.py                Local dev server with HTTP Range request support
└── .github/workflows/
    └── scrape-deploy.yml   CI: test → scrape → deploy
```

---

## How the scraper works

### Version discovery

The scraper fetches the Deltek master index at:
```
https://help.deltek.com/product/Vantagepoint/ReleaseNotes/
```
All version-specific index URLs are extracted from this page automatically. New versions (e.g. `2026.3`) are picked up on the next weekly run with no code changes. A hardcoded fallback list is used if the master page is unreachable.

### Resume and change detection

```
All releases sorted newest-first
├── Last N releases (--recheck N, default 5) → always re-fetched
│   ├── content_hash unchanged → update timestamp only, skip DB write
│   └── content_hash changed  → re-upsert all issues, record diff
│       ├── added_keys   (new defect numbers on the page)
│       ├── removed_keys (defect numbers no longer listed)
│       └── modified_keys (same defect, description text amended)
└── Older releases → skipped entirely
```

Every run appends a row to `scrape_history` so you have a complete audit trail of when each page changed and what changed in it.

### Database schema

```sql
releases        -- one row per patch release (7.0.11, 2025.1.4, …)
issues          -- one row per unique issue (defect, enhancement, regulatory, security)
                -- issue_key: stable SHA-256 identity, excludes description
                -- first_seen_at / updated_at: timestamps
issue_versions  -- many-to-many: which releases contain which issue (ported defects)
scrape_log      -- latest scrape state per URL (content_hash, page_last_updated)
scrape_history  -- append-only audit: every scrape attempt, diffs as JSON arrays
issues_fts      -- FTS5 virtual table for full-text search
```

### Parser design

`parse_html(html: str)` is a pure function decoupled from HTTP — pass any HTML string to it. This makes it fully unit-testable without network access.

Key parsing decisions:

| Problem | Solution |
|---|---|
| `Defect1541790` (no space) consumed as breadcrumb | `DEFECT_RE` uses `\s*` not `\s+` |
| Enhancement titles in `<p><strong>` mistaken for module names | `is_heading_only AND len ≤ 25 chars` threshold |
| Enhancement title and description were emitted as two issues | `pending_title` buffer — bold title held until next plain paragraph |
| Regulatory jurisdiction names (`Federal`, `California`) pollute Application Area | `_nav_for_section()` remaps to `Regulatory >> Federal` etc. |
| Same defect number appearing twice on one page | Deduplication uses `make_issue_key()` (same hash as DB) |

---

## Running tests

```bash
# No pytest needed
python tests/test_parser.py

# With pytest
pip install pytest
pytest tests/ -v
```

Tests are offline — no network access required. The 30 tests cover:
- `DEFECT_RE` matching (standard and no-space variants)
- Breadcrumb parsing at all three levels
- Enhancement title + description merging
- Regulatory/Security nav remapping
- `_nav_for_section()` all section types
- `make_issue_key` stability across description changes
- `make_content_hash` change detection
- `_diff` add/remove/modify classification
- Deduplication
- `page_last_updated` extraction

---

## Notes

- **Not affiliated with Deltek.** The underlying release note content is © Deltek Inc.
- The scraper uses a 0.5 s delay between requests per worker. Please be respectful of Deltek's servers.
- **Version 7.3** does not exist — Deltek skipped it. It is absent from the master index and from the scraper's fallback list.
