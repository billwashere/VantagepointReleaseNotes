# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A self-updating static website for browsing Deltek Vantagepoint release notes. A Python scraper builds a SQLite database; the frontend runs that database entirely in the browser via sql.js (SQLite compiled to WebAssembly). No server is needed at runtime.

## Commands

```bash
# Install dependencies
pip install -r scraper/requirements.txt

# Run tests (offline, no network required)
pytest tests/ -v

# Run a single test
pytest tests/test_parser.py::TestParsing::test_name -v

# Generate demo database (no network)
python scraper/seed_demo.py && cp db/release_notes.db web/

# Serve locally (supports HTTP Range for accurate progress bar)
python serve.py                  # http://localhost:8001
python serve.py 8080             # custom port

# Production scrape
python scraper/scraper.py --resume --recheck 10
python scraper/scraper.py --dry-run --versions 2025.1   # preview only
python scraper/scraper.py --help
```

After any scrape: `cp db/release_notes.db web/`

## Architecture

**3-tier pipeline: Python scraper → SQLite DB → Browser frontend**

### Scraper (`scraper/scraper.py`)
- `discover_index_urls()` — auto-discovers all version URLs from Deltek master page
- `parse_html(html: str)` — **pure function**, fully decoupled from HTTP; this is the only thing tested
- `fetch_and_parse()` — concurrent fetches with HTTPAdapter + urllib3 Retry (exponential backoff)
- DB writes are atomic per release (`with conn:`)
- Issues are identified by SHA-256 `issue_key` (stable across description changes)
- `scrape_history` table stores append-only JSON diffs (added/removed/modified)
- `--resume` skips already-scraped releases; `--recheck N` re-fetches last N regardless

### Database (`db/release_notes.db`, `web/release_notes.db`)
Key tables: `releases`, `issues`, `issue_versions` (many-to-many for ported defects), `scrape_log`, `scrape_history`, `issues_fts` (FTS5 virtual table). Schema lives in `scraper.py`'s `create_db()`.

### Frontend (`web/index.html`)
Single-file static site — all CSS and JS embedded, no build step. Uses a Web Worker to run sql.js queries off the main thread. Features: full-text search (debounced), 3-level Application Area navigation, version filtering, pagination (250/page), ported-defect badges.

### Demo data (`scraper/seed_demo.py`)
Generates a plausible database without network access. Must be kept in sync with any schema changes in `create_db()`.

## Key Patterns

- **Parser testing**: All 30 unit tests in `tests/test_parser.py` test `parse_html()` directly — keep the parser a pure function
- **Schema changes**: Update `create_db()` in `scraper.py` AND `seed_demo.py`; the frontend SQL queries in `index.html` may also need updates
- **Frontend iteration**: Edit `web/index.html` directly; test via `serve.py` (plain `python -m http.server` lacks Range support and breaks the progress bar)
- **CI/CD**: `.github/workflows/scrape-deploy.yml` runs every Monday 06:00 UTC — `pytest` → `scraper.py --resume --recheck 10` → deploy to `gh-pages` branch
