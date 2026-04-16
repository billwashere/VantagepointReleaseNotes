# Deltek Vantagepoint Release Notes Explorer

A static website that lets you search and filter all Deltek Vantagepoint release notes — defects, enhancements, and regulatory changes — across every version since 2.0. Runs entirely in the browser using SQL.js (SQLite compiled to WebAssembly).

## ✨ Features

- 🔍 **Full-text search** across titles and descriptions
- 🏷️ **Filter by type**: Defects · Enhancements · Regulatory · Security
- 📦 **Filter by version**: Drill down to any patch release
- 🔀 **Ported defect detection**: See which defects were backported across multiple versions
- 🗂️ **Expandable rows**: Click any row to see the full description and version history
- 🗃️ **Zero backend**: The entire SQLite database is served as a static file

## 🚀 Quick Start

### Option A — Use the demo database (no scraping needed)

```bash
git clone https://github.com/YOUR_USER/vantagepoint-release-notes.git
cd vantagepoint-release-notes

# Install dependencies
pip install requests beautifulsoup4 lxml

# Seed with sample data from the pages we've already parsed
python scraper/seed_demo.py

# Copy db to web folder
cp db/release_notes.db web/

# Serve locally (any static server will do)
python -m http.server 8080 --directory web
# → Open http://localhost:8080
```

### Option B — Full scrape (gets all real data)

```bash
# Scrape all versions (takes ~30–60 min, ~200+ pages)
python scraper/scraper.py

# Or scrape specific versions only
python scraper/scraper.py --versions 7.0 2025.1

# Resume a partially-completed scrape
python scraper/scraper.py --resume

# Use a custom database path
python scraper/scraper.py --db /path/to/my.db

# Copy to web and serve
cp db/release_notes.db web/
python -m http.server 8080 --directory web
```

## 🌐 Deploy to GitHub Pages

### Manual setup

1. Fork / clone this repo
2. Run the scraper and commit the database:
   ```bash
   python scraper/scraper.py
   cp db/release_notes.db web/
   git add web/release_notes.db
   git commit -m "Add release notes database"
   git push
   ```
3. In GitHub repo Settings → Pages:
   - **Source**: Deploy from branch
   - **Branch**: `gh-pages` (created automatically by the Action)

### Automated weekly updates (GitHub Actions)

The included workflow at `.github/workflows/scrape-deploy.yml` will:
- Run every Monday at 06:00 UTC
- Scrape any new release notes (uses `--resume` to skip already-scraped pages)
- Deploy the updated site to GitHub Pages automatically

Enable it by ensuring GitHub Actions has write permissions:
1. Settings → Actions → General → Workflow permissions → "Read and write permissions"
2. Settings → Pages → Source → "GitHub Actions"

## 🗃️ Database Schema

```sql
releases (
    id, major_version, patch_version, build, release_date, url, scraped_at
)

issues (
    id, defect_number, type,      -- type: defect | enhancement | regulatory | security
    category, subcategory,
    title, description
)

issue_versions (
    issue_id → issues.id,
    release_id → releases.id
    -- same issue linked to multiple releases = "ported"
)

issues_fts           -- FTS5 virtual table for fast full-text search
```

**Ported defects**: When the same defect number appears in multiple release rows, the UI shows a "⇢ N versions" badge and the detail panel shows the full version history.

## 📋 Supported Versions

| Range | Notes |
|-------|-------|
| 2.0 – 7.3 | Classic numeric versioning |
| 2025.1 – 2026.2 | Quarterly calendar versioning |

## 🛠️ Project Structure

```
├── scraper/
│   ├── scraper.py      # Full web scraper
│   └── seed_demo.py    # Demo database seeder
├── web/
│   ├── index.html      # Single-file static site (SQL.js)
│   └── release_notes.db  # Generated SQLite database (gitignored by default)
├── db/
│   └── release_notes.db  # Working copy of database
├── .github/
│   └── workflows/
│       └── scrape-deploy.yml
└── README.md
```

## ⚙️ Scraper Details

The scraper:
1. Fetches each version's index page to discover all patch release URLs
2. Fetches each individual `.htm` release note page
3. Parses HTML to extract three section types:
   - **Regulatory Enhancements**: Federal/state payroll and compliance changes
   - **Enhancements**: New features organized by module
   - **Software Issues Resolved**: Defect records with `Defect XXXXXX:` prefix
4. Stores everything in SQLite with deduplication:
   - Defects are globally unique by defect number (same defect backported = one row, multiple `issue_versions` links)
   - Enhancements are deduplicated by title + category (a major feature described in multiple patch notes = one row)
5. Respects the server with a 0.5s delay between requests

## 📝 License

This project is a community tool. The underlying release note content is © Deltek Inc.
