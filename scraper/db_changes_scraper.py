#!/usr/bin/env python3
"""
Deltek Vantagepoint Database Changes Scraper
=============================================
Discovers and scrapes the "DatabaseChanges.htm" pages linked from each
version's release notes index, storing schema-change data into the existing
release_notes.db (db_change_sets + db_change_items tables).

Usage:
    python scraper/db_changes_scraper.py
    python scraper/db_changes_scraper.py --resume
    python scraper/db_changes_scraper.py --resume --recheck 10 --concurrency 4
    python scraper/db_changes_scraper.py --versions 2026.2 2025.1
    python scraper/db_changes_scraper.py --dry-run
"""

from __future__ import annotations

import logging
import queue
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

# Shared utilities from the main scraper
import scraper as _base
from scraper import (
    DB_PATH, fetch_html, discover_index_urls,
    clean_text, NetworkError, ParseError,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Version string normalisation
# ---------------------------------------------------------------------------

def _digits_to_version(digits: str) -> str:
    """Convert a raw digit sequence from a DB-change filename into a dotted version.

    Examples:
        '20262' → '2026.2'
        '20251' → '2025.1'
        '72'    → '7.2'
        '71'    → '7.1'
    """
    d = digits.lstrip("0") or "0"
    if len(digits) >= 5:
        # Modern YYYYM / YYYYMM format
        return f"{digits[:4]}.{int(digits[4:])}"
    if len(digits) == 2:
        return f"{digits[0]}.{digits[1]}"
    # e.g. '3digit' like '100' → '10.0'
    return f"{digits[:-1]}.{digits[-1]}"


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

_DB_CHANGE_FILENAME_RE = re.compile(
    r"DVP(\d+)toDVP(\d+)DatabaseChanges", re.IGNORECASE
)
_DB_CHANGE_SINGLE_RE = re.compile(
    r"DeltekVantagepoint(\d+)DatabaseChanges", re.IGNORECASE
)
_LINK_TEXT_VERSION_RE = re.compile(
    r"(?:(?:Deltek\s+)?Vantagepoint\s+)?([\d.]+)\s+to\s+(?:(?:Deltek\s+)?Vantagepoint\s+)?([\d.]+)",
    re.IGNORECASE,
)


def _extract_versions_from_url(url: str) -> tuple[str, str] | None:
    """Try to extract (from_version, to_version) from a DatabaseChanges URL."""
    m = _DB_CHANGE_FILENAME_RE.search(url)
    if m:
        return _digits_to_version(m.group(1)), _digits_to_version(m.group(2))
    m = _DB_CHANGE_SINGLE_RE.search(url)
    if m:
        v = _digits_to_version(m.group(1))
        return v, v
    return None


def _extract_versions_from_text(text: str) -> tuple[str, str] | None:
    m = _LINK_TEXT_VERSION_RE.search(text)
    if m:
        return m.group(1), m.group(2)
    return None


def discover_db_change_urls(index_url: str) -> list[dict]:
    """Fetch a version release-index page and return all DB-change page descriptors.

    Returns list of {from_version, to_version, url} dicts.
    """
    try:
        html = fetch_html(index_url)
    except NetworkError as exc:
        log.warning("Index unavailable: %s — %s", index_url, exc)
        return []
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    results: list[dict] = []

    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        if not re.search(r"DatabaseChanges", href, re.IGNORECASE):
            continue
        full_url = urljoin(index_url, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        # Try to get versions from URL filename first, then link text
        versions = _extract_versions_from_url(href)
        if not versions:
            versions = _extract_versions_from_text(a.get_text())
        if not versions:
            log.debug("Could not extract versions from %s", full_url)
            continue
        from_v, to_v = versions
        results.append({"from_version": from_v, "to_version": to_v, "url": full_url})

    return results


# ---------------------------------------------------------------------------
# HTML parser — pure function
# ---------------------------------------------------------------------------

_SECTION_HEADING_MAP: dict[re.Pattern, str] = {
    re.compile(r"new\s+tables?", re.I):                          "new_table",
    re.compile(r"removed?\s+tables?", re.I):                     "removed_table",
    re.compile(r"renamed?\s+tables?", re.I):                     "renamed_table",
    re.compile(r"new\s+columns?", re.I):                         "new_column",
    re.compile(r"(changes?\s+to\s+existing|modified)\s+columns?", re.I): "modified_column",
    re.compile(r"removed?\s+columns?", re.I):                    "removed_column",
    re.compile(r"renamed?\s+columns?", re.I):                    "renamed_column",
    re.compile(r"new\s+objects?", re.I):                         "new_object",
    re.compile(r"removed?\s+objects?", re.I):                    "removed_object",
}

_OBJECT_SECTIONS = {"new_object", "removed_object"}


def _classify_heading(text: str) -> str | None:
    for pat, change_type in _SECTION_HEADING_MAP.items():
        if pat.search(text):
            return change_type
    return None


def _is_header_row(cells: list[str]) -> bool:
    """Heuristic: a header row contains words like Table, Column, DataType, Object.
    Uses substring matching (no word boundaries) to handle camelCase like 'TableName'.
    """
    joined = " ".join(cells).lower()
    return bool(re.search(r"table|column|datatype|data.?type|object", joined))


def parse_db_changes_html(html: str) -> list[dict]:
    """Pure function: raw HTML → list of change item dicts.

    Each dict has keys: change_type, table_name, column_name, data_type,
    old_data_type, new_data_type, object_name, object_type.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as exc:
        raise ParseError(f"BeautifulSoup failed: {exc}") from exc

    items: list[dict] = []
    current_type: str | None = None

    # Walk all block elements in document order, tracking headings and tables
    for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "p", "table"]):
        if tag.name == "table":
            if not current_type:
                continue
            rows = tag.find_all("tr")
            for i, row in enumerate(rows):
                cells = [clean_text(td.get_text(" ", strip=True))
                         for td in row.find_all(["td", "th"])]
                if not cells or all(c == "" for c in cells):
                    continue
                # Skip header rows (first row or looks like column headers)
                if i == 0 and _is_header_row(cells):
                    continue

                n = len(cells)
                item: dict = {
                    "change_type":   current_type,
                    "table_name":    None,
                    "column_name":   None,
                    "data_type":     None,
                    "old_data_type": None,
                    "new_data_type": None,
                    "object_name":   None,
                    "object_type":   None,
                }

                if current_type in _OBJECT_SECTIONS:
                    item["object_name"] = cells[0] or None
                    item["object_type"] = cells[1] if n > 1 else None
                elif n == 1:
                    item["table_name"] = cells[0] or None
                elif n == 2:
                    item["table_name"]  = cells[0] or None
                    item["column_name"] = cells[1] or None
                elif n == 3:
                    item["table_name"]  = cells[0] or None
                    item["column_name"] = cells[1] or None
                    item["data_type"]   = cells[2] or None
                elif n >= 4:
                    item["table_name"]    = cells[0] or None
                    item["column_name"]   = cells[1] or None
                    item["old_data_type"] = cells[2] or None
                    item["new_data_type"] = cells[3] or None

                # Skip blank rows
                if all(v is None for k, v in item.items() if k != "change_type"):
                    continue
                items.append(item)
        else:
            # Heading element — detect section type
            text = clean_text(tag.get_text(" ", strip=True))
            ct = _classify_heading(text)
            if ct:
                current_type = ct

    return items


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------

def _upsert_change_set(conn: sqlite3.Connection, cs: dict) -> int | None:
    """Insert a change set; return its id.  Returns None if already present."""
    now = datetime.utcnow().isoformat()
    try:
        cur = conn.execute(
            """INSERT INTO db_change_sets (from_version, to_version, url, scraped_at)
               VALUES (?, ?, ?, ?)""",
            (cs["from_version"], cs["to_version"], cs["url"], now),
        )
        return cur.lastrowid
    except sqlite3.IntegrityError:
        return None  # already scraped


def _insert_items(conn: sqlite3.Connection, change_set_id: int, items: list[dict]) -> None:
    conn.executemany(
        """INSERT INTO db_change_items
           (change_set_id, change_type, table_name, column_name,
            data_type, old_data_type, new_data_type, object_name, object_type)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (change_set_id, i["change_type"], i["table_name"], i["column_name"],
             i["data_type"], i["old_data_type"], i["new_data_type"],
             i["object_name"], i["object_type"])
            for i in items
        ],
    )


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------

def run(
    versions_filter: list[str] | None = None,
    resume: bool = False,
    recheck: int = 5,
    concurrency: int = 3,
    dry_run: bool = False,
) -> int:
    """Discover and scrape all DB-change pages. Returns count of pages scraped."""
    conn = _base.get_db(DB_PATH)
    log.info("DB: %s%s", DB_PATH, "  [DRY-RUN]" if dry_run else "")

    # Discover all version index URLs
    all_index_urls = discover_index_urls()
    if versions_filter:
        all_index_urls = [
            u for u in all_index_urls
            if any(f"/{v}/" in u for v in versions_filter)
        ]

    # Collect all DB-change page descriptors across all versions
    all_cs: list[dict] = []
    for idx_url in all_index_urls:
        found = discover_db_change_urls(idx_url)
        all_cs.extend(found)
        log.info("Index %-8s → %d DB-change pages", idx_url.split("/")[-3], len(found))

    if not all_cs:
        log.warning("No database change pages found.")
        conn.close()
        return 0

    # Deduplicate by URL
    seen_urls: set[str] = set()
    unique_cs = []
    for cs in all_cs:
        if cs["url"] not in seen_urls:
            seen_urls.add(cs["url"])
            unique_cs.append(cs)
    all_cs = unique_cs

    # Resume: skip already-scraped URLs (unless in recheck window)
    if resume:
        existing = {
            row[0]
            for row in conn.execute("SELECT url FROM db_change_sets").fetchall()
        }
        # Identify recheck candidates: last N by insertion order
        recheck_urls: set[str] = set()
        if recheck > 0:
            recent = conn.execute(
                "SELECT url FROM db_change_sets ORDER BY id DESC LIMIT ?", (recheck,)
            ).fetchall()
            recheck_urls = {r[0] for r in recent}

        to_fetch = [
            cs for cs in all_cs
            if cs["url"] not in existing or cs["url"] in recheck_urls
        ]
        skipped = len(all_cs) - len(to_fetch)
        log.info("Resume — skipping %d already-scraped, fetching %d", skipped, len(to_fetch))
    else:
        to_fetch = all_cs

    if not to_fetch:
        log.info("Nothing to fetch.")
        conn.close()
        return 0

    log.info("Fetching %d DB-change pages…", len(to_fetch))

    # Concurrent fetch+parse, serialised DB writes
    result_q: queue.Queue = queue.Queue()

    def worker(cs: dict) -> None:
        try:
            html = fetch_html(cs["url"])
            if html is None:
                result_q.put(("not_found", cs, [], None))
                return
            items = parse_db_changes_html(html)
            result_q.put(("ok", cs, items, None))
        except NetworkError as e:
            result_q.put(("net_error", cs, [], str(e)))
        except ParseError as e:
            result_q.put(("parse_error", cs, [], str(e)))

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        for cs in to_fetch:
            ex.submit(worker, cs)

        done = 0
        scraped = 0
        while done < len(to_fetch):
            try:
                item = result_q.get(timeout=40)
            except queue.Empty:
                continue
            done += 1
            kind, cs, items, err = item

            if kind in ("net_error", "parse_error", "not_found"):
                if kind != "not_found":
                    log.error("%-11s %s→%s: %s", kind, cs["from_version"], cs["to_version"], err)
                else:
                    log.warning("404 %s", cs["url"])
                continue

            if dry_run:
                log.info("  DRY-RUN %s→%s  %d items", cs["from_version"], cs["to_version"], len(items))
                scraped += 1
                continue

            try:
                with conn:
                    # On resume+recheck: delete old items before re-inserting
                    existing_id = conn.execute(
                        "SELECT id FROM db_change_sets WHERE url=?", (cs["url"],)
                    ).fetchone()
                    if existing_id:
                        conn.execute(
                            "DELETE FROM db_change_items WHERE change_set_id=?",
                            (existing_id[0],),
                        )
                        conn.execute(
                            "UPDATE db_change_sets SET scraped_at=? WHERE id=?",
                            (datetime.utcnow().isoformat(), existing_id[0]),
                        )
                        cs_id = existing_id[0]
                    else:
                        cs_id = _upsert_change_set(conn, cs)

                    if cs_id is not None:
                        _insert_items(conn, cs_id, items)
                        log.info("  %s → %s  %d items", cs["from_version"], cs["to_version"], len(items))
                        scraped += 1
            except sqlite3.Error as exc:
                log.error("DB error %s→%s: %s", cs["from_version"], cs["to_version"], exc)

    conn.close()
    log.info("Done — scraped %d DB-change pages", scraped)
    return scraped


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import date

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    p = argparse.ArgumentParser(description="Vantagepoint Database Changes Scraper")
    p.add_argument("--versions",    nargs="+", metavar="V")
    p.add_argument("--resume",      action="store_true")
    p.add_argument("--recheck",     type=int, default=5, metavar="N")
    p.add_argument("--concurrency", type=int, default=3, metavar="N")
    p.add_argument("--dry-run",     action="store_true")
    p.add_argument("--verbose",     action="store_true")
    args = p.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    n = run(
        versions_filter=args.versions,
        resume=args.resume,
        recheck=args.recheck,
        concurrency=args.concurrency,
        dry_run=args.dry_run,
    )
    sys.exit(0 if n >= 0 else 1)
