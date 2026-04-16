#!/usr/bin/env python3
"""
Deltek Vantagepoint Release Notes Scraper
==========================================
Production-hardened with:
  #1  Atomic per-release transactions (with conn:)
  #2  HTTPAdapter + urllib3 Retry (backoff, status retries, connection pooling)
  #3  SHA-256 issue_key for true idempotency (not fragile title+nav matching)
  #4  Typed exceptions: NetworkError, ParseError, PersistError
  #5  Structured stats counters + structured log extras
  #6  CLI: --dry-run  --since DATE  --max-releases N  --concurrency N  --verbose
  #7  Concurrent fetch+parse (ThreadPoolExecutor), serialised DB writes
  #8  Indexes on type, nav_level1, defect_number, release_id, release_date
  #9  Resume checks issue_count too, not just status='ok'
  #10 parse_html(html) decoupled from HTTP — unit-testable with saved fixtures

Usage:
    pip install requests beautifulsoup4 lxml
    python scraper.py                               # full fresh scrape
    python scraper.py --resume                      # skip old, re-check last 5
    python scraper.py --resume --recheck 10
    python scraper.py --versions 7.0 2025.1
    python scraper.py --since 2025-01-01
    python scraper.py --dry-run --versions 2025.1
    python scraper.py --concurrency 8 --resume
    python scraper.py --max-releases 20 --dry-run
"""

from __future__ import annotations

import hashlib
import json
import logging
import queue
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions  (#4)
# ---------------------------------------------------------------------------

class ScraperError(Exception):
    """Base class."""

class NetworkError(ScraperError):
    """HTTP/connection failure after all retries exhausted."""

class ParseError(ScraperError):
    """HTML structure was unexpected or parsing failed."""

class PersistError(ScraperError):
    """Database write failed."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INDEX_URLS: list[str] = [
    "https://help.deltek.com/product/Vantagepoint/2.0/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/3.0/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/3.5/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/4.0/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/4.5/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/5.0/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/5.5/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/6.0/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/6.5/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/7.0/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/7.1/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/7.2/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/7.3/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/2025.1/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/2025.2/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/2025.3/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/2025.4/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/2026.1/ReleaseNotes/",
    "https://help.deltek.com/product/Vantagepoint/2026.2/ReleaseNotes/",
]

HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

REQUEST_DELAY   = 0.5   # polite delay between requests per worker
REQUEST_TIMEOUT = 30
DB_PATH = Path(__file__).parent.parent / "db" / "release_notes.db"


# ---------------------------------------------------------------------------
# Structured stats  (#5)
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    scraped:      int = 0
    skipped:      int = 0
    changed:      int = 0
    issues:       int = 0
    net_errors:   int = 0
    parse_errors: int = 0
    db_errors:    int = 0
    dry_run:      int = 0

    def log_summary(self) -> None:
        log.info(
            "Done — scraped=%d skipped=%d changed=%d issues=%d "
            "net_err=%d parse_err=%d db_err=%d",
            self.scraped, self.skipped, self.changed, self.issues,
            self.net_errors, self.parse_errors, self.db_errors,
        )


# ---------------------------------------------------------------------------
# Schema  (#8 — indexes)
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS releases (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    major_version TEXT NOT NULL,
    patch_version TEXT NOT NULL UNIQUE,
    build         TEXT,
    release_date  TEXT NOT NULL,
    url           TEXT,
    scraped_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS issues (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_key     TEXT NOT NULL UNIQUE,  -- stable SHA-256 identity (see make_issue_key)
    defect_number TEXT,
    type          TEXT NOT NULL CHECK(type IN ('defect','enhancement','regulatory','security')),
    breadcrumb    TEXT,
    nav_level1    TEXT,
    nav_level2    TEXT,
    nav_level3    TEXT,
    category      TEXT,
    subcategory   TEXT,
    title         TEXT NOT NULL,
    description   TEXT,
    first_seen_at TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS issue_versions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id   INTEGER NOT NULL REFERENCES issues(id),
    release_id INTEGER NOT NULL REFERENCES releases(id),
    UNIQUE(issue_id, release_id)
);

CREATE TABLE IF NOT EXISTS scrape_log (
    url               TEXT PRIMARY KEY,
    status            TEXT NOT NULL,
    scraped_at        TEXT NOT NULL,
    page_last_updated TEXT,
    content_hash      TEXT,
    issue_count       INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS scrape_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    url               TEXT NOT NULL,
    scraped_at        TEXT NOT NULL,
    page_last_updated TEXT,
    status            TEXT NOT NULL,
    issue_count       INTEGER NOT NULL DEFAULT 0,
    content_hash      TEXT,
    changed           INTEGER NOT NULL DEFAULT 0,
    added_keys        TEXT NOT NULL DEFAULT '[]',
    removed_keys      TEXT NOT NULL DEFAULT '[]',
    modified_keys     TEXT NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_issues_type     ON issues(type);
CREATE INDEX IF NOT EXISTS idx_issues_nav1     ON issues(nav_level1);
CREATE INDEX IF NOT EXISTS idx_issues_defect   ON issues(defect_number) WHERE defect_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_iv_release      ON issue_versions(release_id);
CREATE INDEX IF NOT EXISTS idx_iv_issue        ON issue_versions(issue_id);
CREATE INDEX IF NOT EXISTS idx_releases_date   ON releases(release_date);
CREATE INDEX IF NOT EXISTS idx_history_changed ON scrape_history(changed);

CREATE VIRTUAL TABLE IF NOT EXISTS issues_fts USING fts5(
    title, description, breadcrumb, nav_level1, nav_level2, nav_level3,
    content='issues', content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS issues_ai AFTER INSERT ON issues BEGIN
    INSERT INTO issues_fts(rowid, title, description, breadcrumb, nav_level1, nav_level2, nav_level3)
    VALUES (new.id, new.title, new.description, new.breadcrumb,
            new.nav_level1, new.nav_level2, new.nav_level3);
END;
"""


def get_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


# ---------------------------------------------------------------------------
# HTTP layer  (#2)
# ---------------------------------------------------------------------------

def create_session() -> requests.Session:
    """Session with connection pooling and automatic exponential-backoff retry."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,            # sleeps: 1s, 2s, 4s, 8s, 16s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=20,
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update(HEADERS)
    return session


SESSION = create_session()


def fetch_html(url: str) -> str | None:
    """
    Return raw HTML string or None on 404.
    Raises NetworkError on non-recoverable failures.
    """
    try:
        resp = SESSION.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 404:
            log.warning("404: %s", url)
            return None
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY)
        return resp.text
    except requests.HTTPError as exc:
        raise NetworkError(f"HTTP {exc.response.status_code}: {url}") from exc
    except requests.RequestException as exc:
        raise NetworkError(f"Connection failed: {url} — {exc}") from exc


# ---------------------------------------------------------------------------
# Hashing  (#3)
# ---------------------------------------------------------------------------

def make_issue_key(issue: dict) -> str:
    """
    Stable 32-char SHA-256 identity key.
    Excludes description so text edits are tracked as modifications, not new records.

    Defects    → SHA-256("D|{defect_number}")
    Everything → SHA-256("E|{type}|{nav1}|{nav2}|{nav3}|{title}")
    """
    if issue.get("defect_number"):
        raw = f"D|{issue['defect_number']}"
    else:
        raw = "|".join([
            "E", issue.get("type",""),
            issue.get("nav_level1",""), issue.get("nav_level2",""),
            issue.get("nav_level3",""), issue.get("title",""),
        ])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def make_desc_hash(desc: str | None) -> str:
    return hashlib.sha256((desc or "").encode()).hexdigest()[:16]


def make_content_hash(issues: list[dict]) -> str:
    parts = sorted(
        f"{make_issue_key(i)}:{make_desc_hash(i.get('description'))}"
        for i in issues
    )
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Parser — pure function, no HTTP  (#10)
# ---------------------------------------------------------------------------

SECTION_PATTERNS = {
    "regulatory": re.compile(r"regulatory\s+enhancements?", re.I),
    "enhancement": re.compile(r"^enhancements?$", re.I),
    "defect":      re.compile(r"software\s+issues?\s+resolved", re.I),
    "security":    re.compile(r"security\s+enhancements?", re.I),
}
# \s* (not \s+) handles "Defect1541790:" (no space between keyword and number),
# which some pages produce when the number is wrapped in <strong> with no trailing space.
DEFECT_RE            = re.compile(r"Defect\s*(\d+)\s*[:–-]\s*(.*)", re.DOTALL | re.IGNORECASE)
BREADCRUMB_SEP_RE    = re.compile(r"\s*>>\s*")
PAGE_LAST_UPDATED_RE = re.compile(
    r"Last\s+Updated\s*:\s*([A-Za-z]+ \d{1,2},\s*\d{4})", re.IGNORECASE
)
# Maximum length for a single-level breadcrumb (no '>>'). Real Vantagepoint
# module names: "API" (3), "Billing" (7), "Ask Dela" (8), "Dashboards" (10),
# "Resource Management" (19), "Deltek Learning Hub" (19) — all well under 25.
# Enhancement/regulatory titles like "New API Endpoint for Report Generation"
# (38) or "Federal Income Tax Withholding 2025 Updates" (43) exceed this and
# must NOT be treated as breadcrumbs.
_MAX_SINGLE_LEVEL_BC = 25


def parse_date(raw: str) -> str | None:
    if not raw:
        return None
    raw = raw.strip().strip("-").strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def clean_text(t: str) -> str:
    t = re.sub(r"\[image\]", "", t)
    return re.sub(r"\s+", " ", t).strip()


def parse_breadcrumb(raw: str) -> tuple[str, str, str, str]:
    parts = [p.strip() for p in BREADCRUMB_SEP_RE.split(raw.strip())]
    l1 = parts[0] if len(parts) > 0 else ""
    l2 = parts[1] if len(parts) > 1 else ""
    l3 = parts[2] if len(parts) > 2 else ""
    return " >> ".join(p for p in parts if p), l1, l2, l3


def parse_html(html: str) -> tuple[list[dict], str | None]:
    """
    Pure function: raw HTML → (issues, page_last_updated).
    Unit-testable — pass saved HTML bytes from a fixture file.
    Raises ParseError on structural failures.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as exc:
        raise ParseError(f"BeautifulSoup failed: {exc}") from exc

    body = soup.find("body") or soup
    page_last_updated: str | None = None
    m = PAGE_LAST_UPDATED_RE.search(body.get_text(" "))
    if m:
        page_last_updated = parse_date(m.group(1))

    issues: list[dict] = []
    current_section: str | None = None
    nav1 = nav2 = nav3 = breadcrumb = ""
    pending_title: str | None = None   # bold enhancement/regulatory title waiting for its description

    for tag in body.descendants:
        if not hasattr(tag, "name") or not tag.name:
            continue

        # ── 1. Detect major section headings ─────────────────────────────
        if tag.name in ("h1", "h2", "h3", "h4", "p", "div", "span", "td"):
            text = tag.get_text(strip=True)

            matched_section = None
            for sec, pat in SECTION_PATTERNS.items():
                if pat.search(text):
                    matched_section = sec
                    break

            if matched_section:
                current_section = matched_section
                nav1 = nav2 = nav3 = breadcrumb = ""
                pending_title = None
                continue   # section heading consumed — move on

            if not current_section:
                continue   # not yet inside a known section

            # ── 2. Detect breadcrumb / category headers ───────────────────
            #
            # A tag is a breadcrumb header when it meets ALL of:
            #   a) Does not match DEFECT_RE (guards against bold defect numbers
            #      like <strong>Defect1541790</strong> slipping through)
            #   b) Either has '>>' separator (any length allowed), OR is a
            #      purely-bold standalone heading ≤ 40 chars (module names like
            #      "Federal", "Billing", "Ask Dela" – NOT enhancement titles)
            #
            # is_heading_only: the <strong>/<b> text IS the entire tag text.
            # This rejects cases like:
            #   <p><strong>Defect1541790</strong>: long description...</p>
            #   → bold text ≠ full text → not a breadcrumb
            # And cases like:
            #   <p><strong>Federal Income Tax Withholding 2025 Updates</strong></p>
            #   → bold == full text BUT len > 40 → not a breadcrumb
            bold         = tag.find(["strong", "b"])
            has_sep      = ">>" in text
            is_heading_only = (
                bold is not None
                and clean_text(bold.get_text()) == clean_text(text)
            )

            if not DEFECT_RE.match(text) and (
                has_sep
                or (is_heading_only and len(text) <= _MAX_SINGLE_LEVEL_BC)
            ):
                if has_sep:
                    bc, l1, l2, l3 = parse_breadcrumb(text)
                    if l1:
                        breadcrumb, nav1, nav2, nav3 = bc, l1, l2, l3
                else:
                    breadcrumb = nav1 = clean_text(text)
                    nav2 = nav3 = ""
                continue   # category header consumed

        # ── 3. Parse content blocks ───────────────────────────────────────
        if tag.name not in ("p", "div", "li", "blockquote") or not current_section:
            continue

        text = clean_text(tag.get_text(" ", strip=True))
        if not text or len(text) < 10:
            continue
        if re.match(r"^(Last Updated|Release Date|Welcome to)", text):
            continue

        dm = DEFECT_RE.match(text)
        if dm and current_section == "defect":
            desc  = clean_text(dm.group(2))
            tm    = re.match(r"^(.{10,200}?[.!?])\s", desc)
            title = tm.group(1) if tm else desc[:200]
            issues.append({
                "type": "defect",          "defect_number": dm.group(1),
                "breadcrumb": breadcrumb,  "nav_level1": nav1,
                "nav_level2": nav2,        "nav_level3": nav3,
                "category": nav1,          "subcategory": nav2,
                "title": title,            "description": desc,
            })
            pending_title = None   # defects never accumulate a pending title
            continue

        if current_section in ("enhancement", "regulatory", "security"):
            if len(text) < 30 or text.lower() == nav1.lower():
                continue

            bold        = tag.find(["strong", "b"])
            is_title    = (
                bold is not None
                and clean_text(bold.get_text()) == clean_text(text)
                and len(text) > _MAX_SINGLE_LEVEL_BC  # short ones were already breadcrumbs
            )

            if is_title:
                # This bold paragraph is an enhancement/regulatory item TITLE.
                # Flush any previous unflushed pending title as a standalone issue,
                # then hold this one until we see its description paragraph.
                if pending_title:
                    tm = re.match(r"^(.{10,200}?[.!?])\s", pending_title)
                    issues.append({
                        "type": current_section,  "defect_number": None,
                        "breadcrumb": breadcrumb,  "nav_level1": nav1,
                        "nav_level2": nav2,        "nav_level3": nav3,
                        "category": nav1,          "subcategory": nav2,
                        "title": tm.group(1) if tm else pending_title[:200],
                        "description": pending_title,
                    })
                pending_title = text
            else:
                # Plain description paragraph.
                if pending_title:
                    # Merge: the pending bold title is the issue title, this is the desc.
                    tm = re.match(r"^(.{10,200}?[.!?])\s", pending_title)
                    issues.append({
                        "type": current_section,  "defect_number": None,
                        "breadcrumb": breadcrumb,  "nav_level1": nav1,
                        "nav_level2": nav2,        "nav_level3": nav3,
                        "category": nav1,          "subcategory": nav2,
                        "title": tm.group(1) if tm else pending_title[:200],
                        "description": text,
                    })
                    pending_title = None
                else:
                    # No pending title: first sentence becomes title as before.
                    tm = re.match(r"^(.{10,200}?[.!?])\s", text)
                    issues.append({
                        "type": current_section,  "defect_number": None,
                        "breadcrumb": breadcrumb,  "nav_level1": nav1,
                        "nav_level2": nav2,        "nav_level3": nav3,
                        "category": nav1,          "subcategory": nav2,
                        "title": tm.group(1) if tm else text[:200],
                        "description": text,
                    })

    # Flush any trailing pending title (last item on page with no following paragraph)
    if pending_title and current_section in ("enhancement", "regulatory", "security"):
        tm = re.match(r"^(.{10,200}?[.!?])\s", pending_title)
        issues.append({
            "type": current_section,  "defect_number": None,
            "breadcrumb": breadcrumb,  "nav_level1": nav1,
            "nav_level2": nav2,        "nav_level3": nav3,
            "category": nav1,          "subcategory": nav2,
            "title": tm.group(1) if tm else pending_title[:200],
            "description": pending_title,
        })

    # Deduplicate within page using the same stable key used by the DB
    seen: set = set()
    deduped = []
    for i in issues:
        k = make_issue_key(i)
        if k not in seen:
            seen.add(k)
            deduped.append(i)

    return deduped, page_last_updated


def fetch_and_parse(url: str) -> tuple[list[dict], str | None]:
    """Fetch then parse. Safe to call from multiple threads."""
    html = fetch_html(url)
    if html is None:
        return [], None
    try:
        return parse_html(html)
    except ParseError:
        raise
    except Exception as exc:
        raise ParseError(f"Unexpected error parsing {url}: {exc}") from exc


# ---------------------------------------------------------------------------
# Index page parser
# ---------------------------------------------------------------------------

def parse_index_page(index_url: str) -> list[dict]:
    try:
        html = fetch_html(index_url)
    except NetworkError as exc:
        log.warning("Index unavailable: %s — %s", index_url, exc)
        return []
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    m = re.search(r"/Vantagepoint/([^/]+)/ReleaseNotes", index_url)
    major = m.group(1) if m else "unknown"

    releases = []
    for a in soup.find_all("a", href=True):
        if not re.search(r"ReleaseNotes\.htm$", a["href"], re.IGNORECASE):
            continue
        text = a.get_text(strip=True)
        vm   = re.search(
            r"Vantagepoint\s+([\d.]+)\s*(?:\(Build\s+([\d.]+)\))?", text, re.I
        )
        if not vm:
            continue
        parent = a.find_parent(["td","li","p"])
        pt     = parent.get_text(" ", strip=True) if parent else text
        dm     = re.search(r"-\s*(\w+ \d{1,2},\s*\d{4})", pt)
        rdate  = parse_date(dm.group(1)) if dm else None
        if not rdate:
            continue
        releases.append({
            "major_version": major,
            "patch_version": vm.group(1),
            "build":         vm.group(2) or vm.group(1),
            "release_date":  rdate,
            "url":           urljoin(index_url, a["href"]),
        })

    log.info("Index %-8s → %d releases", major, len(releases))
    return releases


# ---------------------------------------------------------------------------
# DB persistence  (#1 — atomic transactions)
# ---------------------------------------------------------------------------

def _upsert_release(conn: sqlite3.Connection, release: dict) -> int:
    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT INTO releases (major_version,patch_version,build,release_date,url,scraped_at)
           VALUES (:major_version,:patch_version,:build,:release_date,:url,:now)
           ON CONFLICT(patch_version) DO UPDATE SET
               build=excluded.build, release_date=excluded.release_date,
               url=excluded.url,     scraped_at=excluded.scraped_at""",
        {**release, "now": now},
    )
    return conn.execute(
        "SELECT id FROM releases WHERE patch_version=?", (release["patch_version"],)
    ).fetchone()["id"]


def _upsert_issue(conn: sqlite3.Connection, issue: dict, release_id: int) -> bool:
    """Returns True if description was changed on an existing record."""
    now = datetime.utcnow().isoformat()
    key = make_issue_key(issue)
    existing = conn.execute(
        "SELECT id, description FROM issues WHERE issue_key=?", (key,)
    ).fetchone()

    desc_changed = False
    if existing:
        if existing["description"] != issue.get("description"):
            conn.execute(
                "UPDATE issues SET description=?,title=?,updated_at=? WHERE id=?",
                (issue["description"], issue["title"], now, existing["id"]),
            )
            desc_changed = True
        issue_id = existing["id"]
    else:
        cur = conn.execute(
            """INSERT INTO issues
               (issue_key,defect_number,type,breadcrumb,nav_level1,nav_level2,nav_level3,
                category,subcategory,title,description,first_seen_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (key, issue.get("defect_number"), issue["type"],
             issue.get("breadcrumb",""), issue.get("nav_level1",""),
             issue.get("nav_level2",""), issue.get("nav_level3",""),
             issue.get("category",""),   issue.get("subcategory",""),
             issue["title"], issue.get("description"), now, now),
        )
        issue_id = cur.lastrowid

    conn.execute(
        "INSERT OR IGNORE INTO issue_versions (issue_id,release_id) VALUES (?,?)",
        (issue_id, release_id),
    )
    return desc_changed


def _get_release_fps(conn: sqlite3.Connection, release_id: int) -> dict[str, str]:
    rows = conn.execute(
        """SELECT i.issue_key, i.description
           FROM issues i JOIN issue_versions iv ON i.id=iv.issue_id
           WHERE iv.release_id=?""", (release_id,)
    ).fetchall()
    return {r["issue_key"]: make_desc_hash(r["description"]) for r in rows}


def _diff(old: dict[str, str], new_issues: list[dict]) -> tuple[list, list, list]:
    new = {make_issue_key(i): make_desc_hash(i.get("description")) for i in new_issues}
    return (
        [k for k in new if k not in old],
        [k for k in old if k not in new],
        [k for k in new if k in old and new[k] != old[k]],
    )


def process_release(
    conn: sqlite3.Connection,
    release: dict,
    issues: list[dict],
    page_last_updated: str | None,
    c_hash: str,
    prev_log: sqlite3.Row | None,
) -> tuple[bool, list, list, list]:
    """
    Write one release atomically.  (#1)
    Returns (changed, added_keys, removed_keys, modified_keys).
    Raises PersistError — caller's 'with conn' block rolls back automatically.
    """
    try:
        with conn:  # atomic: commit on success, rollback on exception
            release_id = _upsert_release(conn, release)

            prev_hash  = prev_log["content_hash"] if prev_log else None
            is_changed = prev_hash is not None and c_hash != prev_hash

            old_fps: dict[str, str] = {}
            if is_changed:
                old_fps = _get_release_fps(conn, release_id)

            modified: list[str] = []
            for issue in issues:
                if _upsert_issue(conn, issue, release_id):
                    modified.append(make_issue_key(issue))

            added, removed = [], []
            if is_changed:
                added, removed, _ = _diff(old_fps, issues)

            now = datetime.utcnow().isoformat()
            conn.execute(
                """INSERT OR REPLACE INTO scrape_log
                   (url,status,scraped_at,page_last_updated,content_hash,issue_count)
                   VALUES (?,?,?,?,?,?)""",
                (release["url"], "ok", now, page_last_updated, c_hash, len(issues)),
            )
            conn.execute(
                """INSERT INTO scrape_history
                   (url,scraped_at,page_last_updated,status,issue_count,
                    content_hash,changed,added_keys,removed_keys,modified_keys)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (release["url"], now, page_last_updated, "ok", len(issues),
                 c_hash, 1 if is_changed else 0,
                 json.dumps(added), json.dumps(removed), json.dumps(modified)),
            )
        return is_changed, added, removed, modified

    except sqlite3.Error as exc:
        raise PersistError(f"DB write failed for {release['patch_version']}: {exc}") from exc


def _persist_error(conn: sqlite3.Connection, url: str, msg: str) -> None:
    now = datetime.utcnow().isoformat()
    with conn:
        conn.execute(
            """INSERT OR REPLACE INTO scrape_log
               (url,status,scraped_at,page_last_updated,content_hash,issue_count)
               VALUES (?,?,?,NULL,NULL,0)""", (url, f"error: {msg}", now),
        )
        conn.execute(
            """INSERT INTO scrape_history
               (url,scraped_at,page_last_updated,status,issue_count,
                content_hash,changed,added_keys,removed_keys,modified_keys)
               VALUES (?,?,NULL,?,0,NULL,0,'[]','[]','[]')""",
            (url, now, f"error: {msg}"),
        )


# ---------------------------------------------------------------------------
# Main orchestration  (#6, #7, #9)
# ---------------------------------------------------------------------------

def _should_fetch(
    release: dict,
    recheck_urls: set[str],
    resume: bool,
    conn: sqlite3.Connection,
) -> tuple[bool, sqlite3.Row | None]:
    """#9 — checks both status AND issue_count to avoid optimistic skip."""
    prev = conn.execute(
        "SELECT * FROM scrape_log WHERE url=?", (release["url"],)
    ).fetchone()

    if not resume:
        return True, prev

    # Always re-fetch pages in the recheck window
    if release["url"] in recheck_urls:
        return True, prev

    # Skip only if previously succeeded AND had at least 1 issue
    # (guards against a prior run that wrote "ok" with 0 issues due to parse failure)
    if prev and prev["status"] == "ok" and (prev["issue_count"] or 0) > 0:
        return False, prev

    return True, prev


def run(
    versions_filter: list[str] | None = None,
    resume: bool = False,
    recheck: int = 5,
    since: date | None = None,
    max_releases: int | None = None,
    concurrency: int = 3,
    dry_run: bool = False,
) -> Stats:
    stats = Stats()
    conn  = get_db(DB_PATH)
    log.info("DB: %s%s", DB_PATH, "  [DRY-RUN]" if dry_run else "")

    # --- Collect & filter releases ---
    all_urls = INDEX_URLS
    if versions_filter:
        all_urls = [u for u in all_urls if any(f"/{v}/" in u for v in versions_filter)]

    releases: list[dict] = []
    for idx_url in all_urls:
        releases.extend(parse_index_page(idx_url))

    if since:
        releases = [r for r in releases if r.get("release_date","") >= since.isoformat()]

    releases.sort(key=lambda r: r.get("release_date",""), reverse=True)

    if max_releases:
        releases = releases[:max_releases]

    if not releases:
        log.warning("No releases matched filters.")
        conn.close()
        return stats

    recheck_urls = {r["url"] for r in releases[:recheck]}
    if resume:
        log.info(
            "Resume — re-checking last %d: %s", recheck,
            [r["patch_version"] for r in releases[:recheck]],
        )

    # Partition into to-fetch vs skip
    to_fetch: list[tuple[dict, sqlite3.Row | None]] = []
    for r in releases:
        do_it, prev = _should_fetch(r, recheck_urls, resume, conn)
        if do_it:
            to_fetch.append((r, prev))
        else:
            log.debug("Skip %s", r["patch_version"])
            stats.skipped += 1

    log.info("Fetching %d releases (skipping %d)…", len(to_fetch), stats.skipped)
    if not to_fetch:
        stats.log_summary()
        conn.close()
        return stats

    # --- #7: Concurrent fetch+parse, serialised DB writes ---
    result_q: queue.Queue = queue.Queue()

    def worker(release: dict, prev_log) -> None:
        try:
            issues, plu = fetch_and_parse(release["url"])
            result_q.put(("ok", release, issues, plu, prev_log))
        except NetworkError as e:
            result_q.put(("net_error", release, str(e), None, prev_log))
        except ParseError as e:
            result_q.put(("parse_error", release, str(e), None, prev_log))

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        for r, prev in to_fetch:
            ex.submit(worker, r, prev)

        done = 0
        while done < len(to_fetch):
            try:
                item = result_q.get(timeout=REQUEST_TIMEOUT + 10)
            except queue.Empty:
                continue
            done += 1
            kind    = item[0]
            release = item[1]

            if kind in ("net_error", "parse_error"):
                err = item[2]
                log.error("%-11s %s: %s", kind.upper(), release["patch_version"], err)
                if kind == "net_error":   stats.net_errors   += 1
                else:                     stats.parse_errors += 1
                if not dry_run:
                    _persist_error(conn, release["url"], err)
                continue

            _, _, issues, page_lu, prev_log = item
            c_hash    = make_content_hash(issues)
            prev_hash = prev_log["content_hash"] if prev_log else None
            unchanged = (
                resume
                and prev_log
                and prev_log["status"] == "ok"
                and release["url"] in recheck_urls
                and prev_hash == c_hash
            )

            if unchanged:
                log.info("  %-18s hash unchanged (%s…)", release["patch_version"], c_hash[:10])
                if not dry_run:
                    with conn:
                        now = datetime.utcnow().isoformat()
                        conn.execute(
                            """INSERT OR REPLACE INTO scrape_log
                               (url,status,scraped_at,page_last_updated,content_hash,issue_count)
                               VALUES (?,?,?,?,?,?)""",
                            (release["url"],"ok",now,page_lu,c_hash,len(issues)),
                        )
                        conn.execute(
                            """INSERT INTO scrape_history
                               (url,scraped_at,page_last_updated,status,issue_count,
                                content_hash,changed,added_keys,removed_keys,modified_keys)
                               VALUES (?,?,?,?,?,?,0,'[]','[]','[]')""",
                            (release["url"],now,page_lu,"ok",len(issues),c_hash),
                        )
                stats.skipped += 1
                continue

            if dry_run:
                log.info(
                    "  DRY-RUN %-14s %3d issues  hash=%s…  page_updated=%s",
                    release["patch_version"], len(issues), c_hash[:10], page_lu,
                )
                stats.dry_run += 1
                stats.issues  += len(issues)
                continue

            try:
                is_changed, added, removed, modified = process_release(
                    conn, release, issues, page_lu, c_hash, prev_log
                )
                stats.scraped += 1
                stats.issues  += len(issues)
                if is_changed:
                    stats.changed += 1

                parts = [f"{len(issues)} issues"]
                if page_lu:
                    parts.append(f"page_updated={page_lu}")
                if is_changed:
                    parts.append(f"CHANGED +{len(added)}-{len(removed)}~{len(modified)}")
                    if added:    log.info("    added:    %s", added)
                    if removed:  log.info("    removed:  %s", removed)
                    if modified: log.info("    modified: %s", modified)
                log.info("  %-18s %s", release["patch_version"], " | ".join(parts))

            except PersistError as exc:
                log.error("DB ERROR %s: %s", release["patch_version"], exc)
                stats.db_errors += 1

    conn.close()
    stats.log_summary()
    return stats


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _build_parser():
    import argparse
    p = argparse.ArgumentParser(
        description="Vantagepoint Release Notes Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py
  python scraper.py --resume --recheck 10
  python scraper.py --versions 7.0 2025.1 --resume
  python scraper.py --since 2025-01-01
  python scraper.py --dry-run --versions 2025.1
  python scraper.py --concurrency 8 --resume
  python scraper.py --max-releases 20 --dry-run
        """,
    )
    p.add_argument("--versions",     nargs="+", metavar="V")
    p.add_argument("--resume",       action="store_true")
    p.add_argument("--recheck",      type=int, default=5, metavar="N")
    p.add_argument("--since",        type=date.fromisoformat, metavar="YYYY-MM-DD")
    p.add_argument("--max-releases", type=int, metavar="N")
    p.add_argument("--concurrency",  type=int, default=3, metavar="N")
    p.add_argument("--dry-run",      action="store_true")
    p.add_argument("--db",           type=Path, default=DB_PATH)
    p.add_argument("--verbose",      action="store_true")
    return p


if __name__ == "__main__":
    args = _build_parser().parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.db != DB_PATH:
        DB_PATH = args.db
    s = run(
        versions_filter=args.versions,
        resume=args.resume,
        recheck=args.recheck,
        since=args.since,
        max_releases=getattr(args, "max_releases", None),
        concurrency=args.concurrency,
        dry_run=args.dry_run,
    )
    sys.exit(0 if s.db_errors == 0 else 1)
