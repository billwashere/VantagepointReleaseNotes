#!/usr/bin/env python3
"""
Deltek WorkBook Release Notes Scraper
======================================
Scrapes https://help.deltek.com/product/workbook/releasenotes/ReleaseNotes.html

Supports the modern single-file format (WorkBook 13.6+) where each version's
index page (Intro_*.html) links to a ReleaseNotes_*.html page containing all
CU sections. Each CU URL is parsed in isolation — only the section matching the
URL's own CU is extracted.

Usage:
    python workbook_scraper.py                         # full scrape
    python workbook_scraper.py --resume                # skip already done
    python workbook_scraper.py --resume --recheck 10
    python workbook_scraper.py --dry-run
"""

from __future__ import annotations

import json
import logging
import queue
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

# Shared utilities from the Vantagepoint scraper
from scraper import (
    SCHEMA, get_db, fetch_html, make_issue_key, make_content_hash,
    parse_date, clean_text, Stats, NetworkError, ParseError, PersistError,
    process_release, _persist_error, _should_fetch, parse_breadcrumb,
    REQUEST_DELAY, REQUEST_TIMEOUT,
)

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
# Config
# ---------------------------------------------------------------------------

WORKBOOK_INDEX_URL = "https://help.deltek.com/product/workbook/releasenotes/ReleaseNotes.html"
DB_PATH = Path(__file__).parent.parent / "db" / "workbook_release_notes.db"

# Matches filenames like ReleaseNotes_140.html, ReleaseNotes_14CU02.html, ReleaseNotes_136CU04.html
_RN_FILE_RE = re.compile(
    r"ReleaseNotes_(\d{2,4})(?:CU(\d{2,3}))?(?:Revised\d*)?\.html",
    re.IGNORECASE,
)

TRACKING_RE = re.compile(
    r"Deltek\s+Tracking(?:\s+No\.?)?\s*:?\s*(\d+)", re.IGNORECASE
)

_DATE_RE = re.compile(
    r"(?:Release\s+Date|Last\s+Updated)\s*:?\s*"
    r"([A-Za-z]+\s+\d{1,2}[\s\xa0]*,?[\s\xa0]*\d{4})",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

def _raw_ver_to_dotted(raw: str) -> str:
    """Convert '14' → '14.0', '140' → '14.0', '136' → '13.6', '138' → '13.8'."""
    if len(raw) == 2:
        return f"{raw}.0"
    if len(raw) == 3:
        return f"{raw[:2]}.{raw[2]}"
    if len(raw) == 4:
        return f"{raw[:2]}.{raw[2:]}"
    return raw


def parse_version_from_rn_url(url: str) -> tuple[str, str, str] | None:
    """
    Returns (major_version, patch_version, section_fragment) or None.

    ReleaseNotes_140.html     → ("14.0", "14.0",      "14.0")
    ReleaseNotes_14CU02.html  → ("14.0", "14.0 CU02", "14.0_CU02")
    ReleaseNotes_136CU04.html → ("13.6", "13.6 CU04", "13.6_CU04")
    Revised versions → None (same section, not a separate release).
    """
    fname = url.rsplit("/", 1)[-1]
    if "revised" in fname.lower():
        return None
    m = _RN_FILE_RE.search(fname)
    if not m:
        return None
    major_ver = _raw_ver_to_dotted(m.group(1))
    cu = m.group(2)
    patch_ver = major_ver if not cu else f"{major_ver} CU{int(cu):02d}"
    section_frag = major_ver if not cu else f"{major_ver}_CU{int(cu):02d}"
    return major_ver, patch_ver, section_frag


def _intro_to_rn_url(intro_url: str) -> str | None:
    """
    Derive the ReleaseNotes_*.html URL from an Intro_*.html URL without
    fetching the intro page.

    Intro_140.html      → ReleaseNotes_140.html
    Intro_14_CU02.html  → ReleaseNotes_14CU02.html  (underscore before CU removed)
    intro_136CU04.html  → ReleaseNotes_136CU04.html
    Intro_138_CU01.html → ReleaseNotes_138CU01.html
    """
    fname = intro_url.rsplit("/", 1)[-1]
    if "revised" in fname.lower():
        return None
    base = re.sub(r"^[Ii]ntro_", "", fname)           # strip Intro_ prefix
    base = re.sub(r"_(?=CU\d)", "", base, flags=re.I) # "14_CU02" → "14CU02"
    rn_fname = "ReleaseNotes_" + base
    base_dir = intro_url.rsplit("/", 1)[0] + "/"
    return base_dir + rn_fname


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_workbook_releases() -> list[dict]:
    """
    Fetch the WorkBook index and return release dicts for the modern format
    (13.6+). Derives ReleaseNotes_*.html URLs from Intro_*.html links without
    fetching each intro page.

    Base releases (no CU) are routed to the latest CU file using a URL anchor
    (e.g. ReleaseNotes_14CU02.html#section_WorkBook_Release_14.0) because the
    section-ID format for the base release only exists inside CU pages, not the
    standalone base file. The anchor also acts as a unique scrape_log key.
    """
    try:
        html = fetch_html(WORKBOOK_INDEX_URL)
    except NetworkError as exc:
        log.warning("WorkBook index unavailable: %s", exc)
        return []
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")

    # Collect and parse all intro links, grouped by major version
    by_major: dict[str, list[dict]] = {}

    for a in soup.find_all("a", href=True):
        href = a["href"]
        fname = href.rsplit("/", 1)[-1]
        if not re.match(r"[Ii]ntro_", fname):
            continue

        full_intro = urljoin(WORKBOOK_INDEX_URL, href)
        rn_url = _intro_to_rn_url(full_intro)
        if not rn_url:
            continue

        ver = parse_version_from_rn_url(rn_url)
        if not ver:
            continue

        major_ver, patch_ver, section_frag = ver
        cu_m = re.search(r"CU(\d+)", section_frag, re.I)
        cu_num = int(cu_m.group(1)) if cu_m else 0

        if major_ver not in by_major:
            by_major[major_ver] = []
        # Avoid duplicate patch versions (e.g. from Revised pages)
        if not any(e["patch_ver"] == patch_ver for e in by_major[major_ver]):
            by_major[major_ver].append({
                "patch_ver": patch_ver, "cu_num": cu_num,
                "rn_url": rn_url, "section_frag": section_frag,
            })

    releases: list[dict] = []
    for major_ver, items in by_major.items():
        items.sort(key=lambda x: x["cu_num"])
        # URL of the highest-numbered CU (contains base + all CU sections)
        max_cu_url = max(items, key=lambda x: x["cu_num"])["rn_url"]

        for item in items:
            if item["cu_num"] == 0 and len(items) > 1:
                # Base release: use highest CU file + anchor for unique key
                section_id = f"section_WorkBook_Release_{item['section_frag']}"
                release_url = max_cu_url + "#" + section_id
            else:
                release_url = item["rn_url"]

            releases.append({
                "major_version":    major_ver,
                "patch_version":    item["patch_ver"],
                "build":            item["patch_ver"],
                "release_date":     None,
                "url":              release_url,
                "section_fragment": item["section_frag"],
            })

    releases.sort(key=lambda r: (r["major_version"], r["patch_version"]), reverse=True)
    log.info("Discovered %d WorkBook releases (modern format)", len(releases))
    return releases


# ---------------------------------------------------------------------------
# Parser — pure function, no HTTP
# ---------------------------------------------------------------------------

_BLOCK_TAGS = {"p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "dt", "dd"}
_CONTAINER_TAGS = {"div", "p"}


def _extract_date(soup: BeautifulSoup) -> str | None:
    m = _DATE_RE.search(soup.get_text(" "))
    return parse_date(m.group(1)) if m else None


def parse_workbook_html(html: str, section_fragment: str) -> tuple[list[dict], str | None]:
    """
    Pure function: raw HTML + target section fragment → (issues, release_date).

    Uses flat document traversal because section IDs and subsection IDs are often
    siblings (not parent-child) in the WorkBook HTML.  Iterates all elements from
    the target section anchor up to (but not including) the next sibling section.

    Handles both issue orderings seen across versions:
      13.6 style: heading → Tracking ID → bold title → description
      14.0 style: heading → bold title → Tracking ID → plain description
    """
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception as exc:
        raise ParseError(f"BeautifulSoup failed: {exc}") from exc

    plu = _extract_date(soup)

    target_id = f"section_WorkBook_Release_{section_fragment}"

    # Flat list of all tags in document order
    all_tags = list(soup.find_all(True))

    # Find start index of our target section
    start_idx: int | None = None
    for idx, t in enumerate(all_tags):
        if (t.get("id") or "").lower() == target_id.lower():
            start_idx = idx
            break
    if start_idx is None:
        raise ParseError(f"Section not found in page: {target_id}")

    # Find end: the next 'section_WorkBook_Release_*' that's not ours
    end_idx = len(all_tags)
    for idx in range(start_idx + 1, len(all_tags)):
        tag_id = all_tags[idx].get("id") or ""
        if re.match(r"section_WorkBook_Release_", tag_id, re.I) and \
                tag_id.lower() != target_id.lower():
            end_idx = idx
            break

    issues: list[dict] = []
    issue_type: str | None = None
    nav1 = nav2 = nav3 = ""
    # floating_title: a bold non-tracking paragraph seen BEFORE its tracking ID (14.0 style)
    floating_title: str | None = None
    cur: dict | None = None   # issue being assembled

    def _flush() -> None:
        nonlocal cur, floating_title
        if cur is None:
            floating_title = None
            return
        desc = " ".join(cur["desc"]).strip()
        title = cur["title"] or (desc[:200] if desc else "")
        if title:
            bc = " >> ".join(p for p in [cur["n1"], cur["n2"], cur["n3"]] if p)
            issues.append({
                "type":          cur["t"],
                "defect_number": cur["defect"],
                "breadcrumb":    bc,
                "nav_level1":    cur["n1"], "nav_level2": cur["n2"],
                "nav_level3":    cur["n3"], "category":   cur["n1"],
                "subcategory":   cur["n2"], "title":      title,
                "description":   desc or title,
            })
        cur = None
        floating_title = None

    for el in all_tags[start_idx:end_idx]:
        el_id = el.get("id") or ""

        # Subsection type markers
        if re.match(r"software_issues_resolved_section_", el_id, re.I):
            _flush(); issue_type = "defect"; nav1 = nav2 = nav3 = ""; continue
        if re.match(r"software_enhancements_section_", el_id, re.I):
            _flush(); issue_type = "enhancement"; nav1 = nav2 = nav3 = ""; continue
        if re.match(r"known_issues_section_", el_id, re.I):
            _flush(); break

        if issue_type is None:
            continue

        # Only process leaf block elements (skip containers and inline tags)
        el_classes = [c.lower() for c in (el.get("class") or [])]
        is_wid = "work-item-description" in el_classes
        if el.name not in _BLOCK_TAGS and not is_wid:
            continue
        if el.name in _CONTAINER_TAGS and el.find(list(_BLOCK_TAGS)):
            continue

        text = clean_text(el.get_text(" ", strip=True))
        if not text:
            continue

        # Navigation breadcrumb via CSS class
        if is_wid:
            _flush()
            _, l1, l2, l3 = parse_breadcrumb(text)
            nav1, nav2, nav3 = l1, l2, l3
            continue

        # Heading elements → module-level navigation
        if el.name in ("h2", "h3", "h4", "h5", "h6"):
            if len(text.split()) <= 5 and not TRACKING_RE.search(text):
                _flush()
                nav1 = text; nav2 = nav3 = ""
            continue

        # Tracking ID
        tm = TRACKING_RE.search(text)
        if tm:
            if cur is not None:
                # Already building an issue — emit it (next tracking starts fresh)
                _flush()
            cur = {
                "t": issue_type, "defect": tm.group(1),
                "title": floating_title,   # picks up pre-tracking title (14.0 style)
                "desc": [],
                "n1": nav1, "n2": nav2, "n3": nav3,
            }
            floating_title = None
            continue

        # Bold-only paragraph: either module heading, issue title, or description label
        inner_bold = el.find(["strong", "b"])
        is_all_bold = inner_bold and clean_text(inner_bold.get_text()) == text
        stripped = re.sub(
            r"^(?:Description|Additional\s+Information)\s*:?\s*",
            "", text, flags=re.I,
        ).strip()

        if is_all_bold:
            word_count = len(text.split())
            if word_count <= 5 and not TRACKING_RE.search(text):
                # Short bold → module heading
                _flush()
                nav1 = text; nav2 = nav3 = ""
                continue
            # Long bold → issue title
            if cur is not None and cur["title"] is None:
                # 13.6 style: title comes after tracking
                cur["title"] = stripped
            elif cur is None:
                # 14.0 style: title comes before tracking
                floating_title = stripped
            continue

        # Plain or partial-bold paragraph → description
        if stripped and len(stripped) > 10:
            if cur is not None:
                cur["desc"].append(stripped)
            elif floating_title is not None:
                # description without a tracking ID — treat floating_title as title
                bc = " >> ".join(p for p in [nav1, nav2, nav3] if p)
                issues.append({
                    "type": issue_type, "defect_number": None,
                    "breadcrumb": bc,
                    "nav_level1": nav1, "nav_level2": nav2, "nav_level3": nav3,
                    "category": nav1, "subcategory": nav2,
                    "title": floating_title, "description": stripped,
                })
                floating_title = None

    _flush()

    # Deduplicate via stable key
    seen: set[str] = set()
    result: list[dict] = []
    for i in issues:
        k = make_issue_key(i)
        if k not in seen:
            seen.add(k)
            result.append(i)

    return result, plu


def fetch_and_parse_workbook(release: dict) -> tuple[list[dict], str | None]:
    """Fetch the ReleaseNotes page and parse the target section. Thread-safe."""
    fetch_url = release["url"].split("#")[0]   # strip anchor used as scrape_log key
    html = fetch_html(fetch_url)
    if html is None:
        return [], None
    try:
        return parse_workbook_html(html, release["section_fragment"])
    except ParseError:
        raise
    except Exception as exc:
        raise ParseError(f"Unexpected parse error for {release['patch_version']}: {exc}") from exc


# ---------------------------------------------------------------------------
# Orchestration — mirrors scraper.run() structure
# ---------------------------------------------------------------------------

def run(
    versions_filter: list[str] | None = None,
    resume: bool = False,
    recheck: int = 5,
    since: date | None = None,
    max_releases: int | None = None,
    concurrency: int = 3,
    dry_run: bool = False,
    db_path: Path = DB_PATH,
) -> Stats:
    stats = Stats()
    conn  = get_db(db_path)
    log.info("WorkBook DB: %s%s", db_path, "  [DRY-RUN]" if dry_run else "")

    releases = discover_workbook_releases()

    if versions_filter:
        releases = [r for r in releases if any(v in r["patch_version"] for v in versions_filter)]
        if not releases:
            log.warning("No releases matched versions filter %s", versions_filter)

    if since:
        # Can't filter by date until we fetch pages; skip pre-date filtering here
        pass

    if max_releases:
        releases = releases[:max_releases]

    if not releases:
        log.warning("No WorkBook releases found.")
        conn.close()
        return stats

    recheck_urls = {r["url"] for r in releases[:recheck]}
    if resume:
        log.info(
            "Resume — re-checking last %d: %s", recheck,
            [r["patch_version"] for r in releases[:recheck]],
        )

    to_fetch: list[tuple[dict, object]] = []
    for r in releases:
        do_it, prev = _should_fetch(r, recheck_urls, resume, conn)
        if do_it:
            to_fetch.append((r, prev))
        else:
            log.debug("Skip %s", r["patch_version"])
            stats.skipped += 1

    log.info("Fetching %d WorkBook releases (skipping %d)…", len(to_fetch), stats.skipped)
    if not to_fetch:
        stats.log_summary()
        conn.close()
        return stats

    result_q: queue.Queue = queue.Queue()

    def worker(release: dict, prev_log: object) -> None:
        try:
            issues, plu = fetch_and_parse_workbook(release)
            # 404 or no recognisable content → skip without creating a DB record
            if not issues and plu is None and not release.get("release_date"):
                log.debug("Skip %s — no content found (404?)", release["patch_version"])
                result_q.put(("skip", release, None, None, prev_log))
                return
            # Populate release_date from page if not set during discovery
            if not release.get("release_date") and plu:
                release = {**release, "release_date": plu}
            elif not release.get("release_date"):
                release = {**release, "release_date": datetime.utcnow().strftime("%Y-%m-%d")}
            result_q.put(("ok", release, issues, plu, prev_log))
        except NetworkError as exc:
            result_q.put(("net_error", release, str(exc), None, prev_log))
        except ParseError as exc:
            result_q.put(("parse_error", release, str(exc), None, prev_log))

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

            if kind == "skip":
                log.debug("Skipped (no content) %s", release["patch_version"])
                stats.skipped += 1
                continue

            if kind in ("net_error", "parse_error"):
                err = item[2]
                log.warning("%-11s %s: %s", kind.upper(), release["patch_version"], err)
                if kind == "net_error":
                    stats.net_errors   += 1
                else:
                    stats.parse_errors += 1
                if not dry_run:
                    _persist_error(conn, release["url"], err)
                continue

            _, _, issues, plu, prev_log = item
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
                log.info("  %-20s hash unchanged (%s…)", release["patch_version"], c_hash[:10])
                if not dry_run:
                    now = datetime.utcnow().isoformat()
                    with conn:
                        conn.execute(
                            "INSERT OR REPLACE INTO scrape_log "
                            "(url,status,scraped_at,page_last_updated,content_hash,issue_count) "
                            "VALUES (?,?,?,?,?,?)",
                            (release["url"], "ok", now, plu, c_hash, len(issues)),
                        )
                        conn.execute(
                            "INSERT INTO scrape_history "
                            "(url,scraped_at,page_last_updated,status,issue_count,"
                            "content_hash,changed,added_keys,removed_keys,modified_keys) "
                            "VALUES (?,?,?,?,?,?,0,'[]','[]','[]')",
                            (release["url"], now, plu, "ok", len(issues), c_hash),
                        )
                stats.skipped += 1
                continue

            if dry_run:
                log.info(
                    "  DRY-RUN %-16s %3d issues  hash=%s…  date=%s",
                    release["patch_version"], len(issues), c_hash[:10], plu,
                )
                stats.dry_run += 1
                stats.issues  += len(issues)
                continue

            try:
                is_changed, added, removed, modified = process_release(
                    conn, release, issues, plu, c_hash, prev_log
                )
                stats.scraped += 1
                stats.issues  += len(issues)
                if is_changed:
                    stats.changed += 1

                parts = [f"{len(issues)} issues"]
                if plu:
                    parts.append(f"date={plu}")
                if is_changed:
                    parts.append(f"CHANGED +{len(added)}-{len(removed)}~{len(modified)}")
                log.info("  %-20s %s", release["patch_version"], " | ".join(parts))

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
        description="WorkBook Release Notes Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python workbook_scraper.py
  python workbook_scraper.py --resume --recheck 10
  python workbook_scraper.py --versions 14.0 13.8 --resume
  python workbook_scraper.py --dry-run
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
    s = run(
        versions_filter=args.versions,
        resume=args.resume,
        recheck=args.recheck,
        since=args.since,
        max_releases=getattr(args, "max_releases", None),
        concurrency=args.concurrency,
        dry_run=args.dry_run,
        db_path=args.db,
    )
    sys.exit(0 if s.db_errors == 0 else 1)
