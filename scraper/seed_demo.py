#!/usr/bin/env python3
"""
Seed database with sample data including real breadcrumb paths.
Run this to get a working demo without scraping.
For the full database, run scraper.py.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "release_notes.db"

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS releases (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    major_version TEXT NOT NULL,
    patch_version TEXT NOT NULL UNIQUE,
    build         TEXT,
    release_date  TEXT,
    url           TEXT,
    scraped_at    TEXT
);

CREATE TABLE IF NOT EXISTS issues (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    defect_number  TEXT,
    type           TEXT NOT NULL CHECK(type IN ('defect','enhancement','regulatory','security')),
    breadcrumb     TEXT,
    nav_level1     TEXT,
    nav_level2     TEXT,
    nav_level3     TEXT,
    category       TEXT,
    subcategory    TEXT,
    title          TEXT,
    description    TEXT,
    first_seen_at  TEXT,
    updated_at     TEXT,
    UNIQUE(defect_number, type)
);

CREATE TABLE IF NOT EXISTS issue_versions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    issue_id   INTEGER NOT NULL REFERENCES issues(id),
    release_id INTEGER NOT NULL REFERENCES releases(id),
    UNIQUE(issue_id, release_id)
);

CREATE TABLE IF NOT EXISTS scrape_log (
    url               TEXT PRIMARY KEY,
    status            TEXT,
    scraped_at        TEXT,
    page_last_updated TEXT,
    content_hash      TEXT,
    issue_count       INTEGER
);

CREATE TABLE IF NOT EXISTS scrape_history (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    url           TEXT NOT NULL,
    scraped_at    TEXT NOT NULL,
    page_last_updated TEXT,
    status        TEXT,
    issue_count   INTEGER,
    content_hash  TEXT,
    changed       INTEGER DEFAULT 0,
    added_keys    TEXT,
    removed_keys  TEXT,
    modified_keys TEXT
);

CREATE VIRTUAL TABLE IF NOT EXISTS issues_fts USING fts5(
    title, description, breadcrumb, nav_level1, nav_level2, nav_level3,
    content='issues', content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS issues_ai AFTER INSERT ON issues BEGIN
    INSERT INTO issues_fts(rowid, title, description, breadcrumb, nav_level1, nav_level2, nav_level3)
    VALUES (new.id, new.title, new.description, new.breadcrumb, new.nav_level1, new.nav_level2, new.nav_level3);
END;
"""

RELEASES = [
    ("7.0", "7.0.7", "7.0.7.1511", "2024-10-14"),
    ("7.0", "7.0.8", "7.0.8.1641", "2024-11-11"),
    ("7.0", "7.0.9", "7.0.9.1780", "2024-12-16"),
    ("7.0", "7.0.10", "7.0.10.1916", "2025-01-20"),
    ("7.0", "7.0.11", "7.0.11.2046", "2025-03-03"),
    ("2025.1", "2025.1.0", "2025.1.0.169", "2025-01-09"),
    ("2025.1", "2025.1.1", "2025.1.1.306", "2025-01-30"),
    ("2025.1", "2025.1.2", "2025.1.2.438", "2025-03-03"),
    ("2025.1", "2025.1.3", "2025.1.3.564", "2025-04-07"),
    ("2025.1", "2025.1.4", "2025.1.4.681", "2025-05-12"),
    ("2025.1", "2025.1.5", "2025.1.5.784", "2025-06-02"),
    ("2025.1", "2025.1.6", "2025.1.6.888", "2025-07-14"),
]

def bc(path):
    """Parse 'A >> B >> C' -> (full, l1, l2, l3)."""
    parts = [p.strip() for p in path.split(">>")]
    while len(parts) < 3:
        parts.append("")
    return path.strip(), parts[0], parts[1], parts[2]

# (type, defect_number, bc(), title, description, [versions])
ISSUES = [
    # Real defects from 2025.1.4 page
    ("defect", "2371845", bc("Hubs >> Projects >> Planning"),
     "Migrated Vision projects with empty plan structure missing linked opportunities",
     "When you migrated from Vision to Vantagepoint, if the project had a plan with no plan structure, opportunities that were linked to the project were missing.",
     ["2025.1.4"]),

    ("defect", "2382198", bc("Hubs >> Projects >> Project"),
     "Project Opened date changes to current date when adding a phase or task from a template",
     "When you added a phase/task from a project or template to an existing regular project, the Project Opened date changed to the current date.",
     ["2025.1.4"]),

    ("defect", "2372807", bc("My Stuff >> Expense Report"),
     "Matched expense lines and credit card charges missing from Matched Credit Card dialog",
     "Matching expense lines and credit card charges did not appear in the Matched Credit Card Dialog box. This occurred if the Merchant Description in expense lines did not exactly match the import charges.",
     ["2025.1.4"]),

    ("defect", "2362549", bc("My Stuff >> Reporting"),
     "Budget Hours and Budget Amounts empty on Project Summary and Project Progress reports",
     "When you ran the Project Summary and Project Progress reports, the Budget Hours and Budget Amounts did not display any values. This occurred when you selected Project Planning Budget as the source.",
     ["2025.1.3", "2025.1.4"]),

    ("defect", "2377746", bc("My Stuff >> Reporting"),
     "Object reference error when selecting many records with Warn Before Printing Large Reports enabled",
     "When you selected more than a certain number of records with the Warn Before Printing Large Reports Opt-in feature turned on, an 'Object reference not set to an instance of an object' error occurred on multiple reports.",
     ["2025.1.4"]),

    ("defect", "2373513", bc("Resource Management >> Project View"),
     "Security role view/update filters cause boolean expression error in Project View",
     "When your security role's record access had view and update filters set for Employee Assignment or Generic Assignment hubs, the following error displayed when navigating to Project View: 'An expression of non-boolean type specified in a context where a condition is expected'.",
     ["2025.1.4"]),

    ("defect", "2371844", bc("Resource Management >> Resource View"),
     "Migrated Vision projects excluded from utilization calculations in Resource View",
     "When you viewed the fields that applied utilization calculations, some projects were excluded. This occurred when you migrated from Vision to Vantagepoint, and affected project plan records upgraded from Vision opportunity filled with an empty charge type.",
     ["2025.1.4"]),

    ("defect", "2377831", bc("Resource Management >> Resource View"),
     "Resources do not appear when Organization Level subcodes used in Group Resources By field",
     "When you used any Organization Level subcodes in the Group Resources By field in the Grid Settings, resources did not appear in the Resource View form.",
     ["2025.1.4"]),

    ("defect", "2371849", bc("Search"),
     "Ask Dela error when querying large databases for employees or projects",
     "In Ask Dela, when you queried a large database about employees or projects, an error occurred.",
     ["2025.1.4"]),

    ("defect", "2371842", bc("Settings >> Workflow >> Scheduled Workflows"),
     "User-defined grids missing from Workflow Table drop-down in Scheduled Workflow grid",
     "When you viewed the Scheduled Workflow grid, user-defined grids were not displayed in the Workflow Table drop-down field.",
     ["2025.1.4"]),

    ("defect", "2377741", bc("Transaction Center >> Transaction Entry >> AP Vouchers"),
     "Non-billable AP Voucher uses wrong account when reselecting line from saved Create Voucher from PO",
     "When you reselected a line item in a new Create Voucher from PO entry that was previously saved, for non-billable projects the voucher used the Reimbursable Account instead of the Direct Account.",
     ["2025.1.4"]),

    # Ported defects
    ("defect", "2098774", bc("My Stuff >> Reporting"),
     "Project Summary report hangs when running for all projects with no date filter",
     "The Project Summary report would not complete and the browser tab became unresponsive when configured to run for all projects with no date range filter.",
     ["7.0.7", "7.0.8", "7.0.9", "7.0.10"]),

    ("defect", "2089445", bc("Billing >> Interactive Billing"),
     "Invoice draft discarded without warning when switching between projects",
     "When navigating between projects in Interactive Billing, the draft invoice for the previously viewed project was incorrectly discarded without warning.",
     ["7.0.8", "7.0.9", "2025.1.0"]),

    ("defect", "2101560", bc("Hubs >> Projects >> Planning"),
     "Plan hours not zeroed out when resource is reassigned to a different WBS phase",
     "When reassigning a resource to a different WBS phase in the Planning grid, the planned hours for the original assignment were not correctly zeroed out, resulting in double-counting of planned hours.",
     ["7.0.10", "7.0.11", "2025.1.0"]),

    ("defect", "2091230", bc("My Stuff >> Timesheets"),
     "Timesheet audit report shows incorrect username for admin-submitted timesheets",
     "The Timesheet Audit Detail Report displayed an incorrect username for timesheets that were submitted on behalf of another employee by an administrator.",
     ["7.0.9", "7.0.10"]),

    ("defect", "2112300", bc("Transaction Center >> Transaction Entry >> AP Vouchers"),
     "AP Voucher amounts mismatch Purchase Order totals when partial receipts are posted",
     "When a purchase order had multiple partial receipts posted, creating a voucher from the PO resulted in the vouchered amount not matching the sum of received quantities.",
     ["2025.1.1", "2025.1.2"]),

    ("defect", "2115890", bc("Payroll >> Quarterly Processing"),
     "State quarterly file uses primary company FEIN instead of active company FEIN",
     "When running quarterly payroll processing for a state in a multi-company environment, the generated electronic file used the FEIN from the primary company instead of the active company.",
     ["2025.1.2", "2025.1.3"]),

    ("defect", "2123682", bc("Accounting >> Revenue Generation"),
     "Revenue Generation error with custom formula fields containing incl. Add-ons label",
     "When you ran Revenue Generation with Projects that used a revenue method with a custom formula, and the custom formula had fields with '(incl. Add-ons)' and '(incl. Held and Add-ons)' label, an 'Argument Index is not a valid value' error message displayed.",
     ["2025.1.0"]),

    ("defect", "2027002", bc("Billing >> Batch Billing"),
     "Entire fee amount incorrectly posted to first billing phase with Separate Terms enabled",
     "When using billing groups with Separate Terms set to Yes and Consolidate All Posting to No, the entire fee amount of the main project was incorrectly posted to the first billing phase when accepting an invoice.",
     ["2025.1.0"]),

    # Enhancements with proper breadcrumbs
    ("enhancement", None, bc("Hubs >> Activities"),
     "Add Multiple Activity Attendees at the Same Time",
     "You can now add multiple attendees to an activity record at the same time in Hubs > Activities. Use the Add Attendees button to add existing contacts, new contacts, or employees.",
     ["2025.1.0"]),

    ("enhancement", None, bc("API"),
     "Expose API Endpoints to Run a Report and Receive a Saved PDF",
     "API endpoints to run a report and receive a saved PDF are now available. These new API endpoints honor existing validations and restrictions, as well as security requirements at the application level.",
     ["2025.1.0"]),

    ("enhancement", None, bc("Ask Dela"),
     "Create Contact Records via Ask Dela",
     "You can now use Ask Dela to create contact records by providing details such as first name, last name, phone number, email address, qualified status, and firm name.",
     ["2025.1.0"]),

    ("enhancement", None, bc("Billing >> Batch Billing"),
     "Email Invoice Download Link When Email Message Size Exceeds Limit",
     "You now have the option for Vantagepoint to automatically send a link in an email message for downloading a billing invoice if the invoice file exceeds the email message size limit. The download link is valid for 15 days.",
     ["2025.1.0"]),

    ("enhancement", None, bc("My Stuff >> Expense Report"),
     "Electronically Sign or Certify Expense Report Submissions",
     "Administrators can now require that employees electronically sign their expense reports by selecting a checkbox rather than entering their Vantagepoint password. Set the Electronic Signature When Submitting option to Click to Certify or Password Required.",
     ["2025.1.0"]),

    ("enhancement", None, bc("My Stuff >> Timesheets"),
     "Electronically Sign or Certify Timesheet Submissions",
     "Administrators can now require that employees electronically sign their timesheets by selecting a checkbox rather than entering their Vantagepoint password on the Confirm Electronic Signature dialog box.",
     ["2025.1.0"]),

    ("enhancement", None, bc("Hubs >> Projects"),
     "Improved Performance for Project Searches with Organization Levels",
     "Searches are now processed in less time when you search for projects using various organization levels. Replace organization level substrings in saved SQL Where Clauses searches with the new organization level columns.",
     ["2025.1.0"]),

    ("enhancement", None, bc("Settings >> Billing >> Invoice Approval Process"),
     "Invoice Approval Process in the Browser Application (Opt-In Available)",
     "Invoice approval processes are now available in the browser application. You assign these approval processes to projects that define who can approve and reject invoices and determine how and when notification alerts are sent.",
     ["2025.1.0"]),

    ("enhancement", None, bc("Transaction Center >> Transaction Entry >> AP Vouchers"),
     "Vouchered Amount Field and Show All Toggle Added to AP Vouchers Form",
     "The Vouchered Amount field is now included on the AP Vouchers form. Use the new Show All toggle to control which line details display in the Project Information grid.",
     ["2025.1.0"]),

    ("enhancement", None, bc("My Stuff >> Reporting"),
     "Track Corporate Travel with Expense Line Travel Category Detail Report",
     "You can now create an Expense Line Travel Category Detail report to track employee travel and assess your corporate carbon footprint and ESG initiative status.",
     ["2025.1.0"]),

    # Regulatory
    ("regulatory", None, bc("Payroll >> Federal"),
     "Federal Income Tax Withholding 2025 Updates",
     "The updates for federal income tax withholding effective January 1, 2025 include adjustments to the nonresident alien additional amounts and the percentage method tax brackets.",
     ["2025.1.1"]),

    ("regulatory", None, bc("Payroll >> California"),
     "California SB-1234 Supplemental Wage Reporting Update",
     "Updated payroll reporting to comply with California SB-1234 effective January 1, 2025, which changes reporting requirements for supplemental wages and bonus payments.",
     ["2025.1.0"]),
]


def seed(db_path: Path = DB_PATH):
    import hashlib, json, sys
    sys.path.insert(0, str(Path(__file__).parent))
    from scraper import get_db, make_issue_key

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_db(db_path)   # runs full SCHEMA including indexes

    release_id_map = {}
    for major, patch, build, date in RELEASES:
        conn.execute(
            """INSERT OR REPLACE INTO releases
               (major_version, patch_version, build, release_date, scraped_at)
               VALUES (?,?,?,?,datetime('now'))""",
            (major, patch, build, date),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM releases WHERE patch_version=?", (patch,)).fetchone()
        release_id_map[patch] = row["id"]

    for rec in ISSUES:
        issue_type, defect_num, (bc_full, l1, l2, l3), title, description, versions = rec

        if defect_num:
            existing = conn.execute("SELECT id FROM issues WHERE defect_number=?", (defect_num,)).fetchone()
        else:
            existing = conn.execute("SELECT id FROM issues WHERE type=? AND title=?", (issue_type, title)).fetchone()

        if existing:
            issue_id = existing["id"]
        else:
            ikey = make_issue_key({
                "defect_number": defect_num, "type": issue_type,
                "nav_level1": l1, "nav_level2": l2, "nav_level3": l3, "title": title,
            })
            cur = conn.execute(
                """INSERT INTO issues
                   (issue_key, defect_number, type, breadcrumb, nav_level1, nav_level2, nav_level3,
                    category, subcategory, title, description, first_seen_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))""",
                (ikey, defect_num, issue_type, bc_full, l1, l2, l3, l1, l2, title, description),
            )
            issue_id = cur.lastrowid

        for patch_ver in versions:
            rel_id = release_id_map.get(patch_ver)
            if rel_id:
                conn.execute(
                    "INSERT OR IGNORE INTO issue_versions (issue_id, release_id) VALUES (?,?)",
                    (issue_id, rel_id),
                )
    conn.commit()

    # --- Seed scrape_log and scrape_history with realistic demo data ---
    def fp(issues_for_release):
        parts = []
        for i in issues_for_release:
            key = f"D{i[1]}" if i[1] else f"E{hashlib.sha1((i[0]+':'+i[3]).encode()).hexdigest()[:10]}"
            dh  = hashlib.sha256((i[4] or "").encode()).hexdigest()[:16]
            parts.append(f"{key}:{dh}")
        return hashlib.sha256("\n".join(sorted(parts)).encode()).hexdigest()

    # Build per-release issue lists for hashing
    release_issues = {}
    for rec in ISSUES:
        issue_type, defect_num, (bc_full, l1, l2, l3), title, description, versions = rec
        for v in versions:
            release_issues.setdefault(v, []).append((issue_type, defect_num, bc_full, title, description))

    # All releases scraped initially
    for major, patch, build, date in RELEASES:
        url = f"https://help.deltek.com/product/Vantagepoint/{major}/ReleaseNotes/DeltekVantagepoint{patch.replace('.','')[:8]}ReleaseNotes.htm"
        issues_for_rel = release_issues.get(patch, [])
        c_hash = fp(issues_for_rel)
        conn.execute(
            """INSERT OR REPLACE INTO scrape_log
               (url, status, scraped_at, page_last_updated, content_hash, issue_count)
               VALUES (?,?,?,?,?,?)""",
            (url, "ok", f"{date}T06:00:00", date, c_hash, len(issues_for_rel))
        )
        conn.execute(
            """INSERT INTO scrape_history
               (url, scraped_at, page_last_updated, status, issue_count, content_hash,
                changed, added_keys, removed_keys, modified_keys)
               VALUES (?,?,?,?,?,?,0,'[]','[]','[]')""",
            (url, f"{date}T06:00:00", date, "ok", len(issues_for_rel), c_hash)
        )

    # Simulate a re-check where 2025.1.4 was amended: 2 new defects added
    amended_url = "https://help.deltek.com/product/Vantagepoint/2025.1/ReleaseNotes/DeltekVantagepoint2025141ReleaseNotes.htm"
    conn.execute(
        """INSERT OR REPLACE INTO scrape_log
           (url, status, scraped_at, page_last_updated, content_hash, issue_count)
           VALUES (?,?,?,?,?,?)""",
        (amended_url, "ok", "2025-05-20T06:00:00", "2025-05-20",
         "abc123def456abc1", 13)
    )
    conn.execute(
        """INSERT INTO scrape_history
           (url, scraped_at, page_last_updated, status, issue_count, content_hash,
            changed, added_keys, removed_keys, modified_keys)
           VALUES (?,?,?,?,?,?,1,?,?,?)""",
        (amended_url, "2025-05-20T06:00:00", "2025-05-20", "ok", 13, "abc123def456abc1",
         json.dumps(["D2384001", "D2384215"]),
         json.dumps([]),
         json.dumps(["D2371844"]))
    )
    conn.commit()

    print(f"Seeded: {db_path}")
    for row in conn.execute("SELECT type, COUNT(*) as n FROM issues GROUP BY type ORDER BY n DESC"):
        print(f"  {row[0]:15s}: {row[1]}")
    hist = conn.execute("SELECT COUNT(*) n FROM scrape_history WHERE changed=1").fetchone()
    print(f"  scrape_history : {conn.execute('SELECT COUNT(*) n FROM scrape_history').fetchone()[0]} rows ({hist[0]} with changes)")
    conn.close()


if __name__ == "__main__":
    seed()
