"""
tests/test_parser.py
====================
Unit tests for scraper.parse_html() and related helpers.
These tests run entirely offline — no network access required.

Run with:
    python -m pytest tests/            # all tests
    python -m pytest tests/ -v         # verbose
    python tests/test_parser.py        # standalone (no pytest needed)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))

from scraper import parse_html, make_issue_key, make_desc_hash, make_content_hash, _diff, DEFECT_RE, _MAX_MODULE_NAME_WORDS, _nav_for_section, SECTION_NAV_LABEL, SECTION_PATTERNS, PAGE_LAST_UPDATED_RE
from scraper import parse_date


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def issues_by_dn(issues):
    """Return dict of {defect_number: issue} for easy lookup."""
    return {i["defect_number"]: i for i in issues if i.get("defect_number")}


def assert_eq(label, got, expected):
    assert got == expected, f"{label}: expected {expected!r}, got {got!r}"


# ---------------------------------------------------------------------------
# DEFECT_RE
# ---------------------------------------------------------------------------

def test_defect_re_standard():
    """Standard 'Defect 1234: description' matches."""
    assert DEFECT_RE.match("Defect 2555350: Performance issues when printing.")


def test_defect_re_no_space():
    """'Defect1541790: description' (no space) must also match after fix."""
    m = DEFECT_RE.match("Defect1541790: Searches returned incorrect results when the Starts With operator was used.")
    assert m is not None, "DEFECT_RE must handle no-space variant"
    assert m.group(1) == "1541790"


def test_defect_re_does_not_match_module_names():
    """Module names and enhancement titles must not match DEFECT_RE."""
    non_defects = [
        "Federal Income Tax Withholding 2025 Updates",
        "Hubs >> Projects >> Planning",
        "Search",
        "Billing",
        "Resource Management",
    ]
    for text in non_defects:
        assert not DEFECT_RE.match(text), f"DEFECT_RE must not match: {text!r}"


# ---------------------------------------------------------------------------
# Bug 1: no-space defect number eaten as breadcrumb
# ---------------------------------------------------------------------------

def test_nospace_defect_captured_not_lost():
    """
    Defect1541790 (number directly adjacent with no space) must be captured
    as a defect, not silently consumed as a breadcrumb.
    """
    html = """<html><body>
    <p>Software Issues Resolved</p>
    <p><strong>Billing &gt;&gt; Interactive Billing</strong></p>
    <p><strong>Defect 2555350</strong>: Performance issues when printing retainage lines.</p>
    <p><strong>Defect1541790</strong>: Searches using the Project - Billing Client field returned
    incorrect results when the Starts With, Contains, and Does not Contain operators were used.</p>
    <p><strong>Defect 2262043</strong>: Washington changed to Western Australia.</p>
    <p><strong>Defect 2516657</strong>: Contact search returned non-matching contacts.</p>
    <p><strong>Defect 2559426</strong>: Phase Number operators were unavailable.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    dn_map = issues_by_dn(issues)

    assert len(issues) == 5, f"Expected 5 defects, got {len(issues)}"
    assert "1541790" in dn_map, "Defect 1541790 must be captured (not consumed as breadcrumb)"
    assert dn_map["1541790"]["nav_level1"] == "Billing"
    assert dn_map["1541790"]["nav_level2"] == "Interactive Billing"


def test_nospace_defect_does_not_poison_subsequent_breadcrumbs():
    """
    When a no-space defect number exists, subsequent defects must NOT inherit
    the defect text as their breadcrumb.
    """
    html = """<html><body>
    <p>Software Issues Resolved</p>
    <p><strong>Search</strong></p>
    <p><strong>Defect1541790</strong>: Searches returned wrong results when using Starts With operator.</p>
    <p><strong>Defect 2262043</strong>: Washington changed to Western Australia.</p>
    <p><strong>Defect 2516657</strong>: Firm filter returned non-matching contacts.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 3
    for i in issues:
        assert i["nav_level1"] == "Search", (
            f"Defect {i['defect_number']} has wrong nav_level1: {i['nav_level1']!r}"
        )


# ---------------------------------------------------------------------------
# Bug 2a: regulatory/enhancement title treated as module breadcrumb
# ---------------------------------------------------------------------------

def test_regulatory_title_not_breadcrumb():
    """
    A long bold regulatory title like 'Federal Income Tax Withholding 2025 Updates'
    must NOT overwrite the module breadcrumb ('Federal').
    Regulatory issues have nav_level1='Regulatory', nav_level2=jurisdiction.
    """
    html = """<html><body>
    <p>Regulatory Enhancements</p>
    <p><strong>Federal</strong></p>
    <p><strong>Federal Income Tax Withholding 2025 Updates</strong></p>
    <p>The updates effective January 1, 2025 include adjusted brackets.</p>
    <p><strong>2021 Form W-2 Box 14 (Reporting COVID Leave Wages for Leave Provided in 2021)</strong></p>
    <p>Description of this regulatory change in detail.</p>
    </body></html>"""
    issues, _ = parse_html(html)

    assert len(issues) == 2, f"Expected 2 regulatory items, got {len(issues)}: {[i['title'] for i in issues]}"
    for i in issues:
        # nav_level1 is now the synthetic "Regulatory" grouping
        assert i["nav_level1"] == "Regulatory", (
            f"nav_level1 must be 'Regulatory', got {i['nav_level1']!r} for {i['title']!r}"
        )
        # nav_level2 holds the jurisdiction that was previously nav_level1
        assert i["nav_level2"] == "Federal", (
            f"nav_level2 must be 'Federal', got {i['nav_level2']!r} for {i['title']!r}"
        )
    assert_eq("breadcrumb[0]", issues[0]["breadcrumb"], "Regulatory >> Federal")
    assert_eq("title[0]",      issues[0]["title"],      "Federal Income Tax Withholding 2025 Updates")
    assert_eq("breadcrumb[1]", issues[1]["breadcrumb"], "Regulatory >> Federal")
    assert_eq("title[1]",      issues[1]["title"],      "2021 Form W-2 Box 14 (Reporting COVID Leave Wages for Leave Provided in 2021)")


def test_enhancement_title_not_breadcrumb():
    """
    An enhancement title like 'Ability to Add Assignments Under Inactive WBS Levels'
    must NOT overwrite the module breadcrumb ('Hubs').
    """
    html = """<html><body>
    <p>Enhancements</p>
    <p><strong>Hubs</strong></p>
    <p><strong>Ability to Add Assignments Under Inactive WBS Levels</strong></p>
    <p>You can now add resource assignments to inactive WBS levels for historical reporting.</p>
    <p><strong>Ability to Stop Report Query from Running</strong></p>
    <p>You can now stop a running report query to save system resources.</p>
    </body></html>"""
    issues, _ = parse_html(html)

    assert len(issues) == 2, f"Expected 2 enhancements, got {len(issues)}: {[i['title'] for i in issues]}"
    for i in issues:
        assert i["nav_level1"] == "Hubs", (
            f"nav_level1 must be 'Hubs', got {i['nav_level1']!r} for {i['title']!r}"
        )
    assert_eq("title[0]", issues[0]["title"], "Ability to Add Assignments Under Inactive WBS Levels")
    assert_eq("title[1]", issues[1]["title"], "Ability to Stop Report Query from Running")


# ---------------------------------------------------------------------------
# Bug 3: pending_title title+description merging
# ---------------------------------------------------------------------------

def test_enhancement_title_and_description_merged():
    """
    A bold enhancement title in its own <p> must merge with the following
    description paragraph into a single issue.
    """
    html = """<html><body>
    <p>Enhancements</p>
    <p><strong>API</strong></p>
    <p><strong>New API Endpoint for Report Generation</strong></p>
    <p>API endpoints to run a report and receive a saved PDF are now available.</p>
    </body></html>"""
    issues, _ = parse_html(html)

    assert len(issues) == 1, f"Expected 1 issue, got {len(issues)}"
    assert_eq("title", issues[0]["title"], "New API Endpoint for Report Generation")
    assert "saved PDF" in issues[0]["description"], "Description must be the following paragraph"
    assert_eq("nav_level1", issues[0]["nav_level1"], "API")


def test_trailing_pending_title_flushed():
    """
    If a bold title is the last element with no following description,
    it must still be emitted as an issue.
    """
    html = """<html><body>
    <p>Enhancements</p>
    <p><strong>Mobile</strong></p>
    <p><strong>Sign or Certify Mobile Time and Expense Submissions</strong></p>
    </body></html>"""
    issues, _ = parse_html(html)

    assert len(issues) == 1, f"Expected 1 issue from trailing title, got {len(issues)}"
    assert issues[0]["title"] == "Sign or Certify Mobile Time and Expense Submissions"
    assert issues[0]["nav_level1"] == "Mobile"


def test_multiple_titles_each_get_own_description():
    """
    Multiple sequential bold titles must each get their own following description.
    Uses actual Deltek title wording (6+ words, matching real pages like 2025.1).
    """
    html = """<html><body>
    <p>Enhancements</p>
    <p><strong>Timesheets</strong></p>
    <p><strong>Electronically Sign or Certify Timesheet Submissions</strong></p>
    <p>Employees can now sign timesheets by clicking a checkbox instead of entering a password.</p>
    <p><strong>My Stuff</strong></p>
    <p><strong>Electronically Sign or Certify Expense Report Submissions</strong></p>
    <p>Administrators can require electronic signatures on expense reports for compliance.</p>
    </body></html>"""
    issues, _ = parse_html(html)

    assert len(issues) == 2, f"Expected 2 issues, got {len(issues)}"
    assert issues[0]["nav_level1"] == "Timesheets"
    assert "checkbox" in issues[0]["description"]
    assert issues[1]["nav_level1"] == "My Stuff"
    assert "expense reports" in issues[1]["description"]


# ---------------------------------------------------------------------------
# Breadcrumb parsing
# ---------------------------------------------------------------------------

def test_three_level_breadcrumb():
    html = """<html><body>
    <p>Software Issues Resolved</p>
    <p><strong>Transaction Center &gt;&gt; Transaction Entry &gt;&gt; AP Vouchers</strong></p>
    <p>Defect 2377741: The voucher used the wrong account for non-billable projects.</p>
    </body></html>"""
    issues, _ = parse_html(html)

    assert len(issues) == 1
    assert_eq("nav_level1", issues[0]["nav_level1"], "Transaction Center")
    assert_eq("nav_level2", issues[0]["nav_level2"], "Transaction Entry")
    assert_eq("nav_level3", issues[0]["nav_level3"], "AP Vouchers")
    assert_eq("breadcrumb", issues[0]["breadcrumb"], "Transaction Center >> Transaction Entry >> AP Vouchers")


def test_two_level_breadcrumb():
    html = """<html><body>
    <p>Software Issues Resolved</p>
    <p><strong>My Stuff &gt;&gt; Reporting</strong></p>
    <p>Defect 2362549: Budget Hours did not display when using Project Planning Budget.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert issues[0]["nav_level1"] == "My Stuff"
    assert issues[0]["nav_level2"] == "Reporting"
    assert issues[0]["nav_level3"] == ""


def test_single_level_short_module_is_breadcrumb():
    """Short module names like 'Search', 'API', 'Billing' must be breadcrumbs."""
    for module in ["Search", "API", "Billing", "Payroll", "Dashboards", "Mobile"]:
        html = f"""<html><body>
        <p>Software Issues Resolved</p>
        <p><strong>{module}</strong></p>
        <p>Defect 9999999: Some defect description here for this module area.</p>
        </body></html>"""
        issues, _ = parse_html(html)
        assert len(issues) == 1, f"Expected 1 issue for module {module!r}"
        assert issues[0]["nav_level1"] == module, f"nav_level1 must be {module!r}"


def test_long_module_name_with_sep_is_breadcrumb():
    """Long module names with >> like 'Resource Management >> Resource View' work."""
    html = """<html><body>
    <p>Software Issues Resolved</p>
    <p><strong>Resource Management &gt;&gt; Resource View</strong></p>
    <p>Defect 2377831: Resources did not appear when using Organization Level subcodes.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert issues[0]["nav_level1"] == "Resource Management"
    assert issues[0]["nav_level2"] == "Resource View"


def test_max_module_name_words_constant():
    """_MAX_MODULE_NAME_WORDS must be 6 or less — raising it risks long enhancement titles becoming breadcrumbs."""
    assert _MAX_MODULE_NAME_WORDS <= 6, (
        f"_MAX_MODULE_NAME_WORDS={_MAX_MODULE_NAME_WORDS} is too high; "
        "module names have at most 5 words — keep buffer at 6"
    )


# ---------------------------------------------------------------------------
# Page metadata
# ---------------------------------------------------------------------------

def test_page_last_updated_extracted():
    html = """<html><body>
    <p>Release Date: May 12, 2025</p>
    <p>Last Updated: June 3, 2025</p>
    <p>Software Issues Resolved</p>
    <p><strong>Search</strong></p>
    <p>Defect 1234567: Some issue occurred.</p>
    </body></html>"""
    _, plu = parse_html(html)
    assert_eq("page_last_updated", plu, "2025-06-03")


def test_page_last_updated_missing():
    html = """<html><body>
    <p>Enhancements</p>
    <p><strong>API</strong></p>
    <p>Some enhancement description here that is long enough to pass the length check.</p>
    </body></html>"""
    _, plu = parse_html(html)
    assert plu is None


# ---------------------------------------------------------------------------
# Deduplication within a page
# ---------------------------------------------------------------------------

def test_duplicate_issues_on_page_deduplicated():
    """Same defect number appearing twice on a page must produce one issue."""
    html = """<html><body>
    <p>Software Issues Resolved</p>
    <p><strong>Billing &gt;&gt; Batch Billing</strong></p>
    <p>Defect 1234567: First occurrence of this defect on the page.</p>
    <p><strong>Billing &gt;&gt; Interactive Billing</strong></p>
    <p>Defect 1234567: Duplicate occurrence of the same defect number.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    defect_numbers = [i["defect_number"] for i in issues]
    assert defect_numbers.count("1234567") == 1, "Duplicate defect must be deduplicated"


# ---------------------------------------------------------------------------
# Hashing utilities
# ---------------------------------------------------------------------------

def test_issue_key_stable_across_description_change():
    """issue_key must not depend on description (so text edits = modifications not new rows)."""
    base = {"defect_number": "1234567", "type": "defect", "title": "T",
            "nav_level1": "Billing", "nav_level2": "", "nav_level3": ""}
    amended = {**base, "description": "COMPLETELY DIFFERENT TEXT"}
    assert make_issue_key(base) == make_issue_key(amended)


def test_content_hash_detects_description_change():
    """content_hash must change when any issue's description changes."""
    issue = {"defect_number": "1234567", "type": "defect", "title": "T",
             "nav_level1": "Billing", "nav_level2": "", "nav_level3": "",
             "description": "Original text."}
    amended = {**issue, "description": "Amended text."}
    assert make_content_hash([issue]) != make_content_hash([amended])


def test_content_hash_detects_new_issue():
    """content_hash must change when a new issue is added."""
    i1 = {"defect_number": "111", "type": "defect", "title": "A",
          "nav_level1": "", "nav_level2": "", "nav_level3": "", "description": "d"}
    i2 = {"defect_number": "222", "type": "defect", "title": "B",
          "nav_level1": "", "nav_level2": "", "nav_level3": "", "description": "e"}
    assert make_content_hash([i1]) != make_content_hash([i1, i2])


def test_diff_correctly_identifies_changes():
    """_diff must return correct added/removed/modified key lists."""
    old_issue = {"defect_number": "111", "type": "defect", "title": "A",
                 "nav_level1": "", "nav_level2": "", "nav_level3": "", "description": "old"}
    new_same  = {"defect_number": "111", "type": "defect", "title": "A",
                 "nav_level1": "", "nav_level2": "", "nav_level3": "", "description": "old"}
    new_mod   = {"defect_number": "111", "type": "defect", "title": "A",
                 "nav_level1": "", "nav_level2": "", "nav_level3": "", "description": "new"}
    brand_new = {"defect_number": "999", "type": "defect", "title": "Z",
                 "nav_level1": "", "nav_level2": "", "nav_level3": "", "description": "x"}

    old_fps = {make_issue_key(old_issue): make_desc_hash(old_issue["description"])}

    added, removed, modified = _diff(old_fps, [new_same])
    assert added == [] and removed == [] and modified == []

    added, removed, modified = _diff(old_fps, [new_mod])
    assert added == [] and removed == [] and len(modified) == 1

    added, removed, modified = _diff(old_fps, [new_same, brand_new])
    assert len(added) == 1 and removed == [] and modified == []

    added, removed, modified = _diff(old_fps, [brand_new])
    assert len(added) == 1 and len(removed) == 1 and modified == []


def test_long_module_name_is_breadcrumb_not_title():
    """
    Module names that exceed 25 chars but are ≤ 6 words must still become
    breadcrumbs, not pending titles. Seen in real pages:
      'Batch Billing and Interactive Billing' (5 words)
      'Analysis Cubes for Vantagepoint Intelligence' (5 words)
      'Billing and Draft Invoice Approvals' (5 words)
    """
    cases = [
        "Batch Billing and Interactive Billing",
        "Analysis Cubes for Vantagepoint Intelligence",
        "Billing and Draft Invoice Approvals",
        "Vantagepoint Connect for Gmail Tutorial",
    ]
    for module in cases:
        html = f"""<html><body>
        <p>Enhancements</p>
        <p><strong>{module}</strong></p>
        <p><strong>New Sub-Feature Title That Is Long Enough To Be A Title</strong></p>
        <p>Description of the sub-feature goes here for testing purposes only.</p>
        </body></html>"""
        issues, _ = parse_html(html)
        assert len(issues) == 1, (
            f"Module {module!r}: expected 1 issue, got {len(issues)} — "
            "module name must become breadcrumb, not pending title"
        )
        assert issues[0]["nav_level1"] == module, (
            f"nav_level1 must be {module!r}, got {issues[0]['nav_level1']!r}"
        )


def test_plain_module_heading_older_page_format():
    """
    In older release note pages (2.0–4.x), module headings appear as plain
    <p> tags (no <strong>), not <p><strong>. They must still become breadcrumbs.
    """
    html = """<html><body>
    <p>Enhancements</p>
    <p>Absence Year Utility</p>
    <p>Absence Accrual Year Utility in the Browser Application: Vantagepoint now provides
    an Absence Year utility in the browser application that enables you to open a new year.</p>
    <p>AP Invoice Approvals</p>
    <p>AP Invoice Approvals Grid has been improved with additional grid functionality columns.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 2, f"Expected 2 issues, got {len(issues)}"
    assert issues[0]["nav_level1"] == "Absence Year Utility"
    assert issues[1]["nav_level1"] == "AP Invoice Approvals"


def test_section_alias_new_features():
    """'New Features' heading (used in some older pages) must be treated as enhancement section."""
    html = """<html><body>
    <p>New Features</p>
    <p><strong>Billing</strong></p>
    <p>Invoice Download: You can now download invoices directly from the billing screen.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 1
    assert issues[0]["type"] == "enhancement"
    assert issues[0]["nav_level1"] == "Billing"


def test_section_alias_new_features_and_enhancements():
    """'New Features and Enhancements' heading must also be recognised."""
    html = """<html><body>
    <p>New Features and Enhancements</p>
    <p><strong>API</strong></p>
    <p>Expose new API endpoints for project data with proper authentication controls.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 1
    assert issues[0]["type"] == "enhancement"


def test_section_alias_issues_resolved():
    """'Issues Resolved' (no 'Software' prefix, used in older pages) must be recognised."""
    html = """<html><body>
    <p>Issues Resolved</p>
    <p><strong>Hubs &gt;&gt; Projects</strong></p>
    <p>Defect 1111111: Project phase dates were not saved correctly when edited inline.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 1
    assert issues[0]["type"] == "defect"
    assert issues[0]["defect_number"] == "1111111"
    assert issues[0]["nav_level1"] == "Hubs"


def test_section_alias_resolved_issues():
    """'Resolved Issues' (reversed word order) must also be recognised."""
    html = """<html><body>
    <p>Resolved Issues</p>
    <p><strong>Billing &gt;&gt; Batch Billing</strong></p>
    <p>Defect 2222222: Batch billing failed when projects had no billing terms set up.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 1
    assert issues[0]["type"] == "defect"
    assert issues[0]["defect_number"] == "2222222"


def test_seven_word_title_is_not_breadcrumb():
    """
    An enhancement title with 7 words must NOT become a breadcrumb even if bold.
    'New API Endpoint for Report Generation' = 7 words > _MAX_MODULE_NAME_WORDS.
    """
    html = """<html><body>
    <p>Enhancements</p>
    <p><strong>API</strong></p>
    <p><strong>New API Endpoint for Report Generation</strong></p>
    <p>API endpoints to run a report and receive a saved PDF are now available.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 1
    assert_eq("nav_level1", issues[0]["nav_level1"], "API")
    assert_eq("title", issues[0]["title"], "New API Endpoint for Report Generation")


# ---------------------------------------------------------------------------


def test_nav_for_section_regulatory_shifts_jurisdiction_to_l2():
    """Regulatory items: section label becomes nav1, jurisdiction shifts to nav2."""
    bc, n1, n2, n3 = _nav_for_section("regulatory", "Federal", "", "")
    assert_eq("nav1", n1, "Regulatory")
    assert_eq("nav2", n2, "Federal")
    assert_eq("nav3", n3, "")
    assert_eq("breadcrumb", bc, "Regulatory >> Federal")


def test_nav_for_section_regulatory_three_levels():
    """Regulatory with sub-jurisdiction: all levels shift down correctly."""
    bc, n1, n2, n3 = _nav_for_section("regulatory", "Payroll", "Federal", "")
    assert_eq("nav1", n1, "Regulatory")
    assert_eq("nav2", n2, "Payroll")
    assert_eq("nav3", n3, "Federal")
    assert_eq("breadcrumb", bc, "Regulatory >> Payroll >> Federal")


def test_nav_for_section_security_shifts_to_l2():
    """Security items: section label becomes nav1."""
    bc, n1, n2, n3 = _nav_for_section("security", "API", "", "")
    assert_eq("nav1", n1, "Security")
    assert_eq("nav2", n2, "API")
    assert_eq("breadcrumb", bc, "Security >> API")


def test_nav_for_section_enhancement_unchanged():
    """Enhancements pass through nav levels unchanged."""
    bc, n1, n2, n3 = _nav_for_section("enhancement", "Billing", "Batch Billing", "")
    assert_eq("nav1", n1, "Billing")
    assert_eq("nav2", n2, "Batch Billing")
    assert_eq("breadcrumb", bc, "Billing >> Batch Billing")


def test_nav_for_section_defect_unchanged():
    """Defects pass through nav levels unchanged."""
    bc, n1, n2, n3 = _nav_for_section("defect", "Transaction Center", "Transaction Entry", "AP Vouchers")
    assert_eq("nav1", n1, "Transaction Center")
    assert_eq("nav2", n2, "Transaction Entry")
    assert_eq("nav3", n3, "AP Vouchers")


def test_security_issues_have_security_as_nav1():
    """Parsed security issues must have nav_level1='Security'."""
    html = """<html><body>
    <p>Security Enhancements</p>
    <p><strong>API</strong></p>
    <p><strong>OAuth Token Expiry Now Enforced for All Sessions</strong></p>
    <p>OAuth tokens now expire after the configured timeout period to improve security posture.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 1
    assert_eq("nav_level1", issues[0]["nav_level1"], "Security")
    assert_eq("nav_level2", issues[0]["nav_level2"], "API")
    assert_eq("breadcrumb", issues[0]["breadcrumb"], "Security >> API")


def test_regulatory_issues_use_regulatory_nav1_not_jurisdiction():
    """Parsed regulatory issues must group under 'Regulatory', not raw jurisdiction names."""
    html = """<html><body>
    <p>Regulatory Enhancements</p>
    <p><strong>Federal</strong></p>
    <p><strong>Federal Income Tax Withholding 2026 Updates</strong></p>
    <p>Updated tables per IRS Notice effective January 1, 2026.</p>
    <p><strong>California</strong></p>
    <p><strong>California SDI Rate Update for 2026</strong></p>
    <p>California updated its State Disability Insurance contribution rate for 2026.</p>
    </body></html>"""
    issues, _ = parse_html(html)
    assert len(issues) == 2
    for i in issues:
        assert i["nav_level1"] == "Regulatory", (
            f"Expected Regulatory, got {i['nav_level1']!r} — jurisdiction names must not appear at nav1"
        )
    assert_eq("nav2[0]", issues[0]["nav_level2"], "Federal")
    assert_eq("nav2[1]", issues[1]["nav_level2"], "California")
    assert issues[0]["breadcrumb"] == "Regulatory >> Federal"
    assert issues[1]["breadcrumb"] == "Regulatory >> California"


def test_app_area_sidebar_not_polluted_by_regulatory():
    """
    In a page with both regulatory and defect sections, nav_level1 values for
    regulatory must be 'Regulatory' — not 'Federal', 'Payroll', etc. —
    so they don't pollute the Application Area sidebar.
    """
    html = """<html><body>
    <p>Regulatory Enhancements</p>
    <p><strong>Payroll</strong></p>
    <p><strong>Social Security Wage Base Increase 2026</strong></p>
    <p>The Social Security wage base increases to 180000 effective January 1, 2026.</p>
    <p>Enhancements</p>
    <p><strong>Hubs</strong></p>
    <p>You can now add assignments to inactive WBS levels for better historical tracking purposes.</p>
    <p>Software Issues Resolved</p>
    <p><strong>My Stuff &gt;&gt; Reporting</strong></p>
    <p>Defect 1234567: Budget Hours did not display when Project Planning Budget was selected.</p>
    </body></html>"""
    issues, _ = parse_html(html)

    nav1_values = {i["nav_level1"] for i in issues}

    # Must contain app module names and Regulatory
    assert "Regulatory" in nav1_values
    assert "Hubs" in nav1_values
    assert "My Stuff" in nav1_values

    # Must NOT contain raw jurisdiction names at nav1
    contaminating = {"Federal", "Payroll", "State", "California", "FICA"}
    assert not nav1_values & contaminating, (
        f"Jurisdiction names leaked into nav_level1: {nav1_values & contaminating}"
    )


# ---------------------------------------------------------------------------
# Date parsing — PAGE_LAST_UPDATED_RE and parse_date
# ---------------------------------------------------------------------------

def test_page_last_updated_normal():
    """Standard 'Last Updated: Month D, YYYY' format."""
    html = "<html><body><p>Last Updated: October 3, 2022</p></body></html>"
    _, plu = parse_html(html)
    assert_eq("plu", plu, "2022-10-03")


def test_page_last_updated_space_before_comma():
    """
    '5.5.1 bug': 'Last Updated: October 3 , 2022' — space before comma.
    Confirmed on DeltekVantagepoint551ReleaseNotes.htm
    """
    html = "<html><body><p>Last Updated: October 3 , 2022</p></body></html>"
    _, plu = parse_html(html)
    assert_eq("plu", plu, "2022-10-03")


def test_page_last_updated_nbsp_before_comma():
    """Non-breaking space (\\xa0) before comma — HTML &nbsp; decoded by the parser."""
    html = "<html><body><p>Last Updated: October 3\xa0, 2022</p></body></html>"
    _, plu = parse_html(html)
    assert_eq("plu", plu, "2022-10-03")


def test_page_last_updated_no_comma():
    """Date with no comma at all: 'October 3 2022'."""
    html = "<html><body><p>Last Updated: October 3 2022</p></body></html>"
    _, plu = parse_html(html)
    assert_eq("plu", plu, "2022-10-03")


def test_parse_date_normalises_variants():
    """parse_date handles all observed whitespace/comma variants."""
    assert_eq("space before comma", parse_date("October 3 , 2022"),    "2022-10-03")
    assert_eq("nbsp before comma",  parse_date("October 3\xa0, 2022"), "2022-10-03")
    assert_eq("no comma",           parse_date("October 3 2022"),       "2022-10-03")
    assert_eq("normal",             parse_date("October 3, 2022"),      "2022-10-03")
    assert_eq("abbreviated month",  parse_date("Oct 3, 2022"),          "2022-10-03")
    assert_eq("two-digit day",      parse_date("February 21, 2022"),    "2022-02-21")
    assert_eq("month only",         parse_date("January 2025"),         "2025-01-01")
    assert parse_date("") is None
    assert parse_date("garbage text") is None


def test_page_last_updated_embedded_with_release_date():
    """Both dates on same line with space-before-comma on Last Updated — real 5.5.1 pattern."""
    html = "<html><body><p>Release Date: October 3, 2022  Last Updated: October 3 , 2022</p></body></html>"
    _, plu = parse_html(html)
    assert_eq("plu", plu, "2022-10-03")


# ---------------------------------------------------------------------------


if __name__ == "__main__":
    tests = [
        test_defect_re_standard,
        test_defect_re_no_space,
        test_defect_re_does_not_match_module_names,
        test_nospace_defect_captured_not_lost,
        test_nospace_defect_does_not_poison_subsequent_breadcrumbs,
        test_regulatory_title_not_breadcrumb,
        test_enhancement_title_not_breadcrumb,
        test_enhancement_title_and_description_merged,
        test_trailing_pending_title_flushed,
        test_multiple_titles_each_get_own_description,
        test_three_level_breadcrumb,
        test_two_level_breadcrumb,
        test_single_level_short_module_is_breadcrumb,
        test_long_module_name_with_sep_is_breadcrumb,
        test_max_module_name_words_constant,
        test_page_last_updated_extracted,
        test_page_last_updated_missing,
        test_duplicate_issues_on_page_deduplicated,
        test_issue_key_stable_across_description_change,
        test_content_hash_detects_description_change,
        test_content_hash_detects_new_issue,
        test_diff_correctly_identifies_changes,
        # Breadcrumb threshold and older page formats
        test_long_module_name_is_breadcrumb_not_title,
        test_plain_module_heading_older_page_format,
        test_section_alias_new_features,
        test_section_alias_new_features_and_enhancements,
        test_section_alias_issues_resolved,
        test_section_alias_resolved_issues,
        test_seven_word_title_is_not_breadcrumb,
        # Nav remapping
        test_nav_for_section_regulatory_shifts_jurisdiction_to_l2,
        test_nav_for_section_regulatory_three_levels,
        test_nav_for_section_security_shifts_to_l2,
        test_nav_for_section_enhancement_unchanged,
        test_nav_for_section_defect_unchanged,
        test_security_issues_have_security_as_nav1,
        test_regulatory_issues_use_regulatory_nav1_not_jurisdiction,
        test_app_area_sidebar_not_polluted_by_regulatory,
        # Date parsing
        test_page_last_updated_normal,
        test_page_last_updated_space_before_comma,
        test_page_last_updated_nbsp_before_comma,
        test_page_last_updated_no_comma,
        test_parse_date_normalises_variants,
        test_page_last_updated_embedded_with_release_date,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {t.__name__}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed+failed} passed" + ("" if not failed else f"  — {failed} FAILED"))
    sys.exit(0 if not failed else 1)
