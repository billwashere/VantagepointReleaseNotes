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

from scraper import parse_html, make_issue_key, make_desc_hash, make_content_hash, _diff, DEFECT_RE, _MAX_SINGLE_LEVEL_BC


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
        assert i["nav_level1"] == "Federal", (
            f"nav_level1 must be 'Federal', got {i['nav_level1']!r} for {i['title']!r}"
        )
    assert_eq("title[0]", issues[0]["title"], "Federal Income Tax Withholding 2025 Updates")
    assert_eq("title[1]", issues[1]["title"], "2021 Form W-2 Box 14 (Reporting COVID Leave Wages for Leave Provided in 2021)")


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
    """
    html = """<html><body>
    <p>Enhancements</p>
    <p><strong>Timesheets</strong></p>
    <p><strong>Electronically Sign Timesheet Submissions</strong></p>
    <p>Employees can now sign timesheets by clicking a checkbox instead of entering a password.</p>
    <p><strong>My Stuff</strong></p>
    <p><strong>Electronically Sign Expense Report Submissions</strong></p>
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


def test_max_single_level_bc_constant():
    """_MAX_SINGLE_LEVEL_BC must be 25 or less — if raised it risks title regression."""
    assert _MAX_SINGLE_LEVEL_BC <= 25, (
        f"_MAX_SINGLE_LEVEL_BC={_MAX_SINGLE_LEVEL_BC} is too high; "
        "raising it risks long enhancement titles becoming breadcrumbs"
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


# ---------------------------------------------------------------------------
# Standalone runner (no pytest needed)
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
        test_max_single_level_bc_constant,
        test_page_last_updated_extracted,
        test_page_last_updated_missing,
        test_duplicate_issues_on_page_deduplicated,
        test_issue_key_stable_across_description_change,
        test_content_hash_detects_description_change,
        test_content_hash_detects_new_issue,
        test_diff_correctly_identifies_changes,
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
