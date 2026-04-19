"""
tests/test_db_changes_parser.py
================================
Unit tests for db_changes_scraper — all offline, no network needed.

Run with:
    python -m pytest tests/ -v
    python -m pytest tests/test_db_changes_parser.py -v
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))

from db_changes_scraper import (
    _digits_to_version,
    _extract_versions_from_url,
    _extract_versions_from_text,
    parse_db_changes_html,
)


# ---------------------------------------------------------------------------
# Version normalisation
# ---------------------------------------------------------------------------

def test_digits_to_version_modern():
    assert _digits_to_version("20262") == "2026.2"
    assert _digits_to_version("20251") == "2025.1"
    assert _digits_to_version("20261") == "2026.1"


def test_digits_to_version_old():
    assert _digits_to_version("72") == "7.2"
    assert _digits_to_version("71") == "7.1"



def test_extract_versions_from_url_modern():
    url = "https://help.deltek.com/product/Vantagepoint/2026.2/ReleaseNotes/DVP20261toDVP20262DatabaseChanges.htm"
    result = _extract_versions_from_url(url)
    assert result == ("2026.1", "2026.2")


def test_extract_versions_from_url_old():
    url = "https://help.deltek.com/product/Vantagepoint/2025.1/ReleaseNotes/DVP72toDVP20251DatabaseChanges.htm"
    result = _extract_versions_from_url(url)
    assert result == ("7.2", "2025.1")


def test_extract_versions_from_url_no_match():
    assert _extract_versions_from_url("https://example.com/SomeOtherPage.htm") is None


def test_extract_versions_from_text_short():
    assert _extract_versions_from_text("2026.1 to 2026.2") == ("2026.1", "2026.2")


def test_extract_versions_from_text_full():
    result = _extract_versions_from_text(
        "Database Changes (Deltek Vantagepoint 7.1 to Deltek Vantagepoint 7.2)"
    )
    assert result == ("7.1", "7.2")


# ---------------------------------------------------------------------------
# HTML parser — new tables (single-column)
# ---------------------------------------------------------------------------

_NEW_TABLES_HTML = """
<html><body>
<h3>New Tables</h3>
<table>
  <tr><td>Table Name</td></tr>
  <tr><td>ApprovalActionEmails</td></tr>
  <tr><td>ApprovalActionLanguages</td></tr>
</table>
</body></html>
"""


def test_parse_new_tables():
    items = parse_db_changes_html(_NEW_TABLES_HTML)
    assert len(items) == 2
    assert all(i["change_type"] == "new_table" for i in items)
    names = {i["table_name"] for i in items}
    assert names == {"ApprovalActionEmails", "ApprovalActionLanguages"}
    assert all(i["column_name"] is None for i in items)


# ---------------------------------------------------------------------------
# New columns (3-column)
# ---------------------------------------------------------------------------

_NEW_COLUMNS_HTML = """
<html><body>
<h3>New Columns Added to Existing Tables</h3>
<table>
  <tr><th>Table Name</th><th>Column Name</th><th>DataType</th></tr>
  <tr><td>ApprovalWorkflowActions</td><td>NumDays</td><td>int NOT NULL</td></tr>
  <tr><td>CFGVisionSystem</td><td>ReportingCurrencyCode</td><td>nvarchar(3)</td></tr>
</table>
</body></html>
"""


def test_parse_new_columns():
    items = parse_db_changes_html(_NEW_COLUMNS_HTML)
    assert len(items) == 2
    assert all(i["change_type"] == "new_column" for i in items)
    row = items[0]
    assert row["table_name"] == "ApprovalWorkflowActions"
    assert row["column_name"] == "NumDays"
    assert row["data_type"] == "int NOT NULL"


# ---------------------------------------------------------------------------
# Modified columns (4-column, newer style)
# ---------------------------------------------------------------------------

_MODIFIED_COLUMNS_HTML = """
<html><body>
<h3>Modified Columns</h3>
<table>
  <tr><th>TableName</th><th>ColumnName</th><th>OldDataType</th><th>NewDataType</th></tr>
  <tr><td>ProjectInfo</td><td>Description</td><td>varchar(50)</td><td>nvarchar(max)</td></tr>
</table>
</body></html>
"""


def test_parse_modified_columns():
    items = parse_db_changes_html(_MODIFIED_COLUMNS_HTML)
    assert len(items) == 1
    row = items[0]
    assert row["change_type"] == "modified_column"
    assert row["table_name"] == "ProjectInfo"
    assert row["column_name"] == "Description"
    assert row["old_data_type"] == "varchar(50)"
    assert row["new_data_type"] == "nvarchar(max)"


# ---------------------------------------------------------------------------
# Objects (2-column)
# ---------------------------------------------------------------------------

_NEW_OBJECTS_HTML = """
<html><body>
<h3>New Objects</h3>
<table>
  <tr><th>Object Name</th><th>Object Type</th></tr>
  <tr><td>ApprovalActionEmailsWorkflowUIDIDX</td><td>Index</td></tr>
  <tr><td>buildPRSummaryMainNew</td><td>Stored Procedure</td></tr>
</table>
</body></html>
"""


def test_parse_new_objects():
    items = parse_db_changes_html(_NEW_OBJECTS_HTML)
    assert len(items) == 2
    assert all(i["change_type"] == "new_object" for i in items)
    assert items[0]["object_name"] == "ApprovalActionEmailsWorkflowUIDIDX"
    assert items[0]["object_type"] == "Index"
    assert items[0]["table_name"] is None


# ---------------------------------------------------------------------------
# Multiple sections on one page
# ---------------------------------------------------------------------------

_MULTI_SECTION_HTML = """
<html><body>
<h3>New Tables</h3>
<table>
  <tr><td>Table Name</td></tr>
  <tr><td>NewTable1</td></tr>
</table>
<h3>Removed Tables</h3>
<table>
  <tr><td>Table Name</td></tr>
  <tr><td>OldTable1</td></tr>
</table>
<h3>New Columns</h3>
<table>
  <tr><th>Table Name</th><th>Column Name</th><th>DataType</th></tr>
  <tr><td>SomeTable</td><td>SomeCol</td><td>bit NOT NULL</td></tr>
</table>
<h3>Removed Objects</h3>
<table>
  <tr><th>Object Name</th><th>Object Type</th></tr>
  <tr><td>OldIndex</td><td>Index</td></tr>
</table>
</body></html>
"""


def test_parse_multi_section():
    items = parse_db_changes_html(_MULTI_SECTION_HTML)
    by_type = {}
    for i in items:
        by_type.setdefault(i["change_type"], []).append(i)

    assert len(by_type["new_table"]) == 1
    assert len(by_type["removed_table"]) == 1
    assert len(by_type["new_column"]) == 1
    assert len(by_type["removed_object"]) == 1

    assert by_type["new_table"][0]["table_name"] == "NewTable1"
    assert by_type["removed_object"][0]["object_name"] == "OldIndex"
    assert by_type["removed_object"][0]["object_type"] == "Index"


# ---------------------------------------------------------------------------
# Empty / no-match
# ---------------------------------------------------------------------------

def test_parse_empty_html():
    items = parse_db_changes_html("<html><body><p>No changes.</p></body></html>")
    assert items == []


def test_parse_table_before_any_heading_is_skipped():
    html = """<html><body>
    <table><tr><td>SomeTable</td></tr></table>
    <h3>New Tables</h3>
    <table><tr><td>Table Name</td></tr><tr><td>RealTable</td></tr></table>
    </body></html>"""
    items = parse_db_changes_html(html)
    assert len(items) == 1
    assert items[0]["table_name"] == "RealTable"
