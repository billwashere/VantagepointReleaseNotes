"""Offline test: index pages whose rows link to the Learning Hub (2026.4+)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scraper"))

import scraper

INDEX_URL = "https://help.deltek.com/product/Vantagepoint/2026.4/ReleaseNotes/"

HUB_INDEX_HTML = """
<table>
<tr><td><a href="https://learning.deltek.com/category/vantagepoint_release_notes">Deltek Vantagepoint 2026.4.3.1 (Build 2026.4.3.531)</a> - October 30, 2026</td></tr>
<tr><td><a href="https://learning.deltek.com/category/vantagepoint_release_notes">Deltek Vantagepoint 2026.4 (Build 2026.4.0.177)</a> - September 9, 2026</td></tr>
<tr><td><a href="DVP20263toDVP20264DatabaseChanges.htm">2026.3 to 2026.4</a></td></tr>
</table>
"""

LEGACY_INDEX_HTML = """
<table><tr><td><a href="DeltekVantagepoint202614ReleaseNotes.htm">Deltek Vantagepoint 2026.1.4 (Build 2026.1.4.635)</a> - February 17, 2026</td></tr></table>
"""


def test_hub_linked_index_builds_release_page_urls(monkeypatch):
    monkeypatch.setattr(scraper, "fetch_html", lambda url: HUB_INDEX_HTML)
    rels = scraper.parse_index_page(INDEX_URL)
    assert [r["patch_version"] for r in rels] == ["2026.4.3.1", "2026.4"]
    assert rels[0]["url"] == (
        "https://learning-be.deltek.com/bundle/Vantagepoint_2026.4_Release_Notes"
        "/page/Vantagepoint_2026.4.3.1_Release_Notes.html"
    )
    assert rels[1]["release_date"] == "2026-09-09"


def test_legacy_index_still_uses_relative_htm(monkeypatch):
    monkeypatch.setattr(scraper, "fetch_html", lambda url: LEGACY_INDEX_HTML)
    rels = scraper.parse_index_page("https://help.deltek.com/product/Vantagepoint/2026.1/ReleaseNotes/")
    assert rels[0]["url"].endswith("/2026.1/ReleaseNotes/DeltekVantagepoint202614ReleaseNotes.htm")
