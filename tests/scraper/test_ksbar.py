from unittest.mock import patch, Mock
from scraper.phases.ksbar import (
    scrape_ksbar, is_js_rendered, merge_ksbar_into_firms,
    _scrape_ksbar_static, _parse_member_table,
)

SAMPLE_HTML = """
<html><body>
  <table class="members">
    <tr><td class="name">John Smith</td><td class="firm">Smith & Associates</td>
        <td class="practice">Family Law, Criminal Defense</td><td class="city">Wichita</td></tr>
    <tr><td class="name">Jane Doe</td><td class="firm">Smith & Associates</td>
        <td class="practice">Family Law</td><td class="city">Wichita</td></tr>
    <tr><td class="name">Bob Solo</td><td class="firm"></td>
        <td class="practice">Estate Planning</td><td class="city">Topeka</td></tr>
  </table>
</body></html>
"""

EMPTY_HTML = "<html><body><div id='app'></div></body></html>"

def _mock_get(html):
    r = Mock()
    r.status_code = 200
    r.text = html
    return r

def test_detects_js_rendered():
    assert is_js_rendered(EMPTY_HTML) is True

def test_detects_html_rendered():
    assert is_js_rendered(SAMPLE_HTML) is False

def test_parse_member_table():
    entries = _parse_member_table(SAMPLE_HTML)
    assert len(entries) == 2  # Smith & Associates + Bob Solo
    smith = [e for e in entries if e["firmName"] == "Smith & Associates"]
    assert len(smith) == 1
    assert "Family Law" in smith[0]["practiceAreas"]

def test_solo_practitioner_uses_attorney_name():
    entries = _parse_member_table(SAMPLE_HTML)
    solo = [e for e in entries if e["firmName"] == "Bob Solo"]
    assert len(solo) == 1

def test_scrape_ksbar_falls_back_to_static():
    """When Playwright returns nothing, falls back to static scrape."""
    with patch("scraper.phases.ksbar._scrape_ksbar_playwright", return_value=[]):
        with patch("scraper.phases.ksbar.requests.get", return_value=_mock_get(SAMPLE_HTML)):
            entries = scrape_ksbar()
    firm_names = [e["firmName"] for e in entries]
    assert "Smith & Associates" in firm_names

def test_scrape_ksbar_uses_playwright_when_available():
    """When Playwright returns data, don't fall back."""
    playwright_data = [{"firmName": "Playwright Firm", "practiceAreas": ["Tax Law"], "city": "Topeka"}]
    with patch("scraper.phases.ksbar._scrape_ksbar_playwright", return_value=playwright_data):
        entries = scrape_ksbar()
    assert len(entries) == 1
    assert entries[0]["firmName"] == "Playwright Firm"

def test_merge_adds_practice_areas():
    firms = [{"id": "1", "name": "Smith & Associates", "address": {"city": "Wichita"},
              "practiceAreas": [], "sources": ["google_places"]}]
    ksbar_entries = [{"firmName": "Smith & Associates", "practiceAreas": ["Family Law"], "city": "Wichita"}]
    result = merge_ksbar_into_firms(firms, ksbar_entries)
    assert "Family Law" in result[0]["practiceAreas"]
    assert "ksbar" in result[0]["sources"]

def test_merge_adds_new_firm_when_unmatched():
    firms = []
    ksbar_entries = [{"firmName": "New Firm", "practiceAreas": ["Tax Law"], "city": "Lawrence"}]
    result = merge_ksbar_into_firms(firms, ksbar_entries)
    assert len(result) == 1
    assert result[0]["name"] == "New Firm"
