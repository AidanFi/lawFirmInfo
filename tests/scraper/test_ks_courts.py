import pytest
from unittest.mock import patch, MagicMock
from scraper.phases.ks_courts import (
    _parse_search_results,
    _parse_detail_page,
    _extract_address_parts,
    _group_attorneys_into_firms,
)


SAMPLE_SEARCH_HTML = """
<html><body>
<table>
<tr><th>Attorney Name</th><th>Current Status</th><th>Registration Number</th></tr>
<tr><td>Smith, John A.</td><td>Active</td><td>12345</td></tr>
<tr><td>Jones, Mary B.</td><td>Retired</td><td>12346</td></tr>
<tr><td>Brown, David C.</td><td>Active</td><td>12347</td></tr>
</table>
</body></html>
"""

SAMPLE_DETAIL_HTML = """
<html><body>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Attorney Name</strong></div>
  <div class="col-md-6 text-md-left">Smith, John A.</div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Date Of Admission</strong></div>
  <div class="col-md-6 text-md-left">01/15/2010</div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Registration Number</strong></div>
  <div class="col-md-6 text-md-left">12345</div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Current Status</strong></div>
  <div class="col-md-6 text-md-left">Active</div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Business Mailing Address</strong></div>
  <div class="col-md-6 text-md-left">
    <p class="my-0">Smith & Associates LLC</p>
    <p class="my-0">123 Main St, Suite 200</p>
    <p class="my-0">Wichita, KS 67202</p>
  </div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Business Phone</strong></div>
  <div class="col-md-6 text-md-left">316-555-0100</div>
</div>
</body></html>
"""

SAMPLE_DETAIL_NO_FIRM_HTML = """
<html><body>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Attorney Name</strong></div>
  <div class="col-md-6 text-md-left">Solo, Jane P.</div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Current Status</strong></div>
  <div class="col-md-6 text-md-left">Active</div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Business Mailing Address</strong></div>
  <div class="col-md-6 text-md-left">
    <p class="my-0">456 Oak Ave</p>
    <p class="my-0">Topeka, KS 66603</p>
  </div>
</div>
<div class="row mb-2">
  <div class="col-md-6 text-md-right"><strong>Business Phone</strong></div>
  <div class="col-md-6 text-md-left">785-555-0200</div>
</div>
</body></html>
"""


class TestParseSearchResults:
    def test_parses_all_rows(self):
        results = _parse_search_results(SAMPLE_SEARCH_HTML)
        assert len(results) == 3

    def test_extracts_correct_fields(self):
        results = _parse_search_results(SAMPLE_SEARCH_HTML)
        regnum, name, status = results[0]
        assert regnum == "12345"
        assert name == "Smith, John A."
        assert status == "Active"

    def test_returns_empty_for_no_table(self):
        assert _parse_search_results("<html><body>No table here</body></html>") == []

    def test_skips_header_row(self):
        results = _parse_search_results(SAMPLE_SEARCH_HTML)
        # Should not include the header row
        assert all(r[0].isdigit() for r in results)


class TestParseDetailPage:
    def test_extracts_all_fields(self):
        fields = _parse_detail_page(SAMPLE_DETAIL_HTML)
        assert fields["Attorney Name"] == "Smith, John A."
        assert fields["Current Status"] == "Active"
        assert fields["Registration Number"] == "12345"
        assert fields["Business Phone"] == "316-555-0100"

    def test_extracts_address_lines(self):
        fields = _parse_detail_page(SAMPLE_DETAIL_HTML)
        assert fields["address_lines"] == [
            "Smith & Associates LLC",
            "123 Main St, Suite 200",
            "Wichita, KS 67202",
        ]

    def test_returns_none_for_empty_page(self):
        assert _parse_detail_page("<html><body></body></html>") is None

    def test_no_firm_in_address(self):
        fields = _parse_detail_page(SAMPLE_DETAIL_NO_FIRM_HTML)
        assert fields["address_lines"] == ["456 Oak Ave", "Topeka, KS 66603"]


class TestExtractAddressParts:
    def test_full_address_with_firm(self):
        lines = ["Smith & Associates LLC", "123 Main St", "Wichita, KS 67202"]
        firm, street, city, state, zip_ = _extract_address_parts(lines)
        assert firm == "Smith & Associates LLC"
        assert street == "123 Main St"
        assert city == "Wichita"
        assert state == "KS"
        assert zip_ == "67202"

    def test_address_without_firm(self):
        lines = ["456 Oak Ave", "Topeka, KS 66603"]
        firm, street, city, state, zip_ = _extract_address_parts(lines)
        assert firm == ""
        assert street == "456 Oak Ave"
        assert city == "Topeka"
        assert state == "KS"

    def test_city_only(self):
        lines = ["Lawrence, KS 66044"]
        firm, street, city, state, zip_ = _extract_address_parts(lines)
        assert firm == ""
        assert street == ""
        assert city == "Lawrence"
        assert state == "KS"
        assert zip_ == "66044"

    def test_empty_lines(self):
        firm, street, city, state, zip_ = _extract_address_parts([])
        assert firm == ""
        assert state == "KS"

    def test_multi_line_street(self):
        lines = ["Big Firm LLP", "100 Corporate Dr", "Suite 500", "Overland Park, KS 66210"]
        firm, street, city, state, zip_ = _extract_address_parts(lines)
        assert firm == "Big Firm LLP"
        assert street == "100 Corporate Dr, Suite 500"
        assert city == "Overland Park"

    def test_out_of_state(self):
        lines = ["DC Firm LLC", "1600 K St NW", "Washington, DC 20006"]
        firm, street, city, state, zip_ = _extract_address_parts(lines)
        assert state == "DC"
        assert city == "Washington"


class TestGroupAttorneysIntoFirms:
    def test_groups_same_firm_same_city(self):
        attorneys = [
            {"name": "Smith, John", "regnum": "1", "firm_name": "Smith Law LLC",
             "phone": "316-555-0100", "street": "123 Main", "city": "Wichita", "state": "KS", "zip": "67202"},
            {"name": "Jones, Mary", "regnum": "2", "firm_name": "Smith Law LLC",
             "phone": "", "street": "123 Main", "city": "Wichita", "state": "KS", "zip": "67202"},
        ]
        firms = _group_attorneys_into_firms(attorneys)
        assert len(firms) == 1
        assert len(firms[0]["attorneys"]) == 2
        assert firms[0]["phone"] == "316-555-0100"

    def test_separates_different_cities(self):
        attorneys = [
            {"name": "A", "regnum": "1", "firm_name": "Acme Law",
             "phone": "", "street": "", "city": "Wichita", "state": "KS", "zip": ""},
            {"name": "B", "regnum": "2", "firm_name": "Acme Law",
             "phone": "", "street": "", "city": "Topeka", "state": "KS", "zip": ""},
        ]
        firms = _group_attorneys_into_firms(attorneys)
        assert len(firms) == 2

    def test_solo_practitioner_uses_attorney_name(self):
        attorneys = [
            {"name": "Solo, Jane", "regnum": "1", "firm_name": "",
             "phone": "785-555-0200", "street": "456 Oak", "city": "Topeka", "state": "KS", "zip": "66603"},
        ]
        firms = _group_attorneys_into_firms(attorneys)
        assert len(firms) == 1
        assert firms[0]["name"] == "Solo, Jane"
        assert firms[0]["sources"] == ["ks_courts"]

    def test_firm_record_shape(self):
        attorneys = [
            {"name": "Test, Attorney", "regnum": "1", "firm_name": "Test Firm",
             "phone": "555-0100", "street": "1 St", "city": "Topeka", "state": "KS", "zip": "66601"},
        ]
        firm = _group_attorneys_into_firms(attorneys)[0]
        assert "id" in firm
        assert firm["practiceAreas"] == []
        assert firm["summary"] is None
        assert firm["website"] is None
        assert firm["email"] is None
        assert firm["coordinates"] is None
        assert firm["referralScore"] == "low"
        assert firm["sources"] == ["ks_courts"]
        assert firm["attorney_count"] == 1
        assert firm["address"]["state"] == "KS"
