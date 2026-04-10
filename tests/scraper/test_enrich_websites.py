import uuid
from unittest.mock import patch, MagicMock

import pytest

from scraper.enrich_websites import (
    _is_directory_url,
    _is_firm_like_name,
    _generate_domain_candidates,
    _pick_best_result,
    _build_firm_index,
)


def _make_firm(name="Test Firm", city="Wichita", website=None, sources=None):
    return {
        "id": str(uuid.uuid4()),
        "name": name,
        "practiceAreas": [],
        "summary": None,
        "website": website,
        "phone": None,
        "email": None,
        "address": {"street": "", "city": city, "county": "", "state": "KS", "zip": ""},
        "coordinates": None,
        "referralScore": "low",
        "sources": sources or ["ks_courts"],
    }


class TestIsDirectoryUrl:
    def test_findlaw(self):
        assert _is_directory_url("https://lawyers.findlaw.com/profile/view/123")

    def test_avvo(self):
        assert _is_directory_url("https://www.avvo.com/attorneys/ks/smith.html")

    def test_justia(self):
        assert _is_directory_url("https://www.justia.com/lawyers/kansas/wichita")

    def test_yelp(self):
        assert _is_directory_url("https://www.yelp.com/biz/smith-law-wichita")

    def test_linkedin(self):
        assert _is_directory_url("https://www.linkedin.com/in/john-smith")

    def test_real_firm_site(self):
        assert not _is_directory_url("https://www.smithlawfirm.com")

    def test_dot_law_domain(self):
        assert not _is_directory_url("https://www.smith.law")

    def test_kscourts_directory(self):
        assert _is_directory_url("https://directory-kard.kscourts.gov/Home/Details?regNum=123")

    def test_facebook(self):
        assert _is_directory_url("https://www.facebook.com/smithlaw")


class TestIsFirmLikeName:
    def test_law_office(self):
        assert _is_firm_like_name("Voss Law Office")

    def test_llc(self):
        assert _is_firm_like_name("Smith & Jones LLC")

    def test_firm(self):
        assert _is_firm_like_name("Kelly Law Firm")

    def test_associates(self):
        assert _is_firm_like_name("Johnson & Associates")

    def test_pa(self):
        assert _is_firm_like_name("Brown P.A.")

    def test_bare_person_name(self):
        assert not _is_firm_like_name("John Smith")

    def test_bare_name_with_middle(self):
        assert not _is_firm_like_name("Robert W. Kaplan")

    def test_comma_name(self):
        assert _is_firm_like_name("Smith, Jones & Brown")

    def test_legal_services(self):
        assert _is_firm_like_name("Kansas Legal Services")

    def test_chartered(self):
        assert _is_firm_like_name("Williams Chartered")


class TestGenerateDomainCandidates:
    def test_basic_law_office(self):
        domains = _generate_domain_candidates("Voss Law Office")
        assert any("vosslaw.com" in d for d in domains)
        assert any("voss.law" in d for d in domains)

    def test_strips_llc(self):
        domains = _generate_domain_candidates("Kelly Law Firm LLC")
        # Should strip LLC before generating
        assert any("kelly" in d.lower() for d in domains)

    def test_returns_urls(self):
        domains = _generate_domain_candidates("Smith Law")
        for d in domains:
            assert d.startswith("https://")

    def test_empty_name(self):
        assert _generate_domain_candidates("") == []

    def test_no_duplicates(self):
        domains = _generate_domain_candidates("Test Law Firm")
        assert len(domains) == len(set(domains))


class TestPickBestResult:
    def test_prefers_matching_domain(self):
        urls = [
            "https://www.yelp.com/biz/voss-law",
            "https://www.vosslaw.com",
            "https://www.randomsite.com",
        ]
        result = _pick_best_result(urls, "Voss Law Office")
        assert result == "https://www.vosslaw.com"

    def test_filters_directories(self):
        urls = [
            "https://www.avvo.com/attorneys/voss",
            "https://lawyers.findlaw.com/voss",
            "https://www.vosslaw.com",
        ]
        result = _pick_best_result(urls, "Voss Law Office")
        assert result == "https://www.vosslaw.com"

    def test_all_directories_returns_none_or_first_non_dir(self):
        urls = [
            "https://www.avvo.com/attorneys/voss",
            "https://lawyers.findlaw.com/voss",
        ]
        result = _pick_best_result(urls, "Voss Law Office")
        assert result is None

    def test_prefers_dot_law(self):
        urls = [
            "https://www.randomsite.com",
            "https://www.smith.law",
        ]
        result = _pick_best_result(urls, "Smith Law Office")
        assert "smith.law" in result


class TestBuildFirmIndex:
    def test_groups_by_city(self):
        firms = [
            _make_firm("Firm A", "Wichita"),
            _make_firm("Firm B", "Wichita"),
            _make_firm("Firm C", "Topeka"),
        ]
        index = _build_firm_index(firms)
        assert len(index["wichita"]) == 2
        assert len(index["topeka"]) == 1

    def test_case_insensitive(self):
        firms = [_make_firm("Firm A", "Kansas City")]
        index = _build_firm_index(firms)
        assert "kansas city" in index
